import { existsSync, mkdirSync, rmSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { homedir } from "node:os";
import { randomUUID } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import { nextCronTime, validateCron } from "./cron.js";
function expandHome(p) {
    if (p === "~")
        return homedir();
    if (p.startsWith("~/"))
        return join(homedir(), p.slice(2));
    return p;
}
export class DependencyValidationError extends Error {
    code;
    questId;
    dependencyId;
    cycle;
    constructor(code, message, details = {}) {
        super(message);
        this.name = "DependencyValidationError";
        this.code = code;
        this.questId = details.questId;
        this.dependencyId = details.dependencyId;
        this.cycle = details.cycle;
    }
}
/** Shared claim-eligibility predicate (parameters: now ×3). Runnable states
 *  gated on schedule and retry backoff, expired-lease reclamation, and no
 *  unfinished / invalid dependencies. */
const ELIGIBLE_SQL = `(
    (state IN ('queued','interrupted') AND scheduled_at <= ? AND COALESCE(retry_at, 0) <= ?)
    OR (state = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at < ?)
  )
  AND NOT EXISTS (
    SELECT 1 FROM quest_deps d LEFT JOIN quests dq ON dq.id = d.depends_on
    WHERE d.quest_id = q.id AND (dq.id IS NULL OR dq.project != q.project OR dq.state != 'done')
  )`;
const DEFAULT_LEASE_TTL_MS = 120_000;
const DEFAULT_MAX_ATTEMPTS = 1;
const DEFAULT_BACKOFF_BASE_MS = 30_000;
const SCHEMA_VERSION = 4;
const STATES = ["queued", "running", "done", "failed", "cancelled", "interrupted"];
function parseChain(raw) {
    if (!raw)
        return undefined;
    try {
        const value = JSON.parse(raw);
        return value && typeof value === "object" ? value : undefined;
    }
    catch {
        return undefined;
    }
}
function fromRunRow(row) {
    return {
        questId: row.quest_id,
        attempt: row.attempt,
        ownerSession: row.owner_session,
        leaseVersion: row.lease_gen,
        agentRunId: row.agent_run_id ?? undefined,
        model: row.model ?? undefined,
        provider: row.provider ?? undefined,
        tier: row.tier ?? undefined,
        turns: row.turns,
        tokens: row.tokens,
        costUsd: row.cost_usd,
        lastActivity: row.last_activity ?? undefined,
        startedAt: row.started_at,
        finishedAt: row.finished_at ?? undefined,
        outcome: row.outcome ?? undefined,
        error: row.error ?? undefined,
    };
}
function fromArtifactRow(row) {
    return {
        questId: row.quest_id,
        attempt: row.attempt,
        leaseVersion: row.lease_gen,
        kind: row.kind,
        path: row.path,
        bytes: row.bytes,
        createdAt: row.created_at,
    };
}
function fromRecurrenceRow(row) {
    return {
        id: row.id,
        project: row.project,
        name: row.name,
        cron: row.cron,
        timezone: row.timezone,
        role: row.role,
        task: row.task,
        context: row.context ?? undefined,
        priority: row.priority,
        maxAttempts: row.max_attempts,
        backoffBaseMs: row.backoff_base_ms,
        catchUp: row.catch_up,
        enabled: row.enabled === 1,
        nextRunAt: row.next_run_at,
        lastEnqueuedAt: row.last_enqueued_at ?? undefined,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
    };
}
function emptyCounts() {
    return { queued: 0, running: 0, done: 0, failed: 0, cancelled: 0, interrupted: 0 };
}
function ensureInt(name, value, min) {
    if (value === undefined)
        return undefined;
    if (!Number.isInteger(value) || !Number.isFinite(value) || value < min)
        throw new Error(`${name} must be an integer >= ${min}`);
    return value;
}
function shortString(value, max) {
    if (value === undefined)
        return undefined;
    return value.slice(0, max);
}
function normalizeTelemetry(t) {
    const out = {};
    if (t.model !== undefined)
        out.model = shortString(String(t.model), 256);
    if (t.provider !== undefined)
        out.provider = shortString(String(t.provider), 128);
    if (t.tier !== undefined)
        out.tier = shortString(String(t.tier), 128);
    if (t.lastActivity !== undefined)
        out.lastActivity = shortString(String(t.lastActivity), 1000);
    if (t.turns !== undefined)
        out.turns = ensureInt("turns", t.turns, 0);
    if (t.tokens !== undefined)
        out.tokens = ensureInt("tokens", t.tokens, 0);
    if (t.costUsd !== undefined) {
        if (typeof t.costUsd !== "number" || !Number.isFinite(t.costUsd) || t.costUsd < 0)
            throw new Error("costUsd must be a finite number >= 0");
        out.costUsd = t.costUsd;
    }
    return out;
}
function pipelineSteps(definition) {
    if (!definition || typeof definition !== "object" || !definition.steps || typeof definition.steps !== "object")
        throw new Error("pipeline definition must contain steps");
    return definition.steps;
}
function validatePipelineDag(definition) {
    const steps = pipelineSteps(definition);
    const names = new Set(Object.keys(steps));
    if (!names.size)
        throw new Error("pipeline must contain at least one step");
    const visiting = new Set();
    const visited = new Set();
    const stack = [];
    const dfs = (name) => {
        if (visiting.has(name))
            throw new Error(`pipeline dependency cycle: ${[...stack, name].join(" -> ")}`);
        if (visited.has(name))
            return;
        const step = steps[name];
        if (!step.role || !step.task)
            throw new Error(`pipeline step ${name} must have role and task`);
        visiting.add(name);
        stack.push(name);
        for (const dep of step.dependsOn ?? []) {
            if (!names.has(dep))
                throw new Error(`pipeline step ${name} depends on unknown step ${dep}`);
            if (dep === name)
                throw new Error(`pipeline step ${name} cannot depend on itself`);
            dfs(dep);
        }
        stack.pop();
        visiting.delete(name);
        visited.add(name);
    };
    for (const name of names)
        dfs(name);
}
/** Durable, provider-neutral queue and append-only event journal for agent work. */
export class QuestStore {
    options;
    path;
    leaseTtlMs;
    db;
    defaultMaxAttempts;
    defaultBackoffBaseMs;
    inTransaction = false;
    constructor(options) {
        this.options = options;
        this.path = resolve(expandHome(options.path));
        this.leaseTtlMs = Math.max(1, options.leaseTtlMs ?? DEFAULT_LEASE_TTL_MS);
        this.defaultMaxAttempts = Math.max(1, options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS);
        this.defaultBackoffBaseMs = Math.max(0, options.backoffBaseMs ?? DEFAULT_BACKOFF_BASE_MS);
        mkdirSync(dirname(this.path), { recursive: true });
        this.db = new DatabaseSync(this.path);
        this.db.exec("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;");
        this.migrate();
    }
    close() {
        this.db.close();
    }
    health(project) {
        const now = Date.now();
        const normalized = project ? resolve(project) : undefined;
        const integrity = this.db.prepare("PRAGMA integrity_check").get()?.integrity_check ?? "unknown";
        const journalMode = this.db.prepare("PRAGMA journal_mode").get()?.journal_mode ?? "unknown";
        const schemaVersion = this.db.prepare("PRAGMA user_version").get()?.user_version ?? 0;
        const counts = emptyCounts();
        const rows = this.db.prepare(`SELECT state, COUNT(*) count FROM quests ${normalized ? "WHERE project=?" : ""} GROUP BY state`).all(...(normalized ? [normalized] : []));
        for (const row of rows)
            counts[row.state] = row.count;
        const expiredLeases = Number(this.db.prepare(`SELECT COUNT(*) count FROM quests WHERE state='running' AND lease_expires_at IS NOT NULL AND lease_expires_at < ? ${normalized ? "AND project=?" : ""}`).get(...(normalized ? [now, normalized] : [now]))?.count ?? 0);
        const blockedByDependencies = Number(this.db.prepare(`SELECT COUNT(DISTINCT q.id) count FROM quests q JOIN quest_deps d ON d.quest_id=q.id LEFT JOIN quests dq ON dq.id=d.depends_on
       WHERE q.state IN ('queued','interrupted') AND (dq.id IS NULL OR dq.project != q.project OR dq.state != 'done') ${normalized ? "AND q.project=?" : ""}`).get(...(normalized ? [normalized] : []))?.count ?? 0);
        const scheduledLater = Number(this.db.prepare(`SELECT COUNT(*) count FROM quests WHERE state IN ('queued','interrupted') AND scheduled_at > ? ${normalized ? "AND project=?" : ""}`).get(...(normalized ? [now, normalized] : [now]))?.count ?? 0);
        const retryBackoff = Number(this.db.prepare(`SELECT COUNT(*) count FROM quests WHERE state='queued' AND COALESCE(retry_at, 0) > ? ${normalized ? "AND project=?" : ""}`).get(...(normalized ? [now, normalized] : [now]))?.count ?? 0);
        const overdueQuests = Number(this.db.prepare(`SELECT COUNT(*) count FROM quests WHERE state IN ('queued','interrupted') AND scheduled_at <= ? AND COALESCE(retry_at, 0) <= ? ${normalized ? "AND project=?" : ""}`).get(...(normalized ? [now, now, normalized] : [now, now]))?.count ?? 0);
        const orphanDependencies = Number(this.db.prepare(`SELECT COUNT(*) count FROM quest_deps d LEFT JOIN quests q ON q.id=d.quest_id LEFT JOIN quests dq ON dq.id=d.depends_on
       WHERE (q.id IS NULL OR dq.id IS NULL) ${normalized ? "AND (q.project=? OR dq.project=? OR q.project IS NULL OR dq.project IS NULL)" : ""}`).get(...(normalized ? [normalized, normalized] : []))?.count ?? 0);
        const crossProjectDependencies = Number(this.db.prepare(`SELECT COUNT(*) count FROM quest_deps d JOIN quests q ON q.id=d.quest_id JOIN quests dq ON dq.id=d.depends_on
       WHERE q.project != dq.project ${normalized ? "AND q.project=?" : ""}`).get(...(normalized ? [normalized] : []))?.count ?? 0);
        const controls = normalized ? this.projectControl(normalized) : { paused: false };
        const staleSchedulers = Number(this.db.prepare(`SELECT COUNT(*) count FROM scheduler_leases WHERE expires_at < ? ${normalized ? "AND project=?" : ""}`).get(...(normalized ? [now, normalized] : [now]))?.count ?? 0);
        const rec = this.db.prepare(`SELECT SUM(CASE WHEN enabled=1 THEN 1 ELSE 0 END) enabled, SUM(CASE WHEN enabled=1 AND next_run_at <= ? THEN 1 ELSE 0 END) due FROM quest_recurrences ${normalized ? "WHERE project=?" : ""}`).get(...(normalized ? [now, normalized] : [now]));
        return {
            path: this.path,
            schemaVersion,
            integrity,
            journalMode,
            project: normalized,
            counts,
            queued: counts.queued,
            running: counts.running,
            interrupted: counts.interrupted,
            expiredLeases,
            blockedByDependencies,
            scheduledLater,
            retryBackoff,
            overdueQuests,
            orphanDependencies,
            crossProjectDependencies,
            dependencyCycles: this.countDependencyCycles(normalized),
            staleSchedulers,
            paused: controls.paused,
            pausedAt: controls.pausedAt,
            pausedReason: controls.reason,
            recurring: { enabled: rec?.enabled ?? 0, due: rec?.due ?? 0 },
        };
    }
    /** Idempotent when dedupeKey is set: an existing active (or done-but-retained,
     *  unconsumed) quest with the same project+key is returned instead of inserting. */
    enqueue(input) {
        const now = Date.now();
        const project = resolve(input.project);
        const name = input.name?.trim() || `${input.role} quest`;
        return this.tx(() => {
            if (input.dedupeKey) {
                const existing = this.db.prepare(`SELECT id FROM quests WHERE project=? AND dedupe_key=? AND (
             state IN ('queued','running','interrupted')
             OR (state='done' AND retain_until_consumed=1 AND consumed_at IS NULL)
           ) LIMIT 1`).get(project, input.dedupeKey);
                if (existing?.id) {
                    this.event(existing.id, "deduped", { dedupeKey: input.dedupeKey });
                    return this.get(existing.id);
                }
            }
            if (input.groupId)
                this.ensureGroup(input.groupId, project);
            const id = `q-${randomUUID().slice(0, 8)}`;
            this.validateDependenciesForNewQuest(project, input.dependsOn ?? []);
            this.insertQuest({ id, project, role: input.role, name, task: input.task, context: input.context, priority: input.priority, scheduledAt: input.scheduledAt, maxAttempts: input.maxAttempts, backoffBaseMs: input.backoffBaseMs, dedupeKey: input.dedupeKey, chain: input.chain, retainUntilConsumed: input.retainUntilConsumed, groupId: input.groupId, groupStep: input.groupStep, recurrenceId: input.recurrenceId, occurrenceAt: input.occurrenceAt }, now);
            for (const dep of new Set(input.dependsOn ?? [])) {
                this.db.prepare("INSERT OR IGNORE INTO quest_deps (quest_id, depends_on) VALUES (?, ?)").run(id, dep);
            }
            this.event(id, "enqueued", { role: input.role, name, priority: input.priority ?? 0, dependsOn: input.dependsOn ?? [] });
            return this.get(id);
        });
    }
    get(id) {
        const row = this.db.prepare("SELECT * FROM quests WHERE id = ?").get(id);
        return row ? this.fromRow(row) : undefined;
    }
    list(project, limit = 30) {
        const rows = this.db.prepare("SELECT * FROM quests WHERE project = ? ORDER BY created_at DESC, rowid DESC LIMIT ?").all(resolve(project), Math.max(1, Math.min(limit, 500)));
        return rows.map((row) => this.fromRow(row));
    }
    /** Per-attempt telemetry history for a quest, newest first. */
    runs(id, limit = 20) {
        const rows = this.db.prepare("SELECT * FROM quest_runs WHERE quest_id=? ORDER BY attempt DESC LIMIT ?").all(id, Math.max(1, Math.min(limit, 100)));
        return rows.map(fromRunRow);
    }
    artifacts(id, attempt) {
        const rows = attempt === undefined
            ? this.db.prepare("SELECT * FROM quest_artifacts WHERE quest_id=? ORDER BY attempt DESC, created_at DESC").all(id)
            : this.db.prepare("SELECT * FROM quest_artifacts WHERE quest_id=? AND attempt=? ORDER BY created_at DESC").all(id, attempt);
        return rows.map(fromArtifactRow);
    }
    claimNext(project, ownerSession = "legacy", options = {}) {
        const normalized = resolve(project);
        const now = Date.now();
        return this.tx(() => {
            if (!options.ignorePause && this.projectControl(normalized).paused)
                return undefined;
            this.propagateFailedDeps(normalized, now);
            const excludeRoles = options.excludeRoles?.filter(Boolean) ?? [];
            const placeholders = excludeRoles.map(() => "?").join(",");
            const roleSql = excludeRoles.length ? `AND q.role NOT IN (${placeholders})` : "";
            const row = this.db.prepare(`SELECT id, state FROM quests q
         WHERE project = ? ${roleSql} AND ${ELIGIBLE_SQL}
         ORDER BY priority DESC, created_at ASC, rowid ASC LIMIT 1`).get(normalized, ...excludeRoles, now, now, now);
            if (!row?.id)
                return undefined;
            return this.claimRow(row.id, row.state, ownerSession, now);
        });
    }
    claimById(id, project, ownerSession = "legacy", options = {}) {
        const normalized = resolve(project);
        const now = Date.now();
        return this.tx(() => {
            if (!options.ignorePause && this.projectControl(normalized).paused)
                return undefined;
            this.propagateFailedDeps(normalized, now);
            const row = this.db.prepare(`SELECT id, state FROM quests q WHERE id = ? AND project = ? AND ${ELIGIBLE_SQL}`).get(id, normalized, now, now, now);
            if (!row?.id)
                return undefined;
            return this.claimRow(row.id, row.state, ownerSession, now);
        });
    }
    claimRow(id, state, ownerSession, now) {
        const reclaimed = state === "running";
        if (reclaimed) {
            const old = this.db.prepare("SELECT lease_gen FROM quests WHERE id=?").get(id);
            if (old?.lease_gen !== undefined) {
                this.db.prepare("UPDATE quest_runs SET finished_at=COALESCE(finished_at, ?), outcome=COALESCE(outcome, 'interrupted'), error=COALESCE(error, 'lease expired and was reclaimed') WHERE quest_id=? AND lease_gen=? AND finished_at IS NULL").run(now, id, old.lease_gen);
                this.event(id, "lease_lost", { leaseVersion: old.lease_gen });
            }
        }
        this.db.prepare(`UPDATE quests SET state='running', attempts=attempts+1, lease_gen=lease_gen+1,
              owner_session=?, agent_run_id=NULL, started_at=?, finished_at=NULL, heartbeat_at=?,
              lease_expires_at=?, retry_at=NULL, error=NULL, failure_kind=NULL, failed_dependency_id=NULL, updated_at=?
       WHERE id=?`).run(ownerSession, now, now, now + this.leaseTtlMs, now, id);
        const quest = this.get(id);
        this.db.prepare(`INSERT OR REPLACE INTO quest_runs (quest_id, attempt, owner_session, lease_gen, turns, tokens, cost_usd, started_at)
       VALUES (?, ?, ?, ?, 0, 0, 0, ?)`).run(id, quest.attempts, ownerSession, quest.leaseVersion, now);
        this.event(id, reclaimed ? "reclaimed" : "claimed", { attempt: quest.attempts, leaseVersion: quest.leaseVersion, ownerSession });
        return { quest, lease: { id, ownerSession, version: quest.leaseVersion } };
    }
    release(lease) {
        const now = Date.now();
        return this.tx(() => {
            const changed = Number(this.db.prepare(`UPDATE quests SET state='queued', retry_at=NULL, owner_session=NULL, agent_run_id=NULL,
                lease_expires_at=NULL, heartbeat_at=NULL, started_at=NULL, finished_at=NULL,
                max_attempts=MAX(max_attempts, attempts + 1), updated_at=?
         WHERE id=? AND state='running' AND owner_session=? AND lease_gen=?`).run(now, lease.id, lease.ownerSession, lease.version).changes) > 0;
            if (changed) {
                this.db.prepare("UPDATE quest_runs SET finished_at=?, outcome='interrupted', error='released by owner' WHERE quest_id=? AND lease_gen=? AND finished_at IS NULL").run(now, lease.id, lease.version);
                this.event(lease.id, "released", { leaseVersion: lease.version });
            }
            return changed;
        });
    }
    requeue(id, project, options = {}) {
        const now = Date.now();
        const normalized = resolve(project);
        return this.tx(() => {
            const changed = Number(this.db.prepare(`UPDATE quests SET state='queued', retry_at=NULL, finished_at=NULL, error=NULL, failure_kind=NULL, failed_dependency_id=NULL,
                max_attempts=MAX(max_attempts, attempts + 1), updated_at=?
         WHERE id=? AND project=? AND state IN ('failed','cancelled','interrupted')`).run(now, id, normalized).changes) > 0;
            if (changed) {
                this.event(id, "requeued", {});
                if (options.cascadeDependents !== false)
                    this.requeueDependencyFailedDescendants(normalized, [id], now);
            }
            return changed;
        });
    }
    heartbeat(lease) {
        const now = Date.now();
        return Number(this.db.prepare("UPDATE quests SET heartbeat_at=?, lease_expires_at=?, updated_at=? WHERE id=? AND state='running' AND owner_session=? AND lease_gen=?").run(now, now + this.leaseTtlMs, now, lease.id, lease.ownerSession, lease.version).changes) > 0;
    }
    attachRun(lease, agentRunId) {
        return this.tx(() => {
            const changed = Number(this.db.prepare("UPDATE quests SET agent_run_id=?, updated_at=? WHERE id=? AND state='running' AND owner_session=? AND lease_gen=? AND agent_run_id IS NULL").run(agentRunId, Date.now(), lease.id, lease.ownerSession, lease.version).changes) > 0;
            if (changed) {
                this.db.prepare("UPDATE quest_runs SET agent_run_id=? WHERE quest_id=? AND lease_gen=?").run(agentRunId, lease.id, lease.version);
                this.event(lease.id, "agent_started", { agentRunId });
            }
            return changed;
        });
    }
    updateTelemetry(lease, t) {
        const telemetry = normalizeTelemetry(t);
        const owns = this.db.prepare("SELECT 1 FROM quests WHERE id=? AND state='running' AND owner_session=? AND lease_gen=?").get(lease.id, lease.ownerSession, lease.version);
        if (!owns)
            return false;
        return Number(this.db.prepare(`UPDATE quest_runs SET model=COALESCE(?, model), provider=COALESCE(?, provider), tier=COALESCE(?, tier), turns=COALESCE(?, turns),
              tokens=COALESCE(?, tokens), cost_usd=COALESCE(?, cost_usd), last_activity=COALESCE(?, last_activity)
       WHERE quest_id=? AND lease_gen=?`).run(telemetry.model ?? null, telemetry.provider ?? null, telemetry.tier ?? null, telemetry.turns ?? null, telemetry.tokens ?? null, telemetry.costUsd ?? null, telemetry.lastActivity ?? null, lease.id, lease.version).changes) > 0;
    }
    complete(lease, agentRunId, result, artifacts = []) {
        const now = Date.now();
        return this.tx(() => {
            const row = this.db.prepare("SELECT attempts, project FROM quests WHERE id=? AND state='running' AND owner_session=? AND lease_gen=? AND agent_run_id=?")
                .get(lease.id, lease.ownerSession, lease.version, agentRunId);
            if (row?.attempts === undefined || !row.project)
                return false;
            const changed = Number(this.db.prepare(`UPDATE quests SET state='done', result=?, error=NULL, finished_at=?, updated_at=?, lease_expires_at=NULL, heartbeat_at=NULL,
                failure_kind=NULL, failed_dependency_id=NULL
         WHERE id=? AND state='running' AND owner_session=? AND lease_gen=? AND agent_run_id=?`).run(result, now, now, lease.id, lease.ownerSession, lease.version, agentRunId).changes) > 0;
            if (changed) {
                this.insertArtifacts(lease, row.attempts, artifacts, now);
                this.finalizeRun(lease, now, "done");
                this.event(lease.id, "completed", { leaseVersion: lease.version });
                this.prune(row.project);
            }
            return changed;
        });
    }
    fail(lease, error, agentRunId, artifacts = []) {
        const now = Date.now();
        return this.tx(() => {
            const row = this.db.prepare("SELECT attempts, max_attempts, backoff_base_ms, project FROM quests WHERE id=? AND state='running' AND owner_session=? AND lease_gen=? AND (? IS NULL OR agent_run_id=?)").get(lease.id, lease.ownerSession, lease.version, agentRunId ?? null, agentRunId ?? null);
            if (row?.attempts === undefined || !row.project)
                return false;
            const retry = row.attempts < (row.max_attempts ?? 1);
            if (retry) {
                const retryAt = now + (row.backoff_base_ms ?? 0) * 2 ** (row.attempts - 1);
                this.db.prepare(`UPDATE quests SET state='queued', retry_at=?, error=?, failure_kind='execution', failed_dependency_id=NULL, updated_at=?, owner_session=NULL, agent_run_id=NULL,
                  lease_expires_at=NULL, heartbeat_at=NULL, started_at=NULL, finished_at=NULL
           WHERE id=?`).run(retryAt, error, now, lease.id);
                this.insertArtifacts(lease, row.attempts, artifacts, now);
                this.finalizeRun(lease, now, "failed", error);
                this.event(lease.id, "retry_scheduled", { error: error.slice(0, 1000), retryAt, attempt: row.attempts });
            }
            else {
                this.db.prepare("UPDATE quests SET state='failed', error=?, failure_kind='execution', failed_dependency_id=NULL, finished_at=?, updated_at=?, lease_expires_at=NULL, heartbeat_at=NULL WHERE id=?").run(error, now, now, lease.id);
                this.insertArtifacts(lease, row.attempts, artifacts, now);
                this.finalizeRun(lease, now, "failed", error);
                this.event(lease.id, "failed", { error: error.slice(0, 1000), attempt: row.attempts });
                this.propagateFailedDeps(row.project, now);
                this.prune(row.project);
            }
            return true;
        });
    }
    cancel(id, project) {
        const now = Date.now();
        const normalized = resolve(project);
        return this.tx(() => {
            const rows = this.db.prepare("SELECT id, lease_gen FROM quests WHERE id=? AND project=? AND state IN ('queued','interrupted','running')").all(id, normalized);
            if (!rows.length)
                return false;
            const changed = Number(this.db.prepare(`UPDATE quests SET state='cancelled', finished_at=?, updated_at=?, retry_at=NULL, owner_session=NULL, agent_run_id=NULL,
                lease_expires_at=NULL, heartbeat_at=NULL
         WHERE id=? AND project=? AND state IN ('queued','interrupted','running')`).run(now, now, id, normalized).changes) > 0;
            if (changed) {
                for (const row of rows) {
                    this.db.prepare("UPDATE quest_runs SET finished_at=?, outcome='cancelled', error='cancelled' WHERE quest_id=? AND lease_gen=? AND finished_at IS NULL").run(now, row.id, row.lease_gen);
                }
                this.event(id, "cancelled", {});
                this.propagateFailedDeps(normalized, now);
                this.prune(normalized);
            }
            return changed;
        });
    }
    markConsumed(id, project) {
        const now = Date.now();
        return this.tx(() => {
            const changed = Number(this.db.prepare("UPDATE quests SET consumed_at=?, updated_at=? WHERE id=? AND project=? AND state='done' AND consumed_at IS NULL").run(now, now, id, resolve(project)).changes) > 0;
            if (changed)
                this.event(id, "consumed", {});
            return changed;
        });
    }
    recoverOwned(project, ownerSession) {
        const now = Date.now();
        const normalized = resolve(project);
        return this.tx(() => {
            const ids = this.db.prepare("SELECT id, lease_gen FROM quests WHERE project=? AND state='running' AND owner_session=?").all(normalized, ownerSession);
            if (!ids.length)
                return 0;
            this.db.prepare(`UPDATE quests SET state='interrupted', updated_at=?, error=COALESCE(error, 'lead session ended before completion'),
                owner_session=NULL, agent_run_id=NULL, lease_expires_at=NULL, heartbeat_at=NULL
         WHERE project=? AND state='running' AND owner_session=?`).run(now, normalized, ownerSession);
            for (const { id, lease_gen } of ids) {
                this.db.prepare("UPDATE quest_runs SET finished_at=?, outcome='interrupted' WHERE quest_id=? AND lease_gen=? AND finished_at IS NULL").run(now, id, lease_gen);
                this.event(id, "recovered", { ownerSession });
            }
            return ids.length;
        });
    }
    reclaimExpired(project) {
        const now = Date.now();
        const normalized = resolve(project);
        return this.tx(() => {
            const ids = this.db.prepare("SELECT id, lease_gen FROM quests WHERE project=? AND state='running' AND lease_expires_at IS NOT NULL AND lease_expires_at < ?").all(normalized, now);
            for (const { id, lease_gen } of ids) {
                this.db.prepare(`UPDATE quests SET state='interrupted', updated_at=?, error=COALESCE(error, 'lease expired without heartbeat'),
                  owner_session=NULL, agent_run_id=NULL, lease_expires_at=NULL, heartbeat_at=NULL WHERE id=?`).run(now, id);
                this.db.prepare("UPDATE quest_runs SET finished_at=?, outcome='interrupted' WHERE quest_id=? AND lease_gen=? AND finished_at IS NULL").run(now, id, lease_gen);
                this.event(id, "lease_expired", {});
            }
            return ids.length;
        });
    }
    acquireSchedulerLease(project, owner, ttlMs) {
        const now = Date.now();
        const normalized = resolve(project);
        return this.tx(() => {
            const row = this.db.prepare("SELECT owner, expires_at FROM scheduler_leases WHERE project=?")
                .get(normalized);
            if (row?.owner && row.owner !== owner && (row.expires_at ?? 0) >= now)
                return false;
            this.db.prepare("INSERT INTO scheduler_leases (project, owner, expires_at) VALUES (?, ?, ?) ON CONFLICT(project) DO UPDATE SET owner=excluded.owner, expires_at=excluded.expires_at").run(normalized, owner, now + Math.max(1, ttlMs));
            return true;
        });
    }
    releaseSchedulerLease(project, owner) {
        this.db.prepare("DELETE FROM scheduler_leases WHERE project=? AND owner=?").run(resolve(project), owner);
    }
    events(id, limit = 50) {
        const rows = this.db.prepare("SELECT event, at, data FROM quest_events WHERE quest_id=? ORDER BY seq DESC LIMIT ?").all(id, Math.max(1, Math.min(limit, 500)));
        return rows.map((row) => {
            try {
                return { event: row.event, at: row.at, data: JSON.parse(row.data) };
            }
            catch {
                return { event: row.event, at: row.at, data: row.data };
            }
        });
    }
    addDependency(questId, dependsOn, project) {
        const normalized = resolve(project);
        return this.tx(() => {
            this.validateDependencyMutation(questId, dependsOn, normalized);
            this.db.prepare("INSERT OR IGNORE INTO quest_deps (quest_id, depends_on) VALUES (?, ?)").run(questId, dependsOn);
            this.event(questId, "dependency_added", { dependsOn });
            return this.get(questId);
        });
    }
    removeDependency(questId, dependsOn, project) {
        const normalized = resolve(project);
        return this.tx(() => {
            const q = this.get(questId);
            if (!q || q.project !== normalized)
                throw new DependencyValidationError("quest_not_found", `quest ${questId} does not exist in this project`, { questId });
            if (q.state !== "queued" && q.state !== "interrupted")
                throw new DependencyValidationError("quest_not_mutable", `quest ${questId} is ${q.state}; dependencies are only mutable while queued/interrupted`, { questId });
            this.db.prepare("DELETE FROM quest_deps WHERE quest_id=? AND depends_on=?").run(questId, dependsOn);
            this.event(questId, "dependency_removed", { dependsOn });
            return this.get(questId);
        });
    }
    pauseProject(project, reason) {
        const normalized = resolve(project);
        const now = Date.now();
        return this.tx(() => {
            this.db.prepare("INSERT INTO project_controls (project, paused_at, paused_reason, updated_at) VALUES (?, ?, ?, ?) ON CONFLICT(project) DO UPDATE SET paused_at=excluded.paused_at, paused_reason=excluded.paused_reason, updated_at=excluded.updated_at").run(normalized, now, reason ?? null, now);
            this.projectEvent(normalized, "queue_paused", { reason });
            return this.projectControl(normalized);
        });
    }
    resumeProject(project) {
        const normalized = resolve(project);
        const now = Date.now();
        return this.tx(() => {
            this.db.prepare("INSERT INTO project_controls (project, paused_at, paused_reason, updated_at) VALUES (?, NULL, NULL, ?) ON CONFLICT(project) DO UPDATE SET paused_at=NULL, paused_reason=NULL, updated_at=excluded.updated_at").run(normalized, now);
            this.projectEvent(normalized, "queue_resumed", {});
            return this.projectControl(normalized);
        });
    }
    projectControl(project) {
        const normalized = resolve(project);
        const row = this.db.prepare("SELECT paused_at, paused_reason FROM project_controls WHERE project=?").get(normalized);
        return { project: normalized, paused: row?.paused_at !== undefined && row.paused_at !== null, pausedAt: row?.paused_at ?? undefined, reason: row?.paused_reason ?? undefined };
    }
    createGroup(input) {
        const now = Date.now();
        const project = resolve(input.project);
        const id = `g-${randomUUID().slice(0, 8)}`;
        return this.tx(() => {
            this.db.prepare("INSERT INTO quest_groups (id, project, name, kind, pipeline_name, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)").run(id, project, input.name, input.kind ?? "group", input.pipelineName ?? null, now, now);
            this.projectEvent(project, "group_created", { id, name: input.name, kind: input.kind ?? "group" });
            return this.getGroup(id, project);
        });
    }
    getGroup(id, project) {
        const normalized = resolve(project);
        const row = this.db.prepare("SELECT * FROM quest_groups WHERE id=? AND project=?").get(id, normalized);
        if (!row)
            return undefined;
        const quests = this.db.prepare("SELECT id, state FROM quests WHERE group_id=? ORDER BY created_at ASC, rowid ASC").all(id);
        const counts = emptyCounts();
        for (const q of quests)
            counts[q.state]++;
        const active = counts.queued + counts.running + counts.interrupted;
        const state = quests.length === 0 ? "empty"
            : active > 0 ? "active"
                : counts.failed > 0 ? "failed"
                    : counts.cancelled > 0 && counts.done === 0 ? "cancelled"
                        : counts.done === quests.length ? "done"
                            : "mixed";
        return { id: row.id, project: row.project, name: row.name, kind: row.kind, pipelineName: row.pipeline_name ?? undefined, state, counts, questIds: quests.map((q) => q.id), createdAt: row.created_at, updatedAt: row.updated_at };
    }
    listGroups(project, limit = 30) {
        const normalized = resolve(project);
        const rows = this.db.prepare("SELECT id FROM quest_groups WHERE project=? ORDER BY created_at DESC LIMIT ?").all(normalized, Math.max(1, Math.min(limit, 200)));
        return rows.map((r) => this.getGroup(r.id, normalized)).filter(Boolean);
    }
    enqueuePipeline(input) {
        validatePipelineDag(input.definition);
        const now = Date.now();
        const project = resolve(input.project);
        return this.tx(() => {
            const groupId = `g-${randomUUID().slice(0, 8)}`;
            this.db.prepare("INSERT INTO quest_groups (id, project, name, kind, pipeline_name, created_at, updated_at) VALUES (?, ?, ?, 'pipeline', ?, ?, ?)").run(groupId, project, input.name ?? input.pipelineName, input.pipelineName, now, now);
            const steps = pipelineSteps(input.definition);
            const ids = new Map();
            for (const stepName of Object.keys(steps))
                ids.set(stepName, `q-${randomUUID().slice(0, 8)}`);
            for (const [stepName, step] of Object.entries(steps)) {
                this.insertQuest({ id: ids.get(stepName), project, role: step.role, name: step.name ?? stepName, task: step.task, context: step.context, priority: step.priority, maxAttempts: step.maxAttempts, backoffBaseMs: step.backoffBaseMs, retainUntilConsumed: step.retainUntilConsumed, groupId, groupStep: stepName }, now);
            }
            for (const [stepName, step] of Object.entries(steps)) {
                for (const depStep of step.dependsOn ?? [])
                    this.db.prepare("INSERT INTO quest_deps (quest_id, depends_on) VALUES (?, ?)").run(ids.get(stepName), ids.get(depStep));
                this.event(ids.get(stepName), "enqueued", { role: step.role, name: step.name ?? stepName, pipeline: input.pipelineName, step: stepName, dependsOn: step.dependsOn ?? [] });
            }
            this.projectEvent(project, "pipeline_enqueued", { groupId, pipelineName: input.pipelineName });
            return this.getGroup(groupId, project);
        });
    }
    cancelGroup(id, project) {
        const normalized = resolve(project);
        return this.tx(() => {
            const quests = this.db.prepare("SELECT id, state FROM quests WHERE project=? AND group_id=?").all(normalized, id);
            let cancelled = 0;
            let running = 0;
            for (const q of quests) {
                if (q.state === "running")
                    running++;
                if (this.cancel(q.id, normalized))
                    cancelled++;
            }
            return { cancelled, running };
        });
    }
    requeueGroup(id, project) {
        const normalized = resolve(project);
        return this.tx(() => {
            const quests = this.db.prepare("SELECT id FROM quests WHERE project=? AND group_id=? AND state IN ('failed','cancelled','interrupted') ORDER BY created_at ASC").all(normalized, id);
            let requeued = 0;
            for (const q of quests)
                if (this.requeue(q.id, normalized))
                    requeued++;
            return { requeued };
        });
    }
    createRecurring(input) {
        validateCron(input.cron, input.timezone ?? "UTC");
        const now = Date.now();
        const id = `r-${randomUUID().slice(0, 8)}`;
        const project = resolve(input.project);
        const timezone = input.timezone ?? "UTC";
        const nextRunAt = nextCronTime(input.cron, now, timezone);
        const catchUp = input.catchUp ?? "one";
        return this.tx(() => {
            this.db.prepare(`INSERT INTO quest_recurrences (id, project, name, cron, timezone, role, task, context, priority, max_attempts, backoff_base_ms, catch_up, enabled, next_run_at, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(id, project, input.name, input.cron, timezone, input.role, input.task, input.context ?? null, input.priority ?? 0, Math.max(1, input.maxAttempts ?? this.defaultMaxAttempts), Math.max(0, input.backoffBaseMs ?? this.defaultBackoffBaseMs), catchUp, input.enabled === false ? 0 : 1, nextRunAt, now, now);
            this.projectEvent(project, "recurring_created", { id, name: input.name, cron: input.cron });
            return this.getRecurring(id, project);
        });
    }
    getRecurring(id, project) {
        const row = this.db.prepare("SELECT * FROM quest_recurrences WHERE id=? AND project=?").get(id, resolve(project));
        return row ? fromRecurrenceRow(row) : undefined;
    }
    listRecurring(project) {
        const rows = this.db.prepare("SELECT * FROM quest_recurrences WHERE project=? ORDER BY created_at DESC").all(resolve(project));
        return rows.map(fromRecurrenceRow);
    }
    setRecurringEnabled(id, project, enabled) {
        const now = Date.now();
        const normalized = resolve(project);
        return this.tx(() => {
            const changed = Number(this.db.prepare("UPDATE quest_recurrences SET enabled=?, updated_at=? WHERE id=? AND project=?").run(enabled ? 1 : 0, now, id, normalized).changes) > 0;
            if (!changed)
                return undefined;
            this.projectEvent(normalized, enabled ? "recurring_resumed" : "recurring_paused", { id });
            return this.getRecurring(id, normalized);
        });
    }
    deleteRecurring(id, project) {
        const normalized = resolve(project);
        return this.tx(() => {
            const changed = Number(this.db.prepare("DELETE FROM quest_recurrences WHERE id=? AND project=?").run(id, normalized).changes) > 0;
            if (changed)
                this.projectEvent(normalized, "recurring_deleted", { id });
            return changed;
        });
    }
    materializeDueRecurring(project, now = Date.now(), limit = 100) {
        const normalized = resolve(project);
        return this.tx(() => {
            if (this.projectControl(normalized).paused)
                return [];
            const materialized = [];
            const rows = this.db.prepare("SELECT * FROM quest_recurrences WHERE project=? AND enabled=1 AND next_run_at <= ? ORDER BY next_run_at ASC").all(normalized, now);
            for (const row of rows) {
                if (materialized.length >= limit)
                    break;
                let next = row.next_run_at;
                if (row.catch_up === "skip") {
                    while (next <= now)
                        next = nextCronTime(row.cron, next + 1, row.timezone);
                    this.db.prepare("UPDATE quest_recurrences SET next_run_at=?, updated_at=? WHERE id=?").run(next, now, row.id);
                    continue;
                }
                const occurrences = [];
                if (row.catch_up === "one") {
                    occurrences.push(next);
                    while (next <= now)
                        next = nextCronTime(row.cron, next + 1, row.timezone);
                }
                else {
                    while (next <= now && materialized.length + occurrences.length < limit) {
                        occurrences.push(next);
                        next = nextCronTime(row.cron, next + 1, row.timezone);
                    }
                }
                for (const occurrence of occurrences) {
                    const existing = this.db.prepare("SELECT id FROM quests WHERE recurrence_id=? AND occurrence_at=?").get(row.id, occurrence);
                    if (existing?.id) {
                        materialized.push(this.get(existing.id));
                        continue;
                    }
                    const q = this.enqueue({ project: normalized, role: row.role, task: row.task, context: row.context ?? undefined, name: `${row.name} @ ${new Date(occurrence).toISOString()}`, priority: row.priority, maxAttempts: row.max_attempts, backoffBaseMs: row.backoff_base_ms, dedupeKey: `recurrence:${row.id}:${occurrence}`, recurrenceId: row.id, occurrenceAt: occurrence });
                    materialized.push(q);
                }
                this.db.prepare("UPDATE quest_recurrences SET next_run_at=?, last_enqueued_at=?, updated_at=? WHERE id=?").run(next, occurrences.at(-1) ?? row.last_enqueued_at, now, row.id);
            }
            return materialized;
        });
    }
    exportArchive(options = {}) {
        const project = options.project ? resolve(options.project) : undefined;
        const quests = (project ? this.list(project, 5000) : this.db.prepare("SELECT * FROM quests ORDER BY project, created_at ASC").all().map((r) => this.fromRow(r)));
        const ids = new Set(quests.map((q) => q.id));
        const projects = [...new Set(quests.map((q) => q.project))];
        const events = this.db.prepare(`SELECT quest_id, event, at, data FROM quest_events ORDER BY seq ASC`).all().filter((e) => ids.has(e.quest_id)).map((e) => ({ questId: e.quest_id, event: e.event, at: e.at, data: JSON.parse(e.data) }));
        const runs = this.db.prepare("SELECT * FROM quest_runs ORDER BY quest_id, attempt ASC").all().filter((r) => ids.has(r.quest_id)).map(fromRunRow);
        const artifacts = this.db.prepare("SELECT * FROM quest_artifacts ORDER BY quest_id, attempt ASC").all().filter((a) => ids.has(a.quest_id)).map(fromArtifactRow);
        const groups = project ? this.listGroups(project, 5000) : projects.flatMap((p) => this.listGroups(p, 5000));
        const recurrences = project ? this.listRecurring(project) : this.db.prepare("SELECT * FROM quest_recurrences ORDER BY project, created_at ASC").all().map(fromRecurrenceRow);
        const projectEvents = this.db.prepare("SELECT project, event, at, data FROM project_events ORDER BY seq ASC").all().filter((e) => !project || e.project === project).map((e) => ({ project: e.project, event: e.event, at: e.at, data: JSON.parse(e.data) }));
        return { format: "questline.archive", version: 1, exportedAt: new Date().toISOString(), projects, groups, quests, events, runs, artifacts, recurrences, projectEvents };
    }
    importArchive(archive, options = {}) {
        if (!archive || archive.format !== "questline.archive" || archive.version !== 1 || !Array.isArray(archive.quests))
            throw new Error("invalid questline archive");
        const conflict = options.conflict ?? "error";
        const idMap = {};
        let groups = 0, quests = 0, recurrences = 0, skipped = 0;
        const now = Date.now();
        return this.tx(() => {
            for (const q of archive.quests) {
                const exists = this.get(q.id);
                let newId = q.id;
                if (exists) {
                    if (conflict === "skip") {
                        skipped++;
                        continue;
                    }
                    if (conflict === "error")
                        throw new Error(`quest ${q.id} already exists`);
                    newId = `q-${randomUUID().slice(0, 8)}`;
                }
                idMap[q.id] = newId;
            }
            const projectFor = (p) => options.projectMap?.[p] ? resolve(options.projectMap[p]) : p;
            for (const g of archive.groups ?? []) {
                const project = projectFor(g.project);
                const gid = this.db.prepare("SELECT id FROM quest_groups WHERE id=?").get(g.id) ? `g-${randomUUID().slice(0, 8)}` : g.id;
                this.db.prepare("INSERT OR IGNORE INTO quest_groups (id, project, name, kind, pipeline_name, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)").run(gid, project, g.name, g.kind, g.pipelineName ?? null, g.createdAt ?? now, g.updatedAt ?? now);
                groups++;
            }
            for (const q of archive.quests) {
                const newId = idMap[q.id];
                if (!newId)
                    continue;
                const project = projectFor(q.project);
                this.insertQuest({ id: newId, project, role: q.role, name: q.name, task: q.task, context: q.context, priority: q.priority, scheduledAt: q.scheduledAt, maxAttempts: q.maxAttempts, backoffBaseMs: q.backoffBaseMs, dedupeKey: q.dedupeKey ? `${q.dedupeKey}:import:${newId}` : undefined, chain: q.chain, retainUntilConsumed: q.retainUntilConsumed, groupId: q.groupId, groupStep: q.groupStep, recurrenceId: q.recurrenceId, occurrenceAt: q.occurrenceAt }, q.createdAt ?? now, q.state === "running" ? "interrupted" : q.state);
                this.db.prepare("UPDATE quests SET attempts=?, retry_at=?, consumed_at=?, lease_gen=?, created_at=?, updated_at=?, started_at=?, finished_at=?, result=?, error=?, failure_kind=?, failed_dependency_id=? WHERE id=?")
                    .run(q.attempts, q.retryAt ?? null, q.consumedAt ?? null, q.leaseVersion, q.createdAt, q.updatedAt, q.startedAt ?? null, q.finishedAt ?? null, q.result ?? null, q.error ?? null, q.failureKind ?? null, q.failedDependencyId ?? null, newId);
                quests++;
            }
            for (const q of archive.quests) {
                const newId = idMap[q.id];
                if (!newId)
                    continue;
                for (const dep of q.dependsOn ?? []) {
                    const mappedDep = idMap[dep] ?? dep;
                    if (this.get(mappedDep))
                        this.db.prepare("INSERT OR IGNORE INTO quest_deps (quest_id, depends_on) VALUES (?, ?)").run(newId, mappedDep);
                }
            }
            for (const e of archive.events ?? [])
                if (idMap[e.questId])
                    this.db.prepare("INSERT INTO quest_events (quest_id, event, at, data) VALUES (?, ?, ?, ?)").run(idMap[e.questId], e.event, e.at, JSON.stringify(e.data));
            for (const r of archive.runs ?? [])
                if (idMap[r.questId])
                    this.db.prepare("INSERT OR IGNORE INTO quest_runs (quest_id, attempt, owner_session, lease_gen, agent_run_id, model, provider, tier, turns, tokens, cost_usd, last_activity, started_at, finished_at, outcome, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").run(idMap[r.questId], r.attempt, r.ownerSession, r.leaseVersion, r.agentRunId ?? null, r.model ?? null, r.provider ?? null, r.tier ?? null, r.turns, r.tokens, r.costUsd, r.lastActivity ?? null, r.startedAt, r.finishedAt ?? null, r.outcome ?? null, r.error ?? null);
            for (const a of archive.artifacts ?? [])
                if (idMap[a.questId])
                    this.db.prepare("INSERT OR IGNORE INTO quest_artifacts (quest_id, attempt, lease_gen, kind, path, bytes, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)").run(idMap[a.questId], a.attempt, a.leaseVersion, a.kind, a.path, a.bytes, a.createdAt);
            for (const r of archive.recurrences ?? []) {
                const project = projectFor(r.project);
                const rid = this.db.prepare("SELECT id FROM quest_recurrences WHERE id=?").get(r.id) ? `r-${randomUUID().slice(0, 8)}` : r.id;
                this.db.prepare("INSERT INTO quest_recurrences (id, project, name, cron, timezone, role, task, context, priority, max_attempts, backoff_base_ms, catch_up, enabled, next_run_at, last_enqueued_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").run(rid, project, r.name, r.cron, r.timezone, r.role, r.task, r.context ?? null, r.priority, r.maxAttempts, r.backoffBaseMs, r.catchUp, r.enabled ? 1 : 0, r.nextRunAt, r.lastEnqueuedAt ?? null, r.createdAt, r.updatedAt);
                recurrences++;
            }
            for (const p of new Set(archive.quests.map((q) => projectFor(q.project))))
                this.prune(p);
            return { groups, quests, recurrences, skipped, idMap };
        });
    }
    backup(destination, options = {}) {
        const dest = resolve(expandHome(destination));
        if (dest === this.path)
            throw new Error("cannot back up over the live database");
        if (existsSync(dest)) {
            if (!options.overwrite)
                throw new Error(`${dest} already exists (use --force to overwrite)`);
            rmSync(dest, { force: true });
        }
        mkdirSync(dirname(dest), { recursive: true });
        this.db.exec(`VACUUM INTO '${dest.replaceAll("'", "''")}'`);
        return dest;
    }
    insertQuest(input, now, state = "queued") {
        this.db.prepare(`INSERT INTO quests (id, project, role, name, task, context, state, attempts, max_attempts, backoff_base_ms,
                           priority, scheduled_at, dedupe_key, chain_meta, retain_until_consumed, lease_gen, created_at, updated_at,
                           group_id, group_step, recurrence_id, occurrence_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?)`).run(input.id, input.project, input.role, input.name, input.task, input.context ?? null, state, Math.max(1, input.maxAttempts ?? this.defaultMaxAttempts), Math.max(0, input.backoffBaseMs ?? this.defaultBackoffBaseMs), input.priority ?? 0, Math.max(0, input.scheduledAt ?? 0), input.dedupeKey ?? null, input.chain ? JSON.stringify(input.chain) : null, input.retainUntilConsumed ? 1 : 0, now, now, input.groupId ?? null, input.groupStep ?? null, input.recurrenceId ?? null, input.occurrenceAt ?? null);
    }
    fromRow(row) {
        const deps = this.db.prepare("SELECT depends_on FROM quest_deps WHERE quest_id=? ORDER BY depends_on")
            .all(row.id);
        return {
            id: row.id,
            project: row.project,
            role: row.role,
            name: row.name,
            task: row.task,
            context: row.context ?? undefined,
            state: row.state,
            attempts: row.attempts,
            maxAttempts: row.max_attempts,
            backoffBaseMs: row.backoff_base_ms,
            priority: row.priority,
            scheduledAt: row.scheduled_at,
            retryAt: row.retry_at ?? undefined,
            dedupeKey: row.dedupe_key ?? undefined,
            chain: parseChain(row.chain_meta),
            retainUntilConsumed: row.retain_until_consumed === 1,
            consumedAt: row.consumed_at ?? undefined,
            leaseVersion: row.lease_gen,
            leaseExpiresAt: row.lease_expires_at ?? undefined,
            heartbeatAt: row.heartbeat_at ?? undefined,
            dependsOn: deps.map((d) => d.depends_on),
            createdAt: row.created_at,
            updatedAt: row.updated_at,
            startedAt: row.started_at ?? undefined,
            finishedAt: row.finished_at ?? undefined,
            agentRunId: row.agent_run_id ?? undefined,
            ownerSession: row.owner_session ?? undefined,
            result: row.result ?? undefined,
            error: row.error ?? undefined,
            failureKind: row.failure_kind ?? undefined,
            failedDependencyId: row.failed_dependency_id ?? undefined,
            groupId: row.group_id ?? undefined,
            groupStep: row.group_step ?? undefined,
            recurrenceId: row.recurrence_id ?? undefined,
            occurrenceAt: row.occurrence_at ?? undefined,
        };
    }
    validateDependenciesForNewQuest(project, deps) {
        for (const dep of new Set(deps)) {
            const row = this.db.prepare("SELECT project FROM quests WHERE id=?").get(dep);
            if (!row?.project)
                throw new DependencyValidationError("dependency_not_found", `dependency ${dep} does not exist`, { dependencyId: dep });
            if (row.project !== project)
                throw new DependencyValidationError("dependency_cross_project", `dependency ${dep} belongs to a different project`, { dependencyId: dep });
        }
    }
    validateDependencyMutation(questId, dependsOn, project) {
        const q = this.get(questId);
        if (!q || q.project !== project)
            throw new DependencyValidationError("quest_not_found", `quest ${questId} does not exist in this project`, { questId });
        if (q.state !== "queued" && q.state !== "interrupted")
            throw new DependencyValidationError("quest_not_mutable", `quest ${questId} is ${q.state}; dependencies are only mutable while queued/interrupted`, { questId });
        if (questId === dependsOn)
            throw new DependencyValidationError("dependency_self", `quest ${questId} cannot depend on itself`, { questId, dependencyId: dependsOn, cycle: [questId, dependsOn] });
        this.validateDependenciesForNewQuest(project, [dependsOn]);
        const cycle = this.findCycleIfAdd(questId, dependsOn, project);
        if (cycle)
            throw new DependencyValidationError("dependency_cycle", `dependency cycle: ${cycle.join(" -> ")}`, { questId, dependencyId: dependsOn, cycle });
    }
    findCycleIfAdd(questId, dependsOn, project) {
        const rows = this.db.prepare("SELECT d.quest_id, d.depends_on FROM quest_deps d JOIN quests q ON q.id=d.quest_id WHERE q.project=?").all(project);
        const graph = new Map();
        for (const row of rows) {
            if (!graph.has(row.quest_id))
                graph.set(row.quest_id, []);
            graph.get(row.quest_id).push(row.depends_on);
        }
        graph.set(questId, [...(graph.get(questId) ?? []), dependsOn]);
        const stack = [];
        const seen = new Set();
        const dfs = (node) => {
            if (node === questId && stack.length)
                return [...stack, node];
            if (seen.has(node))
                return undefined;
            seen.add(node);
            stack.push(node);
            for (const dep of graph.get(node) ?? []) {
                const cycle = dfs(dep);
                if (cycle)
                    return cycle;
            }
            stack.pop();
            return undefined;
        };
        return dfs(dependsOn);
    }
    countDependencyCycles(project) {
        const rows = this.db.prepare(`SELECT d.quest_id, d.depends_on FROM quest_deps d JOIN quests q ON q.id=d.quest_id ${project ? "WHERE q.project=?" : ""}`).all(...(project ? [project] : []));
        const graph = new Map();
        for (const row of rows) {
            if (!graph.has(row.quest_id))
                graph.set(row.quest_id, []);
            graph.get(row.quest_id).push(row.depends_on);
        }
        let cycles = 0;
        const visiting = new Set();
        const visited = new Set();
        const dfs = (node) => {
            if (visiting.has(node)) {
                cycles++;
                return;
            }
            if (visited.has(node))
                return;
            visiting.add(node);
            for (const dep of graph.get(node) ?? [])
                dfs(dep);
            visiting.delete(node);
            visited.add(node);
        };
        for (const node of graph.keys())
            dfs(node);
        return cycles;
    }
    propagateFailedDeps(project, now) {
        while (true) {
            const blocked = this.db.prepare(`SELECT q.id, d.depends_on, dq.state dep_state FROM quests q
         JOIN quest_deps d ON d.quest_id=q.id
         JOIN quests dq ON dq.id=d.depends_on
         WHERE q.project=? AND q.state IN ('queued','interrupted') AND dq.state IN ('failed','cancelled')
         ORDER BY q.created_at ASC`).all(project);
            if (!blocked.length)
                break;
            let changedAny = false;
            for (const b of blocked) {
                const changed = Number(this.db.prepare("UPDATE quests SET state='failed', failure_kind='dependency', failed_dependency_id=?, error=?, finished_at=?, updated_at=? WHERE id=? AND state IN ('queued','interrupted')").run(b.depends_on, `dependency ${b.depends_on} ${b.dep_state}`, now, now, b.id).changes) > 0;
                if (changed) {
                    changedAny = true;
                    this.event(b.id, "dependency_failed", { dependsOn: b.depends_on, depState: b.dep_state });
                }
            }
            if (!changedAny)
                break;
        }
    }
    requeueDependencyFailedDescendants(project, roots, now) {
        let frontier = [...roots];
        while (frontier.length) {
            const placeholders = frontier.map(() => "?").join(",");
            const rows = this.db.prepare(`SELECT DISTINCT q.id FROM quests q JOIN quest_deps d ON d.quest_id=q.id
         WHERE q.project=? AND q.state='failed' AND q.failure_kind='dependency' AND d.depends_on IN (${placeholders})`).all(project, ...frontier);
            frontier = [];
            for (const row of rows) {
                const changed = Number(this.db.prepare("UPDATE quests SET state='queued', retry_at=NULL, finished_at=NULL, error=NULL, failure_kind=NULL, failed_dependency_id=NULL, updated_at=? WHERE id=? AND state='failed' AND failure_kind='dependency'").run(now, row.id).changes) > 0;
                if (changed) {
                    this.event(row.id, "dependency_requeued", { roots });
                    frontier.push(row.id);
                }
            }
        }
    }
    insertArtifacts(lease, attempt, artifacts, now) {
        for (const artifact of artifacts) {
            this.db.prepare("INSERT OR REPLACE INTO quest_artifacts (quest_id, attempt, lease_gen, kind, path, bytes, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)").run(lease.id, attempt, lease.version, artifact.kind, artifact.path, artifact.bytes, now);
        }
    }
    finalizeRun(lease, now, outcome, error) {
        this.db.prepare("UPDATE quest_runs SET finished_at=?, outcome=?, error=? WHERE quest_id=? AND lease_gen=? AND finished_at IS NULL").run(now, outcome, error ?? null, lease.id, lease.version);
    }
    event(id, event, data) {
        this.db.prepare("INSERT INTO quest_events (quest_id, event, at, data) VALUES (?, ?, ?, ?)")
            .run(id, event, Date.now(), JSON.stringify(data));
    }
    projectEvent(project, event, data) {
        this.db.prepare("INSERT INTO project_events (project, event, at, data) VALUES (?, ?, ?, ?)")
            .run(project, event, Date.now(), JSON.stringify(data));
    }
    ensureGroup(groupId, project) {
        const row = this.db.prepare("SELECT project FROM quest_groups WHERE id=?").get(groupId);
        if (!row?.project)
            throw new Error(`group ${groupId} does not exist`);
        if (row.project !== project)
            throw new Error(`group ${groupId} belongs to a different project`);
    }
    tx(fn) {
        if (this.inTransaction)
            return fn();
        this.db.exec("BEGIN IMMEDIATE");
        this.inTransaction = true;
        try {
            const out = fn();
            this.db.exec("COMMIT");
            this.inTransaction = false;
            return out;
        }
        catch (err) {
            this.inTransaction = false;
            try {
                this.db.exec("ROLLBACK");
            }
            catch { /* already rolled back */ }
            throw err;
        }
    }
    migrate() {
        this.db.exec("BEGIN IMMEDIATE");
        try {
            const tableExists = this.db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='quests'").get() !== undefined;
            if (!tableExists) {
                this.db.exec(`
          CREATE TABLE quests (
            id TEXT PRIMARY KEY,
            project TEXT NOT NULL,
            role TEXT NOT NULL,
            name TEXT NOT NULL,
            task TEXT NOT NULL,
            context TEXT,
            state TEXT NOT NULL CHECK(state IN ('queued','running','done','failed','cancelled','interrupted')),
            attempts INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 1,
            backoff_base_ms INTEGER NOT NULL DEFAULT 30000,
            priority INTEGER NOT NULL DEFAULT 0,
            scheduled_at INTEGER NOT NULL DEFAULT 0,
            retry_at INTEGER,
            dedupe_key TEXT,
            chain_meta TEXT,
            retain_until_consumed INTEGER NOT NULL DEFAULT 0,
            consumed_at INTEGER,
            lease_gen INTEGER NOT NULL DEFAULT 0,
            lease_expires_at INTEGER,
            heartbeat_at INTEGER,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            started_at INTEGER,
            finished_at INTEGER,
            agent_run_id TEXT,
            owner_session TEXT,
            result TEXT,
            error TEXT,
            failure_kind TEXT CHECK(failure_kind IS NULL OR failure_kind IN ('execution','dependency')),
            failed_dependency_id TEXT,
            group_id TEXT,
            group_step TEXT,
            recurrence_id TEXT,
            occurrence_at INTEGER
          );
        `);
            }
            else {
                const columns = new Set(this.db.prepare("PRAGMA table_info(quests)").all().map((c) => c.name));
                const add = (name, ddl) => {
                    if (!columns.has(name))
                        this.db.exec(`ALTER TABLE quests ADD COLUMN ${ddl}`);
                };
                add("owner_session", "owner_session TEXT");
                add("max_attempts", "max_attempts INTEGER NOT NULL DEFAULT 1");
                add("backoff_base_ms", "backoff_base_ms INTEGER NOT NULL DEFAULT 30000");
                add("priority", "priority INTEGER NOT NULL DEFAULT 0");
                add("scheduled_at", "scheduled_at INTEGER NOT NULL DEFAULT 0");
                add("retry_at", "retry_at INTEGER");
                add("dedupe_key", "dedupe_key TEXT");
                add("chain_meta", "chain_meta TEXT");
                add("retain_until_consumed", "retain_until_consumed INTEGER NOT NULL DEFAULT 0");
                add("consumed_at", "consumed_at INTEGER");
                add("lease_gen", "lease_gen INTEGER NOT NULL DEFAULT 0");
                add("lease_expires_at", "lease_expires_at INTEGER");
                add("heartbeat_at", "heartbeat_at INTEGER");
                add("failure_kind", "failure_kind TEXT CHECK(failure_kind IS NULL OR failure_kind IN ('execution','dependency'))");
                add("failed_dependency_id", "failed_dependency_id TEXT");
                add("group_id", "group_id TEXT");
                add("group_step", "group_step TEXT");
                add("recurrence_id", "recurrence_id TEXT");
                add("occurrence_at", "occurrence_at INTEGER");
            }
            this.db.exec(`
        CREATE INDEX IF NOT EXISTS quests_project_state_created ON quests(project, state, created_at);
        CREATE INDEX IF NOT EXISTS quests_claim_order ON quests(project, state, priority DESC, created_at ASC);
        CREATE INDEX IF NOT EXISTS quests_project_group ON quests(project, group_id, created_at);
        CREATE UNIQUE INDEX IF NOT EXISTS quests_dedupe_active ON quests(project, dedupe_key)
          WHERE dedupe_key IS NOT NULL AND state IN ('queued','running','interrupted');
        CREATE UNIQUE INDEX IF NOT EXISTS quests_recurrence_occurrence_unique ON quests(recurrence_id, occurrence_at)
          WHERE recurrence_id IS NOT NULL AND occurrence_at IS NOT NULL;
        CREATE TABLE IF NOT EXISTS quest_events (
          seq INTEGER PRIMARY KEY AUTOINCREMENT,
          quest_id TEXT NOT NULL REFERENCES quests(id) ON DELETE CASCADE,
          event TEXT NOT NULL,
          at INTEGER NOT NULL,
          data TEXT NOT NULL DEFAULT '{}'
        );
        CREATE INDEX IF NOT EXISTS quest_events_quest_seq ON quest_events(quest_id, seq);
        CREATE TABLE IF NOT EXISTS quest_deps (
          quest_id TEXT NOT NULL REFERENCES quests(id) ON DELETE CASCADE,
          depends_on TEXT NOT NULL,
          PRIMARY KEY (quest_id, depends_on)
        );
        CREATE TABLE IF NOT EXISTS scheduler_leases (
          project TEXT PRIMARY KEY,
          owner TEXT NOT NULL,
          expires_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS quest_runs (
          quest_id TEXT NOT NULL REFERENCES quests(id) ON DELETE CASCADE,
          attempt INTEGER NOT NULL,
          owner_session TEXT NOT NULL,
          lease_gen INTEGER NOT NULL,
          agent_run_id TEXT,
          model TEXT,
          provider TEXT,
          tier TEXT,
          turns INTEGER NOT NULL DEFAULT 0,
          tokens INTEGER NOT NULL DEFAULT 0,
          cost_usd REAL NOT NULL DEFAULT 0,
          last_activity TEXT,
          started_at INTEGER NOT NULL,
          finished_at INTEGER,
          outcome TEXT,
          error TEXT,
          PRIMARY KEY (quest_id, attempt)
        );
        CREATE TABLE IF NOT EXISTS quest_artifacts (
          quest_id TEXT NOT NULL REFERENCES quests(id) ON DELETE CASCADE,
          attempt INTEGER NOT NULL,
          lease_gen INTEGER NOT NULL,
          kind TEXT NOT NULL,
          path TEXT NOT NULL,
          bytes INTEGER NOT NULL,
          created_at INTEGER NOT NULL,
          PRIMARY KEY (quest_id, attempt, kind)
        );
        CREATE TABLE IF NOT EXISTS quest_groups (
          id TEXT PRIMARY KEY,
          project TEXT NOT NULL,
          name TEXT NOT NULL,
          kind TEXT NOT NULL CHECK(kind IN ('group','pipeline')),
          pipeline_name TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS quest_groups_project_created ON quest_groups(project, created_at DESC);
        CREATE TABLE IF NOT EXISTS project_controls (
          project TEXT PRIMARY KEY,
          paused_at INTEGER,
          paused_reason TEXT,
          updated_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS project_events (
          seq INTEGER PRIMARY KEY AUTOINCREMENT,
          project TEXT NOT NULL,
          event TEXT NOT NULL,
          at INTEGER NOT NULL,
          data TEXT NOT NULL DEFAULT '{}'
        );
        CREATE TABLE IF NOT EXISTS quest_recurrences (
          id TEXT PRIMARY KEY,
          project TEXT NOT NULL,
          name TEXT NOT NULL,
          cron TEXT NOT NULL,
          timezone TEXT NOT NULL,
          role TEXT NOT NULL,
          task TEXT NOT NULL,
          context TEXT,
          priority INTEGER NOT NULL DEFAULT 0,
          max_attempts INTEGER NOT NULL DEFAULT 1,
          backoff_base_ms INTEGER NOT NULL DEFAULT 30000,
          catch_up TEXT NOT NULL DEFAULT 'one' CHECK(catch_up IN ('one','all','skip')),
          enabled INTEGER NOT NULL DEFAULT 1,
          next_run_at INTEGER NOT NULL,
          last_enqueued_at INTEGER,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS quest_recurrences_due ON quest_recurrences(project, enabled, next_run_at);
        PRAGMA user_version=${SCHEMA_VERSION};
      `);
            const runColumns = new Set(this.db.prepare("PRAGMA table_info(quest_runs)").all().map((c) => c.name));
            if (!runColumns.has("provider"))
                this.db.exec("ALTER TABLE quest_runs ADD COLUMN provider TEXT");
            this.db.exec("COMMIT");
        }
        catch (err) {
            try {
                this.db.exec("ROLLBACK");
            }
            catch { /* nothing to roll back */ }
            throw err;
        }
    }
    prune(project) {
        const keep = Math.max(1, this.options.maxHistory);
        this.db.prepare(`DELETE FROM quests WHERE id IN (
         SELECT id FROM quests old WHERE project=? AND state IN ('done','failed','cancelled')
           AND NOT (retain_until_consumed=1 AND consumed_at IS NULL AND state='done')
           AND failure_kind IS NOT 'dependency'
           AND NOT EXISTS (
             SELECT 1 FROM quest_deps d JOIN quests child ON child.id=d.quest_id
             WHERE d.depends_on=old.id AND child.project=old.project
           )
         ORDER BY updated_at DESC, rowid DESC LIMIT -1 OFFSET ?
       )`).run(project, keep);
    }
}
