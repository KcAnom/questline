import { mkdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { homedir } from "node:os";
import { randomUUID } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
function expandHome(p) {
    if (p === "~")
        return homedir();
    if (p.startsWith("~/"))
        return join(homedir(), p.slice(2));
    return p;
}
/** Shared claim-eligibility predicate (parameters: now ×3). Runnable states
 *  gated on schedule and retry backoff, expired-lease reclamation, and no
 *  unfinished dependencies. */
const ELIGIBLE_SQL = `(
    (state IN ('queued','interrupted') AND scheduled_at <= ? AND COALESCE(retry_at, 0) <= ?)
    OR (state = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at < ?)
  )
  AND NOT EXISTS (
    SELECT 1 FROM quest_deps d JOIN quests dq ON dq.id = d.depends_on
    WHERE d.quest_id = q.id AND dq.state != 'done'
  )`;
const DEFAULT_LEASE_TTL_MS = 120_000;
const DEFAULT_MAX_ATTEMPTS = 1;
const DEFAULT_BACKOFF_BASE_MS = 30_000;
const SCHEMA_VERSION = 3;
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
/**
 * Durable, provider-neutral queue and append-only event journal for agent work.
 *
 * Schema v2 adds crash-safe ownership: every claim issues a QuestLease whose
 * version fences all write-backs, leases expire without heartbeats and are
 * reclaimable, failures retry with bounded exponential backoff, and quests
 * carry priority / schedule / dependency / dedupe / chain metadata. All state
 * transitions and their journal events commit in one transaction.
 */
export class QuestStore {
    options;
    path;
    leaseTtlMs;
    db;
    defaultMaxAttempts;
    defaultBackoffBaseMs;
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
    health() {
        const integrity = this.db.prepare("PRAGMA integrity_check").get()?.integrity_check ?? "unknown";
        const counts = this.db.prepare(`SELECT
         SUM(CASE WHEN state='queued' THEN 1 ELSE 0 END) queued,
         SUM(CASE WHEN state='interrupted' THEN 1 ELSE 0 END) interrupted,
         SUM(CASE WHEN state='running' THEN 1 ELSE 0 END) running,
         SUM(CASE WHEN state='running' AND lease_expires_at IS NOT NULL AND lease_expires_at < ? THEN 1 ELSE 0 END) expired
       FROM quests`).get(Date.now());
        return {
            path: this.path,
            integrity,
            queued: counts.queued ?? 0,
            interrupted: counts.interrupted ?? 0,
            running: counts.running ?? 0,
            expiredLeases: counts.expired ?? 0,
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
            const id = `q-${randomUUID().slice(0, 8)}`;
            this.db.prepare(`INSERT INTO quests (id, project, role, name, task, context, state, attempts, max_attempts, backoff_base_ms,
                             priority, scheduled_at, dedupe_key, chain_meta, retain_until_consumed, lease_gen, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, 'queued', 0, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)`).run(id, project, input.role, name, input.task, input.context ?? null, Math.max(1, input.maxAttempts ?? this.defaultMaxAttempts), Math.max(0, input.backoffBaseMs ?? this.defaultBackoffBaseMs), input.priority ?? 0, Math.max(0, input.scheduledAt ?? 0), input.dedupeKey ?? null, input.chain ? JSON.stringify(input.chain) : null, input.retainUntilConsumed ? 1 : 0, now, now);
            for (const dep of new Set(input.dependsOn ?? [])) {
                if (dep === id)
                    continue;
                this.db.prepare("INSERT OR IGNORE INTO quest_deps (quest_id, depends_on) VALUES (?, ?)").run(id, dep);
            }
            this.event(id, "enqueued", { role: input.role, name, priority: input.priority ?? 0, dependsOn: input.dependsOn ?? [] });
            this.prune();
            return this.get(id);
        });
    }
    get(id) {
        const row = this.db.prepare("SELECT * FROM quests WHERE id = ?").get(id);
        return row ? this.fromRow(row) : undefined;
    }
    list(project, limit = 30) {
        const rows = this.db.prepare("SELECT * FROM quests WHERE project = ? ORDER BY created_at DESC LIMIT ?").all(resolve(project), Math.max(1, Math.min(limit, 200)));
        return rows.map((row) => this.fromRow(row));
    }
    /** Per-attempt telemetry history for a quest, newest first. */
    runs(id, limit = 20) {
        const rows = this.db.prepare("SELECT * FROM quest_runs WHERE quest_id=? ORDER BY attempt DESC LIMIT ?").all(id, Math.max(1, Math.min(limit, 100)));
        return rows.map(fromRunRow);
    }
    /**
     * Claim the next eligible quest: queued/interrupted work whose schedule,
     * retry backoff, and dependencies allow it, or a running quest whose lease
     * expired (crash reclamation). Returns the claimed record plus its lease.
     */
    claimNext(project, ownerSession = "legacy") {
        const normalized = resolve(project);
        const now = Date.now();
        return this.tx(() => {
            this.sweepFailedDeps(normalized, now);
            const row = this.db.prepare(`SELECT id, state FROM quests q
         WHERE project = ? AND ${ELIGIBLE_SQL}
         ORDER BY priority DESC, created_at ASC LIMIT 1`).get(normalized, now, now, now);
            if (!row?.id)
                return undefined;
            return this.claimRow(row.id, row.state, ownerSession, now);
        });
    }
    /** Claim one specific quest (targeted claim, e.g. dashboard "run" or a chain
     *  step). Fails when the quest is ineligible: wrong state, scheduled later,
     *  in retry backoff, dependency-blocked, or running under a live lease. */
    claimById(id, project, ownerSession = "legacy") {
        const normalized = resolve(project);
        const now = Date.now();
        return this.tx(() => {
            this.sweepFailedDeps(normalized, now);
            const row = this.db.prepare(`SELECT id, state FROM quests q WHERE id = ? AND project = ? AND ${ELIGIBLE_SQL}`).get(id, normalized, now, now, now);
            if (!row?.id)
                return undefined;
            return this.claimRow(row.id, row.state, ownerSession, now);
        });
    }
    claimRow(id, state, ownerSession, now) {
        const reclaimed = state === "running";
        this.db.prepare(`UPDATE quests SET state='running', attempts=attempts+1, lease_gen=lease_gen+1,
              owner_session=?, agent_run_id=NULL, started_at=?, finished_at=NULL, heartbeat_at=?,
              lease_expires_at=?, retry_at=NULL, error=NULL, updated_at=?
       WHERE id=?`).run(ownerSession, now, now, now + this.leaseTtlMs, now, id);
        const quest = this.get(id);
        this.db.prepare(`INSERT OR REPLACE INTO quest_runs (quest_id, attempt, owner_session, lease_gen, turns, tokens, cost_usd, started_at)
       VALUES (?, ?, ?, ?, 0, 0, 0, ?)`).run(id, quest.attempts, ownerSession, quest.leaseVersion, now);
        this.event(id, reclaimed ? "reclaimed" : "claimed", { attempt: quest.attempts, leaseVersion: quest.leaseVersion, ownerSession });
        return { quest, lease: { id, ownerSession, version: quest.leaseVersion } };
    }
    /** Voluntarily hand a claimed quest back to the queue without recording a
     *  failure (e.g. a chain step claimed and then found it can't proceed yet).
     *  The attempt doesn't count against the retry budget. Fenced by the lease. */
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
    /** Explicitly requeue a terminal (failed/cancelled) or interrupted quest,
     *  granting one more attempt if its retry budget is exhausted. */
    requeue(id, project) {
        const now = Date.now();
        return this.tx(() => {
            const changed = Number(this.db.prepare(`UPDATE quests SET state='queued', retry_at=NULL, finished_at=NULL,
                max_attempts=MAX(max_attempts, attempts + 1), updated_at=?
         WHERE id=? AND project=? AND state IN ('failed','cancelled','interrupted')`).run(now, id, resolve(project)).changes) > 0;
            if (changed)
                this.event(id, "requeued", {});
            return changed;
        });
    }
    /** Extend the lease. Returns false when the lease was lost (expired and
     *  reclaimed, or the quest left the running state) — the caller must stop. */
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
    /** Record durable run telemetry for the current attempt. Fenced by the lease. */
    updateTelemetry(lease, t) {
        const owns = this.db.prepare("SELECT 1 FROM quests WHERE id=? AND state='running' AND owner_session=? AND lease_gen=?").get(lease.id, lease.ownerSession, lease.version);
        if (!owns)
            return false;
        return Number(this.db.prepare(`UPDATE quest_runs SET model=COALESCE(?, model), tier=COALESCE(?, tier), turns=COALESCE(?, turns),
              tokens=COALESCE(?, tokens), cost_usd=COALESCE(?, cost_usd), last_activity=COALESCE(?, last_activity)
       WHERE quest_id=? AND lease_gen=?`).run(t.model ?? null, t.tier ?? null, t.turns ?? null, t.tokens ?? null, t.costUsd ?? null, t.lastActivity ?? null, lease.id, lease.version).changes) > 0;
    }
    complete(lease, agentRunId, result) {
        const now = Date.now();
        return this.tx(() => {
            const changed = Number(this.db.prepare(`UPDATE quests SET state='done', result=?, error=NULL, finished_at=?, updated_at=?, lease_expires_at=NULL, heartbeat_at=NULL
         WHERE id=? AND state='running' AND owner_session=? AND lease_gen=? AND agent_run_id=?`).run(result, now, now, lease.id, lease.ownerSession, lease.version, agentRunId).changes) > 0;
            if (changed) {
                this.finalizeRun(lease, now, "done");
                this.event(lease.id, "completed", { leaseVersion: lease.version });
            }
            return changed;
        });
    }
    /**
     * Fail the current attempt. If attempts remain under maxAttempts the quest is
     * requeued with exponential backoff (at-least-once, bounded); otherwise it is
     * terminally failed. Fenced by the lease.
     */
    fail(lease, error, agentRunId) {
        const now = Date.now();
        return this.tx(() => {
            const row = this.db.prepare("SELECT attempts, max_attempts, backoff_base_ms FROM quests WHERE id=? AND state='running' AND owner_session=? AND lease_gen=? AND (? IS NULL OR agent_run_id=?)").get(lease.id, lease.ownerSession, lease.version, agentRunId ?? null, agentRunId ?? null);
            if (row?.attempts === undefined)
                return false;
            const retry = row.attempts < (row.max_attempts ?? 1);
            if (retry) {
                const retryAt = now + (row.backoff_base_ms ?? 0) * 2 ** (row.attempts - 1);
                this.db.prepare(`UPDATE quests SET state='queued', retry_at=?, error=?, updated_at=?, owner_session=NULL, agent_run_id=NULL,
                  lease_expires_at=NULL, heartbeat_at=NULL, started_at=NULL, finished_at=NULL
           WHERE id=?`).run(retryAt, error, now, lease.id);
                this.finalizeRun(lease, now, "failed", error);
                this.event(lease.id, "retry_scheduled", { error: error.slice(0, 1000), retryAt, attempt: row.attempts });
            }
            else {
                this.db.prepare("UPDATE quests SET state='failed', error=?, finished_at=?, updated_at=?, lease_expires_at=NULL, heartbeat_at=NULL WHERE id=?").run(error, now, now, lease.id);
                this.finalizeRun(lease, now, "failed", error);
                this.event(lease.id, "failed", { error: error.slice(0, 1000), attempt: row.attempts });
            }
            return true;
        });
    }
    cancel(id, project) {
        const now = Date.now();
        return this.tx(() => {
            const changed = Number(this.db.prepare("UPDATE quests SET state='cancelled', finished_at=?, updated_at=?, retry_at=NULL WHERE id=? AND project=? AND state IN ('queued','interrupted')").run(now, now, id, resolve(project)).changes) > 0;
            if (changed)
                this.event(id, "cancelled", {});
            return changed;
        });
    }
    /** Mark a retained result as consumed so dedupe stops matching it. */
    markConsumed(id, project) {
        const now = Date.now();
        return this.tx(() => {
            const changed = Number(this.db.prepare("UPDATE quests SET consumed_at=?, updated_at=? WHERE id=? AND project=? AND state='done' AND consumed_at IS NULL").run(now, now, id, resolve(project)).changes) > 0;
            if (changed)
                this.event(id, "consumed", {});
            return changed;
        });
    }
    /** Graceful shutdown: release this session's running quests as interrupted. */
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
    /** Flip running quests with expired leases to interrupted (crash cleanup).
     *  claimNext() also reclaims them directly; this exists for visibility. */
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
    /**
     * Acquire or renew the per-project scheduler lease. At most one scheduler
     * per project across all sessions: succeeds when the slot is free, expired,
     * or already ours (renewal). Returns false while another owner holds it.
     */
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
        const rows = this.db.prepare("SELECT event, at, data FROM quest_events WHERE quest_id=? ORDER BY seq DESC LIMIT ?").all(id, Math.max(1, Math.min(limit, 200)));
        return rows.map((row) => {
            try {
                return { event: row.event, at: row.at, data: JSON.parse(row.data) };
            }
            catch {
                return { event: row.event, at: row.at, data: row.data };
            }
        });
    }
    fromRow(row) {
        const deps = this.db.prepare("SELECT depends_on FROM quest_deps WHERE quest_id=?")
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
        };
    }
    /** Terminally fail queued quests whose dependencies can never complete. */
    sweepFailedDeps(project, now) {
        const blocked = this.db.prepare(`SELECT DISTINCT q.id, d.depends_on, dq.state dep_state FROM quests q
       JOIN quest_deps d ON d.quest_id = q.id
       JOIN quests dq ON dq.id = d.depends_on
       WHERE q.project=? AND q.state IN ('queued','interrupted') AND dq.state IN ('failed','cancelled')`).all(project);
        for (const b of blocked) {
            this.db.prepare("UPDATE quests SET state='failed', error=?, finished_at=?, updated_at=? WHERE id=? AND state IN ('queued','interrupted')").run(`dependency ${b.depends_on} ${b.dep_state}`, now, now, b.id);
            this.event(b.id, "dep_failed", { dependsOn: b.depends_on, depState: b.dep_state });
        }
    }
    finalizeRun(lease, now, outcome, error) {
        this.db.prepare("UPDATE quest_runs SET finished_at=?, outcome=?, error=? WHERE quest_id=? AND lease_gen=? AND finished_at IS NULL").run(now, outcome, error ?? null, lease.id, lease.version);
    }
    event(id, event, data) {
        this.db.prepare("INSERT INTO quest_events (quest_id, event, at, data) VALUES (?, ?, ?, ?)")
            .run(id, event, Date.now(), JSON.stringify(data));
    }
    /** All mutators run inside one immediate transaction so a state change and
     *  its journal events land atomically (or roll back together). Reentrant. */
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
    inTransaction = false;
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
            error TEXT
          );
        `);
            }
            else {
                // v1 → v2: additive column migration; existing rows keep their data.
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
            }
            this.db.exec(`
        CREATE INDEX IF NOT EXISTS quests_project_state_created ON quests(project, state, created_at);
        CREATE INDEX IF NOT EXISTS quests_claim_order ON quests(project, state, priority DESC, created_at ASC);
        CREATE UNIQUE INDEX IF NOT EXISTS quests_dedupe_active ON quests(project, dedupe_key)
          WHERE dedupe_key IS NOT NULL AND state IN ('queued','running','interrupted');
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
        PRAGMA user_version=${SCHEMA_VERSION};
      `);
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
    prune() {
        const keep = Math.max(20, this.options.maxHistory);
        this.db.prepare(`DELETE FROM quests WHERE id IN (
         SELECT id FROM quests WHERE state IN ('done','failed','cancelled')
           AND NOT (retain_until_consumed=1 AND consumed_at IS NULL AND state='done')
         ORDER BY updated_at DESC LIMIT -1 OFFSET ?
       )`).run(keep);
    }
}
