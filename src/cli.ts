/**
 * questline CLI: a durable, crash-safe work queue for CLI coding agents.
 */
import { parseArgs } from "node:util";
import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import { executorFor, inspectConfig, type ConfigError, type QuestlineConfig } from "./config.ts";
import { executeQuest } from "./executor.ts";
import { ANSI_THEME, dashboard, questDetailLines, questTableText } from "./dashboard.ts";
import { runTui } from "./tui.ts";
import { QuestRuntime, type ClaimedQuest } from "./runtime.ts";
import { QuestScheduler } from "./scheduler.ts";
import { DependencyValidationError, QuestStore, type QuestRecord } from "./store.ts";
import { startHttpApi } from "./api.ts";
import { fmtDuration, fmtTokens, fmtUsd } from "./fmt.ts";

const VERSION = "0.1.1";

const HELP = `questline ${VERSION} — durable quest queue for CLI coding agents

Usage: questline <command> [args]

  add <role> <task>      Queue work  (--name, --priority N, --context TEXT,
                         --depends id1,id2, --max-attempts N, --not-before ISO,
                         --dedupe-key KEY, --retain, --group ID)
  list                   Show quests for this project (--json, --limit N)
  show <id>              Full detail: task, attempts, telemetry, events, artifacts, result
  run [id]               Claim (next eligible, or a specific id) and execute now
  daemon                 Run the scheduler: drains the queue continuously
  dash                   Interactive TUI dashboard
  cancel <id>            Cancel queued/interrupted/running work
  retry <id>             Requeue failed/cancelled/interrupted work (+1 attempt)
  consume <id>           Mark a retained result consumed (frees its dedupe key)
  pause/resume           Pause or resume this project's queue
  group ...              create/list/show/cancel/retry quest groups
  pipeline ...           list configured pipelines, run a pipeline
  recurring ...          add/list/pause/resume/remove cron-style recurring quests
  export/import/backup   JSON archive and SQLite backup commands
  api                    Start a small local HTTP API (--port N)
  events <id>            Journal for one quest
  runs <id>              Per-attempt telemetry history
  health                 DB integrity and queue/project health (--json, --all-projects)
  completion             Print bash completion script

Executor (config: ~/.questline/config.json or <project>/.questline.json):
  default: ["claude", "-p", "{task}"] — any print-mode CLI agent works.
  Config files are JSONC. Structured telemetry protocol: protocol "questline-jsonl".
`;

function openStore(cfg: QuestlineConfig): QuestStore {
  return new QuestStore({
    path: cfg.dbPath,
    maxHistory: cfg.maxHistory,
    leaseTtlMs: cfg.leaseTtlMs,
    maxAttempts: cfg.maxAttempts,
    backoffBaseMs: cfg.backoffBaseMs,
  });
}

function parseIntFlag(value: string | undefined, name: string, min: number): number | undefined {
  if (value === undefined) return undefined;
  const n = Number(value);
  if (!Number.isInteger(n) || !Number.isFinite(n) || n < min) throw new Error(`${name} must be an integer >= ${min}`);
  return n;
}

function formatRuns(runs: ReturnType<QuestStore["runs"]>): string[] {
  return runs.map((r) => `attempt ${r.attempt} · ${r.outcome ?? "running"} · ${r.provider ? `${r.provider}/` : ""}${r.model ?? "?"}${r.tier ? ` [${r.tier}]` : ""} · turns ${r.turns} · ${fmtTokens(r.tokens)} tok · ${fmtUsd(r.costUsd)} · ${r.finishedAt ? fmtDuration(r.finishedAt - r.startedAt) : "…"}${r.lastActivity ? ` · ${r.lastActivity}` : ""}${r.error ? ` · ${r.error.slice(0, 120)}` : ""}`);
}

function formatArtifacts(artifacts: ReturnType<QuestStore["artifacts"]>): string[] {
  return artifacts.map((a) => `${a.kind} attempt ${a.attempt}: ${a.path} (${a.bytes} bytes)`);
}

function notify(cfg: QuestlineConfig, q: QuestRecord): void {
  const command = cfg.notifications?.command;
  if (!command?.length) return;
  const [cmd, ...args] = command.map((p) => p
    .replaceAll("{id}", q.id)
    .replaceAll("{state}", q.state)
    .replaceAll("{name}", q.name)
    .replaceAll("{role}", q.role)
    .replaceAll("{project}", q.project));
  try {
    const child = spawn(cmd, args, { cwd: q.project, stdio: "ignore", detached: true });
    child.unref();
  } catch { /* notifications are best-effort */ }
}

async function dispatchClaimed(runtime: QuestRuntime, cfg: QuestlineConfig, claimed: ClaimedQuest, log: (s: string) => void): Promise<boolean> {
  const { quest, lease, signal } = claimed;
  const executor = executorFor(cfg, quest.role);
  const agentRunId = `exec-${randomUUID().slice(0, 8)}`;
  if (!runtime.attachRun(lease, agentRunId)) {
    log(`⚠ ${quest.id} lease lost before executor start`);
    return false;
  }
  runtime.updateTelemetry(lease, { model: executor.command[0], provider: "cli", lastActivity: "started" });
  log(`▶ ${quest.id} ${quest.name} (${quest.role}) → ${executor.command[0]}`);
  const result = await executeQuest(quest, executor, {
    signal,
    agentRunId,
    artifactDir: cfg.artifactDir,
    onTelemetry: (t) => runtime.updateTelemetry(lease, t),
  });
  if (result.telemetry) runtime.updateTelemetry(lease, result.telemetry);
  runtime.updateTelemetry(lease, { lastActivity: result.status });
  if (result.status === "aborted" || signal.aborted) {
    const reason = signal.reason && typeof signal.reason === "object" && "kind" in signal.reason ? String((signal.reason as { kind: unknown }).kind) : "aborted";
    log(`↯ ${quest.id} ${reason}; worker stopped without stale settlement`);
    return false;
  }
  let settled = false;
  if (result.ok) {
    settled = runtime.complete(lease, agentRunId, result.output, result.artifacts);
    log(settled ? `✓ ${quest.id} done in ${fmtDuration(result.durationMs)}` : `⚠ ${quest.id} completed but lease was already lost`);
  } else {
    const message = result.timedOut ? `timed out after ${fmtDuration(result.durationMs)}\n${result.output}` : result.output;
    settled = runtime.fail(lease, message, agentRunId, result.artifacts);
    log(settled ? `✗ ${quest.id} failed (${result.status}${result.exitCode !== null ? `, exit ${result.exitCode}` : ""})` : `⚠ ${quest.id} failed but lease was already lost`);
  }
  const final = runtime.store.get(quest.id);
  if (final) notify(cfg, final);
  return result.ok && settled;
}

function loadOrReportConfig(cwd: string): QuestlineConfig {
  try {
    return inspectConfig(cwd).config;
  } catch (err) {
    const e = err as ConfigError;
    if (Array.isArray(e.diagnostics)) console.error(e.diagnostics.map((d) => `${d.file}${d.line ? `:${d.line}${d.column ? `:${d.column}` : ""}` : ""}${d.path ? ` (${d.path})` : ""}: ${d.message}`).join("\n"));
    else console.error(String(err));
    process.exitCode = 2;
    throw err;
  }
}

export async function main(argv: string[]): Promise<number> {
  const [command, ...rest] = argv;
  const cwd = process.cwd();
  if (!command || command === "help" || command === "--help") { process.stdout.write(HELP); return 0; }
  if (command === "--version" || command === "version") { console.log(VERSION); return 0; }
  if (command === "completion") {
    console.log(`complete -W "add list show run daemon dash cancel retry consume events runs health pause resume group pipeline recurring export import backup api help version" questline`);
    return 0;
  }

  let cfg: QuestlineConfig;
  try { cfg = loadOrReportConfig(cwd); } catch { return 2; }

  try {
    if (command === "add") {
      const { values, positionals } = parseArgs({
        args: rest,
        allowPositionals: true,
        options: {
          name: { type: "string" }, priority: { type: "string" }, context: { type: "string" },
          depends: { type: "string" }, "max-attempts": { type: "string" }, "not-before": { type: "string" },
          "dedupe-key": { type: "string" }, retain: { type: "boolean" }, group: { type: "string" },
        },
      });
      const [role, ...taskParts] = positionals;
      const task = taskParts.join(" ");
      if (!role || !task) { console.error('usage: questline add <role> "task..." [flags]'); return 2; }
      const notBefore = values["not-before"] ? Date.parse(values["not-before"]) : undefined;
      if (values["not-before"] && Number.isNaN(notBefore)) { console.error(`cannot parse --not-before "${values["not-before"]}"`); return 2; }
      const store = openStore(cfg);
      try {
        const q = store.enqueue({
          project: cwd, role, task,
          name: values.name, context: values.context,
          priority: parseIntFlag(values.priority, "--priority", Number.MIN_SAFE_INTEGER),
          dependsOn: values.depends ? values.depends.split(",").map((s) => s.trim()).filter(Boolean) : undefined,
          maxAttempts: parseIntFlag(values["max-attempts"], "--max-attempts", 1),
          scheduledAt: notBefore,
          dedupeKey: values["dedupe-key"],
          retainUntilConsumed: values.retain,
          groupId: values.group,
        });
        console.log(`queued ${q.id} · ${q.role} · ${q.name}${q.dedupeKey && store.events(q.id, 1)[0]?.event === "deduped" ? " (existing quest returned by dedupe key)" : ""}`);
        console.log(`run it:  questline run ${q.id}   (or start the daemon: questline daemon)`);
        return 0;
      } finally { store.close(); }
    }

    if (command === "list") {
      const { values } = parseArgs({ args: rest, options: { json: { type: "boolean" }, limit: { type: "string" } } });
      const store = openStore(cfg);
      try {
        const rows = store.list(cwd, parseIntFlag(values.limit, "--limit", 1) ?? 30);
        if (values.json) console.log(JSON.stringify(rows, null, 2));
        else console.log(rows.length ? questTableText(rows) : "No quests for this project. Add one: questline add <role> \"task\"");
        return 0;
      } finally { store.close(); }
    }

    if (command === "show" || command === "events" || command === "runs") {
      const { values, positionals } = parseArgs({ args: rest, allowPositionals: true, options: { json: { type: "boolean" } } });
      const id = positionals[0];
      if (!id) { console.error(`usage: questline ${command} <id>`); return 2; }
      const store = openStore(cfg);
      try {
        const q = store.get(id);
        if (!q || q.project !== resolve(cwd)) { console.error(`no quest ${id} in this project`); return 1; }
        if (command === "events") {
          const events = store.events(id, 200);
          if (values.json) console.log(JSON.stringify(events, null, 2));
          else for (const e of events) console.log(`${new Date(e.at).toISOString()} · ${e.event} · ${JSON.stringify(e.data)}`);
          return 0;
        }
        if (command === "runs") {
          const runs = store.runs(id);
          if (values.json) console.log(JSON.stringify(runs, null, 2));
          else for (const line of formatRuns(runs)) console.log(line);
          return 0;
        }
        const payload = { quest: q, runs: store.runs(id), events: store.events(id, 20), artifacts: store.artifacts(id) };
        if (values.json) console.log(JSON.stringify(payload, null, 2));
        else {
          console.log(questTableText([q]));
          console.log("");
          for (const l of questDetailLines(q, payload.runs, payload.events, payload.artifacts)) console.log(l);
          if (q.result) console.log(`\nresult preview:\n${q.result}`);
          if (q.error) console.log(`\nerror preview:\n${q.error}`);
        }
        return 0;
      } finally { store.close(); }
    }

    if (command === "cancel" || command === "retry" || command === "consume") {
      const id = rest[0];
      if (!id) { console.error(`usage: questline ${command} <id>`); return 2; }
      const store = openStore(cfg);
      try {
        const ok = command === "cancel" ? store.cancel(id, cwd) : command === "retry" ? store.requeue(id, cwd) : store.markConsumed(id, cwd);
        console.log(ok ? `${command}: ${id} ✓` : `${command}: ${id} refused (wrong state or not in this project)`);
        return ok ? 0 : 1;
      } finally { store.close(); }
    }

    if (command === "pause" || command === "resume") {
      const { values } = parseArgs({ args: rest, options: { reason: { type: "string" } } });
      const store = openStore(cfg);
      try {
        const state = command === "pause" ? store.pauseProject(cwd, values.reason) : store.resumeProject(cwd);
        console.log(state.paused ? `paused: ${state.reason ?? "queue paused"}` : "resumed");
        return 0;
      } finally { store.close(); }
    }

    if (command === "health") {
      const { values } = parseArgs({ args: rest, options: { json: { type: "boolean" }, "all-projects": { type: "boolean" } } });
      const store = openStore(cfg);
      try {
        const h = store.health(values["all-projects"] ? undefined : cwd);
        if (values.json) console.log(JSON.stringify(h, null, 2));
        else {
          console.log(`db: ${h.path}`);
          console.log(`integrity: ${h.integrity} · schema ${h.schemaVersion} · journal ${h.journalMode}`);
          if (h.project) console.log(`project: ${h.project}${h.paused ? ` · paused (${h.pausedReason ?? "no reason"})` : ""}`);
          console.log(`counts: queued ${h.counts.queued} · running ${h.counts.running} · interrupted ${h.counts.interrupted} · done ${h.counts.done} · failed ${h.counts.failed} · cancelled ${h.counts.cancelled}`);
          console.log(`blocked ${h.blockedByDependencies} · scheduled later ${h.scheduledLater} · retry backoff ${h.retryBackoff} · expired leases ${h.expiredLeases} · stale schedulers ${h.staleSchedulers}`);
          console.log(`dependency issues: orphan ${h.orphanDependencies} · cross-project ${h.crossProjectDependencies} · cycles ${h.dependencyCycles}`);
          console.log(`recurring: enabled ${h.recurring.enabled} · due ${h.recurring.due}`);
        }
        return h.integrity === "ok" && h.orphanDependencies === 0 && h.crossProjectDependencies === 0 && h.dependencyCycles === 0 ? 0 : 1;
      } finally { store.close(); }
    }

    if (command === "run") {
      const id = rest.find((a) => !a.startsWith("-"));
      const store = openStore(cfg);
      const runtime = new QuestRuntime({ store, ownerSession: `cli-${randomUUID().slice(0, 8)}`, heartbeatMs: cfg.heartbeatMs });
      let signalCode: number | undefined;
      const shutdown = (sig: NodeJS.Signals) => {
        signalCode = sig === "SIGINT" ? 130 : 143;
        runtime.beginShutdown({ kind: "shutdown", signal: sig });
      };
      process.once("SIGINT", shutdown);
      process.once("SIGTERM", shutdown);
      try {
        const claimed = id ? runtime.claimById(id, cwd) : runtime.claimNext(cwd);
        if (!claimed) {
          console.error(id
            ? `cannot claim ${id} — blocked on dependencies, scheduled later, paused, in retry backoff, already running, or finished`
            : "nothing claimable (queue empty, paused, scheduled later / in backoff / dependency-blocked)");
          return 1;
        }
        const ok = await dispatchClaimed(runtime, cfg, claimed, (s) => console.log(s));
        const final = store.get(claimed.quest.id);
        if (final?.result) console.log(`\n${final.result}`);
        else if (final?.error) console.error(`\n${final.error}`);
        return signalCode ?? (ok ? 0 : 1);
      } finally {
        process.removeListener("SIGINT", shutdown);
        process.removeListener("SIGTERM", shutdown);
        runtime.finishShutdown(cwd);
        store.close();
      }
    }

    if (command === "daemon") {
      const store = openStore(cfg);
      const runtime = new QuestRuntime({ store, ownerSession: `daemon-${randomUUID().slice(0, 8)}`, heartbeatMs: cfg.heartbeatMs });
      const scheduler = new QuestScheduler({
        runtime,
        project: cwd,
        pollMs: cfg.scheduler.pollMs,
        maxConcurrent: cfg.scheduler.maxConcurrent,
        roleConcurrency: cfg.scheduler.roleConcurrency,
        unrefTimer: false,
        dispatch: (claimed) => dispatchClaimed(runtime, cfg, claimed, (s) => console.log(`[${new Date().toISOString()}] ${s}`)),
        onError: (err) => console.error(`[scheduler] ${String(err)}`),
      });
      console.log(`questline daemon · project ${cwd} · concurrency ${cfg.scheduler.maxConcurrent} · poll ${cfg.scheduler.pollMs}ms · Ctrl-C to stop`);
      scheduler.start();
      const code = await new Promise<number>((resolveWait) => {
        const stop = (sig: NodeJS.Signals) => {
          console.log("\nstopping: terminating in-flight workers and releasing scheduler lease…");
          scheduler.stop();
          runtime.beginShutdown({ kind: "shutdown", signal: sig });
          scheduler.waitForIdle().finally(() => {
            runtime.finishShutdown(cwd);
            store.close();
            resolveWait(sig === "SIGINT" ? 130 : 143);
          });
        };
        process.once("SIGINT", stop);
        process.once("SIGTERM", stop);
      });
      return code;
    }

    if (command === "dash" || command === "dashboard") {
      const store = openStore(cfg);
      const runtime = new QuestRuntime({ store, ownerSession: `dash-${randomUUID().slice(0, 8)}`, heartbeatMs: cfg.heartbeatMs });
      const inFlight = new Map<string, Promise<unknown>>();
      await new Promise<void>((resolveQuit) => {
        let stopHost: (() => void) | undefined;
        const component = dashboard({
          store,
          project: cwd,
          theme: ANSI_THEME,
          actions: {
            run: (qid) => {
              const claimed = runtime.claimById(qid, cwd);
              if (!claimed) return `cannot claim ${qid} (blocked, paused, scheduled later, in backoff, or running)`;
              const p = dispatchClaimed(runtime, cfg, claimed, () => { /* TUI repaints on tick */ })
                .catch(() => { /* recorded on the quest */ })
                .finally(() => inFlight.delete(qid));
              inFlight.set(qid, p);
              return undefined;
            },
            retry: (qid) => (store.requeue(qid, cwd) ? undefined : `cannot requeue ${qid}`),
            cancel: (qid) => (store.cancel(qid, cwd) ? undefined : `cannot cancel ${qid}`),
          },
          onQuit: () => stopHost?.(),
        });
        const handle = runTui(component, {
          onQuit: () => {
            runtime.beginShutdown({ kind: "shutdown" });
            Promise.allSettled([...inFlight.values()]).finally(() => {
              runtime.finishShutdown(cwd);
              store.close();
              resolveQuit();
            });
          },
        });
        stopHost = handle.stop;
      });
      return 0;
    }

    if (command === "group") return handleGroup(rest, cfg, cwd);
    if (command === "pipeline") return handlePipeline(rest, cfg, cwd);
    if (command === "recurring") return handleRecurring(rest, cfg, cwd);
    if (command === "export" || command === "import" || command === "backup") return handleArchive(command, rest, cfg, cwd);

    if (command === "api") {
      const { values } = parseArgs({ args: rest, options: { port: { type: "string" }, host: { type: "string" } } });
      const store = openStore(cfg);
      const handle = await startHttpApi({ store, project: resolve(cwd), port: parseIntFlag(values.port, "--port", 0), host: values.host });
      console.log(`questline API listening at ${handle.url}`);
      await new Promise<void>((resolveStop) => {
        const stop = () => void handle.close().finally(() => { store.close(); resolveStop(); });
        process.once("SIGINT", stop);
        process.once("SIGTERM", stop);
      });
      return 0;
    }

    console.error(`unknown command "${command}" — questline help`);
    return 2;
  } catch (err) {
    if (err instanceof DependencyValidationError) console.error(`${err.code}: ${err.message}`);
    else console.error(String(err));
    return 2;
  }
}

async function handleGroup(rest: string[], cfg: QuestlineConfig, cwd: string): Promise<number> {
  const [sub, ...args] = rest;
  const store = openStore(cfg);
  try {
    if (sub === "create") {
      const name = args.join(" ");
      if (!name) { console.error("usage: questline group create <name>"); return 2; }
      const g = store.createGroup({ project: cwd, name });
      console.log(`group ${g.id} · ${g.name}`);
      return 0;
    }
    if (sub === "list") { for (const g of store.listGroups(cwd)) console.log(`${g.id} · ${g.kind} · ${g.state} · ${g.name} · ${g.questIds.length} quests`); return 0; }
    if (sub === "show") { const g = store.getGroup(args[0]!, cwd); if (!g) return 1; console.log(JSON.stringify(g, null, 2)); return 0; }
    if (sub === "cancel") { console.log(JSON.stringify(store.cancelGroup(args[0]!, cwd))); return 0; }
    if (sub === "retry") { console.log(JSON.stringify(store.requeueGroup(args[0]!, cwd))); return 0; }
    console.error("usage: questline group create|list|show|cancel|retry"); return 2;
  } finally { store.close(); }
}

async function handlePipeline(rest: string[], cfg: QuestlineConfig, cwd: string): Promise<number> {
  const [sub, name] = rest;
  if (sub === "list") { console.log(Object.keys(cfg.pipelines ?? {}).join("\n") || "No configured pipelines."); return 0; }
  if (sub === "run") {
    if (!name || !cfg.pipelines?.[name]) { console.error(`unknown pipeline ${name ?? ""}`); return 2; }
    const store = openStore(cfg);
    try {
      const g = store.enqueuePipeline({ project: cwd, pipelineName: name, name, definition: cfg.pipelines[name] });
      console.log(`pipeline ${name} queued as group ${g.id} (${g.questIds.length} quests)`);
      return 0;
    } finally { store.close(); }
  }
  console.error("usage: questline pipeline list|run <name>"); return 2;
}

async function handleRecurring(rest: string[], cfg: QuestlineConfig, cwd: string): Promise<number> {
  const [sub, ...args] = rest;
  const store = openStore(cfg);
  try {
    if (sub === "add") {
      const { values, positionals } = parseArgs({ args, allowPositionals: true, options: { cron: { type: "string" }, role: { type: "string" }, task: { type: "string" }, timezone: { type: "string" }, "catch-up": { type: "string" }, disabled: { type: "boolean" } } });
      const name = positionals.join(" ");
      if (!name || !values.cron || !values.role || !values.task) { console.error("usage: questline recurring add <name> --cron <expr> --role <role> --task <task>"); return 2; }
      const r = store.createRecurring({ project: cwd, name, cron: values.cron, role: values.role, task: values.task, timezone: values.timezone, catchUp: values["catch-up"] as never, enabled: !values.disabled });
      console.log(`recurring ${r.id} · next ${new Date(r.nextRunAt).toISOString()}`);
      return 0;
    }
    if (sub === "list") { for (const r of store.listRecurring(cwd)) console.log(`${r.id} · ${r.enabled ? "on" : "off"} · ${r.cron} · next ${new Date(r.nextRunAt).toISOString()} · ${r.name}`); return 0; }
    if (sub === "pause" || sub === "resume") { const r = store.setRecurringEnabled(args[0]!, cwd, sub === "resume"); console.log(r ? `${r.id} ${r.enabled ? "enabled" : "disabled"}` : "not found"); return r ? 0 : 1; }
    if (sub === "remove") { const ok = store.deleteRecurring(args[0]!, cwd); console.log(ok ? "removed" : "not found"); return ok ? 0 : 1; }
    console.error("usage: questline recurring add|list|pause|resume|remove"); return 2;
  } finally { store.close(); }
}

async function handleArchive(command: string, rest: string[], cfg: QuestlineConfig, cwd: string): Promise<number> {
  const store = openStore(cfg);
  try {
    if (command === "export") {
      const { values, positionals } = parseArgs({ args: rest, allowPositionals: true, options: { "all-projects": { type: "boolean" } } });
      const archive = store.exportArchive(values["all-projects"] ? {} : { project: cwd });
      const text = JSON.stringify(archive, null, 2);
      if (positionals[0]) { writeFileSync(positionals[0], text); console.log(`exported ${archive.quests.length} quests to ${positionals[0]}`); }
      else console.log(text);
      return 0;
    }
    if (command === "import") {
      const file = rest[0];
      if (!file) { console.error("usage: questline import <file>"); return 2; }
      const archive = JSON.parse(readFileSync(file, "utf8"));
      const result = store.importArchive(archive, { projectMap: Object.fromEntries((archive.projects ?? []).map((p: string) => [p, cwd])), conflict: "remap" });
      console.log(JSON.stringify(result, null, 2));
      return 0;
    }
    if (command === "backup") {
      const { values, positionals } = parseArgs({ args: rest, allowPositionals: true, options: { force: { type: "boolean" } } });
      const dest = positionals[0];
      if (!dest) { console.error("usage: questline backup <sqlite-file> [--force]"); return 2; }
      console.log(`backup: ${store.backup(dest, { overwrite: values.force })}`);
      return 0;
    }
    return 2;
  } finally { store.close(); }
}
