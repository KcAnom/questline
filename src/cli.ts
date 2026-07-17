/**
 * questline CLI: a durable, crash-safe work queue for CLI coding agents.
 *
 *   add · list · show · run · daemon · dash · cancel · retry · consume ·
 *   events · runs · health
 */
import { parseArgs } from "node:util";
import { resolve } from "node:path";
import { randomUUID } from "node:crypto";
import { executorFor, loadConfig, type QuestforgeConfig } from "./config.ts";
import { executeQuest } from "./executor.ts";
import { ANSI_THEME, dashboard, questTableText } from "./dashboard.ts";
import { runTui } from "./tui.ts";
import { QuestRuntime, type ClaimedQuest } from "./runtime.ts";
import { QuestScheduler } from "./scheduler.ts";
import { QuestStore } from "./store.ts";
import { fmtDuration, fmtUsd } from "./fmt.ts";

const VERSION = "0.1.1";

const HELP = `questline ${VERSION} — durable quest queue for CLI coding agents

Usage: questline <command> [args]

  add <role> <task>      Queue work  (--name, --priority N, --context TEXT,
                         --depends id1,id2, --max-attempts N, --not-before ISO,
                         --dedupe-key KEY, --retain)
  list                   Show quests for this project (--json, --limit N)
  show <id>              Full detail: task, attempts, telemetry, events, result
  run [id]               Claim (next eligible, or a specific id) and execute now
  daemon                 Run the scheduler: drains the queue continuously
  dash                   Interactive TUI dashboard
  cancel <id>            Cancel queued/interrupted work
  retry <id>             Requeue failed/cancelled/interrupted work (+1 attempt)
  consume <id>           Mark a retained result consumed (frees its dedupe key)
  events <id>            Journal for one quest
  runs <id>              Per-attempt telemetry history
  health                 DB integrity and queue counts

Executor (config: ~/.questline/config.json or <project>/.questline.json):
  default: ["claude", "-p", "{task}"] — any print-mode CLI agent works, e.g.
  {"executor": {"command": ["pi", "--mode", "text", "{task}"]}}
  Per-role: {"executors": {"build": {"command": [...]}}}
  Share pi's queue: {"dbPath": "~/.pi/agent/fairy-tales-quests.sqlite"}
`;

function openStore(cfg: QuestforgeConfig): QuestStore {
  return new QuestStore({
    path: cfg.dbPath,
    maxHistory: cfg.maxHistory,
    leaseTtlMs: cfg.leaseTtlMs,
    maxAttempts: cfg.maxAttempts,
    backoffBaseMs: cfg.backoffBaseMs,
  });
}

async function dispatchClaimed(runtime: QuestRuntime, cfg: QuestforgeConfig, claimed: ClaimedQuest, log: (s: string) => void): Promise<boolean> {
  const { quest, lease } = claimed;
  const executor = executorFor(cfg, quest.role);
  const agentRunId = `exec-${randomUUID().slice(0, 8)}`;
  runtime.attachRun(lease, agentRunId);
  runtime.updateTelemetry(lease, { model: executor.command[0], lastActivity: "started" });
  log(`▶ ${quest.id} ${quest.name} (${quest.role}) → ${executor.command[0]}`);
  const result = await executeQuest(quest, executor);
  runtime.updateTelemetry(lease, { lastActivity: result.timedOut ? "timed out" : `exit ${result.exitCode}` });
  if (result.ok) {
    runtime.complete(lease, agentRunId, result.output);
    log(`✓ ${quest.id} done in ${fmtDuration(result.durationMs)}`);
  } else {
    runtime.fail(lease, result.timedOut ? `timed out after ${fmtDuration(result.durationMs)}\n${result.output}` : result.output, agentRunId);
    log(`✗ ${quest.id} failed (exit ${result.exitCode}${result.timedOut ? ", timeout" : ""})`);
  }
  return result.ok;
}

export async function main(argv: string[]): Promise<number> {
  const [command, ...rest] = argv;
  const cwd = process.cwd();
  const cfg = loadConfig(cwd);

  if (!command || command === "help" || command === "--help") { process.stdout.write(HELP); return 0; }
  if (command === "--version" || command === "version") { console.log(VERSION); return 0; }

  if (command === "add") {
    const { values, positionals } = parseArgs({
      args: rest,
      allowPositionals: true,
      options: {
        name: { type: "string" }, priority: { type: "string" }, context: { type: "string" },
        depends: { type: "string" }, "max-attempts": { type: "string" }, "not-before": { type: "string" },
        "dedupe-key": { type: "string" }, retain: { type: "boolean" },
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
        priority: values.priority ? Number(values.priority) : undefined,
        dependsOn: values.depends ? values.depends.split(",").map((s) => s.trim()).filter(Boolean) : undefined,
        maxAttempts: values["max-attempts"] ? Number(values["max-attempts"]) : undefined,
        scheduledAt: notBefore,
        dedupeKey: values["dedupe-key"],
        retainUntilConsumed: values.retain,
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
      const rows = store.list(cwd, values.limit ? Number(values.limit) : 30);
      if (values.json) console.log(JSON.stringify(rows, null, 2));
      else console.log(rows.length ? questTableText(rows) : "No quests for this project. Add one: questline add <role> \"task\"");
      return 0;
    } finally { store.close(); }
  }

  if (command === "show" || command === "events" || command === "runs") {
    const id = rest[0];
    if (!id) { console.error(`usage: questline ${command} <id>`); return 2; }
    const store = openStore(cfg);
    try {
      const q = store.get(id);
      if (!q || q.project !== resolve(cwd)) { console.error(`no quest ${id} in this project`); return 1; }
      if (command === "events") {
        for (const e of store.events(id)) console.log(`${new Date(e.at).toISOString()} · ${e.event} · ${JSON.stringify(e.data)}`);
        return 0;
      }
      if (command === "runs") {
        for (const r of store.runs(id)) console.log(`attempt ${r.attempt} · ${r.outcome ?? "running"} · ${r.model ?? "?"} · ${fmtUsd(r.costUsd)} · ${r.finishedAt ? fmtDuration(r.finishedAt - r.startedAt) : "…"}${r.error ? ` · ${r.error.slice(0, 120)}` : ""}`);
        return 0;
      }
      console.log(questTableText([q]));
      console.log(`\ntask: ${q.task}`);
      if (q.context) console.log(`context: ${q.context}`);
      if (q.dependsOn.length) console.log(`depends on: ${q.dependsOn.join(", ")}`);
      if (q.result) console.log(`\nresult:\n${q.result}`);
      if (q.error) console.log(`\nerror:\n${q.error}`);
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

  if (command === "health") {
    const store = openStore(cfg);
    try {
      const h = store.health();
      console.log(`db: ${h.path}`);
      console.log(`integrity: ${h.integrity} · queued ${h.queued} · running ${h.running} · interrupted ${h.interrupted} · expired leases ${h.expiredLeases}`);
      return h.integrity === "ok" ? 0 : 1;
    } finally { store.close(); }
  }

  if (command === "run") {
    const id = rest.find((a) => !a.startsWith("-"));
    const store = openStore(cfg);
    const runtime = new QuestRuntime({ store, ownerSession: `cli-${randomUUID().slice(0, 8)}`, heartbeatMs: cfg.heartbeatMs });
    try {
      const claimed = id ? runtime.claimById(id, cwd) : runtime.claimNext(cwd);
      if (!claimed) {
        console.error(id
          ? `cannot claim ${id} — blocked on dependencies, scheduled later, in retry backoff, already running, or finished`
          : "nothing claimable (queue empty, or work is scheduled later / in backoff / dependency-blocked)");
        return 1;
      }
      const ok = await dispatchClaimed(runtime, cfg, claimed, (s) => console.log(s));
      const final = store.get(claimed.quest.id);
      if (final?.result) console.log(`\n${final.result}`);
      else if (final?.error) console.error(`\n${final.error}`);
      return ok ? 0 : 1;
    } finally { runtime.shutdown(cwd); store.close(); }
  }

  if (command === "daemon") {
    const store = openStore(cfg);
    const runtime = new QuestRuntime({ store, ownerSession: `daemon-${randomUUID().slice(0, 8)}`, heartbeatMs: cfg.heartbeatMs });
    const scheduler = new QuestScheduler({
      runtime,
      project: cwd,
      pollMs: cfg.scheduler.pollMs,
      maxConcurrent: cfg.scheduler.maxConcurrent,
      unrefTimer: false, // the daemon IS the process — the timer must hold it open
      dispatch: (claimed) => dispatchClaimed(runtime, cfg, claimed, (s) => console.log(`[${new Date().toISOString()}] ${s}`)),
      onError: (err) => console.error(`[scheduler] ${String(err)}`),
    });
    console.log(`questline daemon · project ${cwd} · concurrency ${cfg.scheduler.maxConcurrent} · poll ${cfg.scheduler.pollMs}ms · Ctrl-C to stop`);
    scheduler.start();
    await new Promise<void>((resolveWait) => {
      const stop = () => {
        console.log("\nstopping: releasing scheduler lease and interrupting in-flight work…");
        scheduler.stop();
        runtime.shutdown(cwd);
        store.close();
        resolveWait();
      };
      process.once("SIGINT", stop);
      process.once("SIGTERM", stop);
    });
    return 0;
  }

  if (command === "dash" || command === "dashboard") {
    const store = openStore(cfg);
    const runtime = new QuestRuntime({ store, ownerSession: `dash-${randomUUID().slice(0, 8)}`, heartbeatMs: cfg.heartbeatMs });
    const inFlight = new Set<string>();
    await new Promise<void>((resolveQuit) => {
      let stopHost: (() => void) | undefined;
      const component = dashboard({
        store,
        project: cwd,
        theme: ANSI_THEME,
        actions: {
          run: (qid) => {
            const claimed = runtime.claimById(qid, cwd);
            if (!claimed) return `cannot claim ${qid} (blocked, scheduled later, in backoff, or running)`;
            inFlight.add(qid);
            void dispatchClaimed(runtime, cfg, claimed, () => { /* TUI repaints on tick */ })
              .catch(() => { /* recorded on the quest */ })
              .finally(() => inFlight.delete(qid));
            return undefined;
          },
          retry: (qid) => (store.requeue(qid, cwd) ? undefined : `cannot requeue ${qid}`),
          cancel: (qid) => (store.cancel(qid, cwd) ? undefined : `cannot cancel ${qid}`),
        },
        onQuit: () => stopHost?.(),
      });
      const handle = runTui(component, {
        onQuit: () => {
          if (inFlight.size) console.log(`note: ${inFlight.size} in-flight quest(s) interrupted; they are reclaimable (questline run / daemon).`);
          runtime.shutdown(cwd);
          store.close();
          resolveQuit();
        },
      });
      stopHost = handle.stop;
    });
    return 0;
  }

  console.error(`unknown command "${command}" — questline help`);
  return 2;
}
