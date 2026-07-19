import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { test } from "vitest";
import { loadConfig } from "../config.ts";
import { executeQuest } from "../executor.ts";
import { QuestStore } from "../store.ts";
import type { QuestRecord } from "../store.ts";

const BIN = resolve(import.meta.dirname, "..", "..", "bin", "questline.js");
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function fixture() {
  const dir = mkdtempSync(join(tmpdir(), "qf-adv-"));
  const store = new QuestStore({ path: join(dir, "quests.sqlite"), maxHistory: 2, leaseTtlMs: 100 });
  return { dir, store, close: () => { store.close(); rmSync(dir, { recursive: true, force: true }); } };
}

function quest(project: string, over: Partial<QuestRecord> = {}): QuestRecord {
  return {
    id: "q-test", project, role: "build", name: "t", task: "say hello",
    state: "running", attempts: 1, maxAttempts: 1, backoffBaseMs: 0, priority: 0,
    scheduledAt: 0, retainUntilConsumed: false, leaseVersion: 1, dependsOn: [],
    createdAt: 0, updatedAt: 0, ...over,
  };
}

test("rejects invalid, cross-project, self, and cyclic dependencies", () => {
  const f = fixture();
  try {
    assert.throws(() => f.store.enqueue({ project: "/tmp/p", role: "build", task: "bad", dependsOn: ["q-nope"] }), /does not exist/);
    const a = f.store.enqueue({ project: "/tmp/p", role: "build", task: "a" });
    const other = f.store.enqueue({ project: "/tmp/other", role: "build", task: "other" });
    assert.throws(() => f.store.enqueue({ project: "/tmp/p", role: "build", task: "bad", dependsOn: [other.id] }), /different project/);
    const b = f.store.enqueue({ project: "/tmp/p", role: "build", task: "b", dependsOn: [a.id] });
    assert.throws(() => f.store.addDependency(b.id, b.id, "/tmp/p"), /cannot depend on itself/);
    assert.throws(() => f.store.addDependency(a.id, b.id, "/tmp/p"), /dependency cycle/);
  } finally { f.close(); }
});

test("dependency failure cascades immediately and requeue revives dependency-failed descendants", () => {
  const f = fixture();
  try {
    const root = f.store.enqueue({ project: "/tmp/p", role: "build", task: "root" });
    const child = f.store.enqueue({ project: "/tmp/p", role: "build", task: "child", dependsOn: [root.id] });
    const grand = f.store.enqueue({ project: "/tmp/p", role: "build", task: "grand", dependsOn: [child.id] });
    assert.equal(f.store.cancel(root.id, "/tmp/p"), true);
    assert.equal(f.store.get(child.id)?.state, "failed");
    assert.equal(f.store.get(child.id)?.failureKind, "dependency");
    assert.equal(f.store.get(grand.id)?.state, "failed");
    assert.equal(f.store.requeue(root.id, "/tmp/p"), true);
    assert.equal(f.store.get(child.id)?.state, "queued");
    assert.equal(f.store.get(grand.id)?.state, "queued");
  } finally { f.close(); }
});

test("project-aware pruning preserves each project and retained results", () => {
  const f = fixture();
  try {
    const done = (project: string, task: string, retain = false) => {
      const q = f.store.enqueue({ project, role: "build", task, retainUntilConsumed: retain, dedupeKey: retain ? `${project}/keep` : undefined });
      const c = f.store.claimNext(project, "s")!;
      f.store.attachRun(c.lease, `a-${task}`);
      f.store.complete(c.lease, `a-${task}`, task);
      return q.id;
    };
    const retained = done("/tmp/a", "keep", true);
    for (let i = 0; i < 5; i++) done("/tmp/a", `a${i}`);
    for (let i = 0; i < 3; i++) done("/tmp/b", `b${i}`);
    assert.equal(f.store.get(retained)?.state, "done");
    assert.ok(f.store.list("/tmp/a", 20).filter((q) => q.state === "done" && !q.retainUntilConsumed).length <= 2);
    assert.ok(f.store.list("/tmp/b", 20).filter((q) => q.state === "done").length <= 2);
  } finally { f.close(); }
});

test("config supports JSONC and rejects invalid values", () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-cfg-"));
  try {
    writeFileSync(join(dir, ".questline.json"), `{
      // comment
      "dbPath": "${join(dir, "db.sqlite")}",
      "executor": { "command": ["sh", "-c", "printf ok"], },
    }`);
    assert.equal(loadConfig(dir).executor.command[0], "sh");
    writeFileSync(join(dir, ".questline.json"), `{ "heartbeatMs": 1000, "leaseTtlMs": 100 }`);
    assert.throws(() => loadConfig(dir), /heartbeatMs/);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("large output is saved as an artifact with bounded preview", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-art-"));
  try {
    const q = quest(dir);
    const r = await executeQuest(q, { command: ["sh", "-c", "python3 - <<'PY'\nprint('x'*20000)\nPY"], maxInlineOutputBytes: 4096, outputPreviewBytes: 1024 }, { artifactDir: join(dir, "artifacts"), agentRunId: "a1" });
    assert.equal(r.ok, true);
    assert.equal(r.artifacts.length, 1);
    assert.ok(r.output.includes("omitted"));
    assert.ok(existsSync(r.artifacts[0]!.path));
    assert.ok(readFileSync(r.artifacts[0]!.path, "utf8").length >= 20000);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("structured JSONL protocol captures telemetry and result", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-jsonl-"));
  try {
    const q = quest(dir);
    const seen: unknown[] = [];
    const script = `printf '%s\n' '{"protocol":"questline/1","type":"output","text":"hello"}' '{"protocol":"questline/1","type":"telemetry","provider":"p","model":"m","turns":2,"tokens":5,"costUsd":0.01}' '{"protocol":"questline/1","type":"result","ok":true}'`;
    const r = await executeQuest(q, { command: ["sh", "-c", script], protocol: "questline-jsonl" }, { artifactDir: join(dir, "artifacts"), agentRunId: "a1", onTelemetry: (t) => seen.push(t) });
    assert.equal(r.ok, true);
    assert.equal(r.output, "hello");
    assert.equal(r.telemetry?.model, "m");
    assert.equal(seen.length, 1);

    const bad = await executeQuest(q, { command: ["sh", "-c", "printf '%s\\n' '{bad json'"], protocol: "questline-jsonl" }, { artifactDir: join(dir, "artifacts"), agentRunId: "a2" });
    assert.equal(bad.ok, false);
    assert.equal(bad.status, "protocol-error");
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("lease loss aborts an executing child", { timeout: 15_000 }, async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-lease-loss-"));
  try {
    writeFileSync(join(dir, ".questline.json"), JSON.stringify({
      dbPath: join(dir, "quests.sqlite"),
      leaseTtlMs: 100,
      heartbeatMs: 50,
      executor: { command: ["sh", "-c", "trap 'echo term > term.flag; exit 0' TERM; while true; do sleep 1; done"], terminateGraceMs: 1000 },
    }));
    const add = execFileSync(process.execPath, [BIN, "add", "build", "slow"], { cwd: dir, encoding: "utf8" });
    const id = /queued (q-\w+)/.exec(add)![1];
    const child = spawn(process.execPath, [BIN, "run", id], { cwd: dir, stdio: ["ignore", "pipe", "pipe"] });
    await sleep(150);
    const store = new QuestStore({ path: join(dir, "quests.sqlite"), maxHistory: 20, leaseTtlMs: 100 });
    const q = store.get(id);
    const owner = q?.ownerSession;
    assert.ok(owner);
    store.recoverOwned(q!.project, owner);
    store.close();
    await new Promise((resolveDone) => child.once("close", resolveDone));
    assert.ok(existsSync(join(dir, "term.flag")));
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("SDK exports are available from package entry", async () => {
  const mod = await import("../index.ts");
  assert.equal(typeof mod.QuestStore, "function");
  assert.equal(typeof mod.QuestRuntime, "function");
  assert.equal(typeof mod.QuestScheduler, "function");
  assert.equal(typeof mod.loadConfig, "function");
});
