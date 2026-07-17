import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "vitest";
import { QuestStore } from "../store.ts";
import { QuestRuntime } from "../runtime.ts";

function fixture(opts: { leaseTtlMs?: number; heartbeatMs?: number } = {}) {
  const dir = mkdtempSync(join(tmpdir(), "fairy-runtime-"));
  const store = new QuestStore({ path: join(dir, "quests.sqlite"), maxHistory: 20, leaseTtlMs: opts.leaseTtlMs });
  const runtime = new QuestRuntime({ store, ownerSession: "session-1", heartbeatMs: opts.heartbeatMs });
  return { store, runtime, close: () => { store.close(); rmSync(dir, { recursive: true, force: true }); } };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

test("runs a claimed quest to completion with telemetry", async () => {
  const f = fixture();
  try {
    const q = f.store.enqueue({ project: "/tmp/project", role: "build", task: "Do it" });
    const claimed = f.runtime.claimNext("/tmp/project")!;
    assert.equal(claimed.quest.id, q.id);
    assert.equal(f.runtime.owns(q.id), true);
    const outcome = await f.runtime.run(claimed, async (_quest, report) => {
      assert.equal(f.runtime.attachRun(claimed.lease, "a1"), true);
      report({ model: "prov/m", turns: 2, costUsd: 0.01 });
      return { agentRunId: "a1", ok: true, text: "all done" };
    });
    assert.equal(outcome.ok, true);
    assert.equal(f.runtime.owns(q.id), false);
    assert.equal(f.store.get(q.id)?.state, "done");
    assert.equal(f.store.runs(q.id)[0]?.model, "prov/m");
  } finally { f.close(); }
});

test("a thrown dispatch fails the quest under the lease", async () => {
  const f = fixture();
  try {
    const q = f.store.enqueue({ project: "/tmp/project", role: "build", task: "Explode" });
    const claimed = f.runtime.claimNext("/tmp/project")!;
    await assert.rejects(f.runtime.run(claimed, async () => { throw new Error("kaboom"); }));
    assert.equal(f.store.get(q.id)?.state, "failed");
    assert.match(f.store.get(q.id)?.error ?? "", /kaboom/);
  } finally { f.close(); }
});

test("heartbeats keep a slow quest owned past the lease TTL", async () => {
  const f = fixture({ leaseTtlMs: 200, heartbeatMs: 40 });
  try {
    f.store.enqueue({ project: "/tmp/project", role: "build", task: "Slow" });
    const claimed = f.runtime.claimNext("/tmp/project")!;
    await sleep(400); // several TTLs — heartbeats must have renewed the lease
    assert.equal(f.store.health().expiredLeases, 0);
    assert.equal(f.store.claimNext("/tmp/project", "session-2"), undefined);
    f.runtime.complete(claimed.lease, "a?", "late"); // wrong run id → fenced, but heartbeat stops
  } finally { f.close(); }
});

test("a lost lease fires the handler and stops local ownership", async () => {
  const f = fixture({ leaseTtlMs: 1, heartbeatMs: 50 });
  try {
    const q = f.store.enqueue({ project: "/tmp/project", role: "build", task: "Contested" });
    const claimed = f.runtime.claimNext("/tmp/project")!;
    let aborted = false;
    f.runtime.setLeaseLostHandler(q.id, () => { aborted = true; });
    await sleep(10);
    const stolen = f.store.claimNext("/tmp/project", "session-2"); // lease expired → reclaim
    assert.equal(stolen?.quest.ownerSession, "session-2");
    await sleep(80); // next heartbeat notices the stolen lease
    assert.equal(aborted, true);
    assert.equal(f.runtime.owns(q.id), false);
    assert.equal(f.runtime.complete(claimed.lease, "a1", "zombie"), false);
  } finally { f.close(); }
});

test("shutdown releases heartbeats and recovers running work as interrupted", () => {
  const f = fixture();
  try {
    const q = f.store.enqueue({ project: "/tmp/project", role: "build", task: "Ongoing" });
    f.runtime.claimNext("/tmp/project");
    f.runtime.shutdown("/tmp/project");
    assert.equal(f.store.get(q.id)?.state, "interrupted");
    assert.equal(f.store.runs(q.id)[0]?.outcome, "interrupted");
    assert.equal(f.runtime.claimNext("/tmp/project"), undefined); // closed runtime claims nothing
  } finally { f.close(); }
});
