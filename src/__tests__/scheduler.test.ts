import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "vitest";
import { QuestStore } from "../store.ts";
import { QuestRuntime, type ClaimedQuest } from "../runtime.ts";
import { QuestScheduler, type QuestSchedulerOptions } from "../scheduler.ts";

const PROJECT = "/tmp/sched-project";
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function fixture(opts: Partial<QuestSchedulerOptions> & { owner?: string } = {}) {
  const dir = mkdtempSync(join(tmpdir(), "fairy-sched-"));
  const store = new QuestStore({ path: join(dir, "quests.sqlite"), maxHistory: 50 });
  const runtime = new QuestRuntime({ store, ownerSession: opts.owner ?? "session-1" });
  const dispatched: string[] = [];
  const scheduler = new QuestScheduler({
    runtime,
    project: PROJECT,
    pollMs: 25,
    maxConcurrent: 2,
    dispatch: async (claimed: ClaimedQuest) => {
      dispatched.push(claimed.quest.id);
      runtime.attachRun(claimed.lease, `run-${claimed.quest.id}`);
      runtime.complete(claimed.lease, `run-${claimed.quest.id}`, "ok");
    },
    ...opts,
  });
  return {
    store, runtime, scheduler, dispatched,
    close: () => { scheduler.stop(); store.close(); rmSync(dir, { recursive: true, force: true }); },
  };
}

test("drains queued quests in priority order and settles them", async () => {
  const f = fixture();
  try {
    const low = f.store.enqueue({ project: PROJECT, role: "build", task: "low" });
    const high = f.store.enqueue({ project: PROJECT, role: "build", task: "high", priority: 9 });
    f.scheduler.start();
    await sleep(120);
    assert.deepEqual(f.dispatched.slice(0, 2).sort(), [high.id, low.id].sort());
    assert.equal(f.dispatched[0], high.id);
    assert.equal(f.store.get(low.id)?.state, "done");
    assert.equal(f.store.get(high.id)?.state, "done");
    assert.equal(f.scheduler.status().leaseHeld, true);
  } finally { f.close(); }
});

test("respects the concurrency cap for slow work", async () => {
  let active = 0;
  let peak = 0;
  const f = fixture({
    dispatch: async (claimed) => {
      active++; peak = Math.max(peak, active);
      await sleep(60);
      active--;
      f.runtime.attachRun(claimed.lease, "a");
      f.runtime.complete(claimed.lease, "a", "ok");
    },
  });
  try {
    for (let i = 0; i < 5; i++) f.store.enqueue({ project: PROJECT, role: "build", task: `t${i}` });
    f.scheduler.start();
    await sleep(400);
    assert.equal(f.store.list(PROJECT).filter((q) => q.state === "done").length, 5);
    assert.ok(peak <= 2, `peak concurrency ${peak} exceeded cap`);
  } finally { f.close(); }
});

test("waits for future-scheduled quests and runs unblocked dependents", async () => {
  const f = fixture();
  try {
    const dep = f.store.enqueue({ project: PROJECT, role: "build", task: "dep" });
    const child = f.store.enqueue({ project: PROJECT, role: "build", task: "child", dependsOn: [dep.id] });
    const future = f.store.enqueue({ project: PROJECT, role: "build", task: "future", scheduledAt: Date.now() + 150 });
    f.scheduler.start();
    await sleep(100);
    assert.equal(f.store.get(dep.id)?.state, "done");
    await sleep(300);
    assert.equal(f.store.get(child.id)?.state, "done");
    assert.equal(f.store.get(future.id)?.state, "done");
    // Dependency ran strictly before its dependent; future quest never ran early.
    assert.ok(f.dispatched.indexOf(dep.id) < f.dispatched.indexOf(child.id));
  } finally { f.close(); }
});

test("pause hook stops new claims and reports the reason", async () => {
  let paused: string | undefined = "cost cap reached";
  const f = fixture({ isPaused: () => paused });
  try {
    const q = f.store.enqueue({ project: PROJECT, role: "build", task: "held" });
    f.scheduler.start();
    await sleep(90);
    assert.equal(f.store.get(q.id)?.state, "queued");
    assert.equal(f.scheduler.status().pausedReason, "cost cap reached");
    paused = undefined;
    await sleep(90);
    assert.equal(f.store.get(q.id)?.state, "done");
  } finally { f.close(); }
});

test("only one scheduler per project claims work (scheduler lease)", async () => {
  const f1 = fixture({ owner: "session-A" });
  // Second scheduler on the same database file? Use a separate store handle on
  // the same path to model a second session.
  const store2 = new QuestStore({ path: f1.store.path, maxHistory: 50 });
  const runtime2 = new QuestRuntime({ store: store2, ownerSession: "session-B" });
  const dispatched2: string[] = [];
  const sched2 = new QuestScheduler({
    runtime: runtime2, project: PROJECT, pollMs: 25,
    dispatch: async (claimed) => {
      dispatched2.push(claimed.quest.id);
      runtime2.attachRun(claimed.lease, "b");
      runtime2.complete(claimed.lease, "b", "ok");
    },
  });
  try {
    f1.scheduler.start();
    await sleep(40); // session-A takes the scheduler lease
    sched2.start();
    for (let i = 0; i < 3; i++) f1.store.enqueue({ project: PROJECT, role: "build", task: `t${i}` });
    await sleep(150);
    assert.equal(dispatched2.length, 0, "standby scheduler must not claim while the lease is held");
    assert.equal(sched2.status().leaseHeld, false);
    assert.equal(f1.dispatched.length, 3);
    // When the holder stops, the standby takes over.
    f1.scheduler.stop();
    const late = f1.store.enqueue({ project: PROJECT, role: "build", task: "late" });
    await sleep(150);
    assert.equal(f1.store.get(late.id)?.state, "done");
    assert.deepEqual(dispatched2, [late.id]);
  } finally {
    sched2.stop();
    store2.close();
    f1.close();
  }
});

test("stop() releases the scheduler lease immediately", async () => {
  const f = fixture();
  try {
    f.scheduler.start();
    await sleep(40);
    assert.equal(f.scheduler.status().leaseHeld, true);
    f.scheduler.stop();
    // Another owner can acquire at once — no TTL wait needed.
    assert.equal(f.store.acquireSchedulerLease(PROJECT, "someone-else", 1000), true);
  } finally { f.close(); }
});

test("failed dispatch surfaces through onError and bounded retries still apply", async () => {
  const errors: unknown[] = [];
  const f = fixture({
    onError: (err) => errors.push(err),
    dispatch: async (claimed) => {
      f.runtime.attachRun(claimed.lease, "a");
      f.runtime.fail(claimed.lease, "worker exploded", "a");
      throw new Error("worker exploded");
    },
  });
  try {
    const q = f.store.enqueue({ project: PROJECT, role: "build", task: "flaky", maxAttempts: 2, backoffBaseMs: 0 });
    f.scheduler.start();
    await sleep(200);
    assert.equal(f.store.get(q.id)?.state, "failed");
    assert.equal(f.store.get(q.id)?.attempts, 2);
    assert.ok(errors.length >= 2);
  } finally { f.close(); }
});
