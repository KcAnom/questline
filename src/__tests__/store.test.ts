import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "vitest";
import { QuestStore, type QuestStoreOptions } from "../store.ts";

function fixture(overrides: Partial<QuestStoreOptions> = {}) {
  const dir = mkdtempSync(join(tmpdir(), "fairy-quests-"));
  const path = join(dir, "quests.sqlite");
  const store = new QuestStore({ path, maxHistory: 20, ...overrides });
  return { store, path, dir, close: () => { store.close(); rmSync(dir, { recursive: true, force: true }); } };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

test("enqueues, claims, journals, and completes a quest", () => {
  const f = fixture();
  try {
    const queued = f.store.enqueue({ project: "/tmp/project", role: "build", name: "Build it", task: "Implement the feature" });
    assert.equal(queued.state, "queued");
    assert.equal(queued.maxAttempts, 1);
    const claimed = f.store.claimNext("/tmp/project", "session-1");
    assert.equal(claimed?.quest.id, queued.id);
    assert.equal(claimed?.quest.state, "running");
    assert.equal(claimed?.quest.attempts, 1);
    assert.equal(claimed?.lease.version, 1);
    assert.ok(claimed?.quest.leaseExpiresAt && claimed.quest.leaseExpiresAt > Date.now());
    assert.equal(f.store.attachRun(claimed!.lease, "a1"), true);
    assert.equal(f.store.complete(claimed!.lease, "a1", "done"), true);
    const complete = f.store.get(queued.id);
    assert.equal(complete?.state, "done");
    assert.equal(complete?.result, "done");
    assert.deepEqual(f.store.events(queued.id).map((e) => e.event), ["completed", "agent_started", "claimed", "enqueued"]);
  } finally { f.close(); }
});

test("recovers running work as interrupted and claims it again", () => {
  const f = fixture();
  try {
    const queued = f.store.enqueue({ project: "/tmp/project", role: "explore", task: "Map it" });
    assert.equal(f.store.claimNext("/tmp/project", "session-1")?.quest.state, "running");
    assert.equal(f.store.recoverOwned("/tmp/project", "another-session"), 0);
    assert.equal(f.store.recoverOwned("/tmp/project", "session-1"), 1);
    assert.equal(f.store.get(queued.id)?.state, "interrupted");
    const retried = f.store.claimNext("/tmp/project", "session-2");
    assert.equal(retried?.quest.id, queued.id);
    assert.equal(retried?.quest.attempts, 2);
    assert.equal(retried?.lease.version, 2);
  } finally { f.close(); }
});

test("rejects stale callbacks: wrong owner, wrong run, or stale lease version", () => {
  const f = fixture();
  try {
    const q = f.store.enqueue({ project: "/tmp/project", role: "build", task: "Build" });
    const claimed = f.store.claimNext("/tmp/project", "session-1")!;
    assert.equal(f.store.attachRun(claimed.lease, "a1"), true);
    assert.equal(f.store.complete({ ...claimed.lease, ownerSession: "session-2" }, "a1", "stale"), false);
    assert.equal(f.store.complete(claimed.lease, "a2", "stale"), false);
    assert.equal(f.store.complete({ ...claimed.lease, version: claimed.lease.version - 1 }, "a1", "stale"), false);
    assert.equal(f.store.fail(claimed.lease, "stale", "a2"), false);
    assert.equal(f.store.get(q.id)?.state, "running");
    assert.equal(f.store.complete(claimed.lease, "a1", "fresh"), true);
    assert.equal(f.store.fail(claimed.lease, "late", "a1"), false);
    assert.equal(f.store.get(q.id)?.result, "fresh");
  } finally { f.close(); }
});

test("cancels only queued work and isolates projects", () => {
  const f = fixture();
  try {
    const first = f.store.enqueue({ project: "/tmp/one", role: "plan", task: "Plan" });
    f.store.enqueue({ project: "/tmp/two", role: "plan", task: "Other" });
    assert.equal(f.store.list("/tmp/one").length, 1);
    assert.equal(f.store.cancel(first.id, "/tmp/two"), false);
    assert.equal(f.store.cancel(first.id, "/tmp/one"), true);
    assert.equal(f.store.cancel(first.id, "/tmp/one"), false);
    assert.equal(f.store.get(first.id)?.state, "cancelled");
    assert.equal(f.store.health().integrity, "ok");
  } finally { f.close(); }
});

test("migrates a v1 database in place and keeps its data claimable", () => {
  const dir = mkdtempSync(join(tmpdir(), "fairy-quests-v1-"));
  const path = join(dir, "quests.sqlite");
  try {
    const raw = new DatabaseSync(path);
    raw.exec(`
      CREATE TABLE quests (
        id TEXT PRIMARY KEY, project TEXT NOT NULL, role TEXT NOT NULL, name TEXT NOT NULL,
        task TEXT NOT NULL, context TEXT,
        state TEXT NOT NULL CHECK(state IN ('queued','running','done','failed','cancelled','interrupted')),
        attempts INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
        started_at INTEGER, finished_at INTEGER, agent_run_id TEXT, owner_session TEXT, result TEXT, error TEXT
      );
      CREATE TABLE quest_events (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        quest_id TEXT NOT NULL REFERENCES quests(id) ON DELETE CASCADE,
        event TEXT NOT NULL, at INTEGER NOT NULL, data TEXT NOT NULL DEFAULT '{}'
      );
      PRAGMA user_version=1;
    `);
    raw.prepare(
      "INSERT INTO quests (id, project, role, name, task, state, attempts, created_at, updated_at) VALUES ('q-old', '/tmp/project', 'build', 'Old quest', 'Old task', 'queued', 0, 1, 1)",
    ).run();
    raw.close();

    const store = new QuestStore({ path, maxHistory: 20 });
    const migrated = store.get("q-old");
    assert.equal(migrated?.name, "Old quest");
    assert.equal(migrated?.maxAttempts, 1);
    assert.equal(migrated?.priority, 0);
    assert.equal(migrated?.leaseVersion, 0);
    const claimed = store.claimNext("/tmp/project", "session-1");
    assert.equal(claimed?.quest.id, "q-old");
    assert.equal(store.complete(claimed!.lease, "a1", "migrated fine"), false); // no attachRun yet
    assert.equal(store.attachRun(claimed!.lease, "a1"), true);
    assert.equal(store.complete(claimed!.lease, "a1", "migrated fine"), true);
    store.close();
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("expired leases are reclaimed by another session and the old lease is fenced out", async () => {
  const f = fixture({ leaseTtlMs: 1 });
  try {
    const q = f.store.enqueue({ project: "/tmp/project", role: "build", task: "Crashy" });
    const first = f.store.claimNext("/tmp/project", "session-1")!;
    f.store.attachRun(first.lease, "a1");
    await sleep(10);
    assert.equal(f.store.health().expiredLeases, 1);
    const second = f.store.claimNext("/tmp/project", "session-2");
    assert.equal(second?.quest.id, q.id);
    assert.equal(second?.quest.attempts, 2);
    assert.equal(second?.lease.version, 2);
    // The crashed worker resurfaces: its stale lease must not write anything.
    assert.equal(f.store.complete(first.lease, "a1", "zombie result"), false);
    assert.equal(f.store.heartbeat(first.lease), false);
    assert.equal(f.store.get(q.id)?.state, "running");
    assert.equal(f.store.get(q.id)?.ownerSession, "session-2");
  } finally { f.close(); }
});

test("heartbeat extends the lease and keeps the quest unclaimable", async () => {
  const f = fixture({ leaseTtlMs: 150 });
  try {
    f.store.enqueue({ project: "/tmp/project", role: "build", task: "Long haul" });
    const claimed = f.store.claimNext("/tmp/project", "session-1")!;
    await sleep(80);
    assert.equal(f.store.heartbeat(claimed.lease), true);
    await sleep(100); // past the original TTL, within the renewed one
    // Original TTL has passed but the heartbeat renewed it.
    assert.equal(f.store.claimNext("/tmp/project", "session-2"), undefined);
    assert.equal(f.store.health().expiredLeases, 0);
  } finally { f.close(); }
});

test("orders by priority then age, and honors scheduled time", () => {
  const f = fixture();
  try {
    const low = f.store.enqueue({ project: "/tmp/project", role: "build", task: "Low" });
    const high = f.store.enqueue({ project: "/tmp/project", role: "build", task: "High", priority: 5 });
    f.store.enqueue({ project: "/tmp/project", role: "build", task: "Future", priority: 99, scheduledAt: Date.now() + 60_000 });
    assert.equal(f.store.claimNext("/tmp/project", "s")?.quest.id, high.id);
    assert.equal(f.store.claimNext("/tmp/project", "s")?.quest.id, low.id);
    // Only the future quest remains — it must not run early.
    assert.equal(f.store.claimNext("/tmp/project", "s"), undefined);
  } finally { f.close(); }
});

test("dependencies gate claiming and failed dependencies cascade", () => {
  const f = fixture();
  try {
    const dep = f.store.enqueue({ project: "/tmp/project", role: "build", task: "First" });
    const child = f.store.enqueue({ project: "/tmp/project", role: "build", task: "Second", dependsOn: [dep.id] });
    assert.deepEqual(f.store.get(child.id)?.dependsOn, [dep.id]);

    const c1 = f.store.claimNext("/tmp/project", "s")!;
    assert.equal(c1.quest.id, dep.id);
    // Dependency is running, child must not be claimable.
    assert.equal(f.store.claimNext("/tmp/project", "s"), undefined);
    f.store.attachRun(c1.lease, "a1");
    f.store.complete(c1.lease, "a1", "ok");
    assert.equal(f.store.claimNext("/tmp/project", "s")?.quest.id, child.id);

    // A dependent of a failed quest is terminally failed on the next claim sweep.
    const dep2 = f.store.enqueue({ project: "/tmp/project", role: "build", task: "Doomed" });
    const child2 = f.store.enqueue({ project: "/tmp/project", role: "build", task: "Blocked", dependsOn: [dep2.id] });
    assert.equal(f.store.cancel(dep2.id, "/tmp/project"), true);
    f.store.claimNext("/tmp/project", "s");
    assert.equal(f.store.get(child2.id)?.state, "failed");
    assert.match(f.store.get(child2.id)?.error ?? "", /dependency q-.* cancelled/);
  } finally { f.close(); }
});

test("bounded retries: fail requeues with backoff until maxAttempts, then terminal", () => {
  const f = fixture();
  try {
    const q = f.store.enqueue({ project: "/tmp/project", role: "build", task: "Flaky", maxAttempts: 2, backoffBaseMs: 0 });
    const c1 = f.store.claimNext("/tmp/project", "s1")!;
    f.store.attachRun(c1.lease, "a1");
    assert.equal(f.store.fail(c1.lease, "boom", "a1"), true);
    const requeued = f.store.get(q.id)!;
    assert.equal(requeued.state, "queued");
    assert.ok(requeued.retryAt !== undefined);
    assert.equal(f.store.events(q.id)[0]?.event, "retry_scheduled");

    const c2 = f.store.claimNext("/tmp/project", "s1")!;
    assert.equal(c2.quest.attempts, 2);
    f.store.attachRun(c2.lease, "a2");
    assert.equal(f.store.fail(c2.lease, "boom again", "a2"), true);
    assert.equal(f.store.get(q.id)?.state, "failed");
    assert.equal(f.store.claimNext("/tmp/project", "s1"), undefined);
  } finally { f.close(); }
});

test("retry backoff delays the next claim", () => {
  const f = fixture();
  try {
    f.store.enqueue({ project: "/tmp/project", role: "build", task: "Flaky", maxAttempts: 3, backoffBaseMs: 60_000 });
    const c1 = f.store.claimNext("/tmp/project", "s1")!;
    f.store.fail(c1.lease, "boom");
    assert.equal(f.store.get(c1.quest.id)?.state, "queued");
    assert.equal(f.store.claimNext("/tmp/project", "s1"), undefined);
  } finally { f.close(); }
});

test("dedupeKey makes enqueue idempotent while active or retained-unconsumed", () => {
  const f = fixture();
  try {
    const first = f.store.enqueue({ project: "/tmp/project", role: "build", task: "Step 1", dedupeKey: "chain-1/step-1", retainUntilConsumed: true });
    const dup = f.store.enqueue({ project: "/tmp/project", role: "build", task: "Step 1 again", dedupeKey: "chain-1/step-1" });
    assert.equal(dup.id, first.id);
    assert.equal(f.store.events(first.id)[0]?.event, "deduped");

    const claimed = f.store.claimNext("/tmp/project", "s")!;
    f.store.attachRun(claimed.lease, "a1");
    f.store.complete(claimed.lease, "a1", "artifact");
    // Done but retained and unconsumed → still dedupes to the same quest.
    const dupDone = f.store.enqueue({ project: "/tmp/project", role: "build", task: "Step 1 again", dedupeKey: "chain-1/step-1" });
    assert.equal(dupDone.id, first.id);
    assert.equal(dupDone.result, "artifact");

    assert.equal(f.store.markConsumed(first.id, "/tmp/project"), true);
    const fresh = f.store.enqueue({ project: "/tmp/project", role: "build", task: "Step 1 rerun", dedupeKey: "chain-1/step-1" });
    assert.notEqual(fresh.id, first.id);
  } finally { f.close(); }
});

test("records durable per-attempt telemetry", () => {
  const f = fixture();
  try {
    const q = f.store.enqueue({ project: "/tmp/project", role: "build", task: "Measure me", maxAttempts: 2, backoffBaseMs: 0 });
    const c1 = f.store.claimNext("/tmp/project", "s1")!;
    f.store.attachRun(c1.lease, "a1");
    assert.equal(f.store.updateTelemetry(c1.lease, { model: "prov/model-x", tier: "worker", turns: 3, tokens: 1200, costUsd: 0.05, lastActivity: "editing" }), true);
    f.store.fail(c1.lease, "boom", "a1");
    // Stale lease can no longer write telemetry.
    assert.equal(f.store.updateTelemetry(c1.lease, { turns: 99 }), false);

    const c2 = f.store.claimNext("/tmp/project", "s2")!;
    f.store.attachRun(c2.lease, "a2");
    f.store.updateTelemetry(c2.lease, { model: "prov/model-y", turns: 7, tokens: 3400, costUsd: 0.6 });
    f.store.complete(c2.lease, "a2", "done");

    const runs = f.store.runs(q.id);
    assert.equal(runs.length, 2);
    assert.equal(runs[0].attempt, 2);
    assert.equal(runs[0].outcome, "done");
    assert.equal(runs[0].model, "prov/model-y");
    assert.equal(runs[0].turns, 7);
    assert.equal(runs[1].attempt, 1);
    assert.equal(runs[1].outcome, "failed");
    assert.equal(runs[1].model, "prov/model-x");
    assert.equal(runs[1].error, "boom");
    assert.ok(runs.every((r) => r.finishedAt !== undefined));
  } finally { f.close(); }
});

test("stores chain metadata and survives reopen", () => {
  const f = fixture();
  try {
    const q = f.store.enqueue({
      project: "/tmp/project", role: "build", task: "Chained",
      chain: { chain: "feature-ship", runId: "r1", phase: "implement", workId: "w3" },
    });
    f.store.close();
    const reopened = new QuestStore({ path: f.path, maxHistory: 20 });
    try {
      assert.deepEqual(reopened.get(q.id)?.chain, { chain: "feature-ship", runId: "r1", phase: "implement", workId: "w3" });
    } finally { reopened.close(); }
  } finally { rmSync(f.dir, { recursive: true, force: true }); }
});

test("release hands a claimed quest back without burning the retry budget", () => {
  const f = fixture();
  try {
    const q = f.store.enqueue({ project: "/tmp/project", role: "build", task: "Not yet" }); // maxAttempts 1
    const c1 = f.store.claimNext("/tmp/project", "s1")!;
    assert.equal(f.store.release({ ...c1.lease, version: 99 }), false); // fenced
    assert.equal(f.store.release(c1.lease), true);
    const back = f.store.get(q.id)!;
    assert.equal(back.state, "queued");
    assert.equal(back.maxAttempts, 2); // release granted the attempt back
    assert.equal(back.ownerSession, undefined);
    // Re-claimable immediately, and a later real failure still terminates.
    const c2 = f.store.claimNext("/tmp/project", "s2")!;
    assert.equal(c2.quest.attempts, 2);
    f.store.attachRun(c2.lease, "a1");
    assert.equal(f.store.fail(c2.lease, "real failure", "a1"), true);
    assert.equal(f.store.get(q.id)?.state, "failed");
  } finally { f.close(); }
});

test("prune never evicts retained-unconsumed results", () => {
  const f = fixture({ maxHistory: 20 });
  try {
    const keeper = f.store.enqueue({ project: "/tmp/project", role: "build", task: "Keep", dedupeKey: "keep", retainUntilConsumed: true });
    const c = f.store.claimNext("/tmp/project", "s")!;
    f.store.attachRun(c.lease, "a1");
    f.store.complete(c.lease, "a1", "precious");
    for (let i = 0; i < 30; i++) {
      const q = f.store.enqueue({ project: "/tmp/project", role: "build", task: `Filler ${i}` });
      f.store.cancel(q.id, "/tmp/project");
    }
    assert.equal(f.store.get(keeper.id)?.result, "precious");
  } finally { f.close(); }
});
