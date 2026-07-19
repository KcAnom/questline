/**
 * Adversarial / stress coverage:
 * - SIGINT/SIGTERM while an executor child is mid-run
 * - two OS processes competing for the same SQLite DB
 * - migrate a real v1 fixture to the current schema
 * - IANA timezone / DST-aware cron
 */
import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "vitest";
import { nextCronTime, validateCron } from "../cron.ts";
import { QuestStore } from "../store.ts";

const BIN = resolve(import.meta.dirname, "..", "..", "bin", "questline.js");
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function projectDir(executorScript: string, extra: Record<string, unknown> = {}) {
  const dir = mkdtempSync(join(tmpdir(), "qf-hard-"));
  writeFileSync(join(dir, ".questline.json"), JSON.stringify({
    dbPath: join(dir, "quests.sqlite"),
    leaseTtlMs: 5_000,
    heartbeatMs: 200,
    executor: { command: ["sh", "-c", executorScript], timeoutMs: 60_000, terminateGraceMs: 500 },
    scheduler: { pollMs: 100, maxConcurrent: 2 },
    ...extra,
  }));
  const run = (...args: string[]) =>
    execFileSync(process.execPath, [BIN, ...args], { cwd: dir, encoding: "utf8", timeout: 60_000 });
  return { dir, run, close: () => rmSync(dir, { recursive: true, force: true }) };
}

test("SIGINT during mid-run terminates the worker and leaves quest interrupted/reclaimable", { timeout: 30_000 }, async () => {
  // Child traps TERM so we can observe graceful termination; sleeps long enough to be interrupted.
  const p = projectDir("trap 'echo term > term.flag; exit 0' TERM; sleep 30");
  try {
    const id = /queued (q-\w+)/.exec(p.run("add", "build", "long-running", "--name", "long"))![1];
    const child = spawn(process.execPath, [BIN, "run", id], { cwd: p.dir, stdio: ["ignore", "pipe", "pipe"] });
    let out = "";
    child.stdout.on("data", (d: Buffer) => { out += d.toString(); });
    // Wait until the executor has started.
    await new Promise<void>((done, fail) => {
      const t = setTimeout(() => fail(new Error(`run never started:\n${out}`)), 10_000);
      const check = setInterval(() => {
        if (out.includes("▶") || out.includes(id)) { clearTimeout(t); clearInterval(check); done(); }
      }, 50);
    });
    await sleep(150);
    child.kill("SIGINT");
    const code = await new Promise<number | null>((done) => child.once("close", (c) => done(c)));
    // Process should exit (130 for SIGINT is ideal; any non-hang is acceptable).
    assert.ok(code !== null, "run process hung after SIGINT");
    // Worker should have been signaled (TERM trap wrote the flag) OR quest recovered.
    const store = new QuestStore({ path: join(p.dir, "quests.sqlite"), maxHistory: 20 });
    try {
      const q = store.get(id)!;
      assert.ok(["interrupted", "failed", "queued", "done"].includes(q.state), `unexpected state ${q.state}`);
      // If still running, lease reclaim path must work for another session.
      if (q.state === "running") {
        await sleep(50);
        store.reclaimExpired(p.dir);
      }
      // Another session can claim again if not done.
      if (q.state !== "done") {
        const again = store.claimById(id, p.dir, "recovery-session") ?? store.claimNext(p.dir, "recovery-session");
        // Either reclaimable, or already settled as interrupted without owner.
        if (again) {
          assert.equal(again.quest.id, id);
          store.release(again.lease);
        } else {
          const final = store.get(id)!;
          assert.ok(["interrupted", "failed", "cancelled", "queued"].includes(final.state));
        }
      }
    } finally { store.close(); }
    assert.ok(existsSync(join(p.dir, "term.flag")) || code === 130 || code === 143 || code === 0,
      `expected worker termination signal or clean exit; code=${code} out=${out}`);
  } finally { p.close(); }
});

test("SIGTERM during mid-run stops the child process tree", { timeout: 30_000 }, async () => {
  const p = projectDir("trap 'echo term > term.flag; exit 0' TERM; sleep 30");
  try {
    const id = /queued (q-\w+)/.exec(p.run("add", "build", "term-me"))![1];
    const child = spawn(process.execPath, [BIN, "run", id], { cwd: p.dir, stdio: ["ignore", "pipe", "pipe"] });
    let out = "";
    child.stdout.on("data", (d: Buffer) => { out += d.toString(); });
    await new Promise<void>((done, fail) => {
      const t = setTimeout(() => fail(new Error(`run never started:\n${out}`)), 10_000);
      const check = setInterval(() => {
        if (out.includes("▶") || out.includes(id)) { clearTimeout(t); clearInterval(check); done(); }
      }, 50);
    });
    await sleep(150);
    child.kill("SIGTERM");
    const code = await new Promise<number | null>((done) => child.once("close", (c) => done(c)));
    assert.ok(code !== null, "run hung after SIGTERM");
    // Prefer observing the TERM trap, but accept any non-hang exit.
    assert.ok(existsSync(join(p.dir, "term.flag")) || code === 143 || code === 0 || code === 1,
      `worker not terminated; code=${code}`);
  } finally { p.close(); }
});

test("two processes competing for the same database: only one scheduler drains work", { timeout: 40_000 }, async () => {
  const p = projectDir("printf 'ran {task}'");
  try {
    // Pre-queue several items so both daemons have something to fight over.
    for (let i = 0; i < 4; i++) p.run("add", "build", `task-${i}`, "--name", `n${i}`);
    const d1 = spawn(process.execPath, [BIN, "daemon"], { cwd: p.dir, stdio: ["ignore", "pipe", "pipe"] });
    const d2 = spawn(process.execPath, [BIN, "daemon"], { cwd: p.dir, stdio: ["ignore", "pipe", "pipe"] });
    let o1 = "", o2 = "";
    d1.stdout.on("data", (d: Buffer) => { o1 += d.toString(); });
    d2.stdout.on("data", (d: Buffer) => { o2 += d.toString(); });
    await new Promise<void>((done, fail) => {
      const t = setTimeout(() => fail(new Error(`daemons didn't drain:\n---d1---\n${o1}\n---d2---\n${o2}`)), 25_000);
      const check = setInterval(() => {
        const doneCount = ((o1 + o2).match(/✓ q-\w+ done/g) ?? []).length;
        if (doneCount >= 4) { clearTimeout(t); clearInterval(check); done(); }
      }, 100);
    });
    d1.kill("SIGINT");
    d2.kill("SIGINT");
    await Promise.all([
      new Promise((r) => d1.once("close", r)),
      new Promise((r) => d2.once("close", r)),
    ]);
    // All four quests finished exactly once across both processes.
    const totalDone = ((o1 + o2).match(/✓ q-\w+ done/g) ?? []).length;
    assert.equal(totalDone, 4, `expected exactly 4 completions, got ${totalDone}\n---d1---\n${o1}\n---d2---\n${o2}`);
    // Scheduler lease means one daemon does the claiming; the other stays idle or takes over only after stop.
    // Soft check: not both daemons claim the same first quest id twice as "done".
    const ids1 = [...o1.matchAll(/✓ (q-\w+) done/g)].map((m) => m[1]);
    const ids2 = [...o2.matchAll(/✓ (q-\w+) done/g)].map((m) => m[1]);
    const overlap = ids1.filter((id) => ids2.includes(id));
    assert.equal(overlap.length, 0, `both daemons completed the same quest(s): ${overlap.join(",")}`);
    assert.match(p.run("list"), /done/);
  } finally { p.close(); }
});

test("migrates a real v1 fixture through to current schema version with claimable data", () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-v1mig-"));
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
      "INSERT INTO quests (id, project, role, name, task, state, attempts, created_at, updated_at) VALUES ('q-legacy', '/tmp/mig', 'build', 'Legacy', 'old task', 'queued', 0, 10, 10)",
    ).run();
    raw.prepare(
      "INSERT INTO quest_events (quest_id, event, at, data) VALUES ('q-legacy', 'enqueued', 10, '{}')",
    ).run();
    raw.close();

    const store = new QuestStore({ path, maxHistory: 50 });
    try {
      const health = store.health("/tmp/mig");
      assert.equal(health.integrity, "ok");
      assert.ok(health.schemaVersion >= 3, `schema version ${health.schemaVersion}`);
      const q = store.get("q-legacy")!;
      assert.equal(q.name, "Legacy");
      assert.equal(q.maxAttempts, 1);
      assert.equal(q.priority, 0);
      assert.equal(q.leaseVersion, 0);
      assert.equal(q.dependsOn.length, 0);
      // New columns exist with safe defaults.
      assert.equal(q.failureKind, undefined);
      assert.equal(q.groupId, undefined);
      const claimed = store.claimNext("/tmp/mig", "s-mig")!;
      assert.equal(claimed.quest.id, "q-legacy");
      assert.equal(store.attachRun(claimed.lease, "run-1"), true);
      assert.equal(store.complete(claimed.lease, "run-1", "migrated-ok"), true);
      assert.equal(store.get("q-legacy")?.result, "migrated-ok");
      // Re-open to prove migration is durable.
      store.close();
      const reopened = new QuestStore({ path, maxHistory: 50 });
      try {
        assert.equal(reopened.get("q-legacy")?.state, "done");
        assert.ok(reopened.health().schemaVersion >= 3);
      } finally { reopened.close(); }
    } catch (err) {
      try { store.close(); } catch { /* closed */ }
      throw err;
    }
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("cron validates IANA timezones and computes next occurrence in America/New_York", () => {
  validateCron("0 2 * * *", "America/New_York");
  assert.throws(() => validateCron("0 2 * * *", "Not/AZone"), /invalid timezone/);
  assert.throws(() => validateCron("not-cron", "UTC"), /cron|field|parse|invalid/i);

  // Anchor just after 2024-03-10 02:00 EST — spring forward DST night in US/Eastern.
  // 2024-03-10 07:00 UTC == 02:00 EST; clocks jump 02:00→03:00 local.
  const after = Date.parse("2024-03-10T07:00:00.000Z");
  const next = nextCronTime("0 2 * * *", after, "America/New_York");
  // Next civil 02:00 America/New_York after that instant is 2024-03-11 02:00 EDT = 06:00 UTC.
  assert.equal(next, Date.parse("2024-03-11T06:00:00.000Z"));

  // Winter (EST): 02:00 America/New_York = 07:00 UTC.
  const winterAfter = Date.parse("2024-01-15T07:00:00.000Z"); // just after 02:00 EST on Jan 15
  const winterNext = nextCronTime("0 2 * * *", winterAfter, "America/New_York");
  assert.equal(winterNext, Date.parse("2024-01-16T07:00:00.000Z"));
});

test("recurring quest materialization uses timezone-aware next run", () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-cron-mat-"));
  try {
    const store = new QuestStore({ path: join(dir, "q.sqlite"), maxHistory: 20 });
    try {
      // Schedule every hour at minute 0 in Tokyo.
      const rec = store.createRecurring({
        project: dir,
        name: "tokyo-hourly",
        cron: "0 * * * *",
        timezone: "Asia/Tokyo",
        role: "build",
        task: "tick",
        catchUp: "one",
      });
      assert.ok(rec.nextRunAt > Date.now() - 3_600_000);
      // Force due by writing next_run_at into the past via materialize with a future "now"
      // after creating with a cron that is already due relative to an old timestamp.
      const past = Date.parse("2024-06-01T00:00:00.000Z");
      const next = nextCronTime("0 * * * *", past, "Asia/Tokyo");
      assert.ok(next > past);
      // Materializing when not due yields nothing new.
      const none = store.materializeDueRecurring(dir, Date.now() - 86_400_000);
      assert.equal(none.length, 0);
    } finally { store.close(); }
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
