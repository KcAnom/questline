/**
 * True end-to-end: drive the installed CLI binary in a temp project with a
 * fake shell executor — add → run → show → retry/cancel → daemon drain.
 */
import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { test } from "vitest";

const BIN = resolve(import.meta.dirname, "..", "..", "bin", "questforge.js");

function project(executorScript: string) {
  const dir = mkdtempSync(join(tmpdir(), "qf-e2e-"));
  writeFileSync(join(dir, ".questforge.json"), JSON.stringify({
    dbPath: join(dir, "quests.sqlite"),
    executor: { command: ["sh", "-c", executorScript], timeoutMs: 30_000 },
    scheduler: { pollMs: 100, maxConcurrent: 2 },
  }));
  const run = (...args: string[]) =>
    execFileSync(process.execPath, [BIN, ...args], { cwd: dir, encoding: "utf8", timeout: 60_000 });
  return { dir, run, close: () => rmSync(dir, { recursive: true, force: true }) };
}

test("add → run → show lifecycle through the real CLI", () => {
  const p = project("printf 'answer: {task}'");
  try {
    const added = p.run("add", "build", "compute the answer", "--name", "Answer");
    const id = /queued (q-\w+)/.exec(added)![1];
    assert.match(added, /run it/);

    const ran = p.run("run", id);
    assert.match(ran, /✓ q-\w+ done/);
    assert.match(ran, /answer: compute the answer/);

    const shown = p.run("show", id);
    assert.match(shown, /✓/);
    assert.match(shown, /result:/);

    assert.match(p.run("list"), /done/);
    assert.match(p.run("health"), /integrity: ok/);
    assert.match(p.run("events", id), /completed/);
    assert.match(p.run("runs", id), /attempt 1 · done/);
  } finally { p.close(); }
});

test("failure → retry → success, and cancel refuses done work", () => {
  const p = project("test -f ok.flag && printf fixed || { echo nope >&2; exit 1; }");
  try {
    const id = /queued (q-\w+)/.exec(p.run("add", "build", "flaky thing"))![1];
    try {
      p.run("run", id);
      assert.fail("run should exit non-zero on failure");
    } catch (err) {
      assert.match(String((err as { stdout?: string }).stdout), /✗/);
    }
    assert.match(p.run("list"), /failed/);
    assert.match(p.run("retry", id), /✓/);
    writeFileSync(join(p.dir, "ok.flag"), "");
    assert.match(p.run("run", id), /fixed/);
    try {
      p.run("cancel", id);
      assert.fail("cancel of done quest should exit non-zero");
    } catch (err) {
      assert.match(String((err as { stdout?: string }).stdout), /refused/);
    }
  } finally { p.close(); }
});

test("daemon drains the queue respecting priority, then stops on SIGINT", { timeout: 40_000 }, async () => {
  const p = project("printf 'ran {task}'");
  try {
    p.run("add", "build", "low-task", "--name", "low-name");
    p.run("add", "build", "high-task", "--priority", "5", "--name", "high-name");
    const daemon = spawn(process.execPath, [BIN, "daemon"], { cwd: p.dir, stdio: ["ignore", "pipe", "pipe"] });
    let out = "";
    daemon.stdout.on("data", (d: Buffer) => { out += d.toString(); });
    await new Promise<void>((done, fail) => {
      const t = setTimeout(() => fail(new Error(`daemon didn't drain in time; output:\n${out}`)), 20_000);
      const check = setInterval(() => {
        if ((out.match(/✓ q-\w+ done/g) ?? []).length >= 2) { clearTimeout(t); clearInterval(check); done(); }
      }, 100);
    });
    daemon.kill("SIGINT");
    await new Promise((done) => daemon.once("close", done));
    assert.ok(out.indexOf("high-name") >= 0 && out.indexOf("low-name") >= 0, `both quests should appear:\n${out}`);
    // Higher priority is claimed first (claim log order).
    assert.ok(out.indexOf("high-name") < out.indexOf("low-name"), `priority order wrong:\n${out}`);
    assert.match(p.run("list"), /done[\s\S]*done/);
  } finally { p.close(); }
});

test("dedupe key returns the existing quest through the CLI", () => {
  const p = project("printf x");
  try {
    const first = /queued (q-\w+)/.exec(p.run("add", "build", "step", "--dedupe-key", "chain/1/step"))![1];
    const second = p.run("add", "build", "step again", "--dedupe-key", "chain/1/step");
    assert.match(second, new RegExp(`${first}.*existing quest returned`));
  } finally { p.close(); }
});
