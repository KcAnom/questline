import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "vitest";
import { executeQuest } from "../executor.ts";
import type { QuestRecord } from "../store.ts";

function quest(over: Partial<QuestRecord> = {}): QuestRecord {
  const dir = mkdtempSync(join(tmpdir(), "qf-exec-"));
  return {
    id: "q-test", project: dir, role: "build", name: "t", task: "say hello",
    state: "running", attempts: 1, maxAttempts: 1, backoffBaseMs: 0, priority: 0,
    scheduledAt: 0, retainUntilConsumed: false, leaseVersion: 1, dependsOn: [],
    createdAt: 0, updatedAt: 0, ...over,
  };
}

test("captures stdout and exit-0 as success, substituting {task}", async () => {
  const q = quest({ task: "hello-world" });
  try {
    const r = await executeQuest(q, { command: ["sh", "-c", "printf 'got: {task}'"] });
    assert.equal(r.ok, true);
    assert.equal(r.output, "got: hello-world");
    assert.equal(r.exitCode, 0);
  } finally { rmSync(q.project, { recursive: true, force: true }); }
});

test("appends context to {task} and runs in the project cwd", async () => {
  const q = quest({ task: "T", context: "C" });
  try {
    const r = await executeQuest(q, { command: ["sh", "-c", "pwd; printf '%s' '{task}'"] });
    assert.ok(r.output.includes(q.project.split("/").pop()!));
    assert.ok(r.output.includes("Context:"));
    assert.ok(r.output.endsWith("C"));
  } finally { rmSync(q.project, { recursive: true, force: true }); }
});

test("non-zero exit is failure with stderr captured", async () => {
  const q = quest();
  try {
    const r = await executeQuest(q, { command: ["sh", "-c", "echo boom >&2; exit 3"] });
    assert.equal(r.ok, false);
    assert.equal(r.exitCode, 3);
    assert.match(r.output, /boom/);
  } finally { rmSync(q.project, { recursive: true, force: true }); }
});

test("timeout kills the child and reports timedOut", async () => {
  const q = quest();
  try {
    const r = await executeQuest(q, { command: ["sh", "-c", "sleep 30"], timeoutMs: 200 });
    assert.equal(r.ok, false);
    assert.equal(r.timedOut, true);
  } finally { rmSync(q.project, { recursive: true, force: true }); }
});

test("missing binary fails cleanly instead of throwing", async () => {
  const q = quest();
  try {
    const r = await executeQuest(q, { command: ["definitely-not-a-real-binary-xyz"] });
    assert.equal(r.ok, false);
  } finally { rmSync(q.project, { recursive: true, force: true }); }
});
