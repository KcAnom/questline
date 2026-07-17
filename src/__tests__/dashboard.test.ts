import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "vitest";
import { QuestStore } from "../store.ts";
import { dashboard, filterQuests, questTableText } from "../dashboard.ts";

const PROJECT = "/tmp/qf-dash-project";

function fixture() {
  const dir = mkdtempSync(join(tmpdir(), "qf-dash-"));
  const store = new QuestStore({ path: join(dir, "quests.sqlite"), maxHistory: 50 });
  return { store, close: () => { store.close(); rmSync(dir, { recursive: true, force: true }); } };
}

function seed(store: QuestStore) {
  const done = store.enqueue({ project: PROJECT, role: "build", name: "Ship feature", task: "ship it" });
  const c = store.claimNext(PROJECT, "s")!;
  store.attachRun(c.lease, "a1");
  store.updateTelemetry(c.lease, { model: "claude", turns: 4, tokens: 2000, costUsd: 0.12 });
  store.complete(c.lease, "a1", "shipped");
  const failed = store.enqueue({ project: PROJECT, role: "review", name: "Review docs", task: "review" });
  const c2 = store.claimNext(PROJECT, "s")!;
  store.attachRun(c2.lease, "a2");
  store.fail(c2.lease, "reviewer exploded", "a2");
  const queued = store.enqueue({ project: PROJECT, role: "explore", name: "Map codebase", task: "map" });
  return { done: done.id, failed: failed.id, queued: queued.id };
}

test("filters, search, details, and table text", () => {
  const f = fixture();
  try {
    seed(f.store);
    const rows = f.store.list(PROJECT, 50);
    assert.equal(filterQuests(rows, "active", "").length, 1);
    assert.equal(filterQuests(rows, "failed", "").length, 1);
    assert.equal(filterQuests(rows, "all", "REVIEW").length, 1);
    assert.match(questTableText(rows), /Ship feature/);

    const dash = dashboard({ store: f.store, project: PROJECT, rows: () => 30 });
    let out = dash.render(110).join("\n");
    assert.match(out, /3 shown/);
    dash.handleInput("\x1b[C");
    assert.equal(dash.state.filter, "active");
    dash.handleInput("\x1b[D");
    dash.handleInput("j");
    dash.handleInput("j");
    dash.handleInput("\r");
    out = dash.render(120).join("\n");
    assert.match(out, /attempt 1: done/);
    assert.match(out, /result:/);
  } finally { f.close(); }
});

test("actions gate on quest state and quit fires onQuit", () => {
  const f = fixture();
  try {
    const ids = seed(f.store);
    const calls: string[] = [];
    let quit = false;
    const dash = dashboard({
      store: f.store, project: PROJECT, rows: () => 30,
      actions: {
        run: (id) => { calls.push(`run:${id}`); return undefined; },
        retry: (id) => { calls.push(`retry:${id}`); return undefined; },
        cancel: (id) => { calls.push(`cancel:${id}`); return undefined; },
      },
      onQuit: () => { quit = true; },
    });
    dash.render(100);
    dash.handleInput("r"); // row 0 = queued (newest first)
    assert.deepEqual(calls, [`run:${ids.queued}`]);
    dash.handleInput("R");
    assert.equal(calls.length, 1);
    assert.match(dash.state.status ?? "", /cannot retry/);
    dash.handleInput("j");
    dash.handleInput("R");
    assert.equal(calls[1], `retry:${ids.failed}`);
    dash.handleInput("q");
    assert.equal(quit, true);
  } finally { f.close(); }
});
