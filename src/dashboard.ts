/**
 * Dashboard component: pure render/handleInput state machine over the quest
 * store. Host-agnostic — the TUI host feeds it raw key bytes and repaints
 * the lines it returns; tests drive it directly.
 */
import { fmtDuration, fmtTokens, fmtUsd, shortModelId } from "./fmt.ts";
import type { QuestRecord, QuestRunRecord, QuestStore } from "./store.ts";

export interface DashTheme {
  accent(s: string): string;
  dim(s: string): string;
  text(s: string): string;
  muted(s: string): string;
  success(s: string): string;
  error(s: string): string;
  warning(s: string): string;
  bold(s: string): string;
}

export const PLAIN_THEME: DashTheme = {
  accent: (s) => s, dim: (s) => s, text: (s) => s, muted: (s) => s,
  success: (s) => s, error: (s) => s, warning: (s) => s, bold: (s) => s,
};

export const ANSI_THEME: DashTheme = {
  accent: (s) => `\x1b[36m${s}\x1b[39m`,
  dim: (s) => `\x1b[2m${s}\x1b[22m`,
  text: (s) => s,
  muted: (s) => `\x1b[90m${s}\x1b[39m`,
  success: (s) => `\x1b[32m${s}\x1b[39m`,
  error: (s) => `\x1b[31m${s}\x1b[39m`,
  warning: (s) => `\x1b[33m${s}\x1b[39m`,
  bold: (s) => `\x1b[1m${s}\x1b[22m`,
};

export const QUEST_FILTERS = ["all", "active", "failed", "done", "cancelled"] as const;
export type QuestFilter = (typeof QUEST_FILTERS)[number];

const ACTIVE_STATES = new Set(["queued", "running", "interrupted"]);

export function filterQuests(rows: QuestRecord[], filter: QuestFilter, search: string): QuestRecord[] {
  const bySearch = search.trim().toLowerCase();
  return rows.filter((q) => {
    if (filter === "active" && !ACTIVE_STATES.has(q.state)) return false;
    if (filter === "failed" && q.state !== "failed") return false;
    if (filter === "done" && q.state !== "done") return false;
    if (filter === "cancelled" && q.state !== "cancelled") return false;
    if (!bySearch) return true;
    return [q.id, q.name, q.role, q.task, q.state].some((s) => s.toLowerCase().includes(bySearch));
  });
}

const STATE_MARK: Record<string, string> = {
  queued: "☐", running: "◐", interrupted: "◭", done: "✓", failed: "✗", cancelled: "⊘",
};

export function questRowText(q: QuestRecord, now = Date.now()): string {
  const mark = STATE_MARK[q.state] ?? "·";
  const age = fmtDuration(now - q.createdAt);
  const extras =
    (q.priority ? ` p${q.priority}` : "") +
    (q.retryAt && q.state === "queued" && q.retryAt > now ? ` retry ${fmtDuration(q.retryAt - now)}` : "") +
    (q.scheduledAt > now ? ` starts ${fmtDuration(q.scheduledAt - now)}` : "") +
    (q.dependsOn.length ? ` deps ${q.dependsOn.length}` : "");
  return `${mark} ${q.id} ${q.state.padEnd(11)} ${q.role.padEnd(8)} ${q.name} · a${q.attempts}/${q.maxAttempts} · ${age}${extras}`;
}

export function questTableText(rows: QuestRecord[], filter: QuestFilter = "all", search = ""): string {
  const visible = filterQuests(rows, filter, search);
  if (!visible.length) return "No quests match.";
  return visible.map((q) => questRowText(q)).join("\n");
}

export function questDetailLines(
  q: QuestRecord,
  runs: QuestRunRecord[],
  events: Array<{ event: string; at: number; data: unknown }>,
  artifacts: Array<{ kind: string; attempt: number; path: string; bytes: number }> = [],
): string[] {
  const lines: string[] = [];
  lines.push(`task: ${q.task}`);
  if (q.context) lines.push(`context: ${q.context.slice(0, 300)}`);
  if (q.dependsOn.length) lines.push(`depends on: ${q.dependsOn.join(", ")}`);
  if (q.chain) lines.push(`chain: ${JSON.stringify(q.chain)}`);
  if (q.groupId) lines.push(`group: ${q.groupId}${q.groupStep ? ` · step ${q.groupStep}` : ""}`);
  if (q.recurrenceId) lines.push(`recurrence: ${q.recurrenceId}${q.occurrenceAt ? ` · ${new Date(q.occurrenceAt).toISOString()}` : ""}`);
  if (q.dedupeKey) lines.push(`dedupe: ${q.dedupeKey}${q.retainUntilConsumed ? q.consumedAt ? " · consumed" : " · retained" : ""}`);
  for (const r of runs.slice(0, 5)) {
    const dur = r.finishedAt ? fmtDuration(r.finishedAt - r.startedAt) : "…";
    lines.push(
      `attempt ${r.attempt}: ${r.outcome ?? "running"} · ${r.provider ? `${r.provider}/` : ""}${shortModelId(r.model ?? "?")}${r.tier ? ` [${r.tier}]` : ""} · t${r.turns} · ${fmtTokens(r.tokens)} tok · ${fmtUsd(r.costUsd)} · ${dur}${r.lastActivity ? ` · ${r.lastActivity}` : ""}`,
    );
  }
  if (artifacts.length) {
    lines.push("artifacts:");
    for (const a of artifacts.slice(0, 6)) lines.push(`  ${a.kind} attempt ${a.attempt}: ${a.path} (${a.bytes} bytes)`);
  }
  const outcome = q.result ?? q.error;
  if (outcome) {
    lines.push(q.result ? "result:" : "error:");
    for (const l of outcome.split("\n").slice(0, 8)) lines.push(`  ${l.slice(0, 200)}`);
  }
  if (events.length) {
    lines.push("events:");
    for (const e of events.slice(0, 6)) lines.push(`  ${new Date(e.at).toISOString()} ${e.event}`);
  }
  return lines;
}

export interface DashboardActions {
  run?: (id: string) => string | undefined;
  retry?: (id: string) => string | undefined;
  cancel?: (id: string) => string | undefined;
}

export interface DashboardOptions {
  store: QuestStore;
  project: string;
  theme?: DashTheme;
  actions?: DashboardActions;
  onQuit?: () => void;
  rows?: () => number;
  now?: () => number;
}

export interface DashboardComponent {
  render(width: number): string[];
  handleInput(data: string): void;
  readonly state: { filter: QuestFilter; search: string; searchMode: boolean; selected: number; detail: boolean; status?: string };
}

export function dashboard(opts: DashboardOptions): DashboardComponent {
  const theme = opts.theme ?? PLAIN_THEME;
  const { store, project } = opts;
  const now = opts.now ?? Date.now;
  const termRows = opts.rows ?? (() => process.stdout.rows ?? 24);
  const state = {
    filter: "all" as QuestFilter,
    search: "",
    searchMode: false,
    selected: 0,
    offset: 0,
    detail: false,
    status: undefined as string | undefined,
  };

  const visibleRows = (): QuestRecord[] => {
    try {
      return filterQuests(store.list(project, 200), state.filter, state.search);
    } catch {
      return [];
    }
  };

  const viewport = () => Math.max(4, termRows() - (state.detail ? 22 : 8));

  const clampSelection = (rows: QuestRecord[]) => {
    state.selected = Math.max(0, Math.min(state.selected, rows.length - 1));
    const vp = viewport();
    if (state.selected < state.offset) state.offset = state.selected;
    if (state.selected >= state.offset + vp) state.offset = state.selected - vp + 1;
    state.offset = Math.max(0, Math.min(state.offset, Math.max(0, rows.length - vp)));
  };

  const act = (kind: keyof DashboardActions, allowed: (q: QuestRecord) => boolean, verb: string) => {
    const rows = visibleRows();
    const q = rows[state.selected];
    if (!q) { state.status = "nothing selected"; return; }
    if (!allowed(q)) { state.status = `${q.id} is ${q.state} — cannot ${verb}`; return; }
    const fn = opts.actions?.[kind];
    if (!fn) { state.status = `${verb} is not available here`; return; }
    const err = fn(q.id);
    state.status = err ?? `${verb}: ${q.id} ✓`;
  };

  return {
    state,
    render(width: number): string[] {
      const rows = visibleRows();
      clampSelection(rows);
      const t = now();
      const lines: string[] = [];
      lines.push(theme.bold(theme.accent(" ➳ questline")) + theme.dim(`  ${project}  ·  ${rows.length} shown`));
      lines.push(
        QUEST_FILTERS.map((f) => (f === state.filter ? theme.accent(`[${f}]`) : theme.dim(` ${f} `))).join(" ") +
          "  " +
          (state.searchMode
            ? theme.warning(`/${state.search}▏`)
            : state.search
              ? theme.text(`/${state.search}`)
              : theme.dim("/ to search")),
      );
      lines.push(theme.dim("─".repeat(Math.max(10, Math.min(width - 2, 110)))));
      if (!rows.length) lines.push(theme.muted("  No quests match. (add one: questline add <role> \"task\")"));
      const vp = viewport();
      for (let i = state.offset; i < Math.min(rows.length, state.offset + vp); i++) {
        const q = rows[i];
        const rowText = questRowText(q, t).slice(0, Math.max(20, width - 4));
        const colored =
          q.state === "failed" ? theme.error(rowText)
          : q.state === "done" ? theme.success(rowText)
          : q.state === "running" ? theme.accent(rowText)
          : q.state === "cancelled" ? theme.dim(rowText)
          : theme.text(rowText);
        lines.push(i === state.selected ? theme.bold(` ▸ ${colored}`) : `   ${colored}`);
      }
      if (rows.length > vp) lines.push(theme.dim(`   … ${state.offset + 1}–${Math.min(rows.length, state.offset + vp)} of ${rows.length}`));
      const sel = rows[state.selected];
      if (state.detail && sel) {
        lines.push(theme.dim("─".repeat(Math.max(10, Math.min(width - 2, 110)))));
        let runs: QuestRunRecord[] = [];
        let events: Array<{ event: string; at: number; data: unknown }> = [];
        let artifacts: Array<{ kind: string; attempt: number; path: string; bytes: number }> = [];
        try { runs = store.runs(sel.id, 5); events = store.events(sel.id, 6); artifacts = store.artifacts(sel.id); } catch { /* closing */ }
        for (const l of questDetailLines(sel, runs, events, artifacts)) lines.push(theme.muted(`  ${l.slice(0, Math.max(20, width - 4))}`));
      }
      if (state.status) lines.push(theme.warning(` ${state.status}`));
      lines.push(theme.dim(" ↑↓ select · ←→ filter · / search · ⏎ details · r run · R retry · x cancel · q quit"));
      return lines;
    },
    handleInput(data: string): void {
      state.status = undefined;
      if (state.searchMode) {
        if (data === "\x1b") { state.searchMode = false; state.search = ""; return; }
        if (data === "\r" || data === "\n") { state.searchMode = false; return; }
        if (data === "\x7f" || data === "\b") { state.search = state.search.slice(0, -1); return; }
        if (data.length === 1 && data >= " " && data !== "\x7f") { state.search += data; state.selected = 0; return; }
        return;
      }
      const rows = visibleRows();
      switch (data) {
        case "\x1b[A": case "k": state.selected--; break;
        case "\x1b[B": case "j": state.selected++; break;
        case "\x1b[5~": state.selected -= viewport(); break;
        case "\x1b[6~": case " ": state.selected += viewport(); break;
        case "g": state.selected = 0; break;
        case "G": state.selected = rows.length - 1; break;
        case "\x1b[C": case "l": case "\t":
          state.filter = QUEST_FILTERS[(QUEST_FILTERS.indexOf(state.filter) + 1) % QUEST_FILTERS.length];
          state.selected = 0;
          break;
        case "\x1b[D": case "h":
          state.filter = QUEST_FILTERS[(QUEST_FILTERS.indexOf(state.filter) + QUEST_FILTERS.length - 1) % QUEST_FILTERS.length];
          state.selected = 0;
          break;
        case "/": state.searchMode = true; state.search = ""; break;
        case "\r": case "\n": state.detail = !state.detail; break;
        case "r": act("run", (q) => q.state === "queued" || q.state === "interrupted", "run"); break;
        case "R": act("retry", (q) => q.state === "failed" || q.state === "cancelled" || q.state === "interrupted", "retry"); break;
        case "x": act("cancel", (q) => q.state === "queued" || q.state === "interrupted", "cancel"); break;
        case "q": case "\x1b": case "\x03": opts.onQuit?.(); break;
        default: break;
      }
      clampSelection(rows);
    },
  };
}
