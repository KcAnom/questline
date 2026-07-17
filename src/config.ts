/**
 * questforge configuration. Merge order (later wins):
 *   built-in defaults → ~/.questforge/config.json → <project>/.questforge.json
 */
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

export interface ExecutorConfig {
  /** argv template; {task}, {context}, {project}, {role} are substituted.
   *  The command runs with cwd = the quest's project directory. */
  command: string[];
  /** Hard wall-clock limit for one attempt (ms). Default 15 min. */
  timeoutMs?: number;
}

export interface QuestforgeConfig {
  /** SQLite path. Point it at pi's journal to share one queue with pi. */
  dbPath: string;
  maxHistory: number;
  leaseTtlMs: number;
  heartbeatMs: number;
  maxAttempts: number;
  backoffBaseMs: number;
  scheduler: {
    pollMs: number;
    maxConcurrent: number;
  };
  /** Default executor plus optional per-role overrides. */
  executor: ExecutorConfig;
  executors?: Record<string, ExecutorConfig>;
}

export const DEFAULTS: QuestforgeConfig = {
  dbPath: "~/.questforge/quests.sqlite",
  maxHistory: 500,
  leaseTtlMs: 120_000,
  heartbeatMs: 30_000,
  maxAttempts: 1,
  backoffBaseMs: 30_000,
  scheduler: { pollMs: 5_000, maxConcurrent: 2 },
  // Works with any print-mode CLI agent. Claude Code is the default because
  // `claude -p` is the most widely available; swap for pi, codex, etc.:
  //   { "executor": { "command": ["pi", "--mode", "text", "{task}"] } }
  executor: { command: ["claude", "-p", "{task}"], timeoutMs: 15 * 60_000 },
};

function readJson(path: string): Record<string, unknown> {
  try {
    return JSON.parse(readFileSync(path, "utf8")) as Record<string, unknown>;
  } catch {
    return {};
  }
}

function merge<T>(base: T, over: Partial<T>): T {
  if (Array.isArray(base) || Array.isArray(over)) return (over ?? base) as T;
  if (typeof base === "object" && base && typeof over === "object" && over) {
    const out: Record<string, unknown> = { ...(base as Record<string, unknown>) };
    for (const [k, v] of Object.entries(over as Record<string, unknown>)) {
      out[k] = k in out && typeof out[k] === "object" && out[k] !== null && !Array.isArray(out[k]) && typeof v === "object" && v !== null && !Array.isArray(v)
        ? merge(out[k], v as never)
        : v;
    }
    return out as T;
  }
  return (over ?? base) as T;
}

export function loadConfig(cwd: string): QuestforgeConfig {
  const user = readJson(join(homedir(), ".questforge", "config.json"));
  const project = readJson(join(cwd, ".questforge.json"));
  return merge(merge(DEFAULTS, user as Partial<QuestforgeConfig>), project as Partial<QuestforgeConfig>);
}

export function executorFor(cfg: QuestforgeConfig, role: string): ExecutorConfig {
  return cfg.executors?.[role] ?? cfg.executor;
}
