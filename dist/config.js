/**
 * questline configuration. Merge order (later wins):
 *   built-in defaults → ~/.questline/config.json → <project>/.questline.json
 */
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
export const DEFAULTS = {
    dbPath: "~/.questline/quests.sqlite",
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
function readJson(path) {
    try {
        return JSON.parse(readFileSync(path, "utf8"));
    }
    catch {
        return {};
    }
}
function merge(base, over) {
    if (Array.isArray(base) || Array.isArray(over))
        return (over ?? base);
    if (typeof base === "object" && base && typeof over === "object" && over) {
        const out = { ...base };
        for (const [k, v] of Object.entries(over)) {
            out[k] = k in out && typeof out[k] === "object" && out[k] !== null && !Array.isArray(out[k]) && typeof v === "object" && v !== null && !Array.isArray(v)
                ? merge(out[k], v)
                : v;
        }
        return out;
    }
    return (over ?? base);
}
export function loadConfig(cwd) {
    const user = readJson(join(homedir(), ".questline", "config.json"));
    const project = readJson(join(cwd, ".questline.json"));
    return merge(merge(DEFAULTS, user), project);
}
export function executorFor(cfg, role) {
    return cfg.executors?.[role] ?? cfg.executor;
}
