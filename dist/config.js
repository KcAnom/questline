/**
 * questline configuration. Merge order (later wins):
 *   built-in defaults → ~/.questline/config.json → <project>/.questline.json
 *
 * Config files are JSONC: comments and trailing commas are accepted, but parse
 * and validation errors are reported instead of silently falling back.
 */
import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
export class ConfigError extends Error {
    diagnostics;
    constructor(diagnostics) {
        super(diagnostics.map(formatDiagnostic).join("\n"));
        this.name = "ConfigError";
        this.diagnostics = diagnostics;
    }
}
export const DEFAULTS = {
    dbPath: "~/.questline/quests.sqlite",
    artifactDir: "~/.questline/artifacts",
    maxHistory: 500,
    leaseTtlMs: 120_000,
    heartbeatMs: 30_000,
    maxAttempts: 1,
    backoffBaseMs: 30_000,
    scheduler: { pollMs: 5_000, maxConcurrent: 2 },
    // Works with any print-mode CLI agent. Claude Code is the default because
    // `claude -p` is widely available; swap for pi, codex, etc.
    executor: {
        command: ["claude", "-p", "{task}"],
        timeoutMs: 15 * 60_000,
        protocol: "text",
        terminateGraceMs: 5_000,
        maxInlineOutputBytes: 256 * 1024,
        outputPreviewBytes: 16 * 1024,
    },
};
function formatDiagnostic(d) {
    const loc = d.line !== undefined ? `:${d.line}${d.column !== undefined ? `:${d.column}` : ""}` : "";
    const path = d.path ? ` (${d.path})` : "";
    return `${d.file}${loc}${path}: ${d.message}`;
}
function lineColumn(text, index) {
    let line = 1;
    let column = 1;
    for (let i = 0; i < index; i++) {
        if (text.charCodeAt(i) === 10) {
            line++;
            column = 1;
        }
        else
            column++;
    }
    return { line, column };
}
/** Remove JSONC comments and trailing commas while preserving line positions. */
function jsoncToJson(input) {
    let out = "";
    let i = 0;
    let inString = false;
    let stringQuote = "";
    let escaped = false;
    while (i < input.length) {
        const ch = input[i];
        const next = input[i + 1];
        if (inString) {
            out += ch;
            if (escaped)
                escaped = false;
            else if (ch === "\\")
                escaped = true;
            else if (ch === stringQuote)
                inString = false;
            i++;
            continue;
        }
        if (ch === '"') {
            inString = true;
            stringQuote = ch;
            out += ch;
            i++;
            continue;
        }
        if (ch === "/" && next === "/") {
            out += "  ";
            i += 2;
            while (i < input.length && input[i] !== "\n") {
                out += " ";
                i++;
            }
            continue;
        }
        if (ch === "/" && next === "*") {
            out += "  ";
            i += 2;
            while (i < input.length && !(input[i] === "*" && input[i + 1] === "/")) {
                out += input[i] === "\n" ? "\n" : " ";
                i++;
            }
            if (i < input.length) {
                out += "  ";
                i += 2;
            }
            continue;
        }
        out += ch;
        i++;
    }
    // Remove trailing commas before } or ] outside strings.
    let cleaned = "";
    inString = false;
    escaped = false;
    for (let j = 0; j < out.length; j++) {
        const ch = out[j];
        if (inString) {
            cleaned += ch;
            if (escaped)
                escaped = false;
            else if (ch === "\\")
                escaped = true;
            else if (ch === '"')
                inString = false;
            continue;
        }
        if (ch === '"') {
            inString = true;
            cleaned += ch;
            continue;
        }
        if (ch === ",") {
            let k = j + 1;
            while (k < out.length && /\s/.test(out[k]))
                k++;
            if (out[k] === "}" || out[k] === "]") {
                cleaned += " ";
                continue;
            }
        }
        cleaned += ch;
    }
    return cleaned;
}
function parseJsoncFile(path) {
    if (!existsSync(path))
        return undefined;
    let raw = "";
    try {
        raw = readFileSync(path, "utf8");
    }
    catch (err) {
        throw new ConfigError([{ file: path, message: `cannot read config: ${String(err)}` }]);
    }
    try {
        const parsed = JSON.parse(jsoncToJson(raw));
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
            throw new ConfigError([{ file: path, message: "config root must be an object" }]);
        }
        return parsed;
    }
    catch (err) {
        if (err instanceof ConfigError)
            throw err;
        const match = /position (\d+)/.exec(String(err.message));
        const loc = match ? lineColumn(raw, Number(match[1])) : {};
        throw new ConfigError([{ file: path, message: `invalid JSONC: ${err.message}`, ...loc }]);
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
function isPlainObject(v) {
    return !!v && typeof v === "object" && !Array.isArray(v);
}
function assertKeys(obj, allowed, file, path, diagnostics) {
    const allowedSet = new Set(allowed);
    for (const k of Object.keys(obj)) {
        if (!allowedSet.has(k))
            diagnostics.push({ file, path: path ? `${path}.${k}` : k, message: `unknown config key "${k}"` });
    }
}
function validateInteger(value, file, path, diagnostics, min, opts = {}) {
    if (value === undefined) {
        if (!opts.optional)
            diagnostics.push({ file, path, message: "is required" });
        return;
    }
    if (!Number.isInteger(value) || typeof value !== "number" || !Number.isFinite(value) || value < min) {
        diagnostics.push({ file, path, message: `must be an integer >= ${min}` });
    }
}
function validateExecutor(value, file, path, diagnostics, requireCommand = false) {
    if (!isPlainObject(value)) {
        diagnostics.push({ file, path, message: "must be an object" });
        return;
    }
    assertKeys(value, ["command", "timeoutMs", "protocol", "terminateGraceMs", "maxInlineOutputBytes", "outputPreviewBytes"], file, path, diagnostics);
    if (requireCommand && value.command === undefined) {
        diagnostics.push({ file, path: `${path}.command`, message: "is required" });
    }
    if (value.command !== undefined && (!Array.isArray(value.command) || value.command.length === 0 || !value.command.every((x) => typeof x === "string" && x.length > 0))) {
        diagnostics.push({ file, path: `${path}.command`, message: "must be a non-empty array of non-empty strings" });
    }
    validateInteger(value.timeoutMs, file, `${path}.timeoutMs`, diagnostics, 1, { optional: true });
    validateInteger(value.terminateGraceMs, file, `${path}.terminateGraceMs`, diagnostics, 0, { optional: true });
    validateInteger(value.maxInlineOutputBytes, file, `${path}.maxInlineOutputBytes`, diagnostics, 1024, { optional: true });
    validateInteger(value.outputPreviewBytes, file, `${path}.outputPreviewBytes`, diagnostics, 128, { optional: true });
    if (value.protocol !== undefined && value.protocol !== "text" && value.protocol !== "questline-jsonl") {
        diagnostics.push({ file, path: `${path}.protocol`, message: "must be 'text' or 'questline-jsonl'" });
    }
}
function validatePipelineDefinition(value, file, path, diagnostics) {
    if (!isPlainObject(value)) {
        diagnostics.push({ file, path, message: "must be an object" });
        return;
    }
    assertKeys(value, ["description", "steps"], file, path, diagnostics);
    if (value.description !== undefined && typeof value.description !== "string")
        diagnostics.push({ file, path: `${path}.description`, message: "must be a string" });
    if (!isPlainObject(value.steps) || Object.keys(value.steps).length === 0) {
        diagnostics.push({ file, path: `${path}.steps`, message: "must be a non-empty object" });
        return;
    }
    const names = new Set(Object.keys(value.steps));
    const graph = new Map();
    for (const [stepName, step] of Object.entries(value.steps)) {
        const stepPath = `${path}.steps.${stepName}`;
        if (!isPlainObject(step)) {
            diagnostics.push({ file, path: stepPath, message: "must be an object" });
            continue;
        }
        assertKeys(step, ["role", "name", "task", "context", "priority", "maxAttempts", "backoffBaseMs", "retainUntilConsumed", "dependsOn"], file, stepPath, diagnostics);
        if (typeof step.role !== "string" || !step.role)
            diagnostics.push({ file, path: `${stepPath}.role`, message: "must be a non-empty string" });
        if (typeof step.task !== "string" || !step.task)
            diagnostics.push({ file, path: `${stepPath}.task`, message: "must be a non-empty string" });
        if (step.name !== undefined && typeof step.name !== "string")
            diagnostics.push({ file, path: `${stepPath}.name`, message: "must be a string" });
        if (step.context !== undefined && typeof step.context !== "string")
            diagnostics.push({ file, path: `${stepPath}.context`, message: "must be a string" });
        validateInteger(step.priority, file, `${stepPath}.priority`, diagnostics, Number.MIN_SAFE_INTEGER, { optional: true });
        validateInteger(step.maxAttempts, file, `${stepPath}.maxAttempts`, diagnostics, 1, { optional: true });
        validateInteger(step.backoffBaseMs, file, `${stepPath}.backoffBaseMs`, diagnostics, 0, { optional: true });
        if (step.retainUntilConsumed !== undefined && typeof step.retainUntilConsumed !== "boolean")
            diagnostics.push({ file, path: `${stepPath}.retainUntilConsumed`, message: "must be a boolean" });
        if (step.dependsOn !== undefined) {
            if (!Array.isArray(step.dependsOn) || !step.dependsOn.every((d) => typeof d === "string" && d)) {
                diagnostics.push({ file, path: `${stepPath}.dependsOn`, message: "must be an array of step names" });
            }
            else {
                for (const dep of step.dependsOn) {
                    if (dep === stepName)
                        diagnostics.push({ file, path: `${stepPath}.dependsOn`, message: `step "${stepName}" cannot depend on itself` });
                    if (!names.has(dep))
                        diagnostics.push({ file, path: `${stepPath}.dependsOn`, message: `unknown dependency step "${dep}"` });
                }
                graph.set(stepName, step.dependsOn);
            }
        }
        else
            graph.set(stepName, []);
    }
    const visiting = new Set();
    const visited = new Set();
    const stack = [];
    const dfs = (n) => {
        if (visiting.has(n)) {
            diagnostics.push({ file, path: `${path}.steps`, message: `pipeline dependency cycle: ${[...stack, n].join(" -> ")}` });
            return true;
        }
        if (visited.has(n))
            return false;
        visiting.add(n);
        stack.push(n);
        for (const dep of graph.get(n) ?? [])
            if (dfs(dep))
                return true;
        stack.pop();
        visiting.delete(n);
        visited.add(n);
        return false;
    };
    for (const n of names)
        if (dfs(n))
            break;
}
function validateSourceObject(obj, file, diagnostics) {
    assertKeys(obj, ["dbPath", "artifactDir", "maxHistory", "leaseTtlMs", "heartbeatMs", "maxAttempts", "backoffBaseMs", "scheduler", "executor", "executors", "pipelines", "notifications"], file, "", diagnostics);
    if (obj.executor !== undefined)
        validateExecutor(obj.executor, file, "executor", diagnostics);
    if (obj.executors !== undefined) {
        if (!isPlainObject(obj.executors))
            diagnostics.push({ file, path: "executors", message: "must be an object" });
        else
            for (const [role, ex] of Object.entries(obj.executors))
                validateExecutor(ex, file, `executors.${role}`, diagnostics, true);
    }
    if (obj.scheduler !== undefined) {
        if (!isPlainObject(obj.scheduler))
            diagnostics.push({ file, path: "scheduler", message: "must be an object" });
        else {
            assertKeys(obj.scheduler, ["pollMs", "maxConcurrent", "roleConcurrency"], file, "scheduler", diagnostics);
            if (obj.scheduler.roleConcurrency !== undefined) {
                if (!isPlainObject(obj.scheduler.roleConcurrency))
                    diagnostics.push({ file, path: "scheduler.roleConcurrency", message: "must be an object" });
                else
                    for (const [role, cap] of Object.entries(obj.scheduler.roleConcurrency))
                        validateInteger(cap, file, `scheduler.roleConcurrency.${role}`, diagnostics, 1);
            }
        }
    }
    if (obj.pipelines !== undefined) {
        if (!isPlainObject(obj.pipelines))
            diagnostics.push({ file, path: "pipelines", message: "must be an object" });
        else
            for (const [name, p] of Object.entries(obj.pipelines))
                validatePipelineDefinition(p, file, `pipelines.${name}`, diagnostics);
    }
    if (obj.notifications !== undefined) {
        if (!isPlainObject(obj.notifications))
            diagnostics.push({ file, path: "notifications", message: "must be an object" });
        else {
            assertKeys(obj.notifications, ["command"], file, "notifications", diagnostics);
            const command = obj.notifications.command;
            if (command !== undefined && (!Array.isArray(command) || !command.every((x) => typeof x === "string" && x.length > 0))) {
                diagnostics.push({ file, path: "notifications.command", message: "must be an array of non-empty strings" });
            }
        }
    }
}
function validateFinalConfig(cfg) {
    const file = "merged config";
    const diagnostics = [];
    if (typeof cfg.dbPath !== "string" || !cfg.dbPath)
        diagnostics.push({ file, path: "dbPath", message: "must be a non-empty string" });
    if (typeof cfg.artifactDir !== "string" || !cfg.artifactDir)
        diagnostics.push({ file, path: "artifactDir", message: "must be a non-empty string" });
    validateInteger(cfg.maxHistory, file, "maxHistory", diagnostics, 1);
    validateInteger(cfg.leaseTtlMs, file, "leaseTtlMs", diagnostics, 100);
    validateInteger(cfg.heartbeatMs, file, "heartbeatMs", diagnostics, 50);
    validateInteger(cfg.maxAttempts, file, "maxAttempts", diagnostics, 1);
    validateInteger(cfg.backoffBaseMs, file, "backoffBaseMs", diagnostics, 0);
    validateInteger(cfg.scheduler?.pollMs, file, "scheduler.pollMs", diagnostics, 20);
    validateInteger(cfg.scheduler?.maxConcurrent, file, "scheduler.maxConcurrent", diagnostics, 1);
    if (Number.isFinite(cfg.heartbeatMs) && Number.isFinite(cfg.leaseTtlMs) && cfg.heartbeatMs >= cfg.leaseTtlMs) {
        diagnostics.push({ file, path: "heartbeatMs", message: "must be smaller than leaseTtlMs" });
    }
    validateExecutor(cfg.executor, file, "executor", diagnostics, true);
    if (cfg.executors)
        for (const [role, ex] of Object.entries(cfg.executors))
            validateExecutor(ex, file, `executors.${role}`, diagnostics, true);
    if (cfg.pipelines)
        for (const [name, p] of Object.entries(cfg.pipelines))
            validatePipelineDefinition(p, file, `pipelines.${name}`, diagnostics);
    return diagnostics;
}
export function inspectConfig(cwd) {
    const files = [];
    const diagnostics = [];
    const userPath = join(homedir(), ".questline", "config.json");
    const projectPath = join(cwd, ".questline.json");
    let user;
    let project;
    for (const path of [userPath, projectPath]) {
        try {
            const parsed = parseJsoncFile(path);
            if (parsed) {
                files.push(path);
                validateSourceObject(parsed, path, diagnostics);
                if (path === userPath)
                    user = parsed;
                else
                    project = parsed;
            }
        }
        catch (err) {
            if (err instanceof ConfigError)
                diagnostics.push(...err.diagnostics);
            else
                diagnostics.push({ file: path, message: String(err) });
        }
    }
    if (diagnostics.length)
        throw new ConfigError(diagnostics);
    const merged = merge(merge(DEFAULTS, user), project);
    const finalDiagnostics = validateFinalConfig(merged);
    if (finalDiagnostics.length)
        throw new ConfigError(finalDiagnostics);
    return { config: merged, files };
}
export function loadConfig(cwd) {
    return inspectConfig(cwd).config;
}
export function executorFor(cfg, role) {
    return cfg.executors?.[role] ?? cfg.executor;
}
