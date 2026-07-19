export type ExecutorProtocol = "text" | "questline-jsonl";
export interface ExecutorConfig {
    /** argv template; {task}, {context}, {project}, {role} are substituted.
     *  The command runs with cwd = the quest's project directory. */
    command: string[];
    /** Hard wall-clock limit for one attempt (ms). Default 15 min. */
    timeoutMs?: number;
    /** stdout protocol. text = stdout is the result; questline-jsonl = versioned envelopes. */
    protocol?: ExecutorProtocol;
    /** Grace window between SIGTERM and SIGKILL when stopping a worker. */
    terminateGraceMs?: number;
    /** Store full output in an artifact once this byte threshold is exceeded. */
    maxInlineOutputBytes?: number;
    /** Head/tail preview bytes kept in SQLite when the full output is artifacted. */
    outputPreviewBytes?: number;
}
export interface PipelineStepConfig {
    role: string;
    name?: string;
    task: string;
    context?: string;
    priority?: number;
    maxAttempts?: number;
    backoffBaseMs?: number;
    retainUntilConsumed?: boolean;
    dependsOn?: string[];
}
export interface PipelineDefinition {
    description?: string;
    steps: Record<string, PipelineStepConfig>;
}
export interface NotificationConfig {
    /** Optional command invoked after a quest finishes/fails. Placeholders: {id}, {state}, {name}, {role}, {project}. */
    command?: string[];
}
export interface QuestlineConfig {
    /** SQLite path. Point it at pi's journal to share one queue with pi. */
    dbPath: string;
    /** Directory for full stdout/stderr/result artifacts. */
    artifactDir: string;
    /** Terminal history retained per project; active and retained-unconsumed results do not count. */
    maxHistory: number;
    leaseTtlMs: number;
    heartbeatMs: number;
    maxAttempts: number;
    backoffBaseMs: number;
    scheduler: {
        pollMs: number;
        maxConcurrent: number;
        /** Optional per-role concurrency caps, e.g. {"review": 1}. */
        roleConcurrency?: Record<string, number>;
    };
    /** Default executor plus optional per-role overrides. */
    executor: ExecutorConfig;
    executors?: Record<string, ExecutorConfig>;
    pipelines?: Record<string, PipelineDefinition>;
    notifications?: NotificationConfig;
}
/** @deprecated Use QuestlineConfig. */
export type QuestforgeConfig = QuestlineConfig;
export interface ConfigDiagnostic {
    file: string;
    message: string;
    line?: number;
    column?: number;
    path?: string;
}
export declare class ConfigError extends Error {
    readonly diagnostics: ConfigDiagnostic[];
    constructor(diagnostics: ConfigDiagnostic[]);
}
export interface LoadedConfig {
    config: QuestlineConfig;
    files: string[];
}
export declare const DEFAULTS: QuestlineConfig;
export declare function inspectConfig(cwd: string): LoadedConfig;
export declare function loadConfig(cwd: string): QuestlineConfig;
export declare function executorFor(cfg: QuestlineConfig, role: string): ExecutorConfig;
//# sourceMappingURL=config.d.ts.map