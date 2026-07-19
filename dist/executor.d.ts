import type { ExecutorConfig } from "./config.ts";
import type { QuestArtifactInput, QuestRecord, QuestTelemetry } from "./store.ts";
export type ExecutionStatus = "succeeded" | "failed" | "timed-out" | "aborted" | "protocol-error";
export interface ExecutionResult {
    ok: boolean;
    status: ExecutionStatus;
    output: string;
    exitCode: number | null;
    durationMs: number;
    timedOut: boolean;
    aborted: boolean;
    artifacts: QuestArtifactInput[];
    telemetry?: QuestTelemetry;
}
export interface ExecuteQuestOptions {
    signal?: AbortSignal;
    artifactDir?: string;
    agentRunId?: string;
    onTelemetry?: (telemetry: QuestTelemetry) => void;
}
export declare function executeQuest(quest: QuestRecord, executor: ExecutorConfig, signalOrOptions?: AbortSignal | ExecuteQuestOptions): Promise<ExecutionResult>;
//# sourceMappingURL=executor.d.ts.map