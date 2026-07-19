import type { QuestArtifactInput } from "./store.ts";
export interface StreamCollectorOptions {
    artifactDir: string;
    questId: string;
    attempt: number;
    agentRunId: string;
    kind: string;
    maxInlineBytes: number;
    previewBytes: number;
}
export interface StreamCollectorResult {
    text: string;
    bytes: number;
    artifact?: QuestArtifactInput;
    truncated: boolean;
}
export declare class StreamCollector {
    private readonly options;
    private inline;
    private head;
    private tail;
    private bytes;
    private stream?;
    private tmpPath?;
    private finalPath?;
    private writeError?;
    constructor(options: StreamCollectorOptions);
    write(data: Buffer | string): void;
    finish(): Promise<StreamCollectorResult>;
    cleanup(): void;
    private openArtifact;
}
//# sourceMappingURL=artifacts.d.ts.map