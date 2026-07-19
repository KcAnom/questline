/**
 * QuestRuntime: race-safe execution wrapper around QuestStore.
 *
 * Owns lease lifecycle for every quest this session claims. Each claim carries
 * an AbortSignal that is aborted on lease loss or shutdown, so executors can
 * stop their child process tree before another owner produces side effects.
 */
import type { QuestArtifactInput, QuestLease, QuestRecord, QuestStore, QuestTelemetry } from "./store.ts";
export type QuestAbortReason = {
    kind: "lease-lost";
    questId: string;
} | {
    kind: "shutdown";
    questId?: string;
    signal?: NodeJS.Signals;
} | {
    kind: "manual";
    questId?: string;
};
export interface ClaimedQuest {
    quest: QuestRecord;
    lease: QuestLease;
    signal: AbortSignal;
}
export interface QuestRuntimeOptions {
    store: QuestStore;
    ownerSession: string;
    /** Heartbeat interval. Default: a third of the store's lease TTL. */
    heartbeatMs?: number;
}
export declare class QuestRuntime {
    readonly store: QuestStore;
    readonly ownerSession: string;
    private heartbeatMs;
    private active;
    private closed;
    constructor(options: QuestRuntimeOptions);
    /** Claim the next eligible quest and start heartbeating it. */
    claimNext(project: string, options?: {
        ignorePause?: boolean;
        excludeRoles?: string[];
    } | (() => void)): ClaimedQuest | undefined;
    /** Claim one specific eligible quest and start heartbeating it. */
    claimById(id: string, project: string, options?: {
        ignorePause?: boolean;
    } | (() => void)): ClaimedQuest | undefined;
    /** True while this session still holds the lease for a quest. */
    owns(questId: string): boolean;
    /** Backwards-compatible observer hook. Prefer claimed.signal. */
    setLeaseLostHandler(questId: string, cb: () => void): void;
    attachRun(lease: QuestLease, agentRunId: string): boolean;
    updateTelemetry(lease: QuestLease, t: QuestTelemetry): boolean;
    complete(lease: QuestLease, agentRunId: string, result: string, artifacts?: QuestArtifactInput[]): boolean;
    fail(lease: QuestLease, error: string, agentRunId?: string, artifacts?: QuestArtifactInput[]): boolean;
    /** Hand a claimed quest back to the queue without recording a failure. */
    releaseClaim(lease: QuestLease): boolean;
    run(claimed: ClaimedQuest, dispatch: (quest: QuestRecord, report: (t: QuestTelemetry) => void, signal: AbortSignal) => Promise<{
        agentRunId: string;
        ok: boolean;
        text: string;
    }>): Promise<{
        ok: boolean;
        text: string;
    }>;
    /** Stop heartbeating a quest without writing state (complete/fail do this). */
    release(questId: string): void;
    /** Abort all local workers and prevent new claims; call finishShutdown after children exit. */
    beginShutdown(reason?: Omit<Extract<QuestAbortReason, {
        kind: "shutdown";
    }>, "questId">): void;
    /** Stop all heartbeats and hand remaining running work back as interrupted. */
    finishShutdown(project: string): number;
    /** Backwards-compatible shutdown: abort then immediately recover. */
    shutdown(project: string): void;
    private track;
}
//# sourceMappingURL=runtime.d.ts.map