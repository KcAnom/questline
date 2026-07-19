/**
 * QuestScheduler: continuous, crash-safe draining of the durable quest queue.
 */
import type { ClaimedQuest, QuestRuntime } from "./runtime.ts";
export interface QuestSchedulerOptions {
    runtime: QuestRuntime;
    project: string;
    /** Drain-loop interval. Default 5000ms. */
    pollMs?: number;
    /** Max quests this scheduler keeps in flight at once. Default 2. */
    maxConcurrent?: number;
    /** Optional per-role concurrency caps. */
    roleConcurrency?: Record<string, number>;
    /** Scheduler-lease TTL; renewed every tick. Default 3× pollMs. */
    leaseTtlMs?: number;
    /** Return a human-readable reason to pause claiming, or undefined to run. */
    isPaused?: () => string | undefined;
    /** unref() the poll timer so it never keeps the process alive (embedded hosts). */
    unrefTimer?: boolean;
    /** Execute a claimed quest to settlement. */
    dispatch: (claimed: ClaimedQuest) => Promise<unknown>;
    onError?: (err: unknown) => void;
}
export interface QuestSchedulerStatus {
    running: boolean;
    inFlight: number;
    leaseHeld: boolean;
    pausedReason?: string;
    lastTickAt?: number;
}
export declare class QuestScheduler {
    private readonly options;
    private timer?;
    private inFlight;
    private inFlightRoles;
    private leaseHeld;
    private pausedReason?;
    private lastTickAt?;
    private draining;
    private stopped;
    private readonly pollMs;
    private readonly maxConcurrent;
    private readonly leaseTtlMs;
    constructor(options: QuestSchedulerOptions);
    start(): void;
    stop(): void;
    waitForIdle(): Promise<void>;
    status(): QuestSchedulerStatus;
    /** Run one drain pass now (also called on every interval tick). */
    tick(): void;
    private excludedRoles;
}
//# sourceMappingURL=scheduler.d.ts.map