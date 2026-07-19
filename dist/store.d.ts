import { type RecurrenceCatchUp } from "./cron.ts";
import type { PipelineDefinition } from "./config.ts";
export type QuestState = "queued" | "running" | "done" | "failed" | "cancelled" | "interrupted";
export type QuestFailureKind = "execution" | "dependency";
export type QuestRunOutcome = "done" | "failed" | "interrupted" | "cancelled";
/** Fencing token for a claimed quest. `version` is the quest's lease
 *  generation, bumped on every claim — a write-back carrying a stale version
 *  is rejected even if the owner session id happens to match. */
export interface QuestLease {
    id: string;
    ownerSession: string;
    version: number;
}
export interface QuestRecord {
    id: string;
    project: string;
    role: string;
    name: string;
    task: string;
    context?: string;
    state: QuestState;
    attempts: number;
    maxAttempts: number;
    backoffBaseMs: number;
    priority: number;
    /** Epoch ms before which the quest must not run. 0 = immediately. */
    scheduledAt: number;
    /** Epoch ms of the next retry attempt after a bounded failure. */
    retryAt?: number;
    dedupeKey?: string;
    /** Opaque chain metadata (chain, runId, phase, workId) for durable pipelines. */
    chain?: Record<string, unknown>;
    /** Keep the result until markConsumed() — done+retained quests dedupe like active ones. */
    retainUntilConsumed: boolean;
    consumedAt?: number;
    leaseVersion: number;
    leaseExpiresAt?: number;
    heartbeatAt?: number;
    dependsOn: string[];
    createdAt: number;
    updatedAt: number;
    startedAt?: number;
    finishedAt?: number;
    agentRunId?: string;
    ownerSession?: string;
    result?: string;
    error?: string;
    failureKind?: QuestFailureKind;
    failedDependencyId?: string;
    groupId?: string;
    groupStep?: string;
    recurrenceId?: string;
    occurrenceAt?: number;
}
/** Durable per-attempt telemetry, survives restarts (dashboard feed). */
export interface QuestRunRecord {
    questId: string;
    attempt: number;
    ownerSession: string;
    leaseVersion: number;
    agentRunId?: string;
    model?: string;
    provider?: string;
    tier?: string;
    turns: number;
    tokens: number;
    costUsd: number;
    lastActivity?: string;
    startedAt: number;
    finishedAt?: number;
    outcome?: QuestRunOutcome;
    error?: string;
}
export interface QuestTelemetry {
    model?: string;
    provider?: string;
    tier?: string;
    turns?: number;
    tokens?: number;
    costUsd?: number;
    lastActivity?: string;
}
export interface QuestArtifactRecord {
    questId: string;
    attempt: number;
    leaseVersion: number;
    kind: string;
    path: string;
    bytes: number;
    createdAt: number;
}
export interface QuestArtifactInput {
    kind: string;
    path: string;
    bytes: number;
}
export interface EnqueueInput {
    project: string;
    role: string;
    name?: string;
    task: string;
    context?: string;
    priority?: number;
    scheduledAt?: number;
    maxAttempts?: number;
    backoffBaseMs?: number;
    dependsOn?: string[];
    dedupeKey?: string;
    chain?: Record<string, unknown>;
    retainUntilConsumed?: boolean;
    groupId?: string;
    groupStep?: string;
    recurrenceId?: string;
    occurrenceAt?: number;
}
export interface QuestStoreOptions {
    path: string;
    maxHistory: number;
    /** How long a claim stays valid without a heartbeat. Default 120s. */
    leaseTtlMs?: number;
    /** Failure retry defaults for quests that don't specify their own. */
    maxAttempts?: number;
    backoffBaseMs?: number;
}
export type DependencyErrorCode = "dependency_not_found" | "dependency_cross_project" | "dependency_self" | "dependency_cycle" | "quest_not_found" | "quest_not_mutable";
export declare class DependencyValidationError extends Error {
    readonly code: DependencyErrorCode;
    readonly questId?: string;
    readonly dependencyId?: string;
    readonly cycle?: string[];
    constructor(code: DependencyErrorCode, message: string, details?: {
        questId?: string;
        dependencyId?: string;
        cycle?: string[];
    });
}
export type QuestGroupKind = "group" | "pipeline";
export type QuestGroupState = "active" | "done" | "failed" | "cancelled" | "mixed" | "empty";
export interface QuestGroupRecord {
    id: string;
    project: string;
    name: string;
    kind: QuestGroupKind;
    pipelineName?: string;
    state: QuestGroupState;
    counts: Record<QuestState, number>;
    questIds: string[];
    createdAt: number;
    updatedAt: number;
}
export interface RecurringQuestInput {
    project: string;
    name: string;
    cron: string;
    timezone?: string;
    role: string;
    task: string;
    context?: string;
    priority?: number;
    maxAttempts?: number;
    backoffBaseMs?: number;
    catchUp?: RecurrenceCatchUp;
    enabled?: boolean;
}
export interface RecurringQuestRecord {
    id: string;
    project: string;
    name: string;
    cron: string;
    timezone: string;
    role: string;
    task: string;
    context?: string;
    priority: number;
    maxAttempts: number;
    backoffBaseMs: number;
    catchUp: RecurrenceCatchUp;
    enabled: boolean;
    nextRunAt: number;
    lastEnqueuedAt?: number;
    createdAt: number;
    updatedAt: number;
}
export interface ProjectQueueControl {
    project: string;
    paused: boolean;
    pausedAt?: number;
    reason?: string;
}
export interface QuestStoreHealth {
    path: string;
    schemaVersion: number;
    integrity: string;
    journalMode: string;
    project?: string;
    counts: Record<QuestState, number>;
    queued: number;
    interrupted: number;
    running: number;
    expiredLeases: number;
    blockedByDependencies: number;
    scheduledLater: number;
    retryBackoff: number;
    overdueQuests: number;
    orphanDependencies: number;
    crossProjectDependencies: number;
    dependencyCycles: number;
    staleSchedulers: number;
    paused: boolean;
    pausedAt?: number;
    pausedReason?: string;
    recurring: {
        enabled: number;
        due: number;
    };
}
export interface QuestlineArchiveV1 {
    format: "questline.archive";
    version: 1;
    exportedAt: string;
    projects: string[];
    groups: QuestGroupRecord[];
    quests: QuestRecord[];
    events: Array<{
        questId: string;
        event: string;
        at: number;
        data: unknown;
    }>;
    runs: QuestRunRecord[];
    artifacts: QuestArtifactRecord[];
    recurrences: RecurringQuestRecord[];
    projectEvents: Array<{
        project: string;
        event: string;
        at: number;
        data: unknown;
    }>;
}
export interface ImportOptions {
    projectMap?: Record<string, string>;
    conflict?: "error" | "skip" | "remap";
}
export interface ImportResult {
    groups: number;
    quests: number;
    recurrences: number;
    skipped: number;
    idMap: Record<string, string>;
}
/** Durable, provider-neutral queue and append-only event journal for agent work. */
export declare class QuestStore {
    private options;
    readonly path: string;
    readonly leaseTtlMs: number;
    private db;
    private defaultMaxAttempts;
    private defaultBackoffBaseMs;
    private inTransaction;
    constructor(options: QuestStoreOptions);
    close(): void;
    health(project?: string): QuestStoreHealth;
    /** Idempotent when dedupeKey is set: an existing active (or done-but-retained,
     *  unconsumed) quest with the same project+key is returned instead of inserting. */
    enqueue(input: EnqueueInput): QuestRecord;
    get(id: string): QuestRecord | undefined;
    list(project: string, limit?: number): QuestRecord[];
    /** Per-attempt telemetry history for a quest, newest first. */
    runs(id: string, limit?: number): QuestRunRecord[];
    artifacts(id: string, attempt?: number): QuestArtifactRecord[];
    claimNext(project: string, ownerSession?: string, options?: {
        ignorePause?: boolean;
        excludeRoles?: string[];
    }): {
        quest: QuestRecord;
        lease: QuestLease;
    } | undefined;
    claimById(id: string, project: string, ownerSession?: string, options?: {
        ignorePause?: boolean;
    }): {
        quest: QuestRecord;
        lease: QuestLease;
    } | undefined;
    private claimRow;
    release(lease: QuestLease): boolean;
    requeue(id: string, project: string, options?: {
        cascadeDependents?: boolean;
    }): boolean;
    heartbeat(lease: QuestLease): boolean;
    attachRun(lease: QuestLease, agentRunId: string): boolean;
    updateTelemetry(lease: QuestLease, t: QuestTelemetry): boolean;
    complete(lease: QuestLease, agentRunId: string, result: string, artifacts?: QuestArtifactInput[]): boolean;
    fail(lease: QuestLease, error: string, agentRunId?: string, artifacts?: QuestArtifactInput[]): boolean;
    cancel(id: string, project: string): boolean;
    markConsumed(id: string, project: string): boolean;
    recoverOwned(project: string, ownerSession: string): number;
    reclaimExpired(project: string): number;
    acquireSchedulerLease(project: string, owner: string, ttlMs: number): boolean;
    releaseSchedulerLease(project: string, owner: string): void;
    events(id: string, limit?: number): Array<{
        event: string;
        at: number;
        data: unknown;
    }>;
    addDependency(questId: string, dependsOn: string, project: string): QuestRecord;
    removeDependency(questId: string, dependsOn: string, project: string): QuestRecord;
    pauseProject(project: string, reason?: string): ProjectQueueControl;
    resumeProject(project: string): ProjectQueueControl;
    projectControl(project: string): ProjectQueueControl;
    createGroup(input: {
        project: string;
        name: string;
        kind?: QuestGroupKind;
        pipelineName?: string;
    }): QuestGroupRecord;
    getGroup(id: string, project: string): QuestGroupRecord | undefined;
    listGroups(project: string, limit?: number): QuestGroupRecord[];
    enqueuePipeline(input: {
        project: string;
        pipelineName: string;
        name?: string;
        definition: PipelineDefinition;
    }): QuestGroupRecord;
    cancelGroup(id: string, project: string): {
        cancelled: number;
        running: number;
    };
    requeueGroup(id: string, project: string): {
        requeued: number;
    };
    createRecurring(input: RecurringQuestInput): RecurringQuestRecord;
    getRecurring(id: string, project: string): RecurringQuestRecord | undefined;
    listRecurring(project: string): RecurringQuestRecord[];
    setRecurringEnabled(id: string, project: string, enabled: boolean): RecurringQuestRecord | undefined;
    deleteRecurring(id: string, project: string): boolean;
    materializeDueRecurring(project: string, now?: number, limit?: number): QuestRecord[];
    exportArchive(options?: {
        project?: string;
    }): QuestlineArchiveV1;
    importArchive(archive: QuestlineArchiveV1, options?: ImportOptions): ImportResult;
    backup(destination: string, options?: {
        overwrite?: boolean;
    }): string;
    private insertQuest;
    private fromRow;
    private validateDependenciesForNewQuest;
    private validateDependencyMutation;
    private findCycleIfAdd;
    private countDependencyCycles;
    private propagateFailedDeps;
    private requeueDependencyFailedDescendants;
    private insertArtifacts;
    private finalizeRun;
    private event;
    private projectEvent;
    private ensureGroup;
    private tx;
    private migrate;
    private prune;
}
//# sourceMappingURL=store.d.ts.map