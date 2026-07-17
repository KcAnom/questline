/**
 * QuestScheduler: continuous, crash-safe draining of the durable quest queue.
 *
 * One scheduler per project across all sessions (enforced by the store's
 * scheduler lease — losing the lease pauses claiming, it never double-runs
 * work). Eligibility itself lives in QuestStore.claimNext: priority order,
 * not-before schedules, retry backoff, dependency gates, and expired-lease
 * reclamation all apply to scheduled claims exactly as they do to manual ones.
 * A pause hook lets the host stop new claims (e.g. session cost cap) without
 * touching work already in flight.
 */
import type { ClaimedQuest, QuestRuntime } from "./runtime.ts";

export interface QuestSchedulerOptions {
  runtime: QuestRuntime;
  project: string;
  /** Drain-loop interval. Default 5000ms. */
  pollMs?: number;
  /** Max quests this scheduler keeps in flight at once. Default 2. */
  maxConcurrent?: number;
  /** Scheduler-lease TTL; renewed every tick. Default 3× pollMs. */
  leaseTtlMs?: number;
  /** Return a human-readable reason to pause claiming, or undefined to run. */
  isPaused?: () => string | undefined;
  /** unref() the poll timer so it never keeps the process alive (embedded
   *  hosts). A standalone daemon MUST pass false or it exits immediately. */
  unrefTimer?: boolean;
  /** Execute a claimed quest to settlement (complete/fail is the callee's job,
   *  typically via QuestRuntime.run or the extension's dispatch path). */
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

export class QuestScheduler {
  private timer?: ReturnType<typeof setInterval>;
  private inFlight = new Set<string>();
  private leaseHeld = false;
  private pausedReason?: string;
  private lastTickAt?: number;
  private draining = false;
  private readonly pollMs: number;
  private readonly maxConcurrent: number;
  private readonly leaseTtlMs: number;

  constructor(private readonly options: QuestSchedulerOptions) {
    this.pollMs = Math.max(20, options.pollMs ?? 5000);
    this.maxConcurrent = Math.max(1, options.maxConcurrent ?? 2);
    this.leaseTtlMs = Math.max(this.pollMs * 3, options.leaseTtlMs ?? this.pollMs * 3);
  }

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => this.tick(), this.pollMs);
    if (this.options.unrefTimer !== false) (this.timer as { unref?: () => void }).unref?.();
    this.tick();
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
    if (this.leaseHeld) {
      try {
        this.options.runtime.store.releaseSchedulerLease(this.options.project, this.options.runtime.ownerSession);
      } catch { /* store may already be closed */ }
      this.leaseHeld = false;
    }
  }

  status(): QuestSchedulerStatus {
    return {
      running: this.timer !== undefined,
      inFlight: this.inFlight.size,
      leaseHeld: this.leaseHeld,
      pausedReason: this.pausedReason,
      lastTickAt: this.lastTickAt,
    };
  }

  /** Run one drain pass now (also called on every interval tick). */
  tick(): void {
    if (this.draining) return;
    this.draining = true;
    try {
      this.lastTickAt = Date.now();
      const { runtime, project } = this.options;
      this.pausedReason = this.options.isPaused?.();
      if (this.pausedReason) return;
      this.leaseHeld = runtime.store.acquireSchedulerLease(project, runtime.ownerSession, this.leaseTtlMs);
      if (!this.leaseHeld) return;
      while (this.inFlight.size < this.maxConcurrent) {
        const claimed = runtime.claimNext(project);
        if (!claimed) break;
        const id = claimed.quest.id;
        this.inFlight.add(id);
        Promise.resolve()
          .then(() => this.options.dispatch(claimed))
          .catch((err) => this.options.onError?.(err))
          .finally(() => {
            this.inFlight.delete(id);
            // A finished quest may unblock dependents — drain again promptly.
            if (this.timer) this.tick();
          });
      }
    } catch (err) {
      this.options.onError?.(err);
    } finally {
      this.draining = false;
    }
  }
}
