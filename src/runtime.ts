/**
 * QuestRuntime: race-safe execution wrapper around QuestStore.
 *
 * Owns the lease lifecycle for every quest this session claims: heartbeats on
 * an interval while the work runs, detects a lost lease (expired + reclaimed
 * elsewhere) and aborts the local worker, and guarantees the claim is always
 * settled — complete, fail, or recovered as interrupted on shutdown. All
 * write-backs go through the store's lease fencing, so a stale worker can
 * never overwrite a reclaimed quest (at-least-once semantics).
 */
import type { QuestLease, QuestRecord, QuestStore, QuestTelemetry } from "./store.ts";

export interface ClaimedQuest {
  quest: QuestRecord;
  lease: QuestLease;
}

export interface QuestRuntimeOptions {
  store: QuestStore;
  ownerSession: string;
  /** Heartbeat interval. Default: a third of the store's lease TTL. */
  heartbeatMs?: number;
}

interface ActiveClaim {
  lease: QuestLease;
  timer: ReturnType<typeof setInterval>;
  onLeaseLost?: () => void;
  lost: boolean;
}

export class QuestRuntime {
  readonly store: QuestStore;
  readonly ownerSession: string;
  private heartbeatMs: number;
  private active = new Map<string, ActiveClaim>();
  private closed = false;

  constructor(options: QuestRuntimeOptions) {
    this.store = options.store;
    this.ownerSession = options.ownerSession;
    this.heartbeatMs = Math.max(50, options.heartbeatMs ?? Math.floor(options.store.leaseTtlMs / 3));
  }

  /** Claim the next eligible quest and start heartbeating it. */
  claimNext(project: string, onLeaseLost?: () => void): ClaimedQuest | undefined {
    if (this.closed) return undefined;
    const claimed = this.store.claimNext(project, this.ownerSession);
    if (!claimed) return undefined;
    this.track(claimed.lease, onLeaseLost);
    return claimed;
  }

  /** Claim one specific eligible quest and start heartbeating it. */
  claimById(id: string, project: string, onLeaseLost?: () => void): ClaimedQuest | undefined {
    if (this.closed) return undefined;
    const claimed = this.store.claimById(id, project, this.ownerSession);
    if (!claimed) return undefined;
    this.track(claimed.lease, onLeaseLost);
    return claimed;
  }

  /** True while this session still holds the lease for a quest. */
  owns(questId: string): boolean {
    const claim = this.active.get(questId);
    return !!claim && !claim.lost;
  }

  /** Register (or replace) the lost-lease callback for an active claim —
   *  typically set after the worker is spawned, so it can be aborted. */
  setLeaseLostHandler(questId: string, cb: () => void): void {
    const claim = this.active.get(questId);
    if (claim) claim.onLeaseLost = cb;
  }

  attachRun(lease: QuestLease, agentRunId: string): boolean {
    return this.store.attachRun(lease, agentRunId);
  }

  updateTelemetry(lease: QuestLease, t: QuestTelemetry): boolean {
    return this.store.updateTelemetry(lease, t);
  }

  complete(lease: QuestLease, agentRunId: string, result: string): boolean {
    this.release(lease.id);
    return this.store.complete(lease, agentRunId, result);
  }

  fail(lease: QuestLease, error: string, agentRunId?: string): boolean {
    this.release(lease.id);
    return this.store.fail(lease, error, agentRunId);
  }

  /** Hand a claimed quest back to the queue without recording a failure. */
  releaseClaim(lease: QuestLease): boolean {
    this.release(lease.id);
    return this.store.release(lease);
  }

  /**
   * Run a claimed quest to a settled state: dispatch the work, then complete
   * or fail under the lease. The dispatch function receives a telemetry
   * reporter it may call as the run progresses.
   */
  async run(
    claimed: ClaimedQuest,
    dispatch: (quest: QuestRecord, report: (t: QuestTelemetry) => void) => Promise<{ agentRunId: string; ok: boolean; text: string }>,
  ): Promise<{ ok: boolean; text: string }> {
    const { quest, lease } = claimed;
    try {
      const outcome = await dispatch(quest, (t) => this.updateTelemetry(lease, t));
      if (outcome.ok) this.complete(lease, outcome.agentRunId, outcome.text);
      else this.fail(lease, outcome.text, outcome.agentRunId);
      return { ok: outcome.ok, text: outcome.text };
    } catch (err) {
      this.fail(lease, String(err));
      throw err;
    }
  }

  /** Stop heartbeating a quest without writing state (complete/fail do this). */
  release(questId: string): void {
    const claim = this.active.get(questId);
    if (!claim) return;
    clearInterval(claim.timer);
    this.active.delete(questId);
  }

  /** Stop all heartbeats and hand running work back as interrupted. */
  shutdown(project: string): void {
    this.closed = true;
    for (const id of [...this.active.keys()]) this.release(id);
    this.store.recoverOwned(project, this.ownerSession);
  }

  private track(lease: QuestLease, onLeaseLost?: () => void): void {
    const claim: ActiveClaim = { lease, onLeaseLost, lost: false, timer: setInterval(() => {
      let ok = false;
      try {
        ok = this.store.heartbeat(lease);
      } catch {
        return; // transient (e.g. db busy) — keep the timer, retry next tick
      }
      if (!ok) {
        claim.lost = true;
        this.release(lease.id);
        try { claim.onLeaseLost?.(); } catch { /* observer error must not kill the loop */ }
      }
    }, this.heartbeatMs) };
    claim.timer.unref?.();
    this.active.set(lease.id, claim);
  }
}
