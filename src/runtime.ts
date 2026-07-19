/**
 * QuestRuntime: race-safe execution wrapper around QuestStore.
 *
 * Owns lease lifecycle for every quest this session claims. Each claim carries
 * an AbortSignal that is aborted on lease loss or shutdown, so executors can
 * stop their child process tree before another owner produces side effects.
 */
import type { QuestArtifactInput, QuestLease, QuestRecord, QuestStore, QuestTelemetry } from "./store.ts";

export type QuestAbortReason =
  | { kind: "lease-lost"; questId: string }
  | { kind: "shutdown"; questId?: string; signal?: NodeJS.Signals }
  | { kind: "manual"; questId?: string };

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

interface ActiveClaim {
  lease: QuestLease;
  timer: ReturnType<typeof setInterval>;
  controller: AbortController;
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
  claimNext(project: string, options: { ignorePause?: boolean; excludeRoles?: string[] } | (() => void) = {}): ClaimedQuest | undefined {
    if (this.closed) return undefined;
    const onLeaseLost = typeof options === "function" ? options : undefined;
    const claimOptions = typeof options === "function" ? {} : options;
    const claimed = this.store.claimNext(project, this.ownerSession, claimOptions);
    if (!claimed) return undefined;
    return this.track(claimed.lease, claimed.quest, onLeaseLost);
  }

  /** Claim one specific eligible quest and start heartbeating it. */
  claimById(id: string, project: string, options: { ignorePause?: boolean } | (() => void) = {}): ClaimedQuest | undefined {
    if (this.closed) return undefined;
    const onLeaseLost = typeof options === "function" ? options : undefined;
    const claimOptions = typeof options === "function" ? {} : options;
    const claimed = this.store.claimById(id, project, this.ownerSession, claimOptions);
    if (!claimed) return undefined;
    return this.track(claimed.lease, claimed.quest, onLeaseLost);
  }

  /** True while this session still holds the lease for a quest. */
  owns(questId: string): boolean {
    const claim = this.active.get(questId);
    return !!claim && !claim.lost && !claim.controller.signal.aborted;
  }

  /** Backwards-compatible observer hook. Prefer claimed.signal. */
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

  complete(lease: QuestLease, agentRunId: string, result: string, artifacts: QuestArtifactInput[] = []): boolean {
    const ok = this.store.complete(lease, agentRunId, result, artifacts);
    if (ok) this.release(lease.id);
    return ok;
  }

  fail(lease: QuestLease, error: string, agentRunId?: string, artifacts: QuestArtifactInput[] = []): boolean {
    const ok = this.store.fail(lease, error, agentRunId, artifacts);
    if (ok) this.release(lease.id);
    return ok;
  }

  /** Hand a claimed quest back to the queue without recording a failure. */
  releaseClaim(lease: QuestLease): boolean {
    const ok = this.store.release(lease);
    if (ok) this.release(lease.id);
    return ok;
  }

  async run(
    claimed: ClaimedQuest,
    dispatch: (quest: QuestRecord, report: (t: QuestTelemetry) => void, signal: AbortSignal) => Promise<{ agentRunId: string; ok: boolean; text: string }>,
  ): Promise<{ ok: boolean; text: string }> {
    const { quest, lease, signal } = claimed;
    try {
      const outcome = await dispatch(quest, (t) => this.updateTelemetry(lease, t), signal);
      if (signal.aborted) return { ok: false, text: "aborted" };
      const settled = outcome.ok ? this.complete(lease, outcome.agentRunId, outcome.text) : this.fail(lease, outcome.text, outcome.agentRunId);
      return { ok: outcome.ok && settled, text: outcome.text };
    } catch (err) {
      if (!signal.aborted) this.fail(lease, String(err));
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

  /** Abort all local workers and prevent new claims; call finishShutdown after children exit. */
  beginShutdown(reason: Omit<Extract<QuestAbortReason, { kind: "shutdown" }>, "questId"> = { kind: "shutdown" }): void {
    this.closed = true;
    for (const [id, claim] of this.active) {
      if (!claim.controller.signal.aborted) claim.controller.abort({ ...reason, questId: id });
    }
  }

  /** Stop all heartbeats and hand remaining running work back as interrupted. */
  finishShutdown(project: string): number {
    this.closed = true;
    for (const id of [...this.active.keys()]) this.release(id);
    return this.store.recoverOwned(project, this.ownerSession);
  }

  /** Backwards-compatible shutdown: abort then immediately recover. */
  shutdown(project: string): void {
    this.beginShutdown();
    this.finishShutdown(project);
  }

  private track(lease: QuestLease, quest: QuestRecord, onLeaseLost?: () => void): ClaimedQuest {
    const controller = new AbortController();
    const claim: ActiveClaim = { lease, onLeaseLost, lost: false, controller, timer: setInterval(() => {
      let ok = false;
      try {
        ok = this.store.heartbeat(lease);
      } catch {
        return; // transient (e.g. db busy) — keep the timer, retry next tick
      }
      if (!ok) {
        claim.lost = true;
        this.release(lease.id);
        if (!controller.signal.aborted) controller.abort({ kind: "lease-lost", questId: lease.id } satisfies QuestAbortReason);
        try { claim.onLeaseLost?.(); } catch { /* observer error must not kill the loop */ }
      }
    }, this.heartbeatMs) };
    claim.timer.unref?.();
    this.active.set(lease.id, claim);
    return { quest, lease, signal: controller.signal };
  }
}
