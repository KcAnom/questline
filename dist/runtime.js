export class QuestRuntime {
    store;
    ownerSession;
    heartbeatMs;
    active = new Map();
    closed = false;
    constructor(options) {
        this.store = options.store;
        this.ownerSession = options.ownerSession;
        this.heartbeatMs = Math.max(50, options.heartbeatMs ?? Math.floor(options.store.leaseTtlMs / 3));
    }
    /** Claim the next eligible quest and start heartbeating it. */
    claimNext(project, options = {}) {
        if (this.closed)
            return undefined;
        const onLeaseLost = typeof options === "function" ? options : undefined;
        const claimOptions = typeof options === "function" ? {} : options;
        const claimed = this.store.claimNext(project, this.ownerSession, claimOptions);
        if (!claimed)
            return undefined;
        return this.track(claimed.lease, claimed.quest, onLeaseLost);
    }
    /** Claim one specific eligible quest and start heartbeating it. */
    claimById(id, project, options = {}) {
        if (this.closed)
            return undefined;
        const onLeaseLost = typeof options === "function" ? options : undefined;
        const claimOptions = typeof options === "function" ? {} : options;
        const claimed = this.store.claimById(id, project, this.ownerSession, claimOptions);
        if (!claimed)
            return undefined;
        return this.track(claimed.lease, claimed.quest, onLeaseLost);
    }
    /** True while this session still holds the lease for a quest. */
    owns(questId) {
        const claim = this.active.get(questId);
        return !!claim && !claim.lost && !claim.controller.signal.aborted;
    }
    /** Backwards-compatible observer hook. Prefer claimed.signal. */
    setLeaseLostHandler(questId, cb) {
        const claim = this.active.get(questId);
        if (claim)
            claim.onLeaseLost = cb;
    }
    attachRun(lease, agentRunId) {
        return this.store.attachRun(lease, agentRunId);
    }
    updateTelemetry(lease, t) {
        return this.store.updateTelemetry(lease, t);
    }
    complete(lease, agentRunId, result, artifacts = []) {
        const ok = this.store.complete(lease, agentRunId, result, artifacts);
        if (ok)
            this.release(lease.id);
        return ok;
    }
    fail(lease, error, agentRunId, artifacts = []) {
        const ok = this.store.fail(lease, error, agentRunId, artifacts);
        if (ok)
            this.release(lease.id);
        return ok;
    }
    /** Hand a claimed quest back to the queue without recording a failure. */
    releaseClaim(lease) {
        const ok = this.store.release(lease);
        if (ok)
            this.release(lease.id);
        return ok;
    }
    async run(claimed, dispatch) {
        const { quest, lease, signal } = claimed;
        try {
            const outcome = await dispatch(quest, (t) => this.updateTelemetry(lease, t), signal);
            if (signal.aborted)
                return { ok: false, text: "aborted" };
            const settled = outcome.ok ? this.complete(lease, outcome.agentRunId, outcome.text) : this.fail(lease, outcome.text, outcome.agentRunId);
            return { ok: outcome.ok && settled, text: outcome.text };
        }
        catch (err) {
            if (!signal.aborted)
                this.fail(lease, String(err));
            throw err;
        }
    }
    /** Stop heartbeating a quest without writing state (complete/fail do this). */
    release(questId) {
        const claim = this.active.get(questId);
        if (!claim)
            return;
        clearInterval(claim.timer);
        this.active.delete(questId);
    }
    /** Abort all local workers and prevent new claims; call finishShutdown after children exit. */
    beginShutdown(reason = { kind: "shutdown" }) {
        this.closed = true;
        for (const [id, claim] of this.active) {
            if (!claim.controller.signal.aborted)
                claim.controller.abort({ ...reason, questId: id });
        }
    }
    /** Stop all heartbeats and hand remaining running work back as interrupted. */
    finishShutdown(project) {
        this.closed = true;
        for (const id of [...this.active.keys()])
            this.release(id);
        return this.store.recoverOwned(project, this.ownerSession);
    }
    /** Backwards-compatible shutdown: abort then immediately recover. */
    shutdown(project) {
        this.beginShutdown();
        this.finishShutdown(project);
    }
    track(lease, quest, onLeaseLost) {
        const controller = new AbortController();
        const claim = { lease, onLeaseLost, lost: false, controller, timer: setInterval(() => {
                let ok = false;
                try {
                    ok = this.store.heartbeat(lease);
                }
                catch {
                    return; // transient (e.g. db busy) — keep the timer, retry next tick
                }
                if (!ok) {
                    claim.lost = true;
                    this.release(lease.id);
                    if (!controller.signal.aborted)
                        controller.abort({ kind: "lease-lost", questId: lease.id });
                    try {
                        claim.onLeaseLost?.();
                    }
                    catch { /* observer error must not kill the loop */ }
                }
            }, this.heartbeatMs) };
        claim.timer.unref?.();
        this.active.set(lease.id, claim);
        return { quest, lease, signal: controller.signal };
    }
}
