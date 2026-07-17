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
    claimNext(project, onLeaseLost) {
        if (this.closed)
            return undefined;
        const claimed = this.store.claimNext(project, this.ownerSession);
        if (!claimed)
            return undefined;
        this.track(claimed.lease, onLeaseLost);
        return claimed;
    }
    /** Claim one specific eligible quest and start heartbeating it. */
    claimById(id, project, onLeaseLost) {
        if (this.closed)
            return undefined;
        const claimed = this.store.claimById(id, project, this.ownerSession);
        if (!claimed)
            return undefined;
        this.track(claimed.lease, onLeaseLost);
        return claimed;
    }
    /** True while this session still holds the lease for a quest. */
    owns(questId) {
        const claim = this.active.get(questId);
        return !!claim && !claim.lost;
    }
    /** Register (or replace) the lost-lease callback for an active claim —
     *  typically set after the worker is spawned, so it can be aborted. */
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
    complete(lease, agentRunId, result) {
        this.release(lease.id);
        return this.store.complete(lease, agentRunId, result);
    }
    fail(lease, error, agentRunId) {
        this.release(lease.id);
        return this.store.fail(lease, error, agentRunId);
    }
    /** Hand a claimed quest back to the queue without recording a failure. */
    releaseClaim(lease) {
        this.release(lease.id);
        return this.store.release(lease);
    }
    /**
     * Run a claimed quest to a settled state: dispatch the work, then complete
     * or fail under the lease. The dispatch function receives a telemetry
     * reporter it may call as the run progresses.
     */
    async run(claimed, dispatch) {
        const { quest, lease } = claimed;
        try {
            const outcome = await dispatch(quest, (t) => this.updateTelemetry(lease, t));
            if (outcome.ok)
                this.complete(lease, outcome.agentRunId, outcome.text);
            else
                this.fail(lease, outcome.text, outcome.agentRunId);
            return { ok: outcome.ok, text: outcome.text };
        }
        catch (err) {
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
    /** Stop all heartbeats and hand running work back as interrupted. */
    shutdown(project) {
        this.closed = true;
        for (const id of [...this.active.keys()])
            this.release(id);
        this.store.recoverOwned(project, this.ownerSession);
    }
    track(lease, onLeaseLost) {
        const claim = { lease, onLeaseLost, lost: false, timer: setInterval(() => {
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
                    try {
                        claim.onLeaseLost?.();
                    }
                    catch { /* observer error must not kill the loop */ }
                }
            }, this.heartbeatMs) };
        claim.timer.unref?.();
        this.active.set(lease.id, claim);
    }
}
