export class QuestScheduler {
    options;
    timer;
    inFlight = new Set();
    leaseHeld = false;
    pausedReason;
    lastTickAt;
    draining = false;
    pollMs;
    maxConcurrent;
    leaseTtlMs;
    constructor(options) {
        this.options = options;
        this.pollMs = Math.max(20, options.pollMs ?? 5000);
        this.maxConcurrent = Math.max(1, options.maxConcurrent ?? 2);
        this.leaseTtlMs = Math.max(this.pollMs * 3, options.leaseTtlMs ?? this.pollMs * 3);
    }
    start() {
        if (this.timer)
            return;
        this.timer = setInterval(() => this.tick(), this.pollMs);
        if (this.options.unrefTimer !== false)
            this.timer.unref?.();
        this.tick();
    }
    stop() {
        if (this.timer)
            clearInterval(this.timer);
        this.timer = undefined;
        if (this.leaseHeld) {
            try {
                this.options.runtime.store.releaseSchedulerLease(this.options.project, this.options.runtime.ownerSession);
            }
            catch { /* store may already be closed */ }
            this.leaseHeld = false;
        }
    }
    status() {
        return {
            running: this.timer !== undefined,
            inFlight: this.inFlight.size,
            leaseHeld: this.leaseHeld,
            pausedReason: this.pausedReason,
            lastTickAt: this.lastTickAt,
        };
    }
    /** Run one drain pass now (also called on every interval tick). */
    tick() {
        if (this.draining)
            return;
        this.draining = true;
        try {
            this.lastTickAt = Date.now();
            const { runtime, project } = this.options;
            this.pausedReason = this.options.isPaused?.();
            if (this.pausedReason)
                return;
            this.leaseHeld = runtime.store.acquireSchedulerLease(project, runtime.ownerSession, this.leaseTtlMs);
            if (!this.leaseHeld)
                return;
            while (this.inFlight.size < this.maxConcurrent) {
                const claimed = runtime.claimNext(project);
                if (!claimed)
                    break;
                const id = claimed.quest.id;
                this.inFlight.add(id);
                Promise.resolve()
                    .then(() => this.options.dispatch(claimed))
                    .catch((err) => this.options.onError?.(err))
                    .finally(() => {
                    this.inFlight.delete(id);
                    // A finished quest may unblock dependents — drain again promptly.
                    if (this.timer)
                        this.tick();
                });
            }
        }
        catch (err) {
            this.options.onError?.(err);
        }
        finally {
            this.draining = false;
        }
    }
}
