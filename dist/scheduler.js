export class QuestScheduler {
    options;
    timer;
    inFlight = new Map();
    inFlightRoles = new Map();
    leaseHeld = false;
    pausedReason;
    lastTickAt;
    draining = false;
    stopped = false;
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
        this.stopped = false;
        this.timer = setInterval(() => this.tick(), this.pollMs);
        if (this.options.unrefTimer !== false)
            this.timer.unref?.();
        this.tick();
    }
    stop() {
        this.stopped = true;
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
    async waitForIdle() {
        while (this.inFlight.size)
            await Promise.allSettled([...this.inFlight.values()]);
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
        if (this.draining || this.stopped)
            return;
        this.draining = true;
        try {
            this.lastTickAt = Date.now();
            const { runtime, project } = this.options;
            this.leaseHeld = runtime.store.acquireSchedulerLease(project, runtime.ownerSession, this.leaseTtlMs);
            if (!this.leaseHeld)
                return;
            runtime.store.materializeDueRecurring(project);
            const durable = runtime.store.projectControl(project);
            this.pausedReason = durable.paused ? durable.reason ?? "queue paused" : this.options.isPaused?.();
            if (this.pausedReason)
                return;
            while (!this.stopped && this.inFlight.size < this.maxConcurrent) {
                const excluded = this.excludedRoles();
                const claimed = runtime.claimNext(project, { excludeRoles: excluded });
                if (!claimed)
                    break;
                const id = claimed.quest.id;
                this.inFlightRoles.set(claimed.quest.role, (this.inFlightRoles.get(claimed.quest.role) ?? 0) + 1);
                const promise = Promise.resolve()
                    .then(() => this.options.dispatch(claimed))
                    .catch((err) => this.options.onError?.(err))
                    .finally(() => {
                    this.inFlight.delete(id);
                    this.inFlightRoles.set(claimed.quest.role, Math.max(0, (this.inFlightRoles.get(claimed.quest.role) ?? 1) - 1));
                    if (this.timer && !this.stopped)
                        this.tick();
                });
                this.inFlight.set(id, promise);
            }
        }
        catch (err) {
            this.options.onError?.(err);
        }
        finally {
            this.draining = false;
        }
    }
    excludedRoles() {
        const caps = this.options.roleConcurrency ?? {};
        const excluded = [];
        for (const [role, cap] of Object.entries(caps)) {
            if ((this.inFlightRoles.get(role) ?? 0) >= cap)
                excluded.push(role);
        }
        return excluded;
    }
}
