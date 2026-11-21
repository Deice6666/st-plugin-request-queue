/**
 * queue-core.js —— 纯逻辑的请求串行队列。
 *
 * 这一层不碰 DOM、不碰 SillyTavern、不碰 jQuery，所以可以直接在 Node 里跑单元测试。
 * index.js 只负责“判断谁在发请求 + 画界面”，排队算法全部在下面这个类里。
 */

const nowMs = () =>
    typeof performance !== 'undefined' && typeof performance.now === 'function'
        ? performance.now()
        : Date.now();

export const MODE = Object.freeze({ PRIORITY: 'priority', FIFO: 'fifo' });

/**
 * @param {unknown} value
 * @param {number} min
 * @param {number} max
 * @param {number} fallback
 * @returns {number}
 */
function clampNumber(value, min, max, fallback) {
    const n = typeof value === 'number' ? value : Number.parseFloat(String(value));
    if (!Number.isFinite(n)) return fallback;
    return Math.min(max, Math.max(min, Math.round(n)));
}

export function makeAbortError(reason = 'aborted') {
    try {
        return new DOMException(reason, 'AbortError');
    } catch {
        const err = new Error(reason);
        err.name = 'AbortError';
        return err;
    }
}

export class RequestQueue {
    /**
     * @param {object} [options]
     * @param {'priority'|'fifo'} [options.mode]
     * @param {number} [options.settleMs] 上一个请求放行后，再等多久才允许下一个开始
     * @param {number} [options.timeoutMs] 单个请求最长占用时间，超时强制放行（0 = 不限制）
     * @param {number} [options.maxWaitMs] 防饥饿：等待超过这个时间的请求优先升级
     * @param {(key: string) => number} [options.priorityOf] 数字越小越先执行
     * @param {(kind: string, message: string) => void} [options.logger]
     * @param {number} [options.historyLimit]
     */
    constructor(options = {}) {
        this.mode = options.mode === MODE.FIFO ? MODE.FIFO : MODE.PRIORITY;
        this.settleMs = clampNumber(options.settleMs, 0, 5000, 200);
        this.timeoutMs = clampNumber(options.timeoutMs, 0, 3600000, 300000);
        this.maxWaitMs = clampNumber(options.maxWaitMs, 0, 600000, 45000);
        this.priorityOf = typeof options.priorityOf === 'function' ? options.priorityOf : () => 9999;
        this.logger = typeof options.logger === 'function' ? options.logger : () => { };
        this.historyLimit = clampNumber(options.historyLimit, 1, 200, 20);

        /** @type {Array<any>} */
        this.waiters = [];
        /** @type {any|null} */
        this.holder = null;
        this.seq = 0;
        /** @type {Array<any>} */
        this.history = [];
        /** @type {Map<string, any>} */
        this.stats = new Map();
        /** @type {Map<string, Set<Function>>} */
        this.listeners = new Map();
        this.settleTimer = null;
    }

    /** @param {object} patch */
    configure(patch = {}) {
        if ('mode' in patch) this.mode = patch.mode === MODE.FIFO ? MODE.FIFO : MODE.PRIORITY;
        if ('settleMs' in patch) this.settleMs = clampNumber(patch.settleMs, 0, 5000, this.settleMs);
        if ('timeoutMs' in patch) this.timeoutMs = clampNumber(patch.timeoutMs, 0, 3600000, this.timeoutMs);
        if ('maxWaitMs' in patch) this.maxWaitMs = clampNumber(patch.maxWaitMs, 0, 600000, this.maxWaitMs);
        if (typeof patch.priorityOf === 'function') {
            this.priorityOf = patch.priorityOf;
            // 用户改了排序：已经在排队、还没轮到的请求也按新顺序重算优先级
            for (const waiter of this.waiters) {
                const next = Number(this.priorityOf(waiter.key));
                waiter.priority = Number.isFinite(next) ? next : 9999;
            }
        }
        if (typeof patch.logger === 'function') this.logger = patch.logger;
        if ('historyLimit' in patch) this.historyLimit = clampNumber(patch.historyLimit, 1, 200, this.historyLimit);
        if (!this.holder) this.pump();
        this.emit('change');
        return this;
    }

    /**
     * 把一个请求排进队列。拿到的 ticket.promise 会在轮到它时兑现一个 release 函数。
     * @param {string} key 归属者（插件目录名 / __core__）
     * @param {Record<string, any>} [meta] 附加信息（url、label、abort…）
     */
    enqueue(key, meta = {}) {
        const ticket = {
            id: ++this.seq,
            key: String(key || 'unknown'),
            label: String(meta.label || key || 'unknown'),
            meta,
            priority: Number(this.priorityOf(key)) || 0,
            state: 'waiting',
            enqueuedAt: nowMs(),
            grantedAt: 0,
            finishedAt: 0,
            waitMs: 0,
            runMs: 0,
            released: false,
            resolve: null,
            reject: null,
            promise: null,
            timeoutTimer: null,
        };

        ticket.promise = new Promise((resolve, reject) => {
            ticket.resolve = resolve;
            ticket.reject = reject;
        });

        this.waiters.push(ticket);
        this.bumpStats(ticket.key, 'queued');
        this.logger('enqueue', `${ticket.key} 进入队列（前面还有 ${this.waiters.length - 1} 个）`);
        this.emit('change');
        this.pump();
        return ticket;
    }

    /**
     * 等轮到自己。
     * @param {string} key
     * @param {Record<string, any>} [meta]
     * @returns {Promise<(result?: string) => void>}
     */
    async acquire(key, meta = {}) {
        const ticket = this.enqueue(key, meta);
        return await ticket.promise;
    }

    /**
     * 还没轮到就被调用方放弃（例如 AbortSignal 触发）。
     * @param {any} ticket
     * @param {string} [reason]
     */
    cancel(ticket, reason = 'aborted') {
        if (!ticket || ticket.state !== 'waiting') return false;
        const index = this.waiters.indexOf(ticket);
        if (index >= 0) this.waiters.splice(index, 1);
        ticket.state = 'cancelled';
        this.bumpStats(ticket.key, 'cancelled');
        this.logger('cancel', `${ticket.key} 放弃排队：${reason}`);
        ticket.reject(makeAbortError(reason));
        this.emit('change');
        this.pump();
        return true;
    }

    /**
     * 放行：结束当前占用，安排下一个。
     * @param {any} ticket
     * @param {string} [result] done / error / timeout / cancelled / no-body …
     */
    release(ticket, result = 'done') {
        if (!ticket || ticket.released) return;
        ticket.released = true;
        if (ticket.timeoutTimer) {
            clearTimeout(ticket.timeoutTimer);
            ticket.timeoutTimer = null;
        }
        ticket.state = 'finished';
        ticket.finishedAt = nowMs();
        ticket.runMs = ticket.grantedAt ? ticket.finishedAt - ticket.grantedAt : 0;
        this.bumpStats(ticket.key, 'finished', ticket.runMs);
        this.pushHistory({
            key: ticket.key,
            label: ticket.label,
            waitMs: Math.round(ticket.waitMs),
            runMs: Math.round(ticket.runMs),
            result,
            at: Date.now(),
        });
        this.logger('release', `${ticket.key} 结束（${result}，耗时 ${Math.round(ticket.runMs)}ms）`);

        if (this.holder === ticket) {
            this.holder = null;
            if (this.settleMs > 0 && this.waiters.length > 0) {
                if (this.settleTimer) clearTimeout(this.settleTimer);
                this.settleTimer = setTimeout(() => {
                    this.settleTimer = null;
                    this.pump();
                }, this.settleMs);
            } else {
                this.pump();
            }
        }
        this.emit('change');
    }

    /** 真正把锁交给下一个请求 */
    pump() {
        if (this.settleTimer) {
            clearTimeout(this.settleTimer);
            this.settleTimer = null;
        }
        if (this.holder) return;
        if (this.waiters.length === 0) return;
        const index = this.pickNext();
        if (index < 0) return;
        this.grant(this.waiters[index]);
    }

    /**
     * 挑下一个：priority 模式下按 (优先级, 到达序号) 排序，
     * 但等待超过 maxWaitMs 的请求会被临时提到最前，避免低优先级永远饿死。
     * @returns {number} 下标
     */
    pickNext() {
        if (this.waiters.length === 0) return -1;
        if (this.mode === MODE.FIFO) return 0;

        const t = nowMs();
        let best = 0;
        for (let i = 1; i < this.waiters.length; i++) {
            const a = this.waiters[i];
            const b = this.waiters[best];
            const aStarved = this.maxWaitMs > 0 && t - a.enqueuedAt >= this.maxWaitMs;
            const bStarved = this.maxWaitMs > 0 && t - b.enqueuedAt >= this.maxWaitMs;
            if (aStarved !== bStarved) {
                if (aStarved) best = i;
                continue;
            }
            if (a.priority !== b.priority) {
                if (a.priority < b.priority) best = i;
                continue;
            }
            if (a.id < b.id) best = i;
        }
        return best;
    }

    /** @param {any} ticket */
    grant(ticket) {
        const index = this.waiters.indexOf(ticket);
        if (index >= 0) this.waiters.splice(index, 1);
        this.holder = ticket;
        ticket.state = 'running';
        ticket.grantedAt = nowMs();
        ticket.waitMs = ticket.grantedAt - ticket.enqueuedAt;
        this.bumpStats(ticket.key, 'started', ticket.waitMs);
        this.logger('grant', `${ticket.key} 开始执行（排队 ${Math.round(ticket.waitMs)}ms）`);

        if (this.timeoutMs > 0) {
            ticket.timeoutTimer = setTimeout(() => {
                ticket.timeoutTimer = null;
                this.bumpStats(ticket.key, 'timeout');
                this.logger('timeout', `${ticket.key} 占用超过 ${this.timeoutMs}ms，强制放行下一个`);
                try {
                    ticket.meta?.abort?.();
                } catch { /* 调用方自己的 abort 出错不该影响队列 */ }
                this.release(ticket, 'timeout');
            }, this.timeoutMs);
        }

        ticket.resolve((result = 'done') => this.release(ticket, result));
        this.emit('change');
    }

    /**
     * 清空队列：丢掉的等待者统一以 AbortError 拒绝，当前占用者被强制放行。
     * @param {string} [reason]
     */
    flush(reason = 'flushed') {
        const pending = this.waiters.slice();
        this.waiters = [];
        if (this.settleTimer) {
            clearTimeout(this.settleTimer);
            this.settleTimer = null;
        }
        for (const ticket of pending) {
            ticket.state = 'cancelled';
            this.bumpStats(ticket.key, 'cancelled');
            ticket.reject(makeAbortError(reason));
        }
        const holder = this.holder;
        if (holder) this.release(holder, 'flush');
        this.emit('change');
        return pending.length;
    }

    /** 给界面看的快照 */
    snapshot() {
        const t = nowMs();
        return {
            mode: this.mode,
            settleMs: this.settleMs,
            timeoutMs: this.timeoutMs,
            holder: this.holder
                ? {
                    key: this.holder.key,
                    label: this.holder.label,
                    elapsedMs: Math.round(t - this.holder.grantedAt),
                }
                : null,
            waiters: this.waiters.map((w) => ({
                key: w.key,
                label: w.label,
                waitedMs: Math.round(t - w.enqueuedAt),
                priority: w.priority,
            })),
            history: this.history.slice(0, this.historyLimit),
            stats: Object.fromEntries([...this.stats].map(([key, value]) => [key, { ...value }])),
        };
    }

    resetStats() {
        this.stats.clear();
        this.history = [];
        this.emit('change');
    }

    /** @param {string} event @param {Function} handler */
    on(event, handler) {
        if (!this.listeners.has(event)) this.listeners.set(event, new Set());
        this.listeners.get(event).add(handler);
        return () => this.off(event, handler);
    }

    /** @param {string} event @param {Function} handler */
    off(event, handler) {
        this.listeners.get(event)?.delete(handler);
    }

    emit(event, payload) {
        const set = this.listeners.get(event);
        if (!set) return;
        for (const handler of [...set]) {
            try {
                handler(payload);
            } catch (err) {
                this.logger('error', `监听器异常：${err?.message || err}`);
            }
        }
    }

    /** @param {string} key @param {string} field @param {number} [value] */
    bumpStats(key, field, value) {
        let s = this.stats.get(key);
        if (!s) {
            s = {
                queued: 0,
                started: 0,
                finished: 0,
                cancelled: 0,
                timeout: 0,
                waitMsTotal: 0,
                runMsTotal: 0,
                lastWaitMs: 0,
                lastRunMs: 0,
                lastAt: 0,
            };
            this.stats.set(key, s);
        }
        if (field === 'started') {
            s.started += 1;
            s.waitMsTotal += value || 0;
            s.lastWaitMs = Math.round(value || 0);
        } else if (field === 'finished') {
            s.finished += 1;
            s.runMsTotal += value || 0;
            s.lastRunMs = Math.round(value || 0);
            s.lastAt = Date.now();
        } else if (field in s) {
            s[field] += 1;
        }
    }

    /** @param {any} item */
    pushHistory(item) {
        this.history.unshift(item);
        if (this.history.length > 200) this.history.length = 200;
    }
}
