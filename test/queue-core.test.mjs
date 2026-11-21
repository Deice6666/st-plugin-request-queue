/**
 * queue-core.js 的单元测试（纯 Node，不需要浏览器）
 *
 *   node --test test/
 */

import test from 'node:test';
import assert from 'node:assert/strict';

import { MODE, RequestQueue, makeAbortError } from '../queue-core.js';

const tick = (ms = 0) => new Promise((resolve) => setTimeout(resolve, ms));

/** 造一个不会残留定时器的队列 */
function makeQueue(options = {}) {
    return new RequestQueue({
        settleMs: 0,
        timeoutMs: 0,
        maxWaitMs: 0,
        ...options,
    });
}

test('enqueue 兑现一个 release 函数，重复调用不会出问题', async () => {
    const q = makeQueue();
    const ticket = q.enqueue('alpha');
    // 空队列里第一个请求会被立刻放行
    assert.equal(ticket.state, 'running');

    const release = await ticket.promise;
    assert.equal(typeof release, 'function');
    assert.equal(ticket.state, 'running');
    assert.equal(q.holder, ticket);

    release('done');
    assert.equal(ticket.state, 'finished');
    assert.equal(q.holder, null);

    // 再放一次不应该重复计数
    release('done');
    assert.equal(q.snapshot().stats.alpha.finished, 1);
});

test('同一时刻只有一个占用者，后来的必须等前一个 release', async () => {
    const q = makeQueue();
    const order = [];

    const a = q.enqueue('a');
    const relA = await a.promise;
    order.push('a');

    const b = q.enqueue('b');
    b.promise.then(() => order.push('b'));

    await tick(10);
    assert.deepEqual(order, ['a'], 'a 还没放行时 b 不该拿到锁');

    relA('done');
    await tick(10);
    assert.deepEqual(order, ['a', 'b']);
});

test('priority 模式：排序靠前的先拿到锁，而不是先到的先拿', async () => {
    const priorityOf = (key) => ({ core: 0, fast: 1, slow: 2 })[key] ?? 50;
    const q = makeQueue({ mode: MODE.PRIORITY, priorityOf });
    const order = [];

    const core = q.enqueue('core');
    const relCore = await core.promise;
    order.push('core');

    // 故意让 slow 先到，但它优先级低
    q.enqueue('slow').promise.then(() => order.push('slow'));
    q.enqueue('fast').promise.then(() => order.push('fast'));

    relCore('done');
    await tick(10);
    assert.deepEqual(order, ['core', 'fast']);
});

test('fifo 模式：忽略排序，严格先来先到', async () => {
    const priorityOf = (key) => ({ core: 0, fast: 1, slow: 2 })[key] ?? 50;
    const q = makeQueue({ mode: MODE.FIFO, priorityOf });
    const order = [];

    const core = q.enqueue('core');
    const relCore = await core.promise;
    order.push('core');

    q.enqueue('slow').promise.then(() => order.push('slow'));
    q.enqueue('fast').promise.then(() => order.push('fast'));

    relCore('done');
    await tick(10);
    assert.deepEqual(order, ['core', 'slow']);
});

test('settleMs：上一个结束后要等交接间隔才放行下一个', async () => {
    const q = makeQueue({ settleMs: 60 });
    const order = [];

    const a = q.enqueue('a');
    const relA = await a.promise;
    order.push('a');

    q.enqueue('b').promise.then(() => order.push('b'));

    relA('done');
    await tick(20);
    assert.deepEqual(order, ['a'], '交接间隔没到，不该放行');

    await tick(80);
    assert.deepEqual(order, ['a', 'b'], '交接间隔到了才放行');
});

test('cancel：还在排队的请求会以 AbortError 拒绝，并且不再占位', async () => {
    const q = makeQueue();

    const a = q.enqueue('a');
    const relA = await a.promise;

    const b = q.enqueue('b');
    assert.equal(q.snapshot().waiters.length, 1);

    assert.equal(q.cancel(b, 'user-abort'), true);
    await assert.rejects(b.promise, (err) => err.name === 'AbortError');
    assert.equal(q.snapshot().waiters.length, 0);
    assert.equal(q.snapshot().stats.b.cancelled, 1);

    // 已经拿到锁的不能被 cancel 掉
    assert.equal(q.cancel(a, 'later'), false);
    relA('done');
});

test('timeout：占用超时后强制放行，并调用调用方的 abort', async () => {
    let aborted = false;
    const q = makeQueue({ timeoutMs: 30 });

    const a = q.enqueue('a', { abort: () => { aborted = true; } });
    await a.promise;

    await tick(70);
    assert.equal(aborted, true);
    assert.equal(q.holder, null);
    assert.equal(q.snapshot().stats.a.timeout, 1);
    assert.equal(q.snapshot().history[0].result, 'timeout');
});

test('flush：等待者全部被丢弃，占用者被强制放行', async () => {
    const q = makeQueue();

    const a = q.enqueue('a');
    await a.promise;
    const b = q.enqueue('b');
    const c = q.enqueue('c');

    const dropped = q.flush('test');
    assert.equal(dropped, 2);
    assert.equal(q.holder, null);
    await assert.rejects(b.promise, (err) => err.name === 'AbortError');
    await assert.rejects(c.promise, (err) => err.name === 'AbortError');
    assert.equal(q.snapshot().waiters.length, 0);
});

test('防饥饿：等太久的低优先级请求会被临时提权', async () => {
    const priorityOf = (key) => ({ core: 0, high: 1, low: 9 })[key] ?? 50;
    const q = makeQueue({ priorityOf, maxWaitMs: 25 });
    const order = [];

    const core = q.enqueue('core');
    const relCore = await core.promise;

    q.enqueue('low').promise.then(() => order.push('low'));
    await tick(40); // low 已经等超过 maxWaitMs
    q.enqueue('high').promise.then(() => order.push('high'));

    relCore('done');
    await tick(10);
    assert.deepEqual(order, ['low'], '低优先级等待过久应被提上来');
});

test('snapshot 报告占用者、等待队列与统计', async () => {
    const q = makeQueue();
    const a = q.enqueue('a', { label: '插件 A' });
    const relA = await a.promise;

    q.enqueue('b');

    const snap = q.snapshot();
    assert.equal(snap.holder.key, 'a');
    assert.equal(snap.holder.label, '插件 A');
    assert.equal(snap.waiters.length, 1);
    assert.equal(snap.waiters[0].key, 'b');
    assert.equal(snap.mode, MODE.PRIORITY);

    relA('done');
    await tick(10);

    const after = q.snapshot();
    // a 放行后锁交给下一个等待者 b
    assert.equal(after.holder?.key, 'b');
    assert.equal(after.stats.a.finished, 1);
    assert.equal(after.history.length, 1);
    assert.equal(after.history[0].key, 'a');
    assert.equal(after.history[0].result, 'done');
});

test('resetStats 清空统计与历史，但不动队列', async () => {
    const q = makeQueue();
    const a = q.enqueue('a');
    const relA = await a.promise;
    relA('done');

    assert.equal(q.snapshot().stats.a.finished, 1);
    q.resetStats();
    const snap = q.snapshot();
    assert.deepEqual(snap.stats, {});
    assert.deepEqual(snap.history, []);
});

test('configure 可以热改排序与模式', async () => {
    let table = { core: 0, x: 5, y: 3 };
    const q = makeQueue({ priorityOf: (key) => table[key] ?? 50 });

    const core = q.enqueue('core');
    const relCore = await core.promise;
    const order = [];
    q.enqueue('x').promise.then(() => order.push('x'));
    q.enqueue('y').promise.then(() => order.push('y'));

    // 把 x 提到 y 前面
    table = { core: 0, x: 1, y: 3 };
    q.configure({ priorityOf: (key) => table[key] ?? 50 });

    relCore('done');
    await tick(10);
    assert.deepEqual(order, ['x']);
});

test('makeAbortError 产生名字正确的错误', () => {
    const err = makeAbortError('cancelled');
    assert.equal(err.name, 'AbortError');
});
