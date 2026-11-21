/**
 * index.js 的集成测试：在 Node 里用假的 document / SillyTavern / fetch 跑真实的拦截逻辑。
 *
 * 重点是验证核心承诺：
 *   —— 上一个请求的响应体读完（流关闭）之前，下一个请求根本不会发出去。
 */

import test from 'node:test';
import assert from 'node:assert/strict';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/* ---------------- 伪造浏览器 / SillyTavern 环境 ---------------- */

const events = [];

function fakeElement() {
    return {
        id: '',
        innerHTML: '',
        className: '',
        title: '',
        type: '',
        checked: false,
        value: '',
        isConnected: false,
        childElementCount: 0,
        replaceChildren() { },
        append() { },
        appendChild() { },
        remove() { },
        addEventListener() { },
        setAttribute() { },
        getAttribute() { return null; },
        querySelector() { return null; },
        closest() { return null; },
    };
}

globalThis.document = {
    readyState: 'complete',
    addEventListener() { },
    getElementById() { return null; },
    createElement() { return fakeElement(); },
    querySelector() { return null; },
};

globalThis.location = { href: 'http://127.0.0.1:8000/' };
globalThis.toastr = { info() { } };

const extensionSettings = {};
globalThis.SillyTavern = {
    getContext: () => ({
        extensionSettings,
        saveSettingsDebounced() { },
        getRequestHeaders: () => ({ 'x-test': '1' }),
        async renderExtensionTemplateAsync() {
            throw new Error('测试环境没有模板文件');
        },
    }),
};

function makeStreamingResponse(name, chunks, delayMs) {
    let index = 0;
    const encoder = new TextEncoder();
    const stream = new ReadableStream({
        async pull(controller) {
            if (index >= chunks.length) {
                events.push(`${name}:body-end`);
                controller.close();
                return;
            }
            await sleep(delayMs);
            controller.enqueue(encoder.encode(chunks[index++]));
        },
    });
    return new Response(stream, { status: 200, headers: { 'content-type': 'text/event-stream' } });
}

const nativeCalls = [];
let failNext = null;
globalThis.fetch = async (input, init) => {
    const url = typeof input === 'string' ? input : input?.url ?? '';
    if (url.includes('/api/extensions/discover')) {
        return new Response(JSON.stringify([{ name: 'third-party/JS-Slash-Runner' }, 'third-party/qr']), {
            status: 200,
            headers: { 'content-type': 'application/json' },
        });
    }
    const tag = String(init?.body ?? url);
    nativeCalls.push(tag);
    if (failNext) {
        const message = failNext;
        failNext = null;
        events.push(`fetch-throw:${tag}`);
        throw new Error(message);
    }
    events.push(`fetch:${tag}`);
    return makeStreamingResponse(tag, ['data: a\n\n', 'data: b\n\n', 'data: [DONE]\n\n'], 15);
};

/* ---------------- 加载被测扩展 ---------------- */

const mod = await import('../index.js');
const internal = mod.__internal;

await sleep(50); // 等 boot() 里的初始化跑完
internal.queue.configure({ settleMs: 0, timeoutMs: 0, maxWaitMs: 0 });
internal.settings.idleMs = 0; // 测试里不想留下定时器

const GENERATE = '/api/backends/chat-completions/generate';

function reset() {
    events.length = 0;
    nativeCalls.length = 0;
    internal.settings.enabled = true;
    internal.settings.queueCore = true;
    internal.settings.bypass = [];
}

test('脚本 iframe 的请求按脚本名排队：两个脚本不会同时发出去', async () => {
    reset();
    internal.settings.splitScripts = true;
    internal.settings.seen = [];

    const p1 = mod.__scriptFetch('脚本甲', GENERATE, { method: 'POST', body: 'a1' });
    const p2 = mod.__scriptFetch('脚本乙', GENERATE, { method: 'POST', body: 'b1' });
    const r1 = await p1;
    const t1 = await r1.text();
    const r2 = await p2;
    const t2 = await r2.text();

    assert.match(t1, /DONE/);
    assert.match(t2, /DONE/);
    const at = (needle) => events.indexOf(needle);
    assert.ok(at('fetch:a1') >= 0, '脚本甲的请求应该发出去');
    assert.ok(at('fetch:b1') > at('a1:body-end'), '脚本乙必须等脚本甲的响应体读完');
    assert.ok(internal.settings.seen.includes('script:脚本甲'), '脚本甲要记进“发过请求”的名单里');
    assert.ok(internal.settings.seen.includes('script:脚本乙'), '脚本乙要记进“发过请求”的名单里');
});

test('两个并发请求被串起来：第二个必须等第一个读完响应体', async () => {
    reset();

    const first = globalThis.fetch(GENERATE, { method: 'POST', body: 'first' });
    const second = globalThis.fetch(GENERATE, { method: 'POST', body: 'second' });

    const res1 = await first;
    const text1 = await res1.text();
    const res2 = await second;
    const text2 = await res2.text();

    assert.match(text1, /DONE/);
    assert.match(text2, /DONE/);

    const at = (needle) => events.indexOf(needle);
    assert.ok(at('fetch:first') >= 0, '第一个请求应该发出去');
    assert.ok(at('first:body-end') >= 0, '第一个响应体应该被读完');
    assert.ok(at('fetch:second') > at('first:body-end'), '第二个请求必须在第一个读完响应体之后才开始');
    assert.deepEqual(nativeCalls, ['first', 'second'], '同一时刻只允许一个请求在飞');
});

test('关掉总开关后恢复并行发送', async () => {
    reset();
    internal.settings.enabled = false;

    const p1 = globalThis.fetch(GENERATE, { method: 'POST', body: 'first' });
    const p2 = globalThis.fetch(GENERATE, { method: 'POST', body: 'second' });

    await sleep(10);
    assert.deepEqual(nativeCalls, ['first', 'second'], '关闭后两个请求应立刻同时发出');

    await (await p1).text();
    await (await p2).text();
});

test('把本体排除后，本体请求不排队', async () => {
    reset();
    internal.settings.queueCore = false;

    const p1 = globalThis.fetch(GENERATE, { method: 'POST', body: 'first' });
    const p2 = globalThis.fetch(GENERATE, { method: 'POST', body: 'second' });

    await sleep(10);
    assert.deepEqual(nativeCalls, ['first', 'second']);

    await (await p1).text();
    await (await p2).text();
});

test('主动取消响应体也会释放队列', async () => {
    reset();

    const first = await globalThis.fetch(GENERATE, { method: 'POST', body: 'first' });
    const secondPromise = globalThis.fetch(GENERATE, { method: 'POST', body: 'second' });

    await sleep(20);
    assert.deepEqual(nativeCalls, ['first'], '第二个还没被放行');

    await first.body.cancel('用户中断');
    const res2 = await secondPromise;
    await res2.text();

    assert.deepEqual(nativeCalls, ['first', 'second'], '取消后应立即把锁交给下一个');
});

test('只有生成接口被拦，普通接口直接放行', async () => {
    reset();

    const p1 = globalThis.fetch('/api/characters/all', { method: 'POST' });
    const p2 = globalThis.fetch('/api/backends/text-completions/status', { method: 'POST' });
    const p3 = globalThis.fetch(GENERATE, { method: 'POST', body: 'gen' });

    await sleep(10);
    assert.ok(nativeCalls.includes('/api/characters/all'));
    assert.ok(nativeCalls.includes('/api/backends/text-completions/status'));

    await (await p1).text();
    await (await p2).text();
    await (await p3).text();
});

test('附加规则里的 URL 片段也会被排队', async () => {
    reset();
    internal.settings.extraPatterns = ['/api/plugins/custom-plugin/run'];

    const p1 = globalThis.fetch('/api/plugins/custom-plugin/run', { method: 'POST', body: 'x' });
    const p2 = globalThis.fetch('/api/plugins/custom-plugin/run', { method: 'POST', body: 'y' });

    const r1 = await p1;
    await r1.text();
    const r2 = await p2;
    await r2.text();

    assert.deepEqual(nativeCalls, ['x', 'y']);
    assert.ok(events.indexOf('fetch:y') > events.indexOf('x:body-end'));
});

test('请求抛错时也会释放队列，不会把后面全卡死', async () => {
    reset();
    failNext = 'network down';

    await assert.rejects(
        () => globalThis.fetch(GENERATE, { method: 'POST', body: 'boom' }),
        /network down/,
    );

    const res = await globalThis.fetch(GENERATE, { method: 'POST', body: 'after' });
    assert.match(await res.text(), /DONE/);
    assert.deepEqual(nativeCalls, ['boom', 'after'], '第一个失败后第二个应该能正常发出');
});

test('deriveSelfIdentity：不管文件夹叫什么名字，模板路径都跟着真实目录走', () => {
    // 用户实际安装的目录名
    const real = mod.deriveSelfIdentity('/scripts/extensions/third-party/st-plugin-request-queue/');
    assert.equal(real.dir, 'st-plugin-request-queue');
    assert.equal(real.name, 'third-party/st-plugin-request-queue');
    assert.equal(real.path, '/scripts/extensions/third-party/st-plugin-request-queue/');

    // 改名的目录
    const renamed = mod.deriveSelfIdentity('/scripts/extensions/third-party/my-queue/');
    assert.equal(renamed.name, 'third-party/my-queue');

    // 末尾没有斜杠也能认
    assert.equal(mod.deriveSelfIdentity('/scripts/extensions/third-party/x').dir, 'x');

    // 路径不在 extensions 下（比如别处引用）也不炸，回落到 third-party/<dir>
    assert.equal(mod.deriveSelfIdentity('/opt/foo/bar/').name, 'third-party/bar');
});

test('模板返回 undefined 时绝不把 undefined 写进 DOM', async () => {
    const writes = [];
    const host = { append(el) { writes.push(el); } };
    const prevGet = globalThis.document.getElementById;
    const prevCreate = globalThis.document.createElement;
    const prevGetContext = globalThis.SillyTavern.getContext;
    const base = prevGetContext();

    globalThis.document.getElementById = (id) => (id === 'extensions_settings2' ? host : null);
    globalThis.document.createElement = () => {
        const el = fakeElement();
        Object.defineProperty(el, 'innerHTML', {
            get() { return ''; },
            set(v) { writes.push({ bad: v }); },
        });
        return el;
    };
    // 真实 ST 在模板 404 时就是这个行为：不抛错，返回 undefined
    globalThis.SillyTavern.getContext = () => ({ ...base, renderExtensionTemplateAsync: async () => undefined });

    internal.uiRoot = null;
    try {
        await mod.__mountUi();
    } finally {
        globalThis.document.getElementById = prevGet;
        globalThis.document.createElement = prevCreate;
        globalThis.SillyTavern.getContext = prevGetContext;
        internal.uiRoot = null;
    }
    assert.deepEqual(writes, [], '模板失败时不应该往 DOM 里塞任何东西');
});

test('面板默认收起，展开/收起的选择会被记住', () => {
    assert.equal(internal.settings.collapsed, true, '面板默认应该是收起的，避免一进扩展页就占满屏');
    mod.__setCollapsed(false);
    assert.equal(internal.settings.collapsed, false);
    assert.equal(extensionSettings['st-plugin-request-queue']?.collapsed, false, '选择要写进设置，刷新后仍然生效');
    mod.__setCollapsed(true);
    assert.equal(internal.settings.collapsed, true);
    assert.equal(extensionSettings['st-plugin-request-queue']?.collapsed, true);
});

test('请求顺序列表只显示另外安装的插件，内置扩展和本体都不出现', () => {
    const prevDiscovered = internal.discovered;
    const prevOrder = internal.settings.order;
    const prevOnlyActive = internal.settings.onlyActive;
    internal.settings.onlyActive = false; // 关掉“只显示发过请求的”，看完整名单
    internal.queue.resetStats(); // 前面用例留下的 script: 统计不属于这条断言
    try {
        internal.discovered = mod.__normalizeDiscovery([
            { name: 'assets', type: 'system' },
            { name: 'translate', type: 'system' },
            { name: 'third-party/JS-Slash-Runner', type: 'global' },
            { name: 'third-party/st-plugin-request-queue', type: 'global' },
            { name: 'third-party/qr', type: 'global' },
        ]);
        internal.settings.order = [];
        const keys = mod.__currentKeys();
        assert.deepEqual(keys, ['JS-Slash-Runner', 'qr'], '内置扩展与本扩展自己都不该出现在列表里');
        assert.ok(!keys.includes('__core__'), '本体也不进列表，它在调度里恒为最高优先级');
    } finally {
        internal.discovered = prevDiscovered;
        internal.settings.order = prevOrder;
        internal.settings.onlyActive = prevOnlyActive;
    }
});

test('默认只列发过请求的插件和脚本，纯界面类的插件不占位置', () => {
    const prevDiscovered = internal.discovered;
    const prevOrder = internal.settings.order;
    const prevSeen = internal.settings.seen;
    const prevOnlyActive = internal.settings.onlyActive;
    try {
        internal.settings.onlyActive = true;
        internal.discovered = mod.__normalizeDiscovery([
            { name: 'third-party/JS-Slash-Runner', type: 'global' },
            { name: 'third-party/qr', type: 'global' },
            { name: 'third-party/MirrorTranslate', type: 'global' },
        ]);
        internal.settings.order = [];
        internal.settings.seen = [];
        internal.queue.resetStats(); // 清掉前面用例留下的 script: 统计
        assert.deepEqual(mod.__currentKeys(), [], '一个请求都没发过时列表是空的');

        internal.settings.seen = ['MirrorTranslate'];
        assert.deepEqual(mod.__currentKeys(), ['MirrorTranslate'], '发过请求的才出现在列表里');

        internal.settings.seen = ['MirrorTranslate', 'script:摘要测试'];
        const keys = mod.__currentKeys();
        assert.ok(keys.includes('script:摘要测试'), '脚本发过请求也会作为一行出现');
        assert.ok(!keys.includes('qr'), '没发过请求的插件仍然不出现');
    } finally {
        internal.discovered = prevDiscovered;
        internal.settings.order = prevOrder;
        internal.settings.seen = prevSeen;
        internal.settings.onlyActive = prevOnlyActive;
    }
});

test('旧设置里混进来的内置扩展会被清掉，插件之间仍按自定义顺序排优先级', () => {
    const prevDiscovered = internal.discovered;
    const prevOrder = internal.settings.order;
    const prevOnlyActive = internal.settings.onlyActive;
    internal.settings.onlyActive = false;
    internal.queue.resetStats();
    try {
        internal.discovered = mod.__normalizeDiscovery([
            { name: 'assets', type: 'system' },
            { name: 'third-party/JS-Slash-Runner', type: 'global' },
            { name: 'third-party/qr', type: 'global' },
        ]);
        // 旧版本存下来的顺序里带着内置扩展
        internal.settings.order = ['assets', 'qr', '__core__', 'JS-Slash-Runner'];
        assert.deepEqual(mod.__currentKeys(), ['qr', 'JS-Slash-Runner'], '内置扩展不该被列出来');

        assert.equal(mod.__priorityOf('__core__'), 0, '本体最高，先发你的对话');
        assert.ok(
            mod.__priorityOf('qr') < mod.__priorityOf('JS-Slash-Runner'),
            '排在列表前面的插件优先级更高',
        );
        assert.ok(
            mod.__priorityOf('没排过的插件') > mod.__priorityOf('JS-Slash-Runner'),
            '没进排序列表的插件排在最后',
        );
    } finally {
        internal.discovered = prevDiscovered;
        internal.settings.order = prevOrder;
        internal.settings.onlyActive = prevOnlyActive;
    }
});

test('别的插件把 fetch 包了一层时，队列退回被动模式：包装器照常跑、请求只排一次队', async () => {
    reset();
    const patched = globalThis.fetch;
    assert.equal(typeof patched, 'function');

    // 模拟“消息预览/调试面板”这类插件：在队列外面再包一层，往里调到队列
    const native = internal.nativeFetch;
    let relayHits = 0;
    const relay = async (input, init) => {
        relayHits += 1;
        return patched(input, init);
    };
    globalThis.fetch = relay;

    try {
        const res = await globalThis.fetch(GENERATE, { method: 'POST', body: 'through-relay' });
        await res.text();

        assert.equal(relayHits, 1, '中间那层包装器应该被正常调用一次');
        assert.deepEqual(nativeCalls, ['through-relay'], '同一请求绝不能排两次队（否则自己等自己）');
        assert.equal(globalThis.fetch, relay, '队列不去抢外层位置，包装器继续待在它以为的位置');
        assert.equal(internal.nativeFetch, native, '队列的下一层保持不变，不把包装器当成自己的下游');
    } finally {
        globalThis.fetch = patched;
        internal.nativeFetch = native;
    }
});
