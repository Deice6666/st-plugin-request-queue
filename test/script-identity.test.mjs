/**
 * 脚本身份（JS-Slash-Runner 里的用户脚本）相关测试。
 *
 * 背景：那个插件把每个脚本放在 id 为 TH-script--<脚本名>--<id> 的同源 iframe 里，
 * 脚本自己那层 fetch 是原生的，直接调就绕过队列 —— 既排不上队，也认不出是谁。
 * 这里测的就是：认名字、桥接 fetch、以及给插件接口套来源标记。
 */

import test from 'node:test';
import assert from 'node:assert/strict';

const ORIGIN = Symbol.for('st-request-queue.origin');

/** 假的原生 fetch：把收到的参数记下来 */
const calls = [];
const nativeFetch = async (input, init) => {
    calls.push({ input, init });
    return { ok: true, status: 200, body: null, headers: {} };
};

if (!globalThis.document) {
    globalThis.document = {
        readyState: 'loading',
        addEventListener() { },
        getElementById() { return null; },
        querySelector() { return null; },
        querySelectorAll() { return []; },
        body: { tagName: 'BODY' },
    };
}
globalThis.fetch = nativeFetch;

const mod = await import('../index.js');
const {
    parseScriptFrameId,
    scriptKeyOf,
    scriptNameOfKey,
    __internal: internal,
    __bridgeScriptFrame: bridgeScriptFrame,
    __wrapScriptApis: wrapScriptApis,
    __runWithOrigin: runWithOrigin,
    __readOrigin: readOrigin,
    __whoForScript: whoForScript,
    __scriptNameFromWindowLike: scriptNameFromWindowLike,
} = mod;

function makeFrame(id, win = {}) {
    return {
        tagName: 'IFRAME',
        id,
        name: id,
        contentWindow: win,
        addEventListener() { },
    };
}

test('认得出脚本 iframe 的名字，不认消息 iframe', () => {
    assert.equal(parseScriptFrameId('TH-script--我的剧本--1712345678'), '我的剧本');
    assert.equal(parseScriptFrameId('TH-script--a--b--c--9'), 'a--b--c');
    assert.equal(parseScriptFrameId('TH-message--12--1'), null);
    assert.equal(parseScriptFrameId(''), null);
    assert.equal(parseScriptFrameId(undefined), null);
});

test('脚本名和队列键能来回换', () => {
    const key = scriptKeyOf('我的剧本');
    assert.equal(key, 'script:我的剧本');
    assert.equal(scriptNameOfKey(key), '我的剧本');
    assert.equal(scriptNameOfKey('JS-Slash-Runner'), null);
    assert.equal(scriptNameOfKey('script:'), null);
});

test('桥接脚本 iframe：请求走主页面 fetch，并带上脚本名', async () => {
    const win = {};
    const frame = makeFrame('TH-script--摘要器--42', win);
    calls.length = 0;

    assert.equal(bridgeScriptFrame(frame), true);
    assert.equal(typeof win.fetch, 'function');

    await win.fetch('/api/x', { method: 'POST' });
    const last = calls[calls.length - 1];
    assert.equal(last.input, '/api/x');
    assert.equal(last.init.method, 'POST');
    assert.equal(last.init[ORIGIN], '摘要器');
});

test('同一个 iframe 不会重复桥接', () => {
    const win = {};
    const frame = makeFrame('TH-script--重复--7', win);
    assert.equal(bridgeScriptFrame(frame), true);
    assert.equal(bridgeScriptFrame(frame), false);
});

test('消息 iframe 和非 iframe 一律不碰', () => {
    const win = {};
    assert.equal(bridgeScriptFrame(makeFrame('TH-message--3--1', win)), false);
    assert.equal(bridgeScriptFrame({ tagName: 'DIV', id: 'TH-script--x--1', contentWindow: win }), false);
    assert.equal(win.fetch, undefined);
});

test('脚本请求的身份：可拆分，也可合并到插件名下', () => {
    internal.settings.splitScripts = true;
    assert.deepEqual(whoForScript('摘要器'), {
        key: 'script:摘要器',
        label: '脚本 · 摘要器',
        kind: 'third-party',
        script: '摘要器',
    });

    internal.settings.splitScripts = false;
    assert.equal(whoForScript('摘要器').key, 'JS-Slash-Runner');
    internal.settings.splitScripts = true;
});

test('runWithOrigin 跨 await 保留标记，结束后清干净', async () => {
    assert.equal(readOrigin(), null);
    await runWithOrigin('甲', async () => {
        assert.equal(readOrigin(), '甲');
        await new Promise((resolve) => setTimeout(resolve, 1));
        assert.equal(readOrigin(), '甲');
        await runWithOrigin('乙', async () => {
            assert.equal(readOrigin(), '乙'); // 取最内层
        });
        assert.equal(readOrigin(), '甲');
    });
    assert.equal(readOrigin(), null);
});

test('runWithOrigin 出错也会把标记清掉', async () => {
    await assert.rejects(runWithOrigin('丙', async () => {
        throw new Error('boom');
    }), /boom/);
    assert.equal(readOrigin(), null);
});

test('显式记号优先于当前正在跑的脚本', () => {
    internal.originStack.push('甲');
    try {
        assert.equal(readOrigin({ [ORIGIN]: '乙' }), '乙');
        assert.equal(readOrigin({}), '甲');
        assert.equal(readOrigin(null), '甲');
    } finally {
        internal.originStack.length = 0;
    }
});

test('从 this（脚本 window）认出脚本体', () => {
    assert.equal(scriptNameFromWindowLike({ __TH_IFRAME_ID: 'TH-script--侦探--5' }), '侦探');
    assert.equal(scriptNameFromWindowLike({ name: 'TH-script--侦探--5' }), '侦探');
    assert.equal(scriptNameFromWindowLike({ frameElement: { id: 'TH-script--侦探--5' } }), '侦探');
    assert.equal(scriptNameFromWindowLike({ name: 'TH-message--1--1' }), null);
    assert.equal(scriptNameFromWindowLike(null), null);
});

test('给 TavernHelper 的脚本接口套上来源标记', () => {
    const seen = [];
    globalThis.TavernHelper = {
        _bind: {
            _eventEmit() {
                seen.push(readOrigin());
                return 'ok';
            },
            _notAFunction: 42,
        },
    };

    assert.equal(wrapScriptApis(), true);
    const wrapped = globalThis.TavernHelper._bind._eventEmit;
    assert.equal(wrapped.__rqOriginWrapped, true);

    // 脚本里调用时，this 就是脚本自己的 window
    const result = wrapped.call({ __TH_IFRAME_ID: 'TH-script--旁白--9' });
    assert.equal(result, 'ok');
    assert.deepEqual(seen, ['旁白']);

    // 不是脚本调的（认不出 this）就原样执行，不留标记
    seen.length = 0;
    wrapped.call({});
    assert.deepEqual(seen, [null]);

    // 重复包装没有意义，第二次必须什么都不做
    assert.equal(wrapScriptApis(), false);
    delete globalThis.TavernHelper;
});
