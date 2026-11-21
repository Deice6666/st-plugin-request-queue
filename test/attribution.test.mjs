/**
 * 归属判定的单元测试。
 *
 * 场景来自真实酒馆：小白X（LittleWhiteBox）会全局包一层 fetch，
 * 它的栈帧紧贴在我们外面，导致所有请求都被算成它发的。
 */

import test from 'node:test';
import assert from 'node:assert/strict';

// index.js 顶层会碰 document / fetch，先摆一套最小的假环境再动态载入
if (!globalThis.document) {
    globalThis.document = {
        readyState: 'loading',
        addEventListener() { },
        getElementById() { return null; },
        querySelector() { return null; },
    };
}
if (typeof globalThis.fetch !== 'function') {
    globalThis.fetch = async () => ({ ok: true, text: async () => '' });
}

const { parseStack, pickInitiator } = await import('../index.js');

const SELF = '/scripts/extensions/third-party/st-plugin-request-queue/';

function stack(lines) {
    return ['Error'].concat(lines.map((line) => `    at ${line}`)).join('\n');
}

const selfFrame1 = `attribute (http://127.0.0.1:8000${SELF}index.js:370:19)`;
const selfFrame2 = `async queuedFetch (http://127.0.0.1:8000${SELF}index.js:500:20)`;

const lwbDebug = 'async Object.fetch (http://127.0.0.1:8000/scripts/extensions/third-party/LittleWhiteBox/modules/debug-panel/debug-panel.js:95:27)';
const lwbPreview = 'Object.fetch (http://127.0.0.1:8000/scripts/extensions/third-party/LittleWhiteBox/modules/message-preview.js:846:20)';

const qrPlugin = 'async o (http://127.0.0.1:8000/scripts/extensions/third-party/qr/index.js:120:9)';
const chatu8 = 'send (http://127.0.0.1:8000/scripts/extensions/third-party/st-chatu8/index.js:44:13)';
const novel = 'async handler (http://127.0.0.1:8000/scripts/extensions/third-party/sillytavern-novel-chapter-injector/index.js:88:5)';

const coreGenerate = 'async Object.generate (http://127.0.0.1:8000/scripts/custom-request.js:463:9)';
const coreSend = 'async sendMessageAsUser (http://127.0.0.1:8000/script.js:6175:9)';

test('parseStack 分得出 self / third / inner / core', () => {
    const frames = parseStack(stack([selfFrame1, selfFrame2, lwbDebug, qrPlugin, coreGenerate]), SELF);
    assert.deepEqual(frames.map((f) => f.kind), ['self', 'self', 'third', 'third', 'core']);
    assert.equal(frames[2].name, 'LittleWhiteBox');
    assert.equal(frames[3].name, 'qr');
});

test('同一个请求从别的插件发出来时，不会被包了全局 fetch 的小白X抢走归属', () => {
    const frames = parseStack(stack([selfFrame1, selfFrame2, lwbDebug, qrPlugin]), SELF);
    const { who, learned } = pickInitiator(frames, new Set());
    assert.equal(who.key, 'qr', '真正的发起者是栈更外层的 qr，不是转发层小白X');
    assert.equal(who.kind, 'third-party');
    assert.ok(learned.has('LittleWhiteBox'), '小白X 应该被学成转发层，下次直接跳过');
});

test('学到的转发层会被排除；只剩转发层时归酒馆本体', () => {
    const frames = parseStack(stack([selfFrame1, selfFrame2, lwbPreview, coreGenerate]), SELF);
    const { who } = pickInitiator(frames, new Set(['LittleWhiteBox']));
    assert.equal(who.key, '__core__', '小白X 只是转发，真正发起的是酒馆自己');
    assert.equal(who.kind, 'core');
});

test('栈里只剩一个未登记的包装器时，不会把它误学成转发层', () => {
    const frames = parseStack(stack([selfFrame1, selfFrame2, lwbPreview, coreGenerate]), SELF);
    const { who } = pickInitiator(frames, new Set());
    assert.equal(who.key, 'LittleWhiteBox', '没有别的候选时，宁可算成它，也不硬猜');
});

test('探针学到的转发层会被排除，转发来的请求归真正发起者', () => {
    const frames = parseStack(stack([selfFrame1, selfFrame2, lwbPreview, qrPlugin]), SELF);
    const { who } = pickInitiator(frames, new Set(['LittleWhiteBox']));
    assert.equal(who.key, 'qr', '小白X 是转发层，真正的发起者是 qr');
});

test('插件直接发请求（没有转发层）时归属正确', () => {
    const frames = parseStack(stack([selfFrame1, selfFrame2, chatu8]), SELF);
    const { who } = pickInitiator(frames, new Set());
    assert.equal(who.key, 'st-chatu8');
});

test('多层转发取最外层：A 转 B，B 转我们，发起者是 A', () => {
    const frames = parseStack(stack([selfFrame1, selfFrame2, chatu8, novel]), SELF);
    const { who, learned } = pickInitiator(frames, new Set());
    assert.equal(who.key, 'sillytavern-novel-chapter-injector', '最外层那个才是发起者');
    assert.ok(learned.has('st-chatu8'));
});

test('纯酒馆本体的请求归 __core__', () => {
    const frames = parseStack(stack([selfFrame1, selfFrame2, coreSend]), SELF);
    const { who } = pickInitiator(frames, new Set());
    assert.equal(who.key, '__core__');
});

test('内置扩展目录算各自的名字，不会被当成 third-party', () => {
    const frames = parseStack(
        stack([selfFrame1, 'Object.run (http://127.0.0.1:8000/scripts/extensions/vectors/index.js:12:3)']),
        SELF,
    );
    assert.equal(frames.find((f) => f.kind === 'inner').name, 'vectors');
    const { who } = pickInitiator(frames, new Set());
    assert.equal(who.key, 'vectors');
    assert.equal(who.kind, 'extension');
});

test('空栈 / 无信息时退化为酒馆本体，不抛错', () => {
    assert.equal(pickInitiator(parseStack('', SELF)).who.key, '__core__');
    assert.equal(pickInitiator([], new Set()).who.key, '__core__');
});

test('只剩包装器时，算在最外层那层壳身上（JS-Slash-Runner 里跑的用户脚本就是这种情形）', () => {
    // 真实栈：小白X 在外、JS-Slash-Runner 在它下面，真正的发起者（用户脚本）不留帧
    const jsr = 'async fetch (http://127.0.0.1:8000/scripts/extensions/third-party/JS-Slash-Runner/index.js:220:15)';
    const frames = parseStack(stack([selfFrame1, selfFrame2, lwbDebug, jsr, 'async <anonymous> (eval at run)']), SELF);
    const { who } = pickInitiator(frames, new Set(['LittleWhiteBox', 'JS-Slash-Runner']));
    assert.equal(who.key, 'JS-Slash-Runner', '脚本的帧丢了，就归替它发请求的那个壳');
    assert.equal(who.kind, 'third-party');
});

test('只剩转发层但栈里有本体帧时，仍然算酒馆本体', () => {
    const jsr = 'async fetch (http://127.0.0.1:8000/scripts/extensions/third-party/JS-Slash-Runner/index.js:220:15)';
    const frames = parseStack(stack([selfFrame1, selfFrame2, lwbDebug, jsr, coreGenerate]), SELF);
    const { who } = pickInitiator(frames, new Set(['LittleWhiteBox', 'JS-Slash-Runner']));
    assert.equal(who.key, '__core__');
    assert.equal(who.kind, 'core');
});
