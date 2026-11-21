/**
 * 插件请求队列 —— SillyTavern 扩展
 *
 * 解决的问题：多个插件同时向 ST 后端发“生成”请求，回复乱序返回、
 * 又各自往正文里插入内容，于是串消息 / 覆盖 / 楼层错乱。
 *
 * 做法：只拦截一条最窄的缝 —— 生成类 HTTP 请求。任何插件（以及 ST 本体）
 * 调 /api/backends/<backend>/generate 时，都要先向队列申请令牌；拿到令牌才能真正发出去，
 * 并且令牌要等到响应体被读完（流式输出真正结束）才归还。
 *
 * 归属识别：拦到请求时抓一次调用栈，从栈帧里认出是哪个扩展目录发起的。
 */

import { MODE, RequestQueue } from './queue-core.js';

const VERSION = '1.0.0';

/**
 * 从自身路径推出「我是谁」：目录名、模板要用的扩展名、栈帧判断前缀。
 * 抽成纯函数是为了可测试，也为了用户改文件夹名后依然正确。
 * @param {string} pathname 形如 /scripts/extensions/third-party/xxx/
 */
export function deriveSelfIdentity(pathname) {
    const clean = String(pathname || '');
    const dir = clean.replace(/\/+$/, '').split('/').pop() || 'st-plugin-request-queue';
    const root = '/scripts/extensions/';
    const index = clean.indexOf(root);
    const name = index >= 0
        ? clean.slice(index + root.length).replace(/\/+$/, '')
        : `third-party/${dir}`;
    return { dir, name, path: clean };
}

const SELF_URL = new URL('.', import.meta.url);
const SELF_IDENTITY = deriveSelfIdentity(SELF_URL.pathname);
/** 传给 renderExtensionTemplateAsync 用的扩展名，形如 third-party/xxx */
const EXTENSION_NAME = SELF_IDENTITY.name;
/** 设置里的存储键：用真实目录名 */
const MODULE_KEY = SELF_IDENTITY.dir;
/** 判断栈帧是否属于自己 */
const OWN_DIR_MARK = SELF_IDENTITY.path;
/** 标记在 fetch 函数上：记录被包装前的原函数，避免重复接管 */
const PATCH_MARK = Symbol.for('st-request-queue.native-fetch');
const CORE_KEY = '__core__';
const CORE_LABEL = 'SillyTavern 本体';

/**
 * JS-Slash-Runner 把每个用户脚本跑在同源 iframe 里，
 * iframe 的 id / name 都是 `TH-script--<脚本名>--<脚本id>`。
 * 脚本自己的 realm 里那层 fetch 不是我们改过的，直接调就绕过队列，
 * 所以要把这层 fetch 桥接回主页面，并顺手带上脚本名。
 */
// 脚本名允许带 "--"，插件自己也是按“最后一个 --”切出脚本 id 的（贪婪匹配）
const JSR_SCRIPT_FRAME = /^TH-script--(.+)--/;
const SCRIPT_KEY_PREFIX = 'script:';
/** 不拆分时，脚本请求统一记在这个名字下 */
const SCRIPT_HOST = 'JS-Slash-Runner';
/** 跨 realm 传递“这次是哪个脚本发的”的记号（全局符号注册表跨 realm 共享） */
const ORIGIN = Symbol.for('st-request-queue.origin');
const BRIDGE_MARK = '__rqBridgedScript';
const BRIDGE_ORIGINAL = '__rqNativeFetch';

/** 栈帧里连续被认成“夹在中间”多少次，才把这个插件定为转发层 */
const RELAY_CONFIRM = 2;

/** 所有生成类后端接口 */
const BACKEND_GENERATE = /\/api\/backends\/[a-z0-9_-]+\/generate(?:[?#]|$)/i;

const DEFAULT_SETTINGS = {
    enabled: true,
    /** priority：按列表顺序优先（后到的高优先级可插队）；fifo：严格先到先得 */
    mode: MODE.PRIORITY,
    /** ST 本体的生成请求是否也排队（建议开：避免插件请求和你的楼层抢） */
    queueCore: true,
    /** 上一个结束后再等多久才放行下一个，给插件留出“写进正文”的时间 */
    settleMs: 200,
    /** 单个请求最长占用时间，超时强制放行，防止卡死整条队列（0 = 不限） */
    timeoutMs: 600000,
    /** 响应体拿到手却一直没人读，超过这么久就把锁让出去 */
    idleMs: 20000,
    /** 防饥饿：等待这么久还没轮到，就临时提权 */
    maxWaitMs: 45000,
    /** 用户自定义的执行顺序（只存第三方扩展的目录名；本体恒为最高优先级） */
    order: [],
    /** 完全不排队、直接放行的扩展目录名 */
    bypass: [],
    /** 手动指定“转发层”插件：它们只是包了一层 fetch，不算发起者 */
    relays: [],
    /** 额外要排队的 URL 片段或 /正则/ */
    extraPatterns: [],
    /** 脚本运行时（JS-Slash-Runner）里的每个脚本单独成行，还是都记在插件名下 */
    splitScripts: true,
    /** 只列出发过请求的插件/脚本（纯界面类的插件永远不会发请求，列出来只是噪音） */
    onlyActive: true,
    /** 曾经发过请求的 key（持久化，避免刷新后面板空掉） */
    seen: [],
    /** 设置面板是否收起（收起后只剩一行标题，不占地方） */
    collapsed: true,
    debug: false,
};

const state = {
    /** @type {any} */
    settings: { ...DEFAULT_SETTINGS },
    /** @type {RequestQueue|null} */
    queue: null,
    /** @type {typeof fetch|null} */
    nativeFetch: null,
    /** 最底层那个真正的 fetch（不随“抢最外层”变化） */
    baseFetch: null,
    patched: false,
    initialized: false,
    /** @type {HTMLElement|null} */
    uiRoot: null,
    /** @type {{name: string, type: string}[]} */
    discovered: [],
    /** @type {number|null} */
    ticker: null,
    /** @type {string[]} */
    commands: [],
    /** @type {string|null} */
    lastRenderedListKey: null,
    /** 本次会话里自动学到的转发层插件（包了全局 fetch 的那种） */
    learnedRelays: new Set(),
    /** 每个插件被“夹在中间”的次数，够 RELAY_CONFIRM 次才认定为转发层 */
    relayVotes: new Map(),
    /** 最近一次请求归属判定看到的调用栈（排障用） */
    lastTrace: [],
    /** 最近一次请求的原始调用栈（排障用，面板调试模式下显示） */
    lastRawStack: '',
    /** 最近一次请求被判给了谁 */
    lastWho: '',
    /** 转发层探测：定时器与上次探测时看到的外层 fetch 引用 */
    probeTimer: null,
    probedAgainst: undefined,
    outerNoticeShown: false,
    /** 正在替脚本跑的请求：脚本名栈（跨 await 保留，取最内层） */
    originStack: [],
    /** 见过的脚本 iframe 名字 */
    scriptNames: new Set(),
    /** 已经桥接过的 iframe（避免重复注入） */
    bridgedFrames: new WeakSet(),
    /** 监听新 iframe 的观察器 */
    iframeObserver: null,
    /** 重试包装 TavernHelper 接口的定时器 */
    wrapTimer: null,
    wrapTries: 0,
};

/* ------------------------------------------------------------------ *
 * 基础工具
 * ------------------------------------------------------------------ */

function ctx() {
    return globalThis.SillyTavern?.getContext?.() ?? null;
}

function log(kind, ...args) {
    if (!state.settings?.debug) return;
    console.debug(`%c[请求队列] ${kind}`, 'color:#5ac8b0', ...args);
}

function abortError(reason = 'aborted') {
    try {
        return new DOMException(reason, 'AbortError');
    } catch {
        const err = new Error(reason);
        err.name = 'AbortError';
        return err;
    }
}

function setChecked(id, value) {
    const el = document.getElementById(id);
    if (el && 'checked' in el) el.checked = !!value;
}

function setValue(id, value) {
    const el = document.getElementById(id);
    if (el && 'value' in el) el.value = String(value ?? '');
}

function eventElement(ev) {
    const target = ev?.target;
    return target && typeof target.closest === 'function' ? target : null;
}

function urlOf(input) {
    if (typeof input === 'string') return input;
    if (input instanceof URL) return input.href;
    if (input && typeof input.url === 'string') return input.url;
    return '';
}

function shortUrl(url) {
    if (!url) return '(unknown)';
    try {
        const u = new URL(url, location.href);
        return u.pathname;
    } catch {
        return String(url).slice(0, 80);
    }
}

function fmtMs(ms) {
    const n = Math.round(Number(ms) || 0);
    if (n < 1000) return `${n}ms`;
    return `${(n / 1000).toFixed(n < 10000 ? 1 : 0)}s`;
}

/** 从 iframe 的 id / name 里取出脚本体：TH-script--<脚本名>--<id> → 脚本名 */
export function parseScriptFrameId(id) {
    const m = JSR_SCRIPT_FRAME.exec(String(id ?? ''));
    return m ? m[1] : null;
}

/** 脚本名 → 队列内部的键 */
export function scriptKeyOf(name) {
    return SCRIPT_KEY_PREFIX + String(name ?? '');
}

/** 队列内部的键 → 脚本名（不是脚本键就返回 null） */
export function scriptNameOfKey(key) {
    const text = String(key ?? '');
    if (!text.startsWith(SCRIPT_KEY_PREFIX)) return null;
    const name = text.slice(SCRIPT_KEY_PREFIX.length);
    return name || null;
}

/** 队列内部的键 → 面板上给人看的名字 */
function labelOf(key) {
    if (key === CORE_KEY) return CORE_LABEL;
    const script = scriptNameOfKey(key);
    if (script) return `脚本 · ${script}`;
    return key || '';
}

/* ------------------------------------------------------------------ *
 * 设置读写
 * ------------------------------------------------------------------ */

function loadSettings() {
    const store = ctx()?.extensionSettings;
    const saved = store && typeof store === 'object' ? store[MODULE_KEY] : null;
    const merged = { ...DEFAULT_SETTINGS, ...(saved && typeof saved === 'object' ? saved : {}) };

    merged.enabled = merged.enabled !== false;
    merged.mode = merged.mode === MODE.FIFO ? MODE.FIFO : MODE.PRIORITY;
    merged.queueCore = merged.queueCore !== false;
    merged.settleMs = clampNumber(merged.settleMs, 0, 5000, DEFAULT_SETTINGS.settleMs);
    merged.timeoutMs = clampNumber(merged.timeoutMs, 0, 3600000, DEFAULT_SETTINGS.timeoutMs);
    merged.maxWaitMs = clampNumber(merged.maxWaitMs, 0, 600000, DEFAULT_SETTINGS.maxWaitMs);
    merged.idleMs = clampNumber(merged.idleMs, 0, 600000, DEFAULT_SETTINGS.idleMs);
    merged.order = Array.isArray(merged.order)
        ? merged.order.map(String).filter((key) => key && key !== CORE_KEY)
        : [];
    merged.bypass = Array.isArray(merged.bypass) ? merged.bypass.map(String) : [];
    merged.relays = Array.isArray(merged.relays)
        ? merged.relays.map(String).map((s) => s.trim()).filter(Boolean)
        : [];
    merged.extraPatterns = Array.isArray(merged.extraPatterns)
        ? merged.extraPatterns.map(String).map((s) => s.trim()).filter(Boolean)
        : [];
    merged.debug = merged.debug === true;
    merged.splitScripts = merged.splitScripts !== false;
    merged.onlyActive = merged.onlyActive !== false;
    merged.seen = Array.isArray(merged.seen)
        ? merged.seen.map(String).filter((key) => key && key !== CORE_KEY).slice(-300)
        : [];
    merged.collapsed = merged.collapsed !== false;

    if (store && typeof store === 'object') store[MODULE_KEY] = merged;
    return merged;
}

function saveSettings() {
    const c = ctx();
    if (c?.extensionSettings) c.extensionSettings[MODULE_KEY] = state.settings;
    try {
        c?.saveSettingsDebounced?.();
    } catch (err) {
        log('error', '保存设置失败', err);
    }
}

function clampNumber(value, min, max, fallback) {
    const n = typeof value === 'number' ? value : Number.parseFloat(String(value));
    if (!Number.isFinite(n)) return fallback;
    return Math.min(max, Math.max(min, Math.round(n)));
}

/* ------------------------------------------------------------------ *
 * 顺序 / 优先级
 * ------------------------------------------------------------------ */

/** 本体永远排在所有插件前面；插件按面板里的顺序，没排过的排最后 */
function priorityOf(key) {
    if (!key || key === CORE_KEY) return 0;
    const index = state.settings.order.indexOf(key);
    if (index >= 0) return index + 1;
    return 500 + Math.max(0, state.settings.order.length);
}

/** 只把“另外安装的插件”算进来：内置扩展（type=system）和本扩展自己都不显示 */
function isThirdParty(entry) {
    if (!entry || !entry.name) return false;
    if (entry.type === 'system') return false;
    if (entry.name === MODULE_KEY) return false;
    return true;
}

/** 内置扩展的名字（用来把旧设置里混进来的它们清掉） */
function builtinNameSet() {
    const set = new Set();
    for (const entry of state.discovered) {
        if (entry?.name && !isThirdParty(entry)) set.add(entry.name);
    }
    return set;
}

/** 发过请求的 key：本次会话的统计 + 以前记下的 */
function activeKeySet() {
    const set = new Set(state.settings.seen || []);
    const stats = state.queue ? state.queue.snapshot().stats : null;
    for (const key of Object.keys(stats || {})) {
        if (stats[key]?.started || stats[key]?.queued) set.add(key);
    }
    return set;
}

/** 面板里要显示的完整顺序：已排序的 + 新发现的（都不含本体和内置扩展） */
function currentKeys() {
    const builtins = builtinNameSet();
    const ordered = [];
    const push = (key) => {
        if (!key || key === CORE_KEY || key === MODULE_KEY) return;
        if (builtins.has(key)) return;
        if (!ordered.includes(key)) ordered.push(key);
    };
    if (state.settings.onlyActive) {
        // 只列真正发过请求的：纯界面类的插件（主题、按钮、字体…）永远不会出现
        const active = activeKeySet();
        state.settings.order.forEach((key) => {
            if (active.has(key)) push(key);
        });
        active.forEach(push);
        return ordered;
    }
    state.settings.order.forEach(push);
    state.discovered.forEach((entry) => {
        if (isThirdParty(entry)) push(entry.name);
    });
    // 脚本运行时里的脚本：见过的都列出来，这样能单独给每个脚本排顺序
    if (state.settings.splitScripts) {
        for (const name of state.scriptNames) push(scriptKeyOf(name));
        const stats = state.queue ? state.queue.snapshot().stats : null;
        for (const key of Object.keys(stats || {})) {
            if (scriptNameOfKey(key)) push(key);
        }
    }
    return ordered;
}

/** 记下“这个 key 真的发过请求”，供面板过滤用（持久化，刷新后不丢） */
function markSeen(key) {
    if (!key || key === CORE_KEY) return;
    const seen = state.settings.seen;
    if (seen.includes(key)) return;
    seen.push(key);
    if (seen.length > 300) seen.splice(0, seen.length - 300);
    saveSettings();
    state.lastRenderedListKey = null;
    scheduleListRefresh();
}

/** 新插件/新脚本第一次发请求时，把列表刷出来（去抖，避免一次风暴里反复重建 DOM） */
function scheduleListRefresh() {
    if (!state.uiRoot?.isConnected) return;
    if (state.listRefreshTimer) return;
    state.listRefreshTimer = setTimeout(() => {
        state.listRefreshTimer = null;
        if (state.uiRoot?.isConnected) renderList();
    }, 300);
    state.listRefreshTimer?.unref?.();
}

/** 把 order 里已经不该出现的名字（本体/内置/自己/已卸载）清掉 */
function pruneOrder() {
    const known = new Set(state.discovered.filter(isThirdParty).map((entry) => entry.name));
    const cleaned = state.settings.order.filter((key) => {
        if (!key || key === CORE_KEY || key === MODULE_KEY) return false;
        if (scriptNameOfKey(key)) return true; // 脚本不是“已安装插件”，别当成卸载清掉
        if (known.size === 0) return true; // 还没读到插件列表时先不动
        return known.has(key);
    });
    if (cleaned.length === state.settings.order.length) return false;
    state.settings.order = cleaned;
    saveSettings();
    state.queue?.configure({ priorityOf });
    return true;
}

function moveKey(key, delta) {
    const keys = currentKeys();
    const from = keys.indexOf(key);
    const to = from + delta;
    if (from < 0 || to < 0 || to >= keys.length) return;
    const [moved] = keys.splice(from, 1);
    keys.splice(to, 0, moved);
    state.settings.order = keys;
    saveSettings();
    if (state.queue) state.queue.configure({ priorityOf });
    renderList();
}

function setBypass(key, bypassed) {
    const set = new Set(state.settings.bypass);
    if (bypassed) set.add(key);
    else set.delete(key);
    state.settings.bypass = [...set];
    if (key === CORE_KEY) {
        state.settings.queueCore = !bypassed;
        setChecked('rq_queue_core', !bypassed);
    }
    saveSettings();
    renderList();
}

function setEnabled(value) {
    state.settings.enabled = !!value;
    saveSettings();
    setChecked('rq_enabled', state.settings.enabled);
    if (!state.settings.enabled && state.queue) state.queue.flush('disabled');
    renderSummary();
    log('toggle', `enabled=${state.settings.enabled}`);
}

/* ------------------------------------------------------------------ *
 * 谁在发请求：调用栈归属
 *
 * 有个坑：部分插件会全局包一层 fetch（小白X 的调试面板 / 消息预览就是这样），
 * 它的栈帧永远紧贴在我们这层外面 —— 照“第一个命中的插件”算，所有请求都会算成它。
 * 所以改成整条调用链一起看：最外层那个才可能是真正的发起者，中间的都是转发层。
 * ------------------------------------------------------------------ */

/** 把调用栈拆成帧：self=本扩展，third=第三方插件，inner=内置扩展，core=酒馆本体，other=其它 */
export function parseStack(stack, selfMark) {
    const frames = [];
    for (const line of String(stack || '').split('\n').slice(1)) {
        if (!line.trim()) continue;
        if (selfMark && line.includes(selfMark)) {
            frames.push({ kind: 'self' });
            continue;
        }
        let m = line.match(/\/scripts\/extensions\/third-party\/([^/?#\s)'"]+)\//);
        if (m) {
            frames.push({ kind: 'third', name: m[1] });
            continue;
        }
        m = line.match(/\/scripts\/extensions\/([^/?#\s)'"]+)\//);
        if (m && m[1] !== 'third-party') {
            frames.push({ kind: 'inner', name: m[1] });
            continue;
        }
        if (/\/script\.js|\/scripts\/[^/]+\.js/.test(line)) {
            frames.push({ kind: 'core' });
            continue;
        }
        frames.push({ kind: 'other' });
    }
    return frames;
}

/**
 * 从调用链里挑出真正的发起者。
 * - 转发层（包了全局 fetch 的插件）全部排除后，取剩下的最外层那个插件；
 * - 只剩转发层时，算在最外层那层壳身上（脚本运行时替脚本发请求的情形）；
 * - 一个插件帧都没有时才归酒馆本体；
 * - 顺手把“除最外层以外”的插件学成转发层，下次直接排除。
 */
export function pickInitiator(frames, relays = new Set(), options = {}) {
    const dirs = [];
    let innerSeen = null;
    let coreSeen = false;

    for (const frame of frames || []) {
        if (!frame || frame.kind === 'self') continue;
        if (frame.kind === 'third') {
            if (!dirs.includes(frame.name)) dirs.push(frame.name);
            continue;
        }
        if (frame.kind === 'inner' && !innerSeen) innerSeen = frame.name;
        if (frame.kind === 'core') coreSeen = true;
    }

    const learned = new Set(relays || []);
    // 出现在别人前面的插件 = 只是转发别人的请求
    for (const dir of dirs.slice(0, -1)) learned.add(dir);

    const candidates = dirs.filter((dir) => !learned.has(dir));
    const key = candidates.length ? candidates[candidates.length - 1] : null;
    if (key) return { who: { key, label: key, kind: 'third-party' }, learned };

    if (innerSeen) return { who: { key: innerSeen, label: innerSeen, kind: 'extension' }, learned };

    // 栈里有酒馆自己的帧：请求是本体发起的，插件只是把它转下来
    if (coreSeen) return { who: { key: CORE_KEY, label: CORE_LABEL, kind: 'core' }, learned };

    // 只剩转发层、也没有本体帧：发起者的栈帧在 async 链里丢了。
    // 最外层那层壳最接近真正的发起者 —— 典型情况是 JS-Slash-Runner 里跑的用户脚本，
    // 脚本本身不留帧、是插件替它发请求，所以算在这个壳身上（面板里可以单独给它排序）。
    if (dirs.length) {
        const shell = dirs[dirs.length - 1];
        return { who: { key: shell, label: shell, kind: 'third-party' }, learned };
    }
    return { who: { key: CORE_KEY, label: CORE_LABEL, kind: 'core' }, learned };
}

function relaySet() {
    const set = new Set(state.learnedRelays);
    for (const name of state.settings.relays) set.add(name);
    return set;
}

function attribute() {
    let stack = '';
    try {
        stack = new Error().stack || '';
    } catch { /* ignore */ }

    const frames = parseStack(stack, OWN_DIR_MARK);
    if (state.settings.debug) {
        log('stack', frames.map((f) => (f.name ? `${f.kind}:${f.name}` : f.kind)).join(' > '));
    }
    state.lastTrace = frames.map((f) => (f.name ? `${f.kind}:${f.name}` : f.kind));
    state.lastRawStack = stack.split('\n').slice(0, 30).join('\n');
    const { who, learned } = pickInitiator(frames, relaySet());
    state.lastWho = who.key;

    // 栈里推出来的“转发层”要连中两次才认账：
    // 一个插件偶尔夹在中间（调试打印、转手一次）不该被永久当成转发层，
    // 否则它自己真正发起的请求会永远认不出来。
    const committed = new Set(state.learnedRelays);
    for (const dir of learned) {
        if (committed.has(dir)) continue;
        const votes = (state.relayVotes.get(dir) || 0) + 1;
        state.relayVotes.set(dir, votes);
        if (votes >= RELAY_CONFIRM) committed.add(dir);
    }
    if (committed.size !== state.learnedRelays.size) {
        state.learnedRelays = committed; // 只是本次会话的运行时知识，不写进用户设置
        log('relay', `判定为转发层的插件：${[...committed].join('、') || '无'}`);
        renderRelayHint();
    }

    return who;
}

/* ------------------------------------------------------------------ *
 * 转发层探测
 *
 * 有的插件会把 window.fetch 整个包一层（消息预览、调试面板、代理、统计…）。
 * 它们的栈帧永远夹在我们和真正的发起者之间，光靠“第一帧是谁”会认错。
 * 做法：用一段带着假插件路径的探针代码发一次假请求，探针自己的帧就是
 * “真正的发起者”，夹在我们和它之间的所有插件帧，都是转发层。
 * ------------------------------------------------------------------ */

const PROBE_DIR = '__rq_probe__';
const PROBE_PATH = '/__rq_probe__/probe';
const PROBE_SOURCE = `/scripts/extensions/third-party/${PROBE_DIR}/probe.js`;

function isProbeUrl(url) {
    return typeof url === 'string' && url.includes(PROBE_PATH);
}

function learnRelaysFromProbe(frames) {
    const dirs = [];
    for (const frame of frames || []) {
        if (frame?.kind === 'third' && !dirs.includes(frame.name)) dirs.push(frame.name);
    }
    if (!dirs.includes(PROBE_DIR)) return false; // 探针帧没露出来（被拦或被改写），本次不学

    const wrappers = dirs.filter((dir) => dir !== PROBE_DIR);
    if (!wrappers.length) {
        log('probe', '探针结果：没有别的插件包 fetch');
        return true;
    }

    // 探针的帧是“发起者”，夹在它和我们之间的每一层都是转发层——全部记下来。
    //
    // 只记最内层那一层是不够的：真正的发起者常常在 async 链里丢掉自己的栈帧，
    // 这时栈里剩下的可能是一个别人包的外层（例如 JS-Slash-Runner），
    // 它就会被误当成发起者，于是所有请求都记成它。
    // 代价是这些包装器自己发的请求会认不出来，退回到“酒馆本体”，
    // 但绝不会把别人的请求算到它们头上。
    const learned = new Set(state.learnedRelays);
    const added = [];
    for (const dir of wrappers) {
        if (learned.has(dir)) continue;
        learned.add(dir);
        added.push(dir);
    }
    if (added.length) {
        state.learnedRelays = learned;
        log('probe', `探针找到转发层：${added.join('、')}`);
        renderRelayHint();
    }
    return true;
}

function runRelayProbe() {
    if (!state.patched) return;
    let fn;
    try {
        fn = new Function(
            `return fetch(${JSON.stringify(PROBE_PATH)}, { method: 'POST', body: 'rq-probe' })`
            + '.then(() => null).catch(() => null);\n'
            + `//# sourceURL=${PROBE_SOURCE}`,
        );
    } catch (err) {
        log('error', '转发层探测代码构造失败', err);
        return;
    }
    try {
        Promise.resolve(fn()).catch(() => { });
    } catch (err) {
        log('error', '转发层探测执行失败', err);
    }
}

/** 外层又被人换了（新装/热重载了包 fetch 的插件）就重新探测一次 */
function maybeReprobe() {
    if (!state.patched) return;
    const outer = globalThis.fetch;
    if (outer === queuedFetch || outer === state.probedAgainst) return;
    state.probedAgainst = outer;
    if (state.probeTimer) clearTimeout(state.probeTimer);
    state.probeTimer = setTimeout(() => {
        state.probeTimer = null;
        runRelayProbe();
    }, 400);
}

function shouldQueue(input) {
    if (!state.settings.enabled || !state.queue) return false;
    const url = urlOf(input);
    if (!url) return false;
    if (BACKEND_GENERATE.test(url)) return true;
    for (const pattern of state.settings.extraPatterns) {
        try {
            if (pattern.length > 2 && pattern.startsWith('/') && pattern.endsWith('/')) {
                if (new RegExp(pattern.slice(1, -1)).test(url)) return true;
            } else if (url.includes(pattern)) {
                return true;
            }
        } catch { /* 无效正则直接忽略 */ }
    }
    return false;
}

function isBypassed(who) {
    if (!who) return false;
    if (who.kind === 'core' && !state.settings.queueCore) return true;
    return state.settings.bypass.includes(who.key);
}

/* ------------------------------------------------------------------ *
 * 脚本运行时（JS-Slash-Runner）里的脚本身份
 *
 * 那个插件把每个用户脚本放在 id 为 TH-script--<脚本名>--<id> 的同源 iframe 里，
 * 脚本自己的 window 里那层 fetch 是原生、没被我们改过 —— 脚本直接 fetch 时
 * 请求根本不经过队列（既排不上队，也认不出是谁）。
 * 所以这里做两件事：
 *   1. 把脚本 iframe 里的 fetch 桥接回主页面，顺带把脚本名打在这条请求上；
 *   2. 给 TavernHelper 里那一圈“替脚本跑”的接口套一层记号，
 *      这样脚本走插件接口发请求时，也还知道是哪个脚本。
 * ------------------------------------------------------------------ */

/** 从 iframe / window 上认出脚本体 */
function scriptNameFromWindowLike(target) {
    if (!target) return null;
    let id = '';
    try {
        id = target.__TH_IFRAME_ID || target.name || target.frameElement?.id || '';
    } catch {
        return null; // 跨源访问会抛错，直接当作认不出
    }
    return parseScriptFrameId(id);
}

/** 这次请求身上带的脚本名（优先取显式记号，其次取当前正在跑的脚本） */
function readOrigin(init) {
    if (init && typeof init === 'object') {
        const direct = init[ORIGIN];
        if (typeof direct === 'string' && direct) return direct;
    }
    const stack = state.originStack;
    return stack.length ? stack[stack.length - 1] : null;
}

/** 脚本请求在队列里的身份 */
function whoForScript(name) {
    const key = state.settings.splitScripts ? scriptKeyOf(name) : SCRIPT_HOST;
    return { key, label: `脚本 · ${name}`, kind: 'third-party', script: name };
}

/** 在“某个脚本正在跑”的上下文里执行，跨 await 保留标记 */
function runWithOrigin(name, task) {
    state.originStack.push(name);
    const done = () => {
        const index = state.originStack.lastIndexOf(name);
        if (index >= 0) state.originStack.splice(index, 1);
    };
    let out;
    try {
        out = task();
    } catch (err) {
        done();
        throw err;
    }
    if (out && typeof out.then === 'function') {
        return out.then(
            (value) => {
                done();
                return value;
            },
            (err) => {
                done();
                throw err;
            },
        );
    }
    done();
    return out;
}

/** 把一个脚本 iframe 的 fetch 接到主页面上，并带上脚本名 */
function bridgeScriptFrame(frame) {
    if (!frame || frame.tagName !== 'IFRAME') return false;
    let name = null;
    try {
        name = parseScriptFrameId(frame.id || frame.name || '');
    } catch {
        return false;
    }
    if (!name) return false; // 只碰脚本 iframe，消息 iframe（TH-message--）不管

    if (!state.scriptNames.has(name)) {
        state.scriptNames.add(name);
        log('script', `发现脚本：${name}`);
        // 新脚本要在顺序表里露出来，否则用户没法给它排先后
        state.lastRenderedListKey = null;
        if (state.uiRoot?.isConnected) {
            renderList();
            renderScriptHint();
        }
    }

    let win = null;
    try {
        win = frame.contentWindow;
    } catch {
        return false;
    }
    if (!win) return false;

    try {
        if (win[BRIDGE_MARK] === name) return false;
        win[BRIDGE_ORIGINAL] = win.fetch;
        win.fetch = function (input, init) {
            const next = { ...(init || {}) };
            next[ORIGIN] = name;
            // 直接替这个脚本拿令牌：别的插件的包装器会重建 init、把记号弄丢，
            // 所以在桥接这一层就把归属定下来，往下走时只需保证不再排第二次队。
            return scriptFetch(name, input, next);
        };
        win[BRIDGE_MARK] = name;
        log('script', `脚本 iframe 已接管：${name}`);
        return true;
    } catch (err) {
        log('error', `接管脚本 iframe 失败：${name}`, err);
        return false;
    }
}

/**
 * 脚本 iframe 发起的请求：按脚本名排队，然后走完整条 fetch 链（别人的包装器照旧跑），
 * 响应体读完才归还令牌。
 */
async function scriptFetch(name, input, init) {
    const who = whoForScript(name);
    if (!state.queue || !state.settings.enabled || isBypassed(who) || !shouldQueue(input)) {
        return globalThis.fetch(input, init);
    }

    markSeen(who.key);
    const upstream = new AbortController();
    const originalSignal = init?.signal ?? null;
    bindAbort(upstream, originalSignal);
    const startInit = { ...(init || {}), signal: upstream.signal, [REENTRANT]: true };

    let release;
    try {
        release = await acquireWithSignal(
            who.key,
            { url: urlOf(input), label: who.label, script: name, abort: () => upstream.abort() },
            upstream.signal,
        );
    } catch (err) {
        if (err?.name === 'AbortError') throw err;
        log('error', '脚本请求排队异常，直接放行', err);
        return globalThis.fetch(input, withReentrant(init));
    }

    log('start', `脚本 ${name} 开始请求 ${shortUrl(urlOf(input))}`);
    let response;
    try {
        response = await globalThis.fetch(input, startInit);
    } catch (err) {
        release('error');
        throw err;
    }
    return trackResponse(response, release, who);
}

function attachScriptFrame(frame) {
    if (!frame || frame.tagName !== 'IFRAME') return;
    if (bridgeScriptFrame(frame)) {
        // iframe 重新加载后是新的窗口，得再接管一次
        frame.addEventListener?.('load', () => bridgeScriptFrame(frame));
    }
}

/** 扫描已有 + 监听新增的脚本 iframe */
function installScriptBridges() {
    try {
        for (const frame of document.querySelectorAll('iframe')) attachScriptFrame(frame);
    } catch (err) {
        log('error', '扫描 iframe 失败', err);
    }
    if (state.iframeObserver || typeof MutationObserver !== 'function') return;
    const root = document.body || document.documentElement;
    if (!root) return;
    state.iframeObserver = new MutationObserver((records) => {
        for (const record of records) {
            for (const node of record.addedNodes || []) {
                if (!node || node.nodeType !== 1) continue;
                if (node.tagName === 'IFRAME') {
                    attachScriptFrame(node);
                    continue;
                }
                if (typeof node.querySelectorAll === 'function') {
                    for (const frame of node.querySelectorAll('iframe')) attachScriptFrame(frame);
                }
            }
        }
    });
    state.iframeObserver.observe(root, { childList: true, subtree: true });
}

/**
 * 给 JS-Slash-Runner 暴露给脚本的那批接口（TavernHelper._bind）套记号。
 * 这些函数在主页面上跑，但 `this` 是脚本自己的 window，
 * 所以能顺着 `this` 认出是哪个脚本，再把记号带到它内部发出的请求上。
 */
function wrapScriptApis() {
    const bind = globalThis.TavernHelper?._bind;
    if (!bind || typeof bind !== 'object') return false;
    let changed = 0;
    for (const [key, fn] of Object.entries(bind)) {
        if (typeof fn !== 'function' || fn.__rqOriginWrapped) continue;
        const wrapped = function (...args) {
            const name = scriptNameFromWindowLike(this);
            if (!name) return fn.apply(this, args);
            return runWithOrigin(name, () => fn.apply(this, args));
        };
        wrapped.__rqOriginWrapped = true;
        wrapped.__rqOriginal = fn;
        bind[key] = wrapped;
        changed += 1;
    }
    if (changed) log('script', `已给 ${changed} 个脚本接口加上来源标记`);
    return changed > 0;
}

/** 反复试几次：TavernHelper 可能比我们晚就绪，也可能被热重载换掉 */
function scheduleScriptApiWrap() {
    const run = () => {
        state.wrapTimer = null;
        if (!state.settings.enabled) return;
        wrapScriptApis();
        installScriptBridges();
        state.wrapTries += 1;
        if (state.wrapTries < 20) {
            state.wrapTimer = setTimeout(run, 1500);
            state.wrapTimer.unref?.();
        }
    };
    if (state.wrapTimer) return;
    run();
}

function teardownScriptBridges() {
    if (state.wrapTimer) {
        clearTimeout(state.wrapTimer);
        state.wrapTimer = null;
    }
    state.wrapTries = 0;
    try {
        state.iframeObserver?.disconnect();
    } catch { /* ignore */ }
    state.iframeObserver = null;
    const bind = globalThis.TavernHelper?._bind;
    if (bind && typeof bind === 'object') {
        for (const [key, fn] of Object.entries(bind)) {
            if (typeof fn === 'function' && fn.__rqOriginal) bind[key] = fn.__rqOriginal;
        }
    }
    try {
        for (const frame of document.querySelectorAll('iframe')) {
            const win = frame.contentWindow;
            if (!win?.[BRIDGE_MARK]) continue;
            if (win[BRIDGE_ORIGINAL]) win.fetch = win[BRIDGE_ORIGINAL];
            delete win[BRIDGE_MARK];
            delete win[BRIDGE_ORIGINAL];
        }
    } catch { /* ignore */ }
    state.originStack = [];
}

/* ------------------------------------------------------------------ *
 * fetch 拦截
 * ------------------------------------------------------------------ */

function patchFetch() {
    if (state.patched) return;

    // 防止嵌套接管：扩展被重复加载 / 手滑装了两份时，别把 fetch 套成两层队列
    const current = globalThis.fetch;
    if (typeof current === 'function' && current[PATCH_MARK]) {
        state.nativeFetch = current[PATCH_MARK] ?? null;
        state.patched = true;
        log('patch', 'fetch 已被接管过，复用原函数');
        return;
    }

    const original = globalThis.fetch.bind(globalThis);
    state.nativeFetch = original;
    state.baseFetch = original; // 最底下那一层真 fetch，被中间层绕回来时用它兜底，避免自己调自己
    queuedFetch[PATCH_MARK] = original;
    globalThis.fetch = queuedFetch;
    state.patched = true;
    log('patch', '已接管 fetch');
}

function unpatchFetch() {
    if (!state.patched || !state.nativeFetch) return;
    globalThis.fetch = state.nativeFetch;
    state.patched = false;
}

function bindAbort(controller, signal) {
    if (!signal) return;
    if (signal.aborted) {
        controller.abort(signal.reason);
        return;
    }
    signal.addEventListener('abort', () => controller.abort(signal.reason), { once: true });
}

function acquireWithSignal(key, meta, signal) {
    return new Promise((resolve, reject) => {
        if (signal?.aborted) {
            reject(abortError('aborted'));
            return;
        }
        const ticket = state.queue.enqueue(key, meta);
        const onAbort = () => state.queue.cancel(ticket, 'aborted');
        signal?.addEventListener?.('abort', onAbort, { once: true });
        ticket.promise.then(
            (release) => {
                signal?.removeEventListener?.('abort', onAbort);
                resolve(release);
            },
            (err) => {
                signal?.removeEventListener?.('abort', onAbort);
                reject(err);
            },
        );
    });
}

/**
 * 把响应体包一层：流真正结束（或取消 / 报错）时才归还令牌。
 * 流式生成必须在“读完之后”才算完成，否则下一个插件会在模型还在输出时就开跑。
 */
function trackResponse(response, release, who) {
    let finished = false;
    let idleTimer = null;
    const clearIdle = () => {
        if (idleTimer !== null) {
            clearTimeout(idleTimer);
            idleTimer = null;
        }
    };
    const finish = (reason) => {
        if (finished) return;
        finished = true;
        clearIdle();
        try {
            release(reason);
        } catch (err) {
            log('error', '释放令牌失败', err);
        }
        log('done', `${who.key} → ${reason}`);
    };

    if (
        !response ||
        !response.body ||
        response.bodyUsed ||
        response.status === 101 ||
        response.status === 204 ||
        response.status === 205 ||
        response.status === 304
    ) {
        finish('no-body');
        return response;
    }

    // 出错响应（4xx/5xx）不会往正文里插东西：立刻放行，别让一个失败请求占着锁
    if (response.status >= 400) {
        finish('http-error');
        return response;
    }

    const reader = response.body.getReader();
    const stream = new ReadableStream({
        async pull(controller) {
            clearIdle(); // 有人在读，说明这个请求确实活着
            try {
                const { done, value } = await reader.read();
                if (done) {
                    controller.close();
                    finish('done');
                    return;
                }
                controller.enqueue(value);
            } catch (err) {
                controller.error(err);
                finish('error');
            }
        },
        cancel(reason) {
            finish('cancelled');
            try {
                return reader.cancel(reason);
            } catch {
                return undefined;
            }
        },
    });

    let wrapped;
    try {
        wrapped = new Response(stream, {
            status: response.status,
            statusText: response.statusText,
            headers: response.headers,
        });
    } catch (err) {
        // 构造失败就退回原响应，但令牌立刻归还，避免卡死
        log('error', '包装响应失败', err);
        finish('wrap-failed');
        return response;
    }

    // 拿到了响应体却一直没人读（插件只看了状态码就丢下不管）：
    // 等 idleMs 就把锁让出去，否则整条队列会被它一个人堵死。
    // 只放锁、不掐流——之后它再想读仍然能读。
    if (state.settings.idleMs > 0 && !finished) {
        idleTimer = setTimeout(() => {
            idleTimer = null;
            log('idle', `${who.key} 拿到响应体后 ${state.settings.idleMs}ms 没人读取，先让出队列`);
            finish('idle');
        }, state.settings.idleMs);
    }

    for (const prop of ['url', 'redirected', 'type']) {
        try {
            Object.defineProperty(wrapped, prop, { value: response[prop], configurable: true });
        } catch { /* 忽略只读属性 */ }
    }
    return wrapped;
}

/** 我们往下传请求时打的记号，避免内层再排一次队 */
const REENTRANT = Symbol.for('st-request-queue.reentrant');

function withReentrant(init) {
    return { ...(init || {}), [REENTRANT]: true };
}

/**
 * 被动拦截，不抢 `window.fetch` 的位置。
 *
 * 这里踩过一次坑：为了认出“谁在发请求”，一度把 fetch 换回自己这一层。
 * 结果破甲、工具调用这类插件是从自己的包装器里改请求/响应的，位置一被换掉，
 * 它们拿到/写回的东西就不是它们以为的那一份，功能直接失灵。
 * 所以现在只被动等待：谁想盖在上面就让它盖，请求传下来时我们照样拦得到，
 * 往下传时也照旧经过别人的包装器，一个都不跳过。
 */

/**
 * 检查本扩展是否还收得到请求。
 *
 * 被动模式：别人把包装器盖在我们上面完全没问题 —— 他们调用时会把请求传下来，
 * 我们照样拦得到。只有一种情况收不到：某插件把 fetch 整个换成自己的实现、
 * 并且不再往下传。这没法从外面修，只能提示用户。
 */
function ensureQueueIsReachable() {
    if (!state.patched) return false;
    const top = globalThis.fetch;
    if (top === queuedFetch || top == null) return false;
    if (!state.outerNoticeShown) {
        state.outerNoticeShown = true;
        log('patch', '外层有其他插件的 fetch 包装器，队列以被动方式工作（不影响它们改请求/响应）');
    }
    return true;
}

/** 重入深度：包装器绕回来再调我们时，最多放行两层，绝不无限递归 */
let reentryDepth = 0;

async function queuedFetch(input, init) {
    let native = state.nativeFetch;
    if (!native || native === queuedFetch) {
        // 理论上不会走到这里；真走到了也绝不能递归调用自己
        const fallback = state.baseFetch && state.baseFetch !== queuedFetch
            ? state.baseFetch
            : globalThis.fetch;
        if (typeof fallback === 'function' && fallback !== queuedFetch) native = fallback;
        else throw new Error('[请求队列] 找不到原生 fetch');
    }

    // 我们自己往下传的请求带了这个记号：说明令牌已经被外层那一圈拿过了，
    // 内层只管放行，绝不能再排一次队（否则自己等自己，直接死锁）。
    // 放行时仍走 native —— 也就是别人那些包装器照旧各跑一遍，不跳过任何插件。
    if (init && init[REENTRANT] === true) {
        if (reentryDepth >= 2) {
            throw new Error('[请求队列] 检测到 fetch 包装器循环调用，已中止以免死循环');
        }
        reentryDepth += 1;
        try {
            return await native(input, init);
        } finally {
            reentryDepth -= 1;
        }
    }


    ensureQueueIsReachable();

    let who;
    const reqUrl = urlOf(input);
    try {
        // 探针自己的假请求：只用来认转发层，绝不进队列、也不真发出去
        if (isProbeUrl(reqUrl)) {
            let stack = '';
            try {
                stack = new Error().stack || '';
            } catch { /* ignore */ }
            learnRelaysFromProbe(parseStack(stack, OWN_DIR_MARK));
            return new Response(null, { status: 204 });
        }
        maybeReprobe();
        if (!shouldQueue(input)) return native(input, withReentrant(init));
        // 脚本 iframe 桥接过来的请求带着脚本名：直接用，不用去猜调用栈
        const script = readOrigin(init);
        who = script ? whoForScript(script) : attribute();
        markSeen(who.key);
        if (isBypassed(who)) return native(input, withReentrant(init));
    } catch (err) {
        log('error', '判定失败，直接放行', err);
        return native(input, withReentrant(init));
    }

    const url = reqUrl;
    const upstream = new AbortController();
    const originalSignal = init?.signal ?? (input && typeof input === 'object' && 'signal' in input ? input.signal : null);
    bindAbort(upstream, originalSignal);
    const finalInit = { ...(init || {}), signal: upstream.signal, [REENTRANT]: true };

    let release;
    try {
        release = await acquireWithSignal(
            who.key,
            { url, label: who.label, script: who.script, abort: () => upstream.abort() },
            upstream.signal,
        );
    } catch (err) {
        if (err?.name === 'AbortError') throw err;
        log('error', '排队异常，直接放行', err);
        return native(input, withReentrant(init));
    }

    log('start', `${who.key} 开始请求 ${shortUrl(url)}`);
    let response;
    try {
        response = await native(input, finalInit);
    } catch (err) {
        release('error');
        throw err;
    }
    return trackResponse(response, release, who);
}

/* ------------------------------------------------------------------ *
 * 设置面板
 * ------------------------------------------------------------------ */

async function mountUi() {
    if (state.uiRoot?.isConnected) return;
    const c = ctx();
    if (!c?.renderExtensionTemplateAsync) {
        log('error', '拿不到 renderExtensionTemplateAsync，跳过界面');
        return;
    }

    let html = '';
    try {
        html = await c.renderExtensionTemplateAsync(EXTENSION_NAME, 'settings');
    } catch (err) {
        console.warn('[请求队列] 设置面板模板读取失败：', err);
        return;
    }

    // 模板 404 时 ST 会返回 undefined 而不抛错：绝不能把 undefined 写进 DOM
    if (typeof html !== 'string' || !html.trim()) {
        console.error(`[请求队列] 设置面板模板没读到（期望 scripts/extensions/${EXTENSION_NAME}/settings.html），界面跳过`);
        return;
    }

    document.getElementById('rq_root')?.remove();
    const host =
        document.getElementById('extensions_settings2') ??
        document.getElementById('extensions_settings');
    if (!host) {
        log('error', '找不到扩展设置容器');
        return;
    }

    const wrap = document.createElement('div');
    wrap.id = 'rq_root';
    wrap.innerHTML = html;
    host.append(wrap);
    state.uiRoot = wrap;

    bindUi();
    renderAll();
    startTicker();
}

function bindUi() {
    const on = (id, event, handler) => {
        const el = document.getElementById(id);
        if (el) el.addEventListener(event, handler);
    };

    // 折叠交给 SillyTavern 自己的 document 级委托处理（public/script.js 里的
    // `.inline-drawer-toggle` 监听），这里只负责把用户的选择记住。

    on('rq_enabled', 'change', (ev) => setEnabled(ev.target.checked));

    on('rq_mode', 'change', (ev) => {
        state.settings.mode = ev.target.value === MODE.FIFO ? MODE.FIFO : MODE.PRIORITY;
        saveSettings();
        state.queue?.configure({ mode: state.settings.mode });
    });

    on('rq_queue_core', 'change', (ev) => {
        state.settings.queueCore = ev.target.checked;
        const set = new Set(state.settings.bypass);
        if (ev.target.checked) set.delete(CORE_KEY);
        else set.add(CORE_KEY);
        state.settings.bypass = [...set];
        saveSettings();
        renderList();
    });

    on('rq_settle', 'change', (ev) => {
        state.settings.settleMs = clampNumber(ev.target.value, 0, 5000, state.settings.settleMs);
        ev.target.value = String(state.settings.settleMs);
        saveSettings();
        state.queue?.configure({ settleMs: state.settings.settleMs });
    });

    on('rq_timeout', 'change', (ev) => {
        state.settings.timeoutMs = clampNumber(ev.target.value, 0, 3600000, state.settings.timeoutMs);
        ev.target.value = String(state.settings.timeoutMs);
        saveSettings();
        state.queue?.configure({ timeoutMs: state.settings.timeoutMs });
    });

    on('rq_debug', 'change', (ev) => {
        state.settings.debug = ev.target.checked;
        saveSettings();
        console.info('[请求队列] 调试日志已' + (state.settings.debug ? '开启' : '关闭'));
    });

    on('rq_extra', 'change', (ev) => {
        state.settings.extraPatterns = String(ev.target.value || '')
            .split('\n')
            .map((s) => s.trim())
            .filter(Boolean);
        saveSettings();
    });

    on('rq_relays', 'change', (ev) => {
        state.settings.relays = String(ev.target.value || '')
            .split('\n')
            .map((s) => s.trim())
            .filter(Boolean);
        saveSettings();
        renderRelayHint();
    });

    on('rq_split_scripts', 'change', (ev) => {
        state.settings.splitScripts = ev.target.checked !== false;
        saveSettings();
        state.lastRenderedListKey = null;
        renderList();
        toast(state.settings.splitScripts
            ? '脚本运行时的每个脚本会单独成行'
            : `脚本请求统一记在 ${SCRIPT_HOST} 名下`);
    });

    on('rq_only_active', 'change', (ev) => {
        state.settings.onlyActive = ev.target.checked !== false;
        saveSettings();
        state.lastRenderedListKey = null;
        renderList();
        toast(state.settings.onlyActive
            ? '只显示发过请求的插件和脚本'
            : '显示全部已安装插件（含从没发过请求的）');
    });

    on('rq_forget', 'click', () => {
        state.settings.seen = [];
        saveSettings();
        state.queue?.resetStats();
        state.lastRenderedListKey = null;
        renderList();
        toast('已清空“发过请求”的记录');
    });

    on('rq_rescan', 'click', () => {
        const result = rescanScripts();
        toast(result);
    });

    on('rq_refresh', 'click', async () => {
        await refreshDiscovery();
        state.lastRenderedListKey = null;
        renderList();
    });

    on('rq_defaults', 'click', () => {
        state.settings.order = [];
        saveSettings();
        state.queue?.configure({ priorityOf });
        state.lastRenderedListKey = null;
        renderList();
    });

    on('rq_reset_stats', 'click', () => {
        state.queue?.resetStats();
        renderAll();
    });

    on('rq_flush', 'click', () => {
        const dropped = state.queue?.flush('manual') ?? 0;
        toast(`已清空队列（丢弃 ${dropped} 个等待中的请求）`);
        renderAll();
    });

    const list = document.getElementById('rq_sort_list');
    if (list) {
        list.addEventListener('click', (ev) => {
            const btn = eventElement(ev)?.closest('button[data-act]') ?? null;
            if (!btn) return;
            const row = btn.closest('.rq-row');
            const key = row?.getAttribute('data-key');
            if (!key) return;
            const act = btn.getAttribute('data-act');
            if (act === 'up') moveKey(key, -1);
            if (act === 'down') moveKey(key, 1);
        });
    }
}

function renderAll() {
    renderControls();
    renderList();
    renderMonitor();
}

function renderControls() {
    const s = state.settings;
    setChecked('rq_enabled', s.enabled);
    setChecked('rq_queue_core', s.queueCore);
    setChecked('rq_debug', s.debug);
    setChecked('rq_split_scripts', s.splitScripts);
    setChecked('rq_only_active', s.onlyActive);
    setValue('rq_settle', s.settleMs);
    setValue('rq_timeout', s.timeoutMs);
    setValue('rq_extra', s.extraPatterns.join('\n'));
    setValue('rq_relays', s.relays.join('\n'));
    setValue('rq_mode', s.mode);
    renderRelayHint();
    renderScriptHint();
    applyCollapsed();
}

/** 把认出来的脚本名列在提示里 */
function renderScriptHint() {
    const box = document.getElementById('rq_scripts_auto');
    if (!box) return;
    const names = [...state.scriptNames];
    box.textContent = names.length ? names.join('、') : '（还没发现脚本）';
}

/**
 * 手动重新扫描脚本 iframe。
 * 用途：脚本 iframe 嵌在 shadow DOM 里、或者上钩子前就把 fetch 存起来了，
 * 观察器接不到 —— 点一下这里，比刷新整页省事。
 */
function rescanScripts() {
    const before = state.scriptNames.size;
    installScriptBridges();
    wrapScriptApis();
    const after = state.scriptNames.size;
    state.lastRenderedListKey = null;
    if (state.uiRoot?.isConnected) {
        renderList();
        renderScriptHint();
    }
    if (!after) return '没发现脚本 iframe：JS-Slash-Runner 里还没有正在运行的脚本？';
    const fresh = after - before;
    return `已扫描脚本 iframe：${after} 个${fresh > 0 ? `（新认出 ${fresh} 个）` : ''}`;
}

/** 把手动填写 + 本次自动认出来的转发层列在提示里 */
function renderRelayHint() {
    const box = document.getElementById('rq_relays_auto');
    if (!box) return;
    const merged = new Set(state.settings.relays);
    state.learnedRelays.forEach((name) => merged.add(name));
    box.textContent = [...merged].join('、') || '（暂无）';
}

/* ------------------------------------------------------------------ *
 * 折叠 —— 用 SillyTavern 原生的 .inline-drawer 结构
 * 点击由 ST 自己（public/script.js 的 document 级委托）处理：
 * 它切箭头图标、对 .inline-drawer-content 做 slideToggle。
 * 这里只做两件事：按上次的选择还原状态、记住用户的新选择。
 * ------------------------------------------------------------------ */

function isDrawerOpen() {
    const caret = document.getElementById('rq_caret');
    if (caret) return caret.classList.contains('up');
    const body = document.getElementById('rq_body');
    return body ? body.style.display !== 'none' : true;
}

function setCollapsed(value) {
    state.settings.collapsed = !!value;
    saveSettings();
    applyCollapsed();
}

function applyCollapsed() {
    const collapsed = !!state.settings.collapsed;
    const body = document.getElementById('rq_body');
    const caret = document.getElementById('rq_caret');
    // 展开要显式写 block：ST 的样式表里有 `.inline-drawer-content { display:none }`，
    // 靠 jQuery slideToggle 时才补 inline display，清空 inline 反而会被那条规则藏起来。
    if (body) body.style.display = collapsed ? 'none' : 'block';
    if (caret) {
        caret.classList.toggle('down', collapsed);
        caret.classList.toggle('up', !collapsed);
        caret.classList.toggle('fa-circle-chevron-down', collapsed);
        caret.classList.toggle('fa-circle-chevron-up', !collapsed);
    }
    updateCollapseTitle();
}

/** 用户点开了/收起了抽屉之后，把新状态记下来 */
function syncCollapsedFromDom() {
    const collapsed = !isDrawerOpen();
    if (collapsed === !!state.settings.collapsed) return;
    state.settings.collapsed = collapsed;
    saveSettings();
    updateCollapseTitle();
}

/** 收起时把“现在谁在请求”挂在标题的悬停提示上，不额外占地方 */
function updateCollapseTitle() {
    const header = document.querySelector('#rq_panel > .inline-drawer-header');
    if (!header) return;
    if (!state.settings.collapsed) {
        header.removeAttribute('title');
        return;
    }
    const holder = state.queue?.snapshot()?.holder;
    const who = holder ? labelOf(holder.key) : '';
    header.title = holder
        ? `点开设置面板（正在请求：${who} · ${fmtMs(holder.elapsedMs)}）`
        : '点开设置面板';
}

function mkBtn(text, act, disabled) {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'rq-btn';
    btn.setAttribute('data-act', act);
    btn.textContent = text;
    btn.disabled = !!disabled;
    return btn;
}

function renderList() {
    const host = document.getElementById('rq_sort_list');
    if (!host) return;

    const snap = state.queue?.snapshot();
    const keys = currentKeys();
    const renderKey = keys.join('|') + '::' + state.settings.bypass.join(',') + '::' + (state.settings.onlyActive ? 'active' : 'all');
    if (state.lastRenderedListKey === renderKey && host.childElementCount === keys.length) {
        // 顺序没变时只刷新统计文字，避免列表在鼠标操作中跳动
        keys.forEach((key) => {
            const meta = host.querySelector(`.rq-row[data-key="${cssEscape(key)}"] .rq-meta`);
            if (meta) meta.textContent = statsText(snap?.stats?.[key]);
        });
        return;
    }

    host.replaceChildren();
    keys.forEach((key, index) => {
        const row = document.createElement('div');
        row.className = 'rq-row';
        row.setAttribute('data-key', key);

        const name = document.createElement('span');
        name.className = 'rq-name';
        name.textContent = labelOf(key);
        name.title = labelOf(key);

        const meta = document.createElement('span');
        meta.className = 'rq-meta';
        meta.textContent = statsText(snap?.stats?.[key]);

        const up = mkBtn('↑', 'up', index === 0);
        const down = mkBtn('↓', 'down', index === keys.length - 1);

        const toggle = document.createElement('label');
        toggle.className = 'rq-toggle';
        const cb = document.createElement('input');
        cb.type = 'checkbox';
        cb.checked = !state.settings.bypass.includes(key);
        cb.addEventListener('change', () => setBypass(key, !cb.checked));
        const cbText = document.createElement('span');
        cbText.textContent = '排队';
        toggle.append(cb, cbText);

        row.append(name, meta, up, down, toggle);
        host.append(row);
    });

    state.lastRenderedListKey = renderKey;

    const empty = document.getElementById('rq_empty_hint');
    if (empty) empty.hidden = keys.length > 0;
}

function statsText(stat) {
    if (!stat || !stat.finished) return '暂无请求';
    return `${stat.finished}次 · 等${fmtMs(stat.lastWaitMs)} · 用${fmtMs(stat.lastRunMs)}`;
}

function renderMonitor() {
    const box = document.getElementById('rq_monitor');
    if (!box) return;
    const snap = state.queue?.snapshot();
    box.replaceChildren();
    if (!snap) {
        box.textContent = '队列未初始化';
        return;
    }

    const head = document.createElement('div');
    head.className = 'rq-mon-head';
    head.textContent = snap.holder
        ? `正在请求：${labelOf(snap.holder.key)}（已 ${fmtMs(snap.holder.elapsedMs)}）`
        : '空闲';
    box.append(head);

    if (snap.waiters.length) {
        const list = document.createElement('div');
        list.className = 'rq-mon-wait';
        list.textContent = `排队中：${snap.waiters.map((w) => `${labelOf(w.key)}(${fmtMs(w.waitedMs)})`).join(' → ')}`;
        box.append(list);
    }

    if (snap.history.length) {
        const ul = document.createElement('div');
        ul.className = 'rq-mon-hist';
        for (const item of snap.history.slice(0, 6)) {
            const line = document.createElement('div');
            line.textContent = `${labelOf(item.key)} · 等待 ${fmtMs(item.waitMs)} · 耗时 ${fmtMs(item.runMs)} · ${item.result}`;
            ul.append(line);
        }
        box.append(ul);
    }

    // 调试模式：把最近一次请求的原始调用栈摊开，归属不对时可以直接照着看是谁发的
    if (state.settings.debug && state.lastRawStack) {
        const label = document.createElement('div');
        label.className = 'rq-note';
        label.textContent = `最近一次请求的调用栈（归属：${labelOf(state.lastWho)}）`;
        const pre = document.createElement('pre');
        pre.className = 'rq-stack';
        pre.textContent = state.lastRawStack;
        box.append(label, pre);
    }
}

function renderSummary() {
    const el = document.getElementById('rq_summary');
    if (!el) return;
    el.textContent = state.settings.enabled ? '已启用' : '已停用';
    el.className = state.settings.enabled ? 'rq-state rq-on' : 'rq-state rq-off';
}

function cssEscape(value) {
    if (globalThis.CSS?.escape) return CSS.escape(value);
    return String(value).replace(/["\\]/g, '\\$&');
}

function startTicker() {
    if (state.ticker !== null) return;
    state.ticker = globalThis.setInterval(() => {
        if (!state.uiRoot?.isConnected) return;
        syncCollapsedFromDom(); // 折叠状态由 ST 的抽屉切换，这里跟随并记住
        if (!isDrawerOpen()) {
            updateCollapseTitle(); // 收起时不渲染正文，只更新标题提示
            return;
        }
        renderMonitor();
    }, 500);
    state.ticker?.unref?.();
}

function stopTicker() {
    if (state.ticker === null) return;
    clearInterval(state.ticker);
    state.ticker = null;
}

function toast(message) {
    try {
        globalThis.toastr?.info?.(message);
    } catch {
        console.info('[请求队列]', message);
    }
}

/* ------------------------------------------------------------------ *
 * 扩展发现
 * ------------------------------------------------------------------ */

async function refreshDiscovery() {
    state.discovered = await fetchDiscovered();
    log('discover', state.discovered);
    // 旧版本曾经把内置扩展也存进排序里，读到真实列表后顺手清掉
    pruneOrder();
    state.lastRenderedListKey = null;
    return state.discovered;
}

async function fetchDiscovered() {
    const native = state.nativeFetch ?? globalThis.fetch.bind(globalThis);
    try {
        const response = await native('/api/extensions/discover', {
            headers: ctx()?.getRequestHeaders?.() ?? {},
        });
        if (!response.ok) return [];
        return normalizeDiscovery(await response.json());
    } catch (err) {
        log('error', '读取插件列表失败', err);
        return [];
    }
}

function normalizeDiscovery(data) {
    const out = [];
    const seen = new Set();
    const push = (raw) => {
        if (!raw) return;
        const rawName = typeof raw === 'string'
            ? raw
            : (raw.name ?? raw.id ?? raw.folder ?? raw.displayName ?? '');
        if (typeof rawName !== 'string' || !rawName) return;
        const isThirdPartyPath = /^third-party\//i.test(rawName);
        const name = rawName.replace(/^third-party\//i, '').replace(/^\/+|\/+$/g, '');
        if (!name || name === 'third-party' || seen.has(name)) return;
        seen.add(name);
        const type = typeof raw === 'object' && typeof raw.type === 'string' && raw.type
            ? raw.type
            : (isThirdPartyPath ? 'global' : 'system');
        out.push({ name, type });
    };
    if (Array.isArray(data)) data.forEach(push);
    else if (data && typeof data === 'object') Object.values(data).forEach(push);
    return out;
}

/* ------------------------------------------------------------------ *
 * 斜线命令
 * ------------------------------------------------------------------ */

function registerCommands() {
    const c = ctx();
    const Parser = c?.SlashCommandParser;
    const Command = c?.SlashCommand;
    if (!Parser || !Command?.fromProps) return;

    const add = (props) => {
        try {
            Parser.addCommandObject(Command.fromProps(props));
            state.commands.push(props.name, ...(props.aliases ?? []));
        } catch (err) {
            log('error', `注册 /${props.name} 失败`, err);
        }
    };

    add({
        name: 'rq',
        aliases: ['request-queue'],
        helpString: '查看插件请求队列的当前状态',
        callback: () => formatStatus(),
        returns: c.ARGUMENT_TYPE?.STRING,
    });
    add({
        name: 'rq-on',
        helpString: '开启插件请求队列',
        callback: () => {
            setEnabled(true);
            return '插件请求队列：已开启';
        },
        returns: c.ARGUMENT_TYPE?.STRING,
    });
    add({
        name: 'rq-off',
        helpString: '关闭插件请求队列（所有请求恢复并行）',
        callback: () => {
            setEnabled(false);
            return '插件请求队列：已关闭';
        },
        returns: c.ARGUMENT_TYPE?.STRING,
    });
    add({
        name: 'rq-flush',
        helpString: '清空等待中的请求，强制让当前占用者放行',
        callback: () => {
            const dropped = state.queue?.flush('slash') ?? 0;
            return `已清空队列，丢弃 ${dropped} 个等待请求`;
        },
        returns: c.ARGUMENT_TYPE?.STRING,
    });
}

function unregisterCommands() {
    const Parser = ctx()?.SlashCommandParser;
    if (!Parser?.commands) return;
    for (const name of state.commands.splice(0)) {
        try {
            delete Parser.commands[name];
        } catch { /* ignore */ }
    }
}

function formatStatus() {
    const snap = state.queue?.snapshot();
    if (!snap) return '插件请求队列尚未初始化';
    const head = snap.holder ? `当前：${snap.holder.key}（${fmtMs(snap.holder.elapsedMs)}）` : '当前：空闲';
    const waiting = snap.waiters.length
        ? `排队：${snap.waiters.map((w) => w.key).join(' → ')}`
        : '排队：无';
    return [`插件请求队列 v${VERSION}`, `模式：${snap.mode}`, head, waiting].join('\n');
}

/* ------------------------------------------------------------------ *
 * 生命周期
 * ------------------------------------------------------------------ */

function applySettings() {
    state.queue?.configure({
        mode: state.settings.mode,
        settleMs: state.settings.settleMs,
        timeoutMs: state.settings.timeoutMs,
        maxWaitMs: state.settings.maxWaitMs,
        priorityOf,
        logger: log,
    });
}

let initPromise = null;

function init() {
    if (initPromise) return initPromise;
    initPromise = (async () => {
        state.settings = loadSettings();
        if (!state.queue) {
            state.queue = new RequestQueue({
                mode: state.settings.mode,
                settleMs: state.settings.settleMs,
                timeoutMs: state.settings.timeoutMs,
                maxWaitMs: state.settings.maxWaitMs,
                priorityOf,
                logger: log,
            });
        }
        applySettings();
        patchFetch();
        registerCommands();
        exposeApi();
        await mountUi();
        await refreshDiscovery();
        renderAll();
        // 认脚本：扫一遍现有脚本 iframe，并持续盯着新开的
        scheduleScriptApiWrap();
        // 等其它插件都装完再探一次：谁把 fetch 包了一层，这里就能认出来
        setTimeout(() => runRelayProbe(), 1500)?.unref?.();
        setTimeout(() => runRelayProbe(), 5000)?.unref?.();
        toast(`插件请求队列 v${VERSION} 已就绪`);
        log('init', '初始化完成', state.settings);
        state.initialized = true;
    })();
    return initPromise;
}

function teardown() {
    stopTicker();
    teardownScriptBridges();
    if (state.probeTimer) {
        clearTimeout(state.probeTimer);
        state.probeTimer = null;
    }
    unregisterCommands();
    try {
        state.queue?.flush('teardown');
    } catch { /* ignore */ }
    unpatchFetch();
    state.uiRoot?.remove();
    state.uiRoot = null;
    if (globalThis.STRequestQueue) delete globalThis.STRequestQueue;
    state.initialized = false;
    initPromise = null;
}

function exposeApi() {
    globalThis.STRequestQueue = {
        version: VERSION,
        get queue() {
            return state.queue;
        },
        /** 拿到令牌后才能发请求；用完调用返回的 release() */
        acquire: (key, meta) => state.queue.acquire(key, meta),
        /** 包一层：整个异步任务期间独占（适合“请求 + 写正文”这种完整动作） */
        run: async (key, task, meta = {}) => {
            const release = await state.queue.acquire(key, meta);
            try {
                return await task();
            } finally {
                release('done');
            }
        },
        status: () => state.queue.snapshot(),
        /** 最近一次归属判定看到的调用栈，排障用 */
        get lastTrace() {
            return state.lastTrace;
        },
        /** 当前被认定为转发层的插件 */
        get relays() {
            return [...relaySet()];
        },
        /** 认出来的脚本运行时里的脚本名 */
        get scripts() {
            return [...state.scriptNames].map((name) => ({
                name,
                key: state.settings.splitScripts ? scriptKeyOf(name) : SCRIPT_HOST,
            }));
        },
        /** 手动重新桥接脚本 iframe（新装的脚本不用刷新页面） */
        bridgeScripts: () => {
            installScriptBridges();
            wrapScriptApis();
            return `已扫描脚本 iframe：${[...state.scriptNames].join('、') || '无'}`;
        },
        /** 手动重新探测转发层（包了 window.fetch 的插件） */
        probe: () => {
            state.probedAgainst = undefined;
            runRelayProbe();
            return '已发起转发层探测';
        },
        flush: () => state.queue.flush('api'),
        moveKey,
        setEnabled,
    };
}

function boot() {
    let attempt = 0;
    const tryInit = () => {
        if (ctx()) {
            void init();
            return;
        }
        if (++attempt > 100) {
            console.warn('[请求队列] 等不到 SillyTavern 上下文，放弃初始化');
            return;
        }
        setTimeout(tryInit, 100);
    };
    tryInit();
}

/* 顶层就接管 fetch：此刻 ctx 可能还没就绪，先用默认设置，boot 后再按真实设置覆盖 */
state.settings = { ...DEFAULT_SETTINGS };
patchFetch();

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot, { once: true });
} else {
    boot();
}

/* SillyTavern 扩展生命周期钩子 */
export function onInstall() { }

export function onEnable() {
    void init();
}

export function onDisable() {
    teardown();
}

export {
    state as __internal,
    mountUi as __mountUi,
    setCollapsed as __setCollapsed,
    applyCollapsed as __applyCollapsed,
    currentKeys as __currentKeys,
    priorityOf as __priorityOf,
    isThirdParty as __isThirdParty,
    normalizeDiscovery as __normalizeDiscovery,
    bridgeScriptFrame as __bridgeScriptFrame,
    installScriptBridges as __installScriptBridges,
    wrapScriptApis as __wrapScriptApis,
    runWithOrigin as __runWithOrigin,
    scriptFetch as __scriptFetch,
    readOrigin as __readOrigin,
    whoForScript as __whoForScript,
    scriptNameFromWindowLike as __scriptNameFromWindowLike,
};
