// 把本插件收到的 QQ 消息转交给 OneBot shim，并吞掉本插件自己的回话。
//
// 放法：{dataRoot}/.qqbot-extensions/inbound/onebot-shim.mjs
// 改完 POST /api/qqbot-settings/ext-toggle {kind:'inbound',file:'onebot-shim.mjs',enabled:true} 热重载。
//
// 这条扩展同时提供**反向通道**：注册 POST /api/ext/dispatch?id=onebot-shim&p=/send，
// 让 shim 能用被动回复（带 msg_id）把上游的回话发出去。
// 为什么必须被动：群里走主动接口会被 QQ 官方拒绝 —— 40034105「主动消息发送失败，无权限」。
//
// 三个实现要点（踩过的坑）：
//   1. 路由必须在**模块加载时**就写进 globalThis.__qqbotExtRoutes，不能等第一条消息
//      （registerRoute 需要 ctx，而 shim 随时可能来调；Map 的键格式是 '<id>|<METHOD>|<path>'）。
//   2. 发送器要用 manager.channelSender（= 真 QQBotSender），不是中间件 ctx.bot。
//      manager 从 globalThis.__qqbotManagers 取（middleware-setup 按 dataRoot 登记）。
//   3. replyTarget.msgId **不能**从会话记录里拿：我们在早挂点 annihilate 掉了消息，
//      debounce 不会跑，record.replyTarget 永远是旧值。所以这里自己记入站 messageId。

export const name = 'onebot-shim';

const SHIM_INGEST = process.env.SHIM_INGEST || 'http://127.0.0.1:3000/_ingest';
const EXT_ID = process.env.SHIM_EXT_ID || 'onebot-shim';
const PASSIVE_WINDOW_MS = Number(process.env.SHIM_PASSIVE_WINDOW_MS || 4.5 * 60 * 1000);
const MAX_TARGETS = 200;

/** 'group:<openid>' / 'c2c:<openid>' -> { msgId, at } */
const latest = new Map();

function targetKey(scope, peerId) {
  return (scope === 'c2c' ? 'c2c:' : 'group:') + String(peerId || '');
}

function remember(scope, peerId, msgId, senderName) {
  const key = targetKey(scope, peerId);
  if (!peerId) return;
  latest.delete(key);
  latest.set(key, { msgId: String(msgId || ''), at: Date.now(), senderName: String(senderName || '') });
  while (latest.size > MAX_TARGETS) latest.delete(latest.keys().next().value);
}

function managerOf() {
  try {
    const gm = globalThis.__qqbotManagers;
    if (!gm) return null;
    if (gm instanceof Map) {
      for (const m of gm.values()) if (m && typeof m.getSessionRecord === 'function') return m;
      return null;
    }
    for (const k of Object.keys(gm)) {
      const m = gm[k];
      if (m && typeof m.getSessionRecord === 'function') return m;
    }
  } catch { /* ignore */ }
  return null;
}

/**
 * POST /api/ext/dispatch?id=onebot-shim&p=/send
 * body: { scope:'group'|'c2c', peerId: openid, text, msgId? }
 *   msgId 可选：诊断用，显式指定被动回复的消息 id（不传就用该会话最近收到的入站 id）。
 */
async function handleSend({ body }) {
  try {
    const scope = String((body && body.scope) || 'group') === 'c2c' ? 'c2c' : 'group';
    const peerId = String((body && body.peerId) || '').trim();
    const text = String((body && body.text) || '');
    if (!peerId) return { ok: false, reason: '缺 peerId' };
    if (!text.trim()) return { ok: false, reason: '缺 text' };

    const hit = latest.get(targetKey(scope, peerId));
    const msgId = String((body && body.msgId) || (hit && hit.msgId) || '');
    if (!msgId) return { ok: false, reason: '该目标还没收到过入站消息，没有被动窗口' };
    const age = hit ? Date.now() - hit.at : -1;
    if (!body || !body.msgId) {
      if (age > PASSIVE_WINDOW_MS) return { ok: false, reason: '被动窗口已过期（' + Math.round(age / 1000) + 's）' };
    }

    const mgr = managerOf();
    const sender = mgr && (mgr.channelSender || (typeof mgr.sender === 'object' ? mgr.sender : null));
    if (!sender || typeof sender.sendMarkdown !== 'function') {
      return { ok: false, reason: '取不到 dsh-qqbot 的 sender（manager 未登记？）' };
    }
    const target = { scope, targetId: peerId, msgId };
    await sender.sendMarkdown(target, text.slice(0, 2000));
    return { ok: true, via: 'passive', msgId, ageMs: age };
  } catch (e) {
    return { ok: false, reason: String((e && e.message) || e) };
  }
}

function routeTable() {
  const g = globalThis;
  if (!g.__qqbotExtRoutes) g.__qqbotExtRoutes = new Map();
  return g.__qqbotExtRoutes;
}

function registerRouteAtLoad() {
  try {
    const table = routeTable();
    table.set(EXT_ID + '|POST|/send', handleSend);
    table.set(EXT_ID + '|GET|/ping', () => ({ ok: true, targets: latest.size }));
    return true;
  } catch {
    return false;
  }
}

const ROUTE_REGISTERED = registerRouteAtLoad();

export async function onInbound(ctx, next) {
  try {
    const msg = (ctx && ctx.message) || {};
    const senderId = String(msg.senderId || '');
    const scope = msg.kind === 'group' ? 'group' : 'c2c';
    const peerId = scope === 'group' ? String(msg.groupOpenid || '') : senderId;

    // 先记被动窗口（任何 await 之前）
    remember(scope, peerId, msg.messageId, msg.senderName);
    if (!ROUTE_REGISTERED) registerRouteAtLoad();

    const text = String(msg.content || '');
    if (!senderId) return next();
    if (!text.trim() && !(ctx.platform && ctx.platform.mentioned)) return next();

    const res = await fetch(SHIM_INGEST, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        kind: scope,
        senderId,
        senderName: String(msg.senderName || ''),
        groupOpenid: scope === 'group' ? String(msg.groupOpenid || '') : '',
        text,
        messageId: String(msg.messageId || ''),
        mentioned: Boolean(ctx.platform && ctx.platform.mentioned),
        timestamp: Math.floor(Date.now() / 1000),
      }),
      signal: AbortSignal.timeout(3000),
    });
    const body = await res.json().catch(() => ({}));
    if (body && body.handled) {
      try {
        ctx.annihilate({ reason: 'onebot-shim: 已转交上游桥接处理' });
      } catch (e) {
        ctx.logger && ctx.logger.warn && ctx.logger.warn('[onebot-shim] annihilate 失败: ' + e.message);
      }
    }
  } catch {
    // shim 没起来 -> 放行，本插件照常回答（fail-open）
  }
  return next();
}
