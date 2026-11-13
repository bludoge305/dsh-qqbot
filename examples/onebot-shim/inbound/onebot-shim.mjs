// 把 dsh-qqbot 收到的 QQ 消息转交 surfcore（经本地 OneBot shim），并吞掉本插件自己的回话。
//
// 2026-11-13 追加反向通道：注册 POST /api/ext/dispatch?id=onebot-shim&p=/send，
// 让 shim 能用**被动回复**（带 msg_id）把 surfcore 的回话发出去。
// 为什么必须被动：群里走主动接口会被 QQ 官方拒绝 —— 40034105「主动消息发送失败，无权限」。
//
// 多账号说明（2026-11-13）：同一份扩展会装进多个 dataRoot（im-qqbot-2、im-qqbot-3…），
// 于是模块会被加载多份，而 __qqbotExtRoutes 的键是同一个 → 后加载的覆盖先加载的。
// 为此：被动窗口表挂到 globalThis 共享，请求体带 appId/dataRoot，handler 按 dataRoot 精确取 manager。
//
// 三个实现要点（踩过的坑）：
//   1. 路由必须在**模块加载时**就写进 globalThis.__qqbotExtRoutes，不能等第一条消息
//      （registerRoute 需要 ctx，而 shim 随时可能来调；Map 的键格式是 '<id>|<METHOD>|<path>'）。
//   2. 发送器要用 manager.channelSender（= 真 QQBotSender），不是中间件 ctx.bot。
//      manager 从 globalThis.__qqbotManagers 取（middleware-setup 按 dataRoot 登记）。
//   3. replyTarget.msgId **不能**从会话记录里拿：我们在早挂点 annihilate 掉了消息，
//      debounce 不会跑，record.replyTarget 永远是旧值。所以这里自己记入站 messageId。

export const name = 'onebot-shim';

const SHIM_INGEST = 'http://127.0.0.1:3000/_ingest';
const EXT_ID = 'onebot-shim';
const PASSIVE_WINDOW_MS = 4.5 * 60 * 1000;
const MAX_TARGETS = 500;

/** '<appId>|group:<openid>' / '<appId>|c2c:<openid>' -> { msgId, at, senderName }
 *  挂 globalThis：多份模块实例共享同一张表，谁的 handler 被留下都能查到任意账号的窗口。 */
const latest = (globalThis.__onebotShimLatest ||= new Map());

function targetKey(appId, scope, peerId) {
  return String(appId || '') + '|' + (scope === 'c2c' ? 'c2c:' : 'group:') + String(peerId || '');
}

function remember(appId, scope, peerId, msgId, senderName) {
  if (!peerId) return;
  const key = targetKey(appId, scope, peerId);
  latest.delete(key);
  latest.set(key, { msgId: String(msgId || ''), at: Date.now(), senderName: String(senderName || '') });
  while (latest.size > MAX_TARGETS) latest.delete(latest.keys().next().value);
}

function normRoot(p) {
  return String(p || '').replace(/[\\/]+/g, '\\').replace(/\\+$/, '').toLowerCase();
}

/** 按 dataRoot 精确取 manager；取不到再退回“随便挑一个”。 */
function managerOf(dataRoot) {
  try {
    const gm = globalThis.__qqbotManagers;
    if (!gm) return null;
    const entries = gm instanceof Map ? [...gm.entries()] : Object.entries(gm);
    const usable = entries.filter(([, m]) => m && typeof m.getSessionRecord === 'function');
    const want = normRoot(dataRoot);
    if (want) {
      const exact = usable.find(([k]) => normRoot(k) === want);
      if (exact) return exact[1];
      const loose = usable.find(([k]) => normRoot(k).endsWith(want) || want.endsWith(normRoot(k)));
      if (loose) return loose[1];
    }
    return usable.length ? usable[0][1] : null;
  } catch { /* ignore */ }
  return null;
}

/**
 * POST /api/ext/dispatch?id=onebot-shim&p=/send
 * body: { scope:'group'|'c2c', peerId: openid, text, appId?, dataRoot?, msgId? }
 *   msgId 可选：诊断用，显式指定被动回复的消息 id（不传就用本条会话最近收到的入站 id）。
 */
async function handleSend({ body }) {
  try {
    const scope = String((body && body.scope) || 'group') === 'c2c' ? 'c2c' : 'group';
    const peerId = String((body && body.peerId) || '').trim();
    const text = String((body && body.text) || '');
    const appId = String((body && body.appId) || '');
    if (!peerId) return { ok: false, reason: '缺 peerId' };
    if (!text.trim()) return { ok: false, reason: '缺 text' };

    const hit = latest.get(targetKey(appId, scope, peerId));
    const msgId = String((body && body.msgId) || (hit && hit.msgId) || '');
    if (!msgId) return { ok: false, reason: '该目标还没收到过入站消息，没有被动窗口' };
    const age = hit ? Date.now() - hit.at : -1;
    if (!body || !body.msgId) {
      if (age > PASSIVE_WINDOW_MS) return { ok: false, reason: '被动窗口已过期（' + Math.round(age / 1000) + 's）' };
    }

    const mgr = managerOf(body && body.dataRoot);
    const sender = mgr && (mgr.channelSender || (typeof mgr.sender === 'object' ? mgr.sender : null));
    if (!sender || typeof sender.sendMarkdown !== 'function') {
      return { ok: false, reason: '取不到 dsh-qqbot 的 sender（manager 未登记？）' };
    }
    const target = { scope, targetId: peerId, msgId };
    await sender.sendMarkdown(target, text.slice(0, 2000));
    return { ok: true, via: 'passive', msgId, ageMs: age, appId, dataRoot: String((body && body.dataRoot) || '') };
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
    const appId = String((ctx && ctx.appId) || '');
    const dataRoot = String((ctx && ctx.dataRoot) || '');

    // 先记被动窗口（任何 await 之前）
    remember(appId, scope, peerId, msg.messageId, msg.senderName);
    if (!ROUTE_REGISTERED) registerRouteAtLoad();

    const text = String(msg.content || '');
    if (!senderId) return next();
    if (!text.trim() && !(ctx.platform && ctx.platform.mentioned)) return next();

    const res = await fetch(SHIM_INGEST, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        kind: scope,
        appId,
        dshUrl: process.env.DSH_WEB_URL || process.env.DSH_BASE_URL || '',
        dataRoot,
        senderId,
        senderName: String(msg.senderName || ''),
        groupOpenid: scope === 'group' ? String(msg.groupOpenid || '') : '',
        text,
        messageId: String(msg.messageId || ''),
        mentioned: Boolean(ctx.platform && ctx.platform.mentioned),
        isOwner: Boolean(ctx.isOwner),
        timestamp: Math.floor(Date.now() / 1000),
      }),
      signal: AbortSignal.timeout(3000),
    });
    const body = await res.json().catch(() => ({}));
    if (body && body.handled) {
      try {
        ctx.annihilate({ reason: 'onebot-shim: 已转交 surfcore 处理' });
      } catch (e) {
        ctx.logger && ctx.logger.warn && ctx.logger.warn('[onebot-shim] annihilate 失败: ' + e.message);
      }
    }
  } catch {
    // shim 没起来 -> 放行，dsh-qqbot 照常回答
  }
  return next();
}
