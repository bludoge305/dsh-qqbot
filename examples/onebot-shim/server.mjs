// OneBot v11 shim: 让只认 OneBot 协议的 QQ 桥接（如 surfcore）接到本插件的 QQ 通道上。
//
//   3001 WS   : 上游桥接作为客户端连入；本进程推事件、回 action 响应
//   3000 HTTP : 上游直连的 OneBot HTTP action（send_*_msg / get_msg / get_status ...）
//   POST /_ingest : 本插件的入站扩展把收到的 QQ 消息投进来，转成 OneBot 事件
//
// 为什么要这个 shim：本插件没有 OneBot 支持（它走腾讯官方 QQBot SDK），
// 而上游桥接只会说 OneBot v11。两边靠这个进程互转。
//
// 关键约束（踩过的坑）：
//   1. 群消息必须走**被动回复**（带 msg_id），主动发送会被 QQ 官方拒绝：
//      40034105「主动消息发送失败，无权限」。
//   2. 所以出站优先打 /api/ext/dispatch 让入站扩展用它手上的 msg_id 发；
//      没有窗口时才退回主动接口（群里通常会被拒，这是官方限制不是 bug）。
import { createServer } from 'node:http';
import { WebSocketServer } from 'ws';
import { readFile, writeFile, appendFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const STATE = path.join(HERE, 'state');
const IDS_FILE = path.join(STATE, 'ids.json');
const MSGS_FILE = path.join(STATE, 'messages.json');
const LOG_FILE = path.join(STATE, 'shim.log');

// ---- 全部走环境变量：这里有 QQ 号/openid，绝不能硬编码进仓库 ----
// DSH 桌面端每次重启都可能换端口，它会导出一个 DSH_WEB_URL；
// 没有显式配置时跟着它走，否则重启一次 shim 就连不上（fetch failed）。
const DSH = process.env.DSH_BASE_URL || process.env.DSH_WEB_URL || 'http://127.0.0.1:9220';
const NS = process.env.QQBOT_NS || 'im-qqbot';
const BOT_ID = Number(process.env.QQBOT_ID || 0);
const BOT_NAME = process.env.QQBOT_NAME || 'QQ Bot';
const OWNER_QQ = Number(process.env.OWNER_QQ || 0);
const OWNER_OPENID = process.env.OWNER_OPENID || '';
const EXT_ID = process.env.SHIM_EXT_ID || 'onebot-shim';
const HTTP_PORT = Number(process.env.SHIM_HTTP_PORT || 3000);
const WS_PORT = Number(process.env.SHIM_WS_PORT || 3001);
const MAX_MSGS = 400;

await mkdir(STATE, { recursive: true });

function log(...a) {
  const line = '[' + new Date().toISOString() + '] ' + a.join(' ');
  console.log(line);
  appendFile(LOG_FILE, line + '\n').catch(() => {});
}

async function readJson(file, fallback) {
  try { return JSON.parse(await readFile(file, 'utf8')); } catch { return fallback; }
}

const ids = await readJson(IDS_FILE, { private: {}, group: {}, user: {}, seq: 0, msg: 0 });
const msgs = await readJson(MSGS_FILE, []);
const names = {};       // uid -> nickname
const groupNames = {};  // groupOpenid -> name

let saveTimer = null;
function saveSoon() {
  if (saveTimer) return;
  saveTimer = setTimeout(async () => {
    saveTimer = null;
    try { await writeFile(IDS_FILE, JSON.stringify(ids, null, 1)); } catch {}
    try { await writeFile(MSGS_FILE, JSON.stringify(msgs.slice(-MAX_MSGS))); } catch {}
  }, 400);
}

function nextSeq() { ids.seq = (ids.seq || 0) + 1; saveSoon(); return ids.seq; }
function nextMsgId() { ids.msg = (ids.msg || 0) + 1; saveSoon(); return ids.msg; }

// OneBot 用整数 id，官方 QQBot 用字符串 openid —— 这里做双向映射，
// 映射表落 state/ids.json，所以重启后 id 稳定。
function userIdOf(openid) {
  const k = String(openid || '');
  if (!k) return 0;
  if (ids.private[k]) return ids.private[k];
  if (OWNER_OPENID && k === OWNER_OPENID && OWNER_QQ) { ids.private[k] = OWNER_QQ; saveSoon(); return OWNER_QQ; }
  ids.private[k] = 2000000000 + nextSeq();
  saveSoon();
  return ids.private[k];
}

function groupIdOf(openid) {
  const k = String(openid || '');
  if (!k) return 0;
  if (ids.group[k]) return ids.group[k];
  ids.group[k] = 800000000 + nextSeq();
  saveSoon();
  return ids.group[k];
}

function privateOpenidOf(uid) {
  const n = Number(uid);
  if (OWNER_QQ && n === OWNER_QQ) return OWNER_OPENID;
  for (const [openid, id] of Object.entries(ids.private)) if (Number(id) === n) return openid;
  return null;
}

function groupOpenidOf(gid) {
  const n = Number(gid);
  for (const [openid, id] of Object.entries(ids.group)) if (Number(id) === n) return openid;
  return null;
}

function record(m) {
  msgs.push(m);
  if (msgs.length > MAX_MSGS) msgs.splice(0, msgs.length - MAX_MSGS);
  saveSoon();
}

function cqUnescape(s) {
  return String(s)
    .replace(/&#91;/g, '[').replace(/&#93;/g, ']')
    .replace(/&#44;/g, ',').replace(/&amp;/g, '&');
}

function segsToText(segs) {
  if (!Array.isArray(segs)) return cqUnescape(String(segs ?? ''));
  return segs.map((s) => {
    const t = s && s.type;
    const d = (s && s.data) || {};
    if (t === 'text') return cqUnescape(String(d.text ?? ''));
    if (t === 'at') return String(d.qq) === 'all' ? '@全体成员' : '@' + String(d.qq);
    if (t === 'face') return '[表情]';
    if (t === 'image') return '[图片]';
    return '';
  }).join('').trim();
}

// ---------------- 出站：交给本插件 ----------------
async function dshPost(pathname, body) {
  const res = await fetch(DSH + pathname, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(20000),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok || json.ok === false) {
    throw new Error('dsh-qqbot ' + pathname + ' 失败: ' + (json.err ? JSON.stringify(json.err) : res.status));
  }
  return json;
}

function chunk(text, size = 1800) {
  const s = String(text ?? '');
  if (s.length <= size) return [s];
  const out = [];
  for (let i = 0; i < s.length; i += size) out.push(s.slice(i, i + size));
  return out;
}

// 被动优先：请入站扩展用它手上那条消息的 msg_id 回复。
// 注意 dispatch 对「返回值里已含 ok」的 handler 是原样透传（不再包 data），
// 所以两种形状都要认：{ok,via,msgId} 或 {ok:true,data:{ok,via,msgId}}。
async function dshPassive(scope, peerId, text) {
  try {
    const res = await fetch(DSH + '/api/ext/dispatch?id=' + encodeURIComponent(EXT_ID) + '&p=/send', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ scope, peerId, text }),
      signal: AbortSignal.timeout(20000),
    });
    const j = await res.json().catch(() => ({}));
    const d = (j && j.ok === true && j.data && typeof j.data === 'object') ? j.data : (j || {});
    if (d.ok === true) {
      if (d.via) return { ok: true, via: d.via, msgId: d.msgId || '' };
      return { ok: false, reason: d.reason || '被动通道拒绝' };
    }
    return { ok: false, reason: String(d.reason || d.error || (j && j.reason) || 'dispatch 拒绝') };
  } catch (e) {
    return { ok: false, reason: String((e && e.message) || e) };
  }
}

async function sendGroup(groupOpenid, text) {
  if (!groupOpenid) throw new Error('未知群 openid');
  let mid = 0;
  for (const part of chunk(text)) {
    if (!part) continue;
    const pas = await dshPassive('group', groupOpenid, part);
    if (pas.ok) {
      log('group send PASSIVE via=' + pas.via + ' msgId=' + String(pas.msgId).slice(-8));
    } else {
      log('group send passive miss (' + pas.reason + ') -> active fallback');
      await dshPost('/api/qqbot-settings/group/send', { ns: NS, gid: groupOpenid, text: part });
    }
    mid = nextMsgId();
    record({ mid, kind: 'group', convId: groupIdOf(groupOpenid), openid: groupOpenid, uid: BOT_ID, text: part, name: BOT_NAME, time: Math.floor(Date.now() / 1000), self: true });
  }
  return mid;
}

async function sendC2c(openid, text) {
  if (!openid) throw new Error('未知私聊 openid');
  let mid = 0;
  for (const part of chunk(text)) {
    if (!part) continue;
    const pas = await dshPassive('c2c', openid, part);
    if (pas.ok) {
      log('c2c send PASSIVE via=' + pas.via + ' msgId=' + String(pas.msgId).slice(-8));
    } else {
      log('c2c send passive miss (' + pas.reason + ') -> active fallback');
      await dshPost('/api/qqbot-settings/chat/send', { ns: NS, openid, text: part });
    }
    mid = nextMsgId();
    record({ mid, kind: 'private', convId: userIdOf(openid), openid, uid: BOT_ID, text: part, name: BOT_NAME, time: Math.floor(Date.now() / 1000), self: true });
  }
  return mid;
}

// ---------------- OneBot action 分发 ----------------
async function handleAction(action, p = {}) {
  switch (action) {
    case 'get_login_info':
      return { user_id: BOT_ID, nickname: BOT_NAME };
    case 'get_status':
      return { online: true, good: true, app_enabled: true, app_good: true, app_initialized: true };
    case 'get_version_info':
      return { app_name: 'dsh-qqbot-shim', app_version: '0.1.0', protocol_version: 'v11' };
    case 'send_private_msg':
      return { message_id: await sendC2c(privateOpenidOf(p.user_id), segsToText(p.message)) };
    case 'send_group_msg':
      return { message_id: await sendGroup(groupOpenidOf(p.group_id), segsToText(p.message)) };
    case 'send_msg':
      if (p.message_type === 'group') return { message_id: await sendGroup(groupOpenidOf(p.group_id), segsToText(p.message)) };
      return { message_id: await sendC2c(privateOpenidOf(p.user_id), segsToText(p.message)) };
    case 'get_msg': {
      const m = msgs.find((x) => Number(x.mid) === Number(p.message_id));
      if (!m) return null;
      return msgToRaw(m, true);
    }
    case 'get_group_info': {
      const openid = groupOpenidOf(p.group_id);
      return { group_id: Number(p.group_id), group_name: groupNames[openid] || ('群' + Number(p.group_id)), member_count: 0, max_member_count: 0 };
    }
    case 'get_group_member_info': {
      const n = String(p.user_id);
      return { group_id: Number(p.group_id), user_id: Number(p.user_id), nickname: names[n] || n, card: names[n] || '' };
    }
    case 'get_friend_list':
      return OWNER_QQ ? [{ user_id: OWNER_QQ, nickname: names[String(OWNER_QQ)] || 'owner', remark: '' }] : [];
    case 'get_group_list':
      return Object.entries(ids.group).map(([openid, gid]) => ({ group_id: gid, group_name: groupNames[openid] || ('群' + gid), member_count: 0 }));
    case 'get_group_msg_history':
    case 'get_friend_msg_history': {
      const wantGroup = action === 'get_group_msg_history';
      const conv = wantGroup ? Number(p.group_id) : Number(p.user_id);
      const list = msgs.filter((m) => (wantGroup ? m.kind === 'group' && m.convId === conv : m.kind === 'private' && m.convId === conv));
      return { messages: list.slice(-(Number(p.count) || 20)).map((m) => msgToRaw(m)) };
    }
    case 'get_forward_msg':
      return { messages: [] };
    case 'send_poke':
    case 'group_poke':
    case 'set_group_ban':
    case 'set_group_whole_ban':
      return {};
    case 'fetch_ptt_text':
      return { text: '' };
    case 'get_mini_app_ark':
      return null;
    case 'fetch_custom_face_detail':
      return { data: [] };
    default:
      log('unknown action: ' + action);
      return {};
  }
}

function msgToRaw(m, withPostType = false) {
  const base = {
    message_id: m.mid, time: m.time, raw_message: m.text,
    message: [{ type: 'text', data: { text: m.text } }],
    sender: { user_id: m.uid, nickname: m.name || '', card: m.name || '' },
  };
  const head = withPostType ? { post_type: 'message' } : {};
  if (m.kind === 'group') return { ...head, ...base, message_type: 'group', group_id: m.convId, user_id: m.uid, self_id: BOT_ID };
  return { ...head, ...base, message_type: 'private', user_id: m.self ? BOT_ID : m.uid, self_id: BOT_ID };
}

// ---------------- HTTP ----------------
function json(res, obj, code = 200) {
  const body = JSON.stringify(obj);
  res.writeHead(code, { 'content-type': 'application/json; charset=utf-8', 'content-length': Buffer.byteLength(body) });
  res.end(body);
}

function readBody(req) {
  return new Promise((resolve) => {
    let buf = '';
    req.on('data', (c) => { buf += c; if (buf.length > 8e6) req.destroy(); });
    req.on('end', () => { try { resolve(buf ? JSON.parse(buf) : {}); } catch { resolve({}); } });
    req.on('error', () => resolve({}));
  });
}

const server = createServer(async (req, res) => {
  const url = new URL(req.url || '/', 'http://127.0.0.1');
  const body = req.method === 'POST' || req.method === 'PUT' ? await readBody(req) : {};

  if (url.pathname === '/_health') {
    return json(res, { ok: true, clients: wss.clients.size, messages: msgs.length, ids: { private: Object.keys(ids.private).length, group: Object.keys(ids.group).length } });
  }
  if (req.method === 'POST' && url.pathname === '/_ingest') {
    try {
      const handled = await ingest(body);
      return json(res, { handled, clients: wss.clients.size });
    } catch (e) {
      log('ingest error: ' + (e && e.message));
      return json(res, { handled: false, error: String(e && e.message) });
    }
  }

  const action = url.pathname.replace(/^\/+/, '');
  try {
    const data = await handleAction(action, body);
    return json(res, { status: 'ok', retcode: 0, data });
  } catch (e) {
    log('action ' + action + ' failed: ' + (e && e.message));
    return json(res, { status: 'failed', retcode: 1, wording: String((e && e.message) || e), data: null });
  }
});

// ---------------- WS ----------------
const wss = new WebSocketServer({ host: '127.0.0.1', port: WS_PORT });
wss.on('listening', () => log('WS listening 127.0.0.1:' + WS_PORT));
wss.on('connection', (ws, req) => {
  log('upstream bridge connected (' + (req && req.socket ? req.socket.remoteAddress : '?') + '), total=' + wss.clients.size);
  // SDK 只按 echo 匹配响应、只按 post_type 判事件 —— 连接后推一条 lifecycle 让它知道我们活着
  ws.send(JSON.stringify({ post_type: 'meta_event', meta_event_type: 'lifecycle', sub_type: 'connect', self_id: BOT_ID, time: Math.floor(Date.now() / 1000) }));
  ws.on('message', async (buf) => {
    let pkt;
    try { pkt = JSON.parse(String(buf)); } catch { return; }
    if (!pkt || typeof pkt.action !== 'string') return;
    const echo = pkt.echo;
    try {
      const data = await handleAction(pkt.action, pkt.params || {});
      ws.send(JSON.stringify({ status: 'ok', retcode: 0, data, echo }));
    } catch (e) {
      log('ws action ' + pkt.action + ' failed: ' + (e && e.message));
      ws.send(JSON.stringify({ status: 'failed', retcode: 1, wording: String((e && e.message) || e), data: null, echo }));
    }
  });
  ws.on('close', () => log('upstream bridge disconnected, total=' + wss.clients.size));
  ws.on('error', (e) => log('ws error: ' + (e && e.message)));
});

function broadcast(evt) {
  const s = JSON.stringify(evt);
  let n = 0;
  for (const c of wss.clients) if (c.readyState === 1) { c.send(s); n += 1; }
  return n;
}

async function ingest(p) {
  const kind = p && p.kind === 'group' ? 'group' : 'private';
  const time = Number((p && p.timestamp) || 0) || Math.floor(Date.now() / 1000);
  let text = String((p && p.text) || '').trim();
  if (p && p.mentioned) text = '@' + BOT_ID + ' ' + text;
  if (!text) return false;
  const mid = nextMsgId();
  const message = [{ type: 'text', data: { text } }];
  let evt;
  if (kind === 'group') {
    const gid = groupIdOf(p.groupOpenid);
    const uid = userIdOf(p.senderId);
    if (p.senderName) names[String(uid)] = String(p.senderName);
    if (p.groupName) groupNames[String(p.groupOpenid)] = String(p.groupName);
    evt = {
      post_type: 'message', message_type: 'group', sub_type: 'normal', self_id: BOT_ID,
      group_id: gid, user_id: uid, message_id: mid, message, raw_message: text, time,
      sender: { user_id: uid, nickname: String(p.senderName || ''), card: String(p.senderName || '') },
    };
    record({ mid, kind: 'group', convId: gid, openid: p.groupOpenid, uid, text, name: String(p.senderName || ''), time, self: false });
  } else {
    const uid = userIdOf(p.senderId);
    if (p.senderName) names[String(uid)] = String(p.senderName);
    evt = {
      post_type: 'message', message_type: 'private', sub_type: 'friend', self_id: BOT_ID,
      user_id: uid, message_id: mid, message, raw_message: text, time,
      sender: { user_id: uid, nickname: String(p.senderName || '') },
    };
    record({ mid, kind: 'private', convId: uid, openid: p.senderId, uid, text, name: String(p.senderName || ''), time, self: false });
  }
  const n = broadcast(evt);
  log('ingest ' + kind + ' mid=' + mid + ' uid=' + evt.user_id + ' -> ' + n + ' client(s)');
  return n > 0;
}

server.listen(HTTP_PORT, '127.0.0.1', () => log('HTTP listening 127.0.0.1:' + HTTP_PORT));
log('shim up; ids=' + JSON.stringify({ private: Object.keys(ids.private).length, group: Object.keys(ids.group).length }));