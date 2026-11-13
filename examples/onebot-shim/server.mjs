// OneBot v11 shim: dsh-qqbot (@zaofan) <-> surfcore QQ bridge
//   3001 WS   : surfcore 作为客户端连入；本进程推事件、回 action 响应
//   3000 HTTP : surfcore 直连的 OneBot HTTP action（send_*_msg / get_msg / get_status ...）
//   POST /_ingest : dsh-qqbot 入站扩展把收到的 QQ 消息投进来，转成 OneBot 事件
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

// DSH 桌面端每次重启都换端口，所以这份地址是活的：入站扩展每次投递都会带上它自己的
// process.env.DSH_WEB_URL，shim 收到就更新，免得 DSH 换端口后出站全打到旧地址。
let DSH = process.env.DSH_BASE_URL || process.env.DSH_WEB_URL || 'http://127.0.0.1:9220';
// 主人 QQ 与它在机器人下的 openid。openid 按 AppID 隔离，所以「同一个人的 openid」
// 每个机器人各有一串 —— 这里只能给默认号那一份，其余账号由入站扩展的 isOwner 学到后
// 记进 state/accounts.json 的 ownerOpenid。
const OWNER_QQ = Number(process.env.QQBOT_OWNER_QQ || 0);
const OWNER_OPENID = process.env.QQBOT_OWNER_OPENID || '';
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
const names = {};   // uid -> nickname
const groupNames = {}; // groupOpenid -> name

// ── 多账号 ────────────────────────────────────────────────────────────────
// QQ 的 openid 是按 AppID 隔离的：同一个人/同一个群在不同机器人下 openid 完全不同。
// 所以（a）映射表必须按 appId 分区；（b）出站必须按 appId 找对应实例的 ns，
// 否则 A 号收到消息却拿 B 号的 ns 去发，QQ 会直接拒绝。
const ACCOUNTS_FILE = path.join(STATE, 'accounts.json');

async function loadAccounts() {
  const saved = await readJson(ACCOUNTS_FILE, null);
  const list = (saved && saved.accounts) || {};
  // 首次运行（state/accounts.json 还不存在）：从环境变量种一份。
  //   QQBOT_ACCOUNTS='{"<appId>":{"ns":"im-qqbot-2","name":"Bot A"}}'  —— 多号
  //   QQBOT_ID / QQBOT_NS / QQBOT_NAME                                    —— 只有一个号时更省事
  if (!Object.keys(list).length) {
    try {
      const fromEnv = JSON.parse(process.env.QQBOT_ACCOUNTS || '{}');
      for (const [k, v] of Object.entries(fromEnv || {})) if (k && v) list[String(k)] = v;
    } catch {}
    if (!Object.keys(list).length && process.env.QQBOT_ID) {
      list[String(process.env.QQBOT_ID)] = { ns: process.env.QQBOT_NS || 'im-qqbot', name: process.env.QQBOT_NAME || 'QQ Bot' };
    }
  }
  return list;
}

const ACCOUNTS = await loadAccounts();
const DEFAULT_APPID = String(process.env.QQBOT_APPID || Object.keys(ACCOUNTS)[0] || process.env.QQBOT_ID || '');

function accountOf(appId) {
  const id = String(appId || DEFAULT_APPID);
  return ACCOUNTS[id] || { ns: process.env.QQBOT_NS || 'im-qqbot', name: process.env.QQBOT_NAME || 'QQ Bot' };
}

function botIdOf(appId) {
  const n = Number(appId);
  return Number.isFinite(n) && n > 0 ? n : Number(DEFAULT_APPID);
}

async function saveAccounts() {
  try { await writeFile(ACCOUNTS_FILE, JSON.stringify({ accounts: ACCOUNTS }, null, 1)); } catch {}
}

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

// 映射表键 = "<appId>:<openid>"。老数据是裸 openid（等价于默认账号），读时兼容。
function mapKey(appId, openid) {
  return String(appId) + ':' + String(openid);
}

function userIdOf(appId, openid) {
  const k = String(openid || '');
  if (!k) return 0;
  const key = mapKey(appId, k);
  const acct = accountOf(appId);
  // 主人：openid 是按 AppID 隔离的，所以每个账号各记一份（第一次由入站扩展的 isOwner 学到）。
  const isOwner = (acct.ownerOpenid && k === acct.ownerOpenid)
    || (OWNER_OPENID && appId === DEFAULT_APPID && k === OWNER_OPENID);
  if (isOwner && OWNER_QQ) { ids.private[key] = OWNER_QQ; saveSoon(); return OWNER_QQ; }
  if (ids.private[key]) return ids.private[key];
  if (appId === DEFAULT_APPID && ids.private[k]) { ids.private[key] = ids.private[k]; saveSoon(); return ids.private[key]; }
  ids.private[key] = 2000000000 + nextSeq();
  saveSoon();
  return ids.private[key];
}

function groupIdOf(appId, openid) {
  const k = String(openid || '');
  if (!k) return 0;
  const key = mapKey(appId, k);
  if (ids.group[key]) return ids.group[key];
  if (appId === DEFAULT_APPID && ids.group[k]) { ids.group[key] = ids.group[k]; saveSoon(); return ids.group[key]; }
  ids.group[key] = 800000000 + nextSeq();
  saveSoon();
  return ids.group[key];
}

// 反向：伪 id -> { appId, openid }。出站和 get_msg 都靠它定位是哪个号、发给谁。
function targetOf(map, id) {
  const n = Number(id);
  for (const [key, val] of Object.entries(map)) {
    if (Number(val) !== n) continue;
    const i = key.indexOf(':');
    if (i > 0) return { appId: key.slice(0, i), openid: key.slice(i + 1) };
    return { appId: DEFAULT_APPID, openid: key };
  }
  return null;
}

function userTargetOf(uid) {
  // 只按伪 id 反查，**不要**对主人做特例：openid 按 AppID 隔离，主人可能在多个号下
  // 各有一个 openid，硬绑到默认号会让另一个号收到的私聊被拿错号的 ns 去发（400）。
  return targetOf(ids.private, uid);
}

function groupTargetOf(gid) {
  return targetOf(ids.group, gid);
}

function groupListOf(appId) {
  return Object.entries(ids.group)
    .filter(([key]) => key.startsWith(String(appId) + ':'))
    .map(([key, gid]) => ({ openid: key.slice(key.indexOf(':') + 1), gid }));
}

function record(m) {
  msgs.push(m);
  if (msgs.length > MAX_MSGS) msgs.splice(0, msgs.length - MAX_MSGS);
  saveSoon();
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
    if (t === 'reply') return '';
    return '';
  }).join('').trim();
}

function cqUnescape(s) {
  return String(s)
    .replace(/&#91;/g, '[').replace(/&#93;/g, ']')
    .replace(/&#44;/g, ',').replace(/&amp;/g, '&');
}

// ---------------- dsh-qqbot 出站 ----------------
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

// 被动优先：先请 dsh-qqbot 的入站扩展用它手上那条消息的 msg_id 走被动回复
// （群里主动消息会被 QQ 官方拒绝：40034105 主动消息无权限）。
async function dshPassive(scope, peerId, text, appId) {
  try {
    const res = await fetch(DSH + '/api/ext/dispatch?id=onebot-shim&p=/send', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ scope, peerId, text, appId, dataRoot: accountOf(appId).dataRoot || '' }),
      signal: AbortSignal.timeout(20000),
    });
    const j = await res.json().catch(() => ({}));
    // dispatch 对「返回值里已含 ok」的 handler 是原样透传（不再包一层 data），
    // 所以两种形状都要认：{ok,via,msgId} 或 {ok:true,data:{ok,via,msgId}}。
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

async function sendGroup(target, text) {
  const { appId, openid: groupOpenid } = target || {};
  if (!groupOpenid) throw new Error('未知群 openid');
  const acct = accountOf(appId);
  const botId = botIdOf(appId);
  let mid = 0;
  for (const part of chunk(text)) {
    if (!part) continue;
    const pas = await dshPassive('group', groupOpenid, part, appId);
    if (pas.ok) {
      log('group send PASSIVE ns=' + acct.ns + ' via=' + pas.via + ' msgId=' + String(pas.msgId).slice(-8));
    } else {
      log('group send passive miss ns=' + acct.ns + ' (' + pas.reason + ') -> active fallback');
      await dshPost('/api/qqbot-settings/group/send', { ns: acct.ns, gid: groupOpenid, text: part });
    }
    mid = nextMsgId();
    record({ mid, kind: 'group', appId, convId: groupIdOf(appId, groupOpenid), openid: groupOpenid, uid: botId, text: part, name: acct.name, time: Math.floor(Date.now() / 1000), self: true });
  }
  return mid;
}

async function sendC2c(target, text) {
  const { appId, openid } = target || {};
  if (!openid) throw new Error('未知私聊 openid');
  const acct = accountOf(appId);
  const botId = botIdOf(appId);
  let mid = 0;
  for (const part of chunk(text)) {
    if (!part) continue;
    const pas = await dshPassive('c2c', openid, part, appId);
    if (pas.ok) {
      log('c2c send PASSIVE ns=' + acct.ns + ' via=' + pas.via + ' msgId=' + String(pas.msgId).slice(-8));
    } else {
      log('c2c send passive miss ns=' + acct.ns + ' (' + pas.reason + ') -> active fallback');
      await dshPost('/api/qqbot-settings/chat/send', { ns: acct.ns, openid, text: part });
    }
    mid = nextMsgId();
    record({ mid, kind: 'private', appId, convId: userIdOf(appId, openid), openid, uid: botId, text: part, name: acct.name, time: Math.floor(Date.now() / 1000), self: true });
  }
  return mid;
}

// ---------------- OneBot action 分发 ----------------
async function handleAction(action, p = {}) {
  switch (action) {
    case 'get_login_info':
      return { user_id: botIdOf(DEFAULT_APPID), nickname: accountOf(DEFAULT_APPID).name };
    case 'get_status':
      return { online: true, good: true, app_enabled: true, app_good: true, app_initialized: true };
    case 'get_version_info':
      return { app_name: 'dsh-qqbot-shim', app_version: '0.1.0', protocol_version: 'v11' };
    case 'send_private_msg':
      return { message_id: await sendC2c(userTargetOf(p.user_id), segsToText(p.message)) };
    case 'send_group_msg':
      return { message_id: await sendGroup(groupTargetOf(p.group_id), segsToText(p.message)) };
    case 'send_msg':
      if (p.message_type === 'group') return { message_id: await sendGroup(groupTargetOf(p.group_id), segsToText(p.message)) };
      return { message_id: await sendC2c(userTargetOf(p.user_id), segsToText(p.message)) };
    case 'get_msg': {
      const m = msgs.find((x) => Number(x.mid) === Number(p.message_id));
      if (!m) return null;
      const base = {
        message_id: m.mid, time: m.time, raw_message: m.text,
        message: [{ type: 'text', data: { text: m.text } }],
        sender: { user_id: m.uid, nickname: m.name || '', card: m.name || '' },
      };
      const selfId = botIdOf(m.appId || DEFAULT_APPID);
      if (m.kind === 'group') return { ...base, post_type: 'message', message_type: 'group', group_id: m.convId, user_id: m.uid, self_id: selfId };
      return { ...base, post_type: 'message', message_type: 'private', user_id: m.self ? selfId : m.uid, self_id: selfId };
    }
    case 'get_group_info': {
      const t = groupTargetOf(p.group_id);
      return { group_id: Number(p.group_id), group_name: groupNames[t ? t.openid : ''] || ('群' + Number(p.group_id)), member_count: 0, max_member_count: 0 };
    }
    case 'get_group_member_info': {
      const n = String(p.user_id);
      return { group_id: Number(p.group_id), user_id: Number(p.user_id), nickname: names[n] || n, card: names[n] || '' };
    }
    case 'get_friend_list':
      return [{ user_id: OWNER_QQ, nickname: names[String(OWNER_QQ)] || 'owner', remark: '' }];
    case 'get_group_list':
      return Object.entries(ids.group).map(([key, gid]) => {
        const openid = key.indexOf(':') > 0 ? key.slice(key.indexOf(':') + 1) : key;
        return { group_id: gid, group_name: groupNames[openid] || ('群' + gid), member_count: 0 };
      });
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
      return {};
    case 'set_group_ban':
    case 'set_group_whole_ban':
      return {};
    case 'fetch_ptt_text':
      return { text: '' };
    case 'get_mini_app_ark':
      return null;
    case 'fetch_custom_face_detail':
      // surfcore 期望 response.data 本身是数组；这里没有 QQ 收藏表情接口，回空数组即“同步到 0 个”。
      return [];
    default:
      log('unknown action: ' + action);
      return {};
  }
}

function msgToRaw(m) {
  const base = {
    message_id: m.mid, time: m.time, raw_message: m.text,
    message: [{ type: 'text', data: { text: m.text } }],
    sender: { user_id: m.uid, nickname: m.name || '', card: m.name || '' },
  };
  const selfId = botIdOf(m.appId || DEFAULT_APPID);
  if (m.kind === 'group') return { ...base, message_type: 'group', group_id: m.convId, user_id: m.uid, self_id: selfId };
  return { ...base, message_type: 'private', user_id: m.self ? selfId : m.uid, self_id: selfId };
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
    return json(res, {
      ok: true, clients: wss.clients.size, messages: msgs.length, defaultAppId: DEFAULT_APPID,
      accounts: Object.fromEntries(Object.entries(ACCOUNTS).map(([id, a]) => [id, { ns: a.ns, name: a.name, dataRoot: a.dataRoot || '' }])),
      ids: { private: Object.keys(ids.private).length, group: Object.keys(ids.group).length },
    });
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
  log('surfcore bridge connected (' + (req && req.socket ? req.socket.remoteAddress : '?') + '), total=' + wss.clients.size);
  ws.send(JSON.stringify({ post_type: 'meta_event', meta_event_type: 'lifecycle', sub_type: 'connect', self_id: botIdOf(DEFAULT_APPID), time: Math.floor(Date.now() / 1000) }));
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
  ws.on('close', () => log('surfcore bridge disconnected, total=' + wss.clients.size));
  ws.on('error', (e) => log('ws error: ' + (e && e.message)));
});

function broadcast(evt) {
  const s = JSON.stringify(evt);
  let n = 0;
  for (const c of wss.clients) if (c.readyState === 1) { c.send(s); n += 1; }
  return n;
}

async function ingest(p) {
  if (p && p.dshUrl && String(p.dshUrl) !== DSH) {
    DSH = String(p.dshUrl).replace(/\/+$/, '');
    log('DSH base updated -> ' + DSH);
  }
  const appId = String((p && p.appId) || DEFAULT_APPID);
  // 记住这个账号的 dataRoot：被动回复要靠它从 __qqbotManagers 里精确挑出对应的 manager。
  if (p && p.dataRoot) {
    const acct = ACCOUNTS[appId] || (ACCOUNTS[appId] = { ns: appId, name: appId });
    if (acct.dataRoot !== String(p.dataRoot)) { acct.dataRoot = String(p.dataRoot); void saveAccounts(); }
  }
  // 主人 openid 按 AppID 隔离：dsh-qqbot 认得谁是主人，第一次遇到就记下来。
  if (p && p.isOwner && p.senderId) {
    const acct = ACCOUNTS[appId] || (ACCOUNTS[appId] = { ns: appId, name: appId });
    if (acct.ownerOpenid !== String(p.senderId)) { acct.ownerOpenid = String(p.senderId); void saveAccounts(); }
  }
  const kind = p && p.kind === 'group' ? 'group' : 'private';
  const time = Number((p && p.timestamp) || 0) || Math.floor(Date.now() / 1000);
  let text = String((p && p.text) || '').trim();
  if (p && p.mentioned) text = '@' + botIdOf(appId) + ' ' + text;
  if (!text) return false;
  const selfId = botIdOf(appId);
  const mid = nextMsgId();
  const message = [{ type: 'text', data: { text } }];
  let evt;
  if (kind === 'group') {
    const gid = groupIdOf(appId, p.groupOpenid);
    const uid = userIdOf(appId, p.senderId);
    if (p.senderName) names[String(uid)] = String(p.senderName);
    if (p.groupName) groupNames[String(p.groupOpenid)] = String(p.groupName);
    evt = {
      post_type: 'message', message_type: 'group', sub_type: 'normal', self_id: selfId,
      group_id: gid, user_id: uid, message_id: mid, message, raw_message: text, time,
      sender: { user_id: uid, nickname: String(p.senderName || ''), card: String(p.senderName || '') },
    };
    record({ mid, kind: 'group', appId, convId: gid, openid: p.groupOpenid, uid, text, name: String(p.senderName || ''), time, self: false });
  } else {
    const uid = userIdOf(appId, p.senderId);
    if (p.senderName) names[String(uid)] = String(p.senderName);
    evt = {
      post_type: 'message', message_type: 'private', sub_type: 'friend', self_id: selfId,
      user_id: uid, message_id: mid, message, raw_message: text, time,
      sender: { user_id: uid, nickname: String(p.senderName || '') },
    };
    record({ mid, kind: 'private', appId, convId: uid, openid: p.senderId, uid, text, name: String(p.senderName || ''), time, self: false });
  }
  const n = broadcast(evt);
  log('ingest ' + kind + ' app=' + appId + ' mid=' + mid + ' uid=' + evt.user_id + ' peer=' + String(kind === 'group' ? p.groupOpenid : p.senderId) + ' -> ' + n + ' client(s)');
  return n > 0;
}

server.listen(HTTP_PORT, '127.0.0.1', () => log('HTTP listening 127.0.0.1:' + HTTP_PORT));
log('shim up; default=' + DEFAULT_APPID + ' accounts=' + Object.keys(ACCOUNTS).join(',') + ' ids=' + JSON.stringify({ private: Object.keys(ids.private).length, group: Object.keys(ids.group).length }));
