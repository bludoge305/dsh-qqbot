# onebot-shim — 把本插件的 QQ 通道暴露成 OneBot v11

本插件走腾讯官方 QQBot SDK，不会说 OneBot。而上游一些 QQ 桥接（例如 surfcore）
只会说 OneBot v11。这个示例用一个小进程把两边接起来：

```
QQ 群/私聊
   │  官方 SDK
   ▼
dsh-qqbot（本插件）
   │  入站扩展 inbound/onebot-shim.mjs  POST /_ingest
   ▼
onebot-shim（本目录 server.mjs）   ── WS 3001 / HTTP 3000 ──▶  上游桥接
   ▲                                                              │
   └────── 上游回话：POST /api/ext/dispatch?id=onebot-shim&p=/send ┘
```

## 为什么回话必须走被动回复

群里用主动消息接口会被 QQ 官方拒绝：

```
{"code":"40034105","human":"主动消息发送失败，无权限"}
```

只有带 `msg_id` 的**被动回复**能发出去（5 分钟窗口，同一个 msg_id 约 5 条上限）。
所以 shim 出站先请入站扩展用它手上的 msg_id 发送，没有窗口才退回主动接口
（群里通常仍会被拒，这是官方限制，不是 bug）。

## 放哪 / 怎么起

1. 把 `inbound/onebot-shim.mjs` 复制到 `{dataRoot}/.qqbot-extensions/inbound/`；
2. 本目录 `server.mjs` 放在任意位置，`npm i ws` 后 `node server.mjs` 启动；
3. 热重载入站扩展（不用重启 DSH）：

   ```bash
   curl -X POST http://127.0.0.1:<dsh端口>/api/qqbot-settings/ext-toggle \
     -H 'content-type: application/json' \
     -d '{"kind":"inbound","file":"onebot-shim.mjs","enabled":true}'
   ```

4. 上游桥接侧把 OneBot 地址指到 `ws://127.0.0.1:3001`（或 HTTP `http://127.0.0.1:3000`）。

**必须设 `followDiscoveredEndpoint: false`**（如果上游会自愈端点）：否则它会用自己的
端点探测覆盖掉你配的 wsUrl 并写回配置，shim 就丢了。

## 环境变量

全部通过环境变量给，仓库里不留任何 QQ 号 / openid：

| 变量 | 说明 |
| --- | --- |
| `DSH_BASE_URL` | DSH 端口；不填则用 `DSH_WEB_URL`（DSH 桌面端会导出，重启后端口变化也能跟上），再退到 `http://127.0.0.1:9220` |
| `QQBOT_NS` | 账号 settings ns（`GET /api/qqbot-settings/accounts` 里能查到） |
| `QQBOT_ID` | 机器人 QQ 号 / appId |
| `QQBOT_NAME` | 机器人昵称 |
| `OWNER_QQ` | 主人 QQ 号（用于 openid → 整数 id 映射） |
| `OWNER_OPENID` | 主人 openid |
| `SHIM_HTTP_PORT` / `SHIM_WS_PORT` | 默认 3000 / 3001 |
| `SHIM_EXT_ID` | 扩展 id，默认 `onebot-shim`（要和 inbound 扩展一致） |

## 已验证 / 未覆盖

- ✅ 入站：群消息 → `/_ingest` → WS 事件 → 上游收到
- ✅ 出站被动：上游回话 → `/send` → `sender.sendMarkdown({...msgId})` → 群里真实收到
- ✅ `get_status` / `get_login_info` / `get_msg` / `get_group_info` / 历史拉取
- ⚠️ 未实现：图片/语音/文件段（`segsToText` 只留占位符）、主动推送、撤回、禁言
- ⚠️ `fetch_custom_face_detail` 返回 `{data:[]}`，上游若期待数组需自行适配

## 三个坑

1. **路由要在模块加载时注册**。`registerRoute` 需要 `ctx`，而 shim 随时可能来调，
   等第一条消息再注册就已经晚了。直接写 `globalThis.__qqbotExtRoutes`
   （键格式 `<id>|<METHOD>|<path>`）。
2. **`/api/ext/dispatch` 对含 `ok` 的返回值是原样透传，不再包 `data`**。
   客户端别写死 `j.data.ok`，两种形状都要认 —— 否则每次都被判失败、退回主动接口，
   然后群里 40034105。
3. **`msgId` 得自己记**。入站扩展在早挂点 annihilate 掉消息后，debounce 不跑，
   `record.replyTarget.msgId` 永远是旧值。所以在 `onInbound` 里、任何 `await` 之前
   先把 `ctx.message.messageId` 存进 Map。
