# mimo2api Cloudflare Worker 版

这是 mimo2api 的无服务器版本，使用 Cloudflare Worker + KV + Durable Object + Cron 管理 MIMO Claw 账号、bridge 隧道和 OpenAI / Anthropic 兼容 API。

## 功能概览

- `/ws`：Claw 容器内 `bridge.py` 的 WebSocket 连接入口，带 bridge token 鉴权
- AI API 转发：
  - `GET /v1/models`
  - `GET /anthropic/v1/models`
  - `POST /v1/chat/completions`
  - `POST /v1/responses`
  - `POST /v1/audio/speech`
  - `POST /anthropic/v1/messages`
  - `POST /v1/messages`（Anthropic Messages 简短别名）
- 可选 `/v1/responses` → 上游 `/v1/chat/completions` 端点转换，支持 `tool_calls` / `function_call`
- WebUI 名称为 **Mimo2api**，可管理用户、生命周期、模型映射、端点转换和优选连接
- KV 持久化用户、生命周期状态、模型映射、端点转换和优选连接
- Gateway Durable Object 集中维护在线 bridge、请求队列、错误冷却和 WebSocket hibernation
- Cron 自动 tick：创建 / reset / 注入 / 轮换 Claw 实例
- Cloudflare Tunnel：Worker 通过 `MIMO_PROXY_URL` 访问 MIMO AI Studio 管理接口，并把 `MIMO_TUNNEL_TOKEN` 注入 Claw 容器启动 `cloudflared`

## 架构

```text
客户端 / OpenAI SDK / Anthropic SDK
        │
        ▼
Cloudflare Worker
  ├─ WebUI / 管理 API / Cron tick
  ├─ KV: 用户、生命周期、模型映射、配置
  └─ Gateway Durable Object
        │  /ws WebSocket 隧道
        ▼
Claw 容器 bridge.py
        │
        ▼
MIMO API

Worker 管理 MIMO AI Studio：
Worker → https://$MIMO_PROXY_URL/open-apis/...
       → Cloudflare Tunnel → aistudio.xiaomimimo.com
```

> 在线节点和 pending 请求队列集中在 `GatewayDurableObject`，避免普通 Worker isolate 分裂导致“节点在线但请求打不到节点”。当前使用 SQLite-backed Durable Object + WebSocket Hibernation。

## 部署

### 1. 安装依赖

```bash
cd cloudflare-worker
npm install
```

### 2. 创建本地配置

```bash
cp wrangler.example.toml wrangler.toml
```

`wrangler.toml` 已被 `.gitignore` 忽略，可写入自己的 KV id、Worker 域名、路由等本地信息。

### 3. 创建 KV Namespace

```bash
wrangler kv namespace create MIMO_KV
```

把输出中的 `id` 写入 `wrangler.toml`：

```toml
[[kv_namespaces]]
binding = "MIMO_KV"
id = "你的 KV namespace id"
```

### 4. 配置 Worker WS 地址

`MIMO2API_WS_URL` 必须指向 Worker 自身 `/ws`：

```toml
[vars]
MIMO2API_WS_URL = "wss://你的-worker-域名/ws"
MIMO_ENDPOINT_CONVERSION_ENABLED = "false"
```

如果使用自定义域名，请写自定义域名；后续在 WebUI 顶部“优选连接”中配置的只是 bridge 实际 TCP 连接地址，Host/SNI 仍以 Worker 域名为准。

### 5. 配置 Secret

敏感信息不要写入仓库，使用 `wrangler secret put`：

```bash
# 客户端调用 AI API 的 Bearer Key；不设置则 AI API 不鉴权
wrangler secret put MIMO_RELAY_OPENAI_KEY

# WebUI 登录；建议设置
wrangler secret put MIMO_WEBUI_USERNAME
wrangler secret put MIMO_WEBUI_PASSWORD

# Worker 管理 MIMO AI Studio 的代理/Tunnel
wrangler secret put MIMO_PROXY_URL
wrangler secret put MIMO_TUNNEL_TOKEN
```

`MIMO_PROXY_URL` 指向可访问 `aistudio.xiaomimimo.com` 的代理 / Tunnel 域名。`MIMO_TUNNEL_TOKEN` 会被注入到 Claw 容器内启动 `cloudflared`；如果已有其它代理方式，可只配置 `MIMO_PROXY_URL`。

### 6. 可选：本地 dev

```bash
cp .dev.vars.example .dev.vars
npm run dev
```

`.dev.vars` 只用于本地 `wrangler dev`，已被 `.gitignore` 忽略。

### 7. 构建、类型检查、部署

```bash
npm run bundle
npx tsc --noEmit
npm run deploy
```

部署后访问：

```text
https://你的-worker-域名/webui
```

## WebUI 使用要点

- 页面名称统一为 **Mimo2api**。
- 顶部按钮：
  - **端点转换**：开启 / 关闭 `/v1/responses` → `/v1/chat/completions`。
  - **优选连接**：配置 Xray 风格 Cloudflare 优选地址。
- “Claw 运行账号管理”标题右侧会提示：`（首次运行需要手动注入消息，请至少保持一个账号在线）`。
- 所有账号都不可用时，“网关运行中”下方只显示提示：请在账号操作菜单（…）复制两条注入消息，并按顺序发送到 mimoclaw 聊天页面。
- 每个账号的操作菜单（…）可随时执行：
  - 创建实例
  - 销毁实例
  - 重建实例
  - 删除账号
  - 复制 reset 消息
  - 复制 bridge 消息
- 复制消息使用 `GET /api/manual_injection_messages?user_id=xxx` 动态生成，不保存到 `debug` 目录，也不写入仓库文件。

## 自动 / 手动注入流程

Worker 生命周期 tick 会按状态自动处理 Claw 实例：

1. 创建或确认实例 `AVAILABLE`。
2. 第一次注入发送 reset 消息，让环境恢复并重启。
3. 下一轮注入发送 bridge 消息，写入 `/tmp/bridge.py`，并要求后台运行 `bridge.py` 和 `cloudflard`。
4. bridge 成功连上 Worker `/ws` 后，账号进入可调度状态。
5. 如果当前实例周期内 bridge 曾经在线，之后离线只等待重连，不重发注入消息。
6. 如果实例 `AVAILABLE` 但 bridge 从未在线，会重发第二条 bridge 消息。

第二条 bridge 注入消息当前前缀为：

```text
好，先确保 websockets 和 httpx 可用。把下面 Python 代码保存为 /tmp/bridge.py，然后请用 nohup 后台静默运行bridge.py和cloudflard（请务必在后台运行，不要阻塞我们的对话！）。
```

WebUI 复制的 bridge 消息与自动注入使用同一个模板，会带入当前 Worker WS、bridge token、账号 ID、Tunnel Token 和优选连接配置。

## 端点转换

WebUI 顶部“端点转换”可以开启/关闭：

- 关闭：`/v1/responses` 原样透传到上游 `/v1/responses`。
- 开启：客户端请求 `/v1/responses` 时，Worker 转换为上游 `/v1/chat/completions`，再把 Chat Completions 响应转换回 Responses API 格式。

也可以在 `wrangler.toml` 中设置默认值：

```toml
[vars]
MIMO_ENDPOINT_CONVERSION_ENABLED = "false"
```

WebUI 保存后会把配置写入 KV，并优先使用 KV 中的值。

## 优选连接

WebUI 顶部“优选连接”保存的是 bridge 连接 Worker 时的优选地址：

- Worker 对外域名 / Host/SNI：仍来自 `MIMO2API_WS_URL`
- Bridge TCP 连接地址：来自 WebUI 保存的优选地址或 `MIMO2API_PREFERRED_BASE_URL`
- 清空优选连接后，bridge 恢复直连 Worker 域名

适用于需要让 Claw 容器通过 Cloudflare 优选 IP / 域名连接 Worker，但仍保持正确 Host/SNI 的场景。

## 手动安装脚本端点

用于 Worker 不能直接操作控制面，或你想在当前 Claw 容器里快速接入 bridge 的场景：

| 路由 | 说明 |
| --- | --- |
| `GET /install` / `GET /install.sh` | 返回只安装 / 启动 bridge 的 shell 脚本 |
| `GET /bootstrap.sh` | 返回 bridge + cloudflared bootstrap 脚本 |
| `GET /install-help` | 返回手动安装提示和 curl 示例 |

示例：

```bash
curl -fsSL 'https://你的-worker-域名/install?user_id=你的用户ID' | bash
```

如果启用了 WebUI 用户名 / 密码，脚本端点会按需要使用 Basic Auth 或 bridge token 校验。

## 常用管理 API

| 路由 | 说明 |
| --- | --- |
| `GET /webui` | Web 管理面板 |
| `GET /api/auth/session` | WebUI 会话与鉴权状态 |
| `POST /api/auth/login` | WebUI 登录 |
| `POST /api/auth/logout` | WebUI 退出 |
| `GET /api/system/status` | WebUI 运行状态、bridge 节点与 proxy 健康状态 |
| `GET /api/stats` | Gateway + 生命周期状态 |
| `GET /api/errors` | Gateway 错误列表 |
| `GET /api/status/history` | 状态历史占位数据 |
| `GET /api/users/list` | WebUI 用户列表，包含 Claw 状态 |
| `POST /api/users/add` | WebUI 导入用户凭证 |
| `DELETE /api/users/delete/:uid` | WebUI 删除用户 |
| `POST /api/users/create/:uid` | 创建 / 启动单用户实例 |
| `POST /api/users/destroy/:uid` | 销毁单用户实例 |
| `POST /api/users/rebuild/:uid` | 重建单用户实例 |
| `GET /api/manual_injection_messages?user_id=xxx` | 返回 reset / bridge 手动注入消息 |
| `GET/PUT/DELETE /api/model_mapping` | 模型映射管理，持久化到 KV |
| `GET/PUT /api/endpoint_conversion` | 端点转换开关，持久化到 KV |
| `GET/PUT /api/network_config` | 优选连接配置，持久化到 KV |
| `POST /api/test_model` | 模型连通性测试 |
| `POST /api/tick` | 手动触发所有用户生命周期 tick |
| `POST /api/tick/:uid` | 手动触发单用户生命周期 tick |
| `GET /api/users` / `POST /api/users` / `DELETE /api/users/:uid` | 管理 API 用户 CRUD |
| `POST /api/users/import` | 管理 API 批量导入用户 |
| `GET /api/lifecycle` / `GET /api/lifecycle/:uid` | 生命周期状态 |
| `POST /api/rebuild` / `POST /api/rebuild/:uid` | 发出重建信号 |

## 本地验证

```bash
npm run bundle
npx tsc --noEmit
npx wrangler deploy --dry-run --outdir /tmp/mimo2api-worker-dryrun
```

部署后可查看实时日志：

```bash
npm run tail
```

## 配置与隐私约定

为了方便发布到 GitHub：

- 提交 `wrangler.example.toml` 和 `.dev.vars.example`。
- 不提交真实 `wrangler.toml`、`.dev.vars`、`worker.js`、`worker.js.map`、`node_modules/`。
- 用户凭证、Cookie、Tunnel Token、WebUI 密码、AI API Key 一律通过 WebUI/KV 或 `wrangler secret put` 配置。
- KV 中的用户数据和运行状态不在仓库内。
- reset / bridge 注入消息运行时动态生成，不写入 `cloudflare-worker/debug/` 或其它仓库目录。
