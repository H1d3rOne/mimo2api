# mimo2api - MIMO API WebSocket 隧道网关

mimo2api 把运行在 MIMO Claw 容器里的内网 API 通过 WebSocket bridge 暴露为 OpenAI / Anthropic 兼容接口。项目同时维护两个版本：

- **Python 网关版**：FastAPI 服务，适合已有 VPS / 宿主机 / Docker 部署。
- **Cloudflare Worker 版**：Worker + KV + Durable Object + Cron，无需自建服务器即可管理账号、生命周期和 bridge。

默认模式下请求直转，只做模型名映射；两个版本都支持在 WebUI 中开启 `/v1/responses` → 上游 `/v1/chat/completions` 的端点转换，并包含 `tool_calls` / `function_call` 兼容处理。

## 当前主要特性

- OpenAI 兼容接口：`/v1/models`、`/v1/chat/completions`、`/v1/responses`、`/v1/audio/speech`
- Anthropic 兼容接口：`/anthropic/v1/models`、`/anthropic/v1/messages`；Worker 版额外支持 `/v1/messages` 简短别名
- WebSocket bridge 隧道转发 MIMO 内网 API，支持流式响应
- 多账号 / 多 bridge 接入、调度、错误冷却和基础状态统计
- 模型映射：WebUI 和管理 API 均可维护
- 端点转换：可选把客户端 Responses API 请求转换为上游 Chat Completions 请求
- WebUI 名称统一为 **Mimo2api**，支持登录鉴权、AI 端点 Bearer Key 状态提示、模型连通性测试
- Cloudflare Worker 版支持：
  - KV 持久化用户、模型映射、端点转换、优选连接和生命周期状态
  - Durable Object 集中管理 bridge WebSocket 与 pending 请求
  - Cron 自动执行 Claw 实例创建 / reset / bridge 注入 / 轮换
  - Xray 风格 Cloudflare 优选连接配置
  - 手动复制第 1 条 reset 消息和第 2 条 bridge 消息；消息运行时动态生成，不写入 `debug` 目录或仓库文件

## 架构

```text
客户端 / OpenAI SDK / Anthropic SDK
        │
        ▼
公网网关
  ├─ Python FastAPI 网关，或
  └─ Cloudflare Worker + Durable Object
        │
        │ WebSocket /ws
        ▼
MIMO Claw 容器 bridge.py
        │
        ▼
MIMO API / MIMO_API_ENDPOINT
```

Cloudflare Worker 版额外包含：

```text
Cloudflare Worker
  ├─ WebUI / 管理 API / Cron tick
  ├─ KV：用户、模型映射、配置、生命周期状态
  └─ Gateway Durable Object：bridge WebSocket 网关
```

## 对外 AI API

| 端点 | 说明 |
| --- | --- |
| `GET /v1/models` | OpenAI models 列表 |
| `GET /anthropic/v1/models` | Anthropic models 列表 |
| `POST /v1/chat/completions` | OpenAI Chat Completions |
| `POST /v1/responses` | Responses API；可选转换到上游 Chat Completions |
| `POST /v1/audio/speech` | TTS |
| `POST /anthropic/v1/messages` | Anthropic Messages |
| `POST /v1/messages` | Worker 版 Anthropic Messages 简短别名 |
| `GET /webui` | Web 管理面板 |

如果设置了 `MIMO_RELAY_OPENAI_KEY`，AI API 需要使用 `Authorization: Bearer <key>`。

## Python 网关版快速开始

### 本地运行

```bash
pip install -r requirements.txt
cp env.example .env
# 编辑 .env，至少设置 WS_TUNNEL_URL
python main.py
```

Python 版主要配置见 [`env.example`](env.example)。常用项：

```env
SERVER_HOST=0.0.0.0
SERVER_PORT=8000
WS_TUNNEL_URL=ws://你的公网域名:8000/ws
MIMO_RELAY_OPENAI_KEY=sk-your-random-secret-here
MIMO_WEBUI_USERNAME=admin
MIMO_WEBUI_PASSWORD=change-me
MIMO_ENDPOINT_CONVERSION_ENABLED=false
MIMO_ENDPOINT_CONVERSION_FILE=/app/data/endpoint_conversion.json
```

账号凭证放在 `users/` 目录，格式与 WebUI 导入的用户 JSON 一致。`users/*.json` 已被 `.gitignore` 忽略，请勿提交真实 Cookie / token。

### Docker 运行

```bash
cp env.example .env
# 编辑 .env
mkdir -p users logs data
docker compose up -d --build
```

Docker 会把以下目录持久化到项目目录：

- `users/`：账号凭证
- `logs/`：运行日志
- `data/`：模型映射、端点转换、指标快照等运行态数据

### Python 版 WebUI / 管理能力

WebUI：`http://你的域名:8000/webui`

- 顶部可查看 WebUI / AI 端点鉴权状态
- 支持用户凭证导入、删除、创建 / 销毁 / 重建实例
- 支持模型映射管理和模型连通性测试
- 支持端点转换开关：
  - 关闭：`/v1/responses` 原样透传到上游 `/v1/responses`
  - 开启：客户端 `/v1/responses` 转为上游 `/v1/chat/completions`，再把响应转回 Responses API 格式

## Cloudflare Worker 版快速开始

Worker 版位于 [`cloudflare-worker/`](cloudflare-worker/)，详细说明见 [`cloudflare-worker/README.md`](cloudflare-worker/README.md)。

```bash
cd cloudflare-worker
npm install
cp wrangler.example.toml wrangler.toml
wrangler kv namespace create MIMO_KV
# 把 KV id 写入 wrangler.toml，并配置 MIMO2API_WS_URL
npm run bundle
npx tsc --noEmit
npm run deploy
```

敏感信息通过 secret 配置，不写入仓库：

```bash
wrangler secret put MIMO_RELAY_OPENAI_KEY
wrangler secret put MIMO_WEBUI_USERNAME
wrangler secret put MIMO_WEBUI_PASSWORD
wrangler secret put MIMO_PROXY_URL
wrangler secret put MIMO_TUNNEL_TOKEN
```

Worker 版 WebUI：`https://你的-worker-域名/webui`

当前 Worker WebUI 特性：

- 顶部包含“端点转换”和“优选连接”配置入口
- “Claw 运行账号管理”标题右侧提示：首次运行需要手动注入消息，请至少保持一个账号在线
- 所有账号都不可用时，网关状态下方只显示手动注入提示：到对应账号的操作菜单（…）复制两条注入消息并按顺序发送到 mimoclaw 聊天页面
- 每个账号的操作菜单（…）可随时复制：
  - 第 1 条 reset 消息
  - 第 2 条 bridge 消息
- 复制接口会按当前账号、Worker WS 地址、bridge token、优选连接和 Tunnel Token 动态生成消息；不会把消息保存到 `debug` 目录

Worker 版注入流程：

1. 第一轮发送 reset 消息，让 Claw 环境回到干净状态并重启。
2. 第二轮发送 bridge 消息，要求保存 `/tmp/bridge.py` 并用 `nohup` 后台静默运行 `bridge.py` 和 `cloudflard`。
3. 如果当前实例周期内 bridge 曾经在线，后续断开只等待重连，不重复注入；如果实例 `AVAILABLE` 但 bridge 从未在线，会重发第二条 bridge 消息。

## 管理 API 概览

### 通用 / Python WebUI API

| 路由 | 说明 |
| --- | --- |
| `GET /api/auth/session` | WebUI 会话与鉴权状态 |
| `POST /api/auth/login` | WebUI 登录 |
| `POST /api/auth/logout` | WebUI 退出 |
| `GET /api/system/status` | WebUI 实时 bridge 状态 |
| `GET /api/stats` | 网关统计 |
| `GET /api/status/history` | 状态历史占位 / 趋势数据 |
| `GET /api/errors` | 网关错误环形缓冲 |
| `GET/PUT/DELETE /api/model_mapping` | 模型映射管理 |
| `GET/PUT /api/endpoint_conversion` | 端点转换配置 |
| `POST /api/test_model` | 模型连通性测试 |
| `GET /api/users/list` | WebUI 用户列表 |
| `POST /api/users/add` | 导入用户凭证 |
| `DELETE /api/users/delete/:uid` | 删除用户凭证 |
| `POST /api/users/create/:uid` | 创建 / 启动 Claw 实例 |
| `POST /api/users/destroy/:uid` | 销毁 Claw 实例 |
| `POST /api/users/rebuild/:uid` | 重建 Claw 实例 |
| `POST /api/rebuild` | 批量重建 |

### Worker 版额外 / 差异 API

| 路由 | 说明 |
| --- | --- |
| `GET /api/manual_injection_messages?user_id=xxx` | 返回当前账号的 reset / bridge 手动注入消息 |
| `GET/PUT /api/network_config` | 优选连接配置，持久化到 KV |
| `POST /api/tick` | 手动触发所有用户生命周期 tick |
| `POST /api/tick/:uid` | 手动触发单用户 tick |
| `GET /api/users` / `POST /api/users` / `DELETE /api/users/:uid` | Worker 管理 API 的用户 CRUD |
| `POST /api/users/import` | Worker 管理 API 批量导入用户 |
| `GET /api/lifecycle` / `GET /api/lifecycle/:uid` | Worker 生命周期状态 |
| `POST /api/rebuild` / `POST /api/rebuild/:uid` | Worker 生命周期重建信号 |
| `GET /install` / `GET /install.sh` | 生成当前容器手动安装 bridge 的 shell 脚本 |
| `GET /bootstrap.sh` | 生成 bridge + cloudflared bootstrap 脚本 |
| `GET /install-help` | 返回手动安装提示 |

## 端点转换说明

端点转换关闭时，`/v1/responses` 会按原路径转发给上游。开启时：

```text
客户端 POST /v1/responses
        │
        ▼
mimo2api 转换请求体
        │
        ▼
上游 POST /v1/chat/completions
        │
        ▼
mimo2api 把 Chat Completions 响应转换回 Responses API 格式
```

两个版本都支持非流式和流式场景的基础转换，并处理 `tools`、`tool_choice`、`tool_calls` / `function_call`。

## 发布 / 隐私约定

已配置 `.gitignore`，默认不会提交：

- `.env` / `.dev.vars`
- `cloudflare-worker/wrangler.toml`
- `cloudflare-worker/worker.js` / `worker.js.map`
- `node_modules/`
- `users/*.json`
- `endpoint_conversion.json`
- `logs/`、`data/`、抓包归档等运行时数据

可以提交的模板文件：

- [`env.example`](env.example)
- [`cloudflare-worker/wrangler.example.toml`](cloudflare-worker/wrangler.example.toml)
- [`cloudflare-worker/.dev.vars.example`](cloudflare-worker/.dev.vars.example)

不要提交真实 Cookie、serviceToken、Tunnel Token、WebUI 密码、AI API Key 或本地部署产物。注入消息只在运行时按当前配置生成，不应落盘保存到仓库目录。

## 原项目执行流程

详细流程见：[`docs/original-execution-flow.md`](docs/original-execution-flow.md)。
