# mimo2api

本项目基于 [`wkeqin/mimi3`](https://github.com/wkeqin/mimi3) 二次开发，用于把 MIMO Claw 容器内的 MIMO API 通过 WebSocket bridge 暴露为 OpenAI / Anthropic 兼容接口。

当前主要包含两个版本：

- **mimo2api 版**：原 Python / FastAPI 网关，适合 VPS、宿主机或 Docker 部署。
- **cloudflare-worker 版**：Cloudflare Worker + KV + Durable Object + Cron，无需自建服务器。

## 共同能力

- OpenAI 兼容：`/v1/models`、`/v1/chat/completions`、`/v1/responses`、`/v1/audio/speech`
- Anthropic 兼容：`/anthropic/v1/models`、`/anthropic/v1/messages`
- WebSocket bridge 转发 MIMO 内网 API，支持流式响应
- 多账号 / 多 bridge 调度、错误冷却、基础状态统计
- WebUI 名称统一为 **Mimo2api**
- 模型映射管理
- 可选端点转换：`/v1/responses` → 上游 `/v1/chat/completions`，支持 `tool_calls` / `function_call`
- 可选 AI API 鉴权：`MIMO_RELAY_OPENAI_KEY`

## mimo2api 版（Python / FastAPI）

适合已有公网机器的部署方式。

### 快速启动

```bash
pip install -r requirements.txt
cp env.example .env
# 编辑 .env，至少配置 WS_TUNNEL_URL
python main.py
```

Docker：

```bash
cp env.example .env
mkdir -p users logs data
docker compose up -d --build
```

### 重点配置

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

### WebUI

访问：

```text
http://你的域名:8000/webui
```

主要功能：

- 导入 / 删除账号凭证
- 创建、销毁、重建 Claw 实例
- 查看 bridge 在线状态
- 模型连通性测试
- 模型映射管理
- 端点转换开关

运行数据默认放在：

- `users/`：账号凭证
- `logs/`：日志
- `data/`：指标、模型映射、端点转换等运行态数据

## cloudflare-worker 版

适合无服务器部署。目录：[`cloudflare-worker/`](cloudflare-worker/)。

### 重点能力

- Worker 承担 API 网关和 WebUI
- Durable Object 统一维护 `/ws` bridge 连接和请求队列
- KV 持久化用户、模型映射、端点转换、优选连接和生命周期状态
- Cron 自动执行 Claw 实例创建、reset、bridge 注入和轮换
- 支持 Cloudflare Tunnel 访问 MIMO AI Studio 管理接口
- 支持 Cloudflare 优选连接配置
- WebUI 账号操作菜单（…）可随时复制：
  - 第 1 条 reset 消息
  - 第 2 条 bridge 消息
- 所有账号不可用时，只在“网关运行中”下方提示手动注入，不额外展示复制按钮
- 注入消息按当前账号和 Worker 配置动态生成，不保存到项目目录文件

### 部署

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

常用 secret：

```bash
wrangler secret put MIMO_RELAY_OPENAI_KEY
wrangler secret put MIMO_WEBUI_USERNAME
wrangler secret put MIMO_WEBUI_PASSWORD
wrangler secret put MIMO_PROXY_URL
wrangler secret put MIMO_TUNNEL_TOKEN
```

访问：

```text
https://你的-worker-域名/webui
```

更多 Worker 部署和 API 说明见 [`cloudflare-worker/README.md`](cloudflare-worker/README.md)。

## 常用 API

| 端点 | 说明 |
| --- | --- |
| `GET /v1/models` | OpenAI models 列表 |
| `GET /anthropic/v1/models` | Anthropic models 列表 |
| `POST /v1/chat/completions` | Chat Completions |
| `POST /v1/responses` | Responses API；可选转换到上游 Chat Completions |
| `POST /v1/audio/speech` | TTS |
| `POST /anthropic/v1/messages` | Anthropic Messages |
| `POST /v1/messages` | Worker 版 Anthropic Messages 简短别名 |
| `GET /webui` | Web 管理面板 |
| `GET/PUT/DELETE /api/model_mapping` | 模型映射 |
| `GET/PUT /api/endpoint_conversion` | 端点转换开关 |
| `GET /api/manual_injection_messages?user_id=xxx` | Worker 版手动注入消息 |
| `GET/PUT /api/network_config` | Worker 版优选连接配置 |

## 隐私与提交约定

不要提交真实 Cookie、serviceToken、Tunnel Token、WebUI 密码、AI API Key 或本地部署产物。

默认忽略：

- `.env` / `.dev.vars`
- `users/*.json`
- `logs/`、`data/`
- `endpoint_conversion.json`
- `cloudflare-worker/wrangler.toml`
- `cloudflare-worker/worker.js` / `worker.js.map`
- `node_modules/`

可提交模板：

- [`env.example`](env.example)
- [`cloudflare-worker/wrangler.example.toml`](cloudflare-worker/wrangler.example.toml)
- [`cloudflare-worker/.dev.vars.example`](cloudflare-worker/.dev.vars.example)

## 原项目流程记录

见：[`docs/original-execution-flow.md`](docs/original-execution-flow.md)。
