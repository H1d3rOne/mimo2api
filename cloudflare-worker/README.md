# mimo2api Cloudflare Worker Gateway

基于 Cloudflare Workers + Durable Objects 的 API 网关，通过 WebSocket 隧道转发请求到内网节点。

## 架构

```
Cloudflare Worker (公网)
        ↑
        │ WebSocket 隧道 (/ws)
        │
内网 bridge.py (在 MIMO Claw 容器内)
        │
        ↓
    MIMO API (内网)
```

## 功能

- 请求原样转发，只做模型名映射
- WebSocket 隧道穿透内网
- 流式响应支持
- 多节点负载均衡
- 自动故障转移

## 部署

### 1. 安装依赖

```bash
npm install
```

### 2. 配置 (可选)

在 `wrangler.toml` 中添加环境变量：

```toml
[vars]
AI_AUTH_KEY = "your-api-key"
```

或使用 wrangler secrets：

```bash
wrangler secret put AI_AUTH_KEY
```

### 3. 部署

```bash
npm run deploy
```

部署后会获得一个 `*.workers.dev` 域名。

### 4. 配置内网节点

将部署后的 Worker URL 设置为内网节点的 WebSocket 隧道地址：

```bash
# 在 bridge.py 所在环境设置
WS_URL=wss://your-worker.your-subdomain.workers.dev/ws
```

## API 端点

所有端点都是原样转发到内网节点：

- `GET /v1/models` - 列出可用模型
- `POST /v1/chat/completions` - Chat Completions API
- `POST /v1/responses` - Responses API
- `POST /v1/audio/speech` - TTS API
- `POST /anthropic/v1/messages` - Anthropic Messages API
- `WS /ws` - WebSocket 隧道端点

## 本地开发

```bash
npm run dev
```

## 与 Python 版本的区别

| 功能 | Python (FastAPI) | Cloudflare Worker |
|------|------------------|-------------------|
| 部署方式 | 自托管/VPS | Cloudflare Edge |
| 持久化 | SQLite (metrics) | 无 |
| WebUI | ✅ | ❌ |
| Manager | ✅ | ❌ |
| 成本 | 服务器费用 | 免费额度内 |

## 注意事项

1. Cloudflare Workers 有 CPU 时间限制（免费 10ms，付费 30s）
2. WebSocket 连接由 Durable Objects 维护
3. 流式响应通过 HTTP 响应返回，而非真正的 SSE 流
