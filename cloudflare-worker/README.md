# mimo2api Cloudflare Worker 版

单文件 Worker，可直接复制到 Cloudflare Dashboard 运行。

## 文件说明

| 文件 | 说明 |
|------|------|
| `worker.js` | Cloudflare Worker 主文件，复制到 Dashboard |
| `bridge_cf.py` | 内网节点连接脚本，运行在 MIMO 容器内 |
| `env.example` | 环境变量配置示例 |

## 部署步骤

### 1. 部署 Worker

1. 登录 [Cloudflare Dashboard](https://dash.cloudflare.com)
2. 进入 Workers & Pages → Create Worker
3. 将 `worker.js` 内容粘贴到编辑器
4. 点击 Deploy
5. 记下分配的域名，如 `mimo2api.xxx.workers.dev`

### 2. 配置内网节点

在内网 MIMO 容器中设置环境变量：

```bash
# Cloudflare Worker 地址
export WS_URL="wss://mimo2api.xxx.workers.dev/ws"

# MIMO API 凭证
export MIMO_API_KEY="your-api-key"
export MIMO_API_ENDPOINT="https://api.mimo.ai/v1/chat/completions"

# 可选：用户标识
export USER_ID="user_xxx"
```

### 3. 启动 bridge

```bash
python bridge_cf.py
```

看到 `✅ 已连接到网关` 表示连接成功。

## 架构

```
用户请求
    ↓
Cloudflare Worker (公网)
    ↓ WebSocket 隧道
bridge_cf.py (内网)
    ↓
MIMO API (内网)
```

## API 端点

Worker 暴露的端点：

- `GET /` - 状态页
- `GET /health` - 健康检查
- `GET /api/stats` - 节点统计
- `GET /v1/models` - 模型列表
- `POST /v1/chat/completions` - Chat API
- `POST /v1/responses` - Responses API
- `POST /v1/audio/speech` - TTS API
- `POST /anthropic/v1/messages` - Anthropic API
- `WS /ws` - WebSocket 隧道 (内网节点连接)

## 可选：API 鉴权

编辑 `worker.js` 顶部的配置：

```javascript
const AI_AUTH_KEY = 'your-secret-key';  // 设置后，请求需携带 Authorization: Bearer your-secret-key
```

## 与 Python 版本的区别

| | Python (FastAPI) | Cloudflare Worker |
|---|---|---|
| 部署 | 自托管服务器 | Cloudflare Edge |
| 费用 | 服务器费用 | 免费额度内 |
| WebUI | ✅ | ❌ |
| Manager | ✅ | ❌ |
| 持久化 | SQLite | 无 |
| 多节点 | ✅ | ✅ |
