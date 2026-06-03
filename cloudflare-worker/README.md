# mimo2api Cloudflare Worker 版本

完整的 Cloudflare Worker 实现，包含原项目所有功能。

## 文件说明

| 文件 | 说明 |
|------|------|
| `worker.js` | Cloudflare Worker 网关，复制到 Dashboard 运行 |
| `bridge_cf.py` | 内网节点桥接脚本，在 MIMO 容器内运行 |
| `manager_cf.py` | 账号池管理器，自动管理 Claw 容器生命周期 |
| `main_cf.py` | 统一启动入口 |
| `env.example` | 环境变量配置示例 |

## 架构

```
用户请求
    ↓
Cloudflare Worker (公网，全球边缘节点)
    ↓ WebSocket 隧道 (/ws)
bridge_cf.py (内网 MIMO 容器)
    ↓
MIMO API (内网)
```

## 快速开始

### 1. 部署 Cloudflare Worker

1. 登录 [Cloudflare Dashboard](https://dash.cloudflare.com)
2. 进入 Workers & Pages → Create Worker
3. 将 `worker.js` 内容粘贴到编辑器
4. 点击 Deploy
5. 记下分配的域名，如 `mimo2api.xxx.workers.dev`

### 2. 配置环境变量

复制配置文件：

```bash
cd cloudflare-worker
cp env.example .env
```

编辑 `.env`：

```bash
# Cloudflare Worker 地址 (必需)
CF_WS_URL=wss://mimo2api.xxx.workers.dev/ws

# MIMO API 凭证 (可选)
MIMO_API_KEY=your-api-key
MIMO_API_ENDPOINT=https://api.mimo.ai/v1/chat/completions
```

### 3. 准备账号配置

在项目根目录的 `users/` 下放置账号文件：

```bash
# 复制原项目的 users 目录
cp -r ../users ./users/
```

或手动创建 `users/user_xxx.json`：

```json
{
  "userId": "xxx",
  "serviceToken": "xxx",
  "xiaomichatbot_ph": "xxx",
  "name": "Account1"
}
```

### 4. 启动 Manager

```bash
python main_cf.py
```

看到以下日志表示启动成功：

```
🚀 mimo2api Cloudflare Worker 版本
📡 Cloudflare Worker: wss://mimo2api.xxx.workers.dev/ws
✅ 内网节点已接入: xxx
```

## 工作流程

```
1. Manager 启动
   ↓
2. 读取 users/ 目录下的账号配置
   ↓
3. 为每个账号创建/复用 Claw 容器
   ↓
4. 通过 AI 对话注入 bridge_cf.py
   ↓
5. bridge_cf.py 连接到 Cloudflare Worker
   ↓
6. 用户请求 → Worker → bridge_cf.py → MIMO API
   ↓
7. 55 分钟后自动重建容器
```

## API 端点

Worker 暴露的端点：

| 端点 | 方法 | 说明 |
|------|------|------|
| `/` | GET | 状态页 |
| `/health` | GET | 健康检查 |
| `/api/stats` | GET | 节点统计 |
| `/v1/models` | GET | 模型列表 |
| `/v1/chat/completions` | POST | Chat Completions API |
| `/v1/responses` | POST | Responses API |
| `/v1/audio/speech` | POST | TTS API |
| `/anthropic/v1/messages` | POST | Anthropic Messages API |
| `/ws` | WS | WebSocket 隧道 (内网节点连接) |

## 可选配置

### API 鉴权

编辑 `worker.js` 顶部的配置：

```javascript
const AI_AUTH_KEY = 'your-secret-key';
```

设置后，请求需携带 `Authorization: Bearer your-secret-key`。

### 自定义模型列表

编辑 `worker.js` 中的 `MODELS` 数组。

## 与 Python 版本的区别

| 功能 | Python 版 | Cloudflare 版 |
|------|-----------|---------------|
| 网关 | FastAPI (自托管) | Cloudflare Worker (边缘) |
| 部署 | VPS/服务器 | Cloudflare Dashboard |
| 费用 | 服务器费用 | 免费额度内 |
| WebUI | ✅ 完整 | ❌ 简化状态页 |
| Manager | ✅ | ✅ |
| 多账号 | ✅ | ✅ |
| 自动轮换 | ✅ | ✅ |
| 持久化统计 | ✅ SQLite | ❌ 内存 |
| 全球加速 | ❌ | ✅ 边缘节点 |

## 常见问题

### Q: bridge_cf.py 连接失败

检查：
1. Worker 是否部署成功
2. CF_WS_URL 是否正确 (注意是 `wss://` 不是 `ws://`)
3. 容器是否能访问外网

### Q: Manager 无法注入脚本

检查：
1. users/ 目录下是否有有效账号
2. 账号的 serviceToken 是否过期
3. Claw 容器是否创建成功

### Q: 请求返回 503

表示没有可用的内网节点。检查：
1. Manager 是否正常运行
2. bridge_cf.py 是否成功连接到 Worker

## 目录结构

```
cloudflare-worker/
├── worker.js          # Cloudflare Worker 网关
├── bridge_cf.py       # 内网桥接脚本
├── manager_cf.py      # 账号池管理器
├── main_cf.py         # 启动入口
├── env.example        # 配置示例
├── README.md          # 本文档
└── logs/              # 日志目录 (自动创建)
```

## 单独使用 bridge_cf.py

如果不需要 Manager 自动管理，可以手动运行：

```bash
# 在 MIMO 容器内
export WS_URL="wss://mimo2api.xxx.workers.dev/ws"
export MIMO_API_KEY="your-key"
export MIMO_API_ENDPOINT="https://api.mimo.ai/v1/chat/completions"

python bridge_cf.py
```

## 基于原项目

> 基于 [wkeqin/mimi3](https://github.com/wkeqin/mimi3) 的二次开发项目
