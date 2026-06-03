# mimo2api Cloudflare Worker

单文件 Worker，一键安装，无需你的机器参与。

## 使用方法

### 1. 部署 Worker

1. 登录 [Cloudflare Dashboard](https://dash.cloudflare.com)
2. Workers & Pages → Create Worker
3. 复制 `worker.js` 内容，粘贴
4. Deploy
5. 获得 `xxx.workers.dev` 域名

### 2. 在 MIMO Claw 容器内一键安装

访问你的 Worker 首页，会显示安装命令：

```
curl -s https://xxx.workers.dev/install?key=YOUR_MIMO_KEY | python
```

或者不带参数（需要在容器内设置 `MIMO_API_KEY` 环境变量）：

```
curl -s https://xxx.workers.dev/install | python
```

### 3. 完成

你的机器完全不参与，架构如下：

```
用户请求
    ↓
Cloudflare Worker (公网)
    ↓ WebSocket
MIMO Claw 容器 (小米云端)
    ↓
MIMO API
```

## API 端点

| 端点 | 说明 |
|------|------|
| `/` | 状态页（含安装命令） |
| `/install` | 安装脚本 |
| `/v1/models` | 模型列表 |
| `/v1/chat/completions` | Chat API |
| `/v1/responses` | Responses API |
| `/v1/audio/speech` | TTS API |
| `/anthropic/v1/messages` | Anthropic API |

## 可选配置

编辑 `worker.js` 顶部：

```javascript
const AI_AUTH_KEY = 'your-secret-key';  // API 鉴权
```
