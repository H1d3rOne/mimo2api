# mimo2api Cloudflare Worker 版

这是 mimo2api 的无服务器版本，使用 Cloudflare Worker + KV + Durable Object + Cron 管理 MIMO Claw 账号、bridge 隧道和 OpenAI / Anthropic 兼容 API。

## 重点能力

- `/ws` 接收 Claw 容器内 `bridge.py` 的 WebSocket 连接，带 bridge token 鉴权
- Durable Object 统一管理在线 bridge、请求队列、错误冷却和 WebSocket hibernation
- KV 保存用户、模型映射、端点转换、优选连接和生命周期状态
- Cron 自动执行实例创建、reset、bridge 注入和轮换
- Worker WebUI 名称为 **Mimo2api**
- 支持 `/v1/responses` → 上游 `/v1/chat/completions` 端点转换
- 支持 Cloudflare 优选连接：bridge 连接优选地址，Host/SNI 仍使用 Worker 域名
- 支持两种 MIMO AI Studio 管理通道：
  - Cloudflare Zero Trust VPC Service / Gateway（推荐）
  - Cloudflare Tunnel / 反代：`MIMO_PROXY_URL` + `MIMO_TUNNEL_TOKEN`

## 架构

```text
客户端 / SDK
   ↓
Cloudflare Worker
   ├─ WebUI / 管理 API / Cron
   ├─ KV
   └─ Gateway Durable Object
          ↓ /ws
Claw 容器 bridge.py
          ↓
MIMO API
```

## 部署

```bash
cd cloudflare-worker
npm install
cp wrangler.example.toml wrangler.toml
wrangler kv namespace create MIMO_KV
```

编辑 `wrangler.toml`：

```toml
[[kv_namespaces]]
binding = "MIMO_KV"
id = "你的 KV namespace id"

[vars]
MIMO2API_WS_URL = "wss://你的-worker-域名/ws"
MIMO_ENDPOINT_CONVERSION_ENABLED = "false"
```

配置敏感项：

```bash
wrangler secret put MIMO_RELAY_OPENAI_KEY
wrangler secret put MIMO_WEBUI_USERNAME
wrangler secret put MIMO_WEBUI_PASSWORD
```

### 管理通道

Worker 需要访问 `https://aistudio.xiaomimimo.com` 的控制面接口来查询、创建、销毁实例和获取 WS ticket。

推荐使用 Cloudflare Zero Trust VPC Service。实测 MIMO 控制面直走公网 EGRESS 可能返回 `destination_ip_prohibited`；专用 VPC Service 绑定到一个健康的 Zero Trust tunnel，并固定到 MIMO AI Studio 当前可用 ALB IP 时可正常访问，同时请求 URL 的 Host/SNI 仍保持 `aistudio.xiaomimimo.com`。

> 如果当前 `wrangler` 不能识别 `[[vpc_networks]]`，先 `npm install` 使用本项目锁定的 Wrangler 4.x。

先创建 VPC Service：

```bash
npx wrangler vpc service create mimo-aistudio \
  --type http \
  --tunnel-id YOUR_TUNNEL_ID \
  --ipv4 39.101.90.223 \
  --https-port 443 \
  --cert-verification-mode disabled
```

```toml
# wrangler.toml
[[vpc_services]]
binding = "MIMO_AISTUDIO"
service_id = "上一步返回的 service id"
remote = true

[[vpc_networks]]
binding = "EGRESS"
network_id = "cf1:network"
remote = true

[vars]
MIMO_CONTROL_CHANNEL = "gateway"
USE_VPC_EGRESS = "true"
```

配置后 Worker 会优先通过 `env.MIMO_AISTUDIO.fetch()` 访问 MIMO AI Studio；未配置 VPC Service 时才回退到 `env.EGRESS.fetch()`。`MIMO_CONTROL_CHANNEL="gateway"` 会强制走 Zero Trust/VPC 通道，即使 Cloudflare secret 里还残留旧 `MIMO_PROXY_URL` 也不会回到 Tunnel 反代。

如果继续使用 Tunnel / 反代，则配置：

```bash
wrangler secret put MIMO_PROXY_URL
wrangler secret put MIMO_TUNNEL_TOKEN
```

未显式设置 `MIMO_CONTROL_CHANNEL` 时，自动选择顺序为：`MIMO_PROXY_URL` 代理/Tunnel > `MIMO_AISTUDIO` VPC Service > `EGRESS` Gateway > Worker direct fetch；显式设置后以 `MIMO_CONTROL_CHANNEL` 为准。

构建和部署：

```bash
npm run bundle
npx tsc --noEmit
npm run deploy
```

访问：

```text
https://你的-worker-域名/webui
```

## WebUI 重点

- 顶部有“端点转换”和“优选连接”入口。
- “Claw 运行账号管理”标题右侧提示：`（首次运行需要手动注入消息，请至少保持一个账号在线）`。
- 所有账号不可用时，“网关运行中”下方只提示需要手动注入。
- 每个账号操作菜单（…）可随时复制：
  - reset 消息
  - bridge 消息
- 复制接口：`GET /api/manual_injection_messages?user_id=xxx`
- 注入消息动态生成，不保存到项目目录文件。

## 注入逻辑

自动生命周期流程：

1. 实例可用后，先发送第 1 条 reset 消息。
2. 下一轮发送第 2 条 bridge 消息，写入 `/tmp/bridge.py` 并后台运行。
3. 如果当前实例周期内 bridge 曾经在线，断开后只等待重连，不重发注入。
4. 如果实例 `AVAILABLE` 但 bridge 从未在线，会重发第 2 条 bridge 消息。

第二条 bridge 消息前缀当前为：

```text
好，先确保 websockets 和 httpx 可用。把下面 Python 代码保存为 /tmp/bridge.py，然后请用 nohup 后台静默运行bridge.py和cloudflard（请务必在后台运行，不要阻塞我们的对话！）。
```

WebUI 复制 bridge 消息和自动注入使用同一个模板。

## API 重点

| 路由 | 说明 |
| --- | --- |
| `GET /webui` | WebUI |
| `GET /api/stats` | Gateway + 生命周期状态 |
| `GET /api/system/status` | WebUI 实时状态 |
| `GET /api/users/list` | WebUI 用户列表 |
| `POST /api/users/add` | 导入用户 |
| `POST /api/users/create/:uid` | 创建实例 |
| `POST /api/users/destroy/:uid` | 销毁实例 |
| `POST /api/users/rebuild/:uid` | 重建实例 |
| `GET /api/manual_injection_messages?user_id=xxx` | reset / bridge 注入消息 |
| `GET/PUT/DELETE /api/model_mapping` | 模型映射 |
| `GET/PUT /api/endpoint_conversion` | 端点转换 |
| `GET/PUT /api/network_config` | 优选连接 |
| `POST /api/tick` / `POST /api/tick/:uid` | 手动触发生命周期 tick |
| `GET /install` / `GET /install.sh` | 手动安装 bridge 脚本 |
| `GET /bootstrap.sh` | bridge + cloudflared bootstrap 脚本 |

AI API：

- `GET /v1/models`
- `GET /anthropic/v1/models`
- `POST /v1/chat/completions`
- `POST /v1/responses`
- `POST /v1/audio/speech`
- `POST /anthropic/v1/messages`
- `POST /v1/messages`

## 本地验证

```bash
npm run bundle
npx tsc --noEmit
npx wrangler deploy --dry-run --outdir /tmp/mimo2api-worker-dryrun
```

## 隐私约定

不要提交真实 `wrangler.toml`、`.dev.vars`、`worker.js`、`worker.js.map`、Cookie、Tunnel Token、WebUI 密码或 AI API Key。
