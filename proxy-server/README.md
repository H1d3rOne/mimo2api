# Cloudflare Tunnel 方案（在 Mimo 容器中运行）

解决 CF Worker 无法直接 `fetch()` 受 Cloudflare 保护的站点（error 1002）。

## 原理

```
用户请求 → CF Worker (/v1/chat/completions)
                ↓
          Gateway (负载均衡选择节点)
                ↓ WS 转发
          bridge.py (容器内) → aistudio API

CF Worker 调 aistudio API:
  fetch("https://mimo-tunnel.your-domain.com/open-apis/...")
        ↓ (Cloudflare 内部网络，不触发 1002)
  Cloudflare Tunnel (mimo-tunnel.your-domain.com)
        ↓ (cloudflared 转发)
  aistudio.xiaomimimo.com
        ↑
  Mimo 容器中运行的 cloudflared (通过 Tunnel Token 自动连接)

bridge.py 连回 Worker:
  容器 bridge.py ──WS──→ CF Worker /ws (注册为节点)
  (不需要 Tunnel，直接走公网)
```

**关键点**：
- `cloudflared` 运行在 Mimo 容器内部，不需要你自己的服务器
- 容器每次创建时，注入脚本会自动下载并启动 `cloudflared`
- **多个容器共享同一个 Tunnel Token**，Cloudflare 会自动做负载均衡
- bridge.py 仍然通过公网 WS 连回 Worker 的 `/ws`，不需要走 Tunnel

## 配置步骤

### 1. 在 Cloudflare Dashboard 创建 Tunnel

1. 登录 [Cloudflare Zero Trust](https://one.dash.cloudflare.com/)
2. **Networks** → **Tunnels** → **Create a tunnel**
3. 选择 **Cloudflared** 类型，给 Tunnel 起个名字（如 `mimo-proxy`）
4. **保存**后会生成一个 **Tunnel Token**，复制保存下来

### 2. 配置 Tunnel 的 Public Hostname

在 Tunnel 设置页面，添加一条 **Public Hostname** 规则：

| 字段 | 值 |
|------|-----|
| Subdomain | `mimo-tunnel`（或你喜欢的子域名前缀） |
| Domain | 你的域名（如 `your-domain.com`） |
| Type | `HTTPS` |
| URL | `aistudio.xiaomimimo.com` |

高级设置中勾选 **No TLS Verify**（因为 Tunnel 转发到外部 HTTPS 站点）。

这样 `https://mimo-tunnel.your-domain.com` 就等于 `https://aistudio.xiaomimimo.com`。

### 3. 配置 Worker 环境变量

```bash
# 设置 Tunnel Token（注入到容器中，让 cloudflared 自动连接）
wrangler secret put MIMO_TUNNEL_TOKEN
# 粘贴步骤 1 中获取的 Token

# 设置代理 URL（CF Worker 用此域名替代 aistudio.xiaomimimo.com）
wrangler secret put MIMO_PROXY_URL
# 输入: https://mimo-tunnel.your-domain.com
```

或者直接在 `wrangler.toml` 的 `[vars]` 中设置（不推荐明文写 Token）：

```toml
MIMO_PROXY_URL = "https://mimo-tunnel.your-domain.com"
# MIMO_TUNNEL_TOKEN 建议通过 secret 设置
```

### 4. 重新部署 Worker

```bash
cd cloudflare-worker
wrangler deploy
```

## 工作流程

1. Worker 创建 Claw 实例后，进入注入阶段
2. 注入脚本告诉容器里的 AI 助手：
   - 下载 `cloudflared`
   - 用 `cloudflared tunnel run <token>` 启动 Tunnel
   - 同时安装并运行 `bridge.py`
3. 容器里的 `cloudflared` 连接到 Cloudflare 网络
4. CF Worker 的所有请求通过 `mimo-tunnel.your-domain.com` 走 Cloudflare 内部网络
5. 不会触发 1002 错误 ✅

## 多容器 / 多账号

**所有容器共享同一个 Tunnel Token**。Cloudflare Tunnel 原生支持多副本（multiple replicas）：
- 每个容器各自运行 `cloudflared tunnel run <同一个token>`
- Cloudflare 自动在多个连接之间做负载均衡
- 一个容器销毁不影响其他容器的 Tunnel 连接

例如 3 个 Mimo 账号 = 3 个容器，都用同一个 Token 启动 `cloudflared`，
Tunnel 的域名 `mimo-tunnel.your-domain.com` 只有一个，但背后有 3 条连接。

## 验证 Tunnel 是否工作

1. 在 WebUI 中查看容器日志，确认 `cloudflared` 已启动
2. 测试 Tunnel 域名：`curl -s https://mimo-tunnel.your-domain.com | head -5`
3. 检查 Worker 日志，确认不再出现 1002 错误

## 故障排除

- **容器中 cloudflared 未启动**：检查注入日志，确认 AI 助手执行了安装命令
- **502 Bad Gateway**：检查 Tunnel Dashboard 中的 Public Hostname 配置
- **WebSocket 不工作**：Tunnel 原生支持 WebSocket，无需额外配置
- **Tunnel Token 无效**：重新从 Dashboard 复制 Token
- **容器重建后 Tunnel 断开**：这是正常的，注入脚本会在新容器中重新启动 Tunnel
