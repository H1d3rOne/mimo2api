# mimo2api - 隧道直转模式

> 基于 [wkeqin/mimi3](https://github.com/wkeqin/mimi3) 的二次开发项目

小米 AI Studio MIMO 模型 API 网关，通过 WebSocket 隧道转发请求，不做任何格式转换。

## 架构

```
公网网关（监听 WebSocket /ws）
        ↑
        │ WebSocket 隧道
        │
内网 bridge.py（在 MIMO Claw 容器内）
        │
        ↓
    MIMO API（内网）
```

## 功能

- 请求原样转发，只做模型名映射
- WebSocket 隧道穿透内网
- 流式响应支持
- 多账号负载均衡

## 快速开始

```bash
# 安装依赖
pip install -r requirements.txt

# 复制并配置环境变量
cp env.example .env

# 编辑 .env，设置 WS_TUNNEL_URL
# WS_TUNNEL_URL=ws://你的公网域名:8000/ws

# 将账号凭证放到 users/ 目录
# users/user_xxx.json

# 启动服务
python main.py
```

## API 端点

所有端点都是原样转发到内网节点：

- `GET /v1/models` -- 列出可用模型
- `POST /v1/chat/completions` -- 原样转发
- `POST /v1/responses` -- 原样转发
- `POST /v1/audio/speech` -- 原样转发
- `POST /anthropic/v1/messages` -- 原样转发
