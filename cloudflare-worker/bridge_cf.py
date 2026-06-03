#!/usr/bin/env python3
"""
Cloudflare Worker 版本的 bridge.py

内网节点通过 WebSocket 连接到 Cloudflare Worker 网关，
接收请求并转发到本地 MIMO API。

使用方法:
    1. 设置环境变量: export WS_URL=wss://your-worker.workers.dev/ws
    2. 设置 MIMO API 凭证
    3. 运行: python bridge_cf.py
"""
import asyncio
import websockets
import httpx
import json
import os

# ============== 配置 ==============
KEY = os.getenv("MIMO_API_KEY")
URL = os.getenv("MIMO_API_ENDPOINT")
BASE = URL.split("/v1/")[0] if URL and "/v1/" in URL else URL
WS_URL = os.getenv("WS_URL")  # Cloudflare Worker WebSocket 地址
USER_ID = os.getenv("USER_ID", "")  # 可选：用户标识

if not KEY:
    raise ValueError("缺少 MIMO_API_KEY 环境变量")
if not URL:
    raise ValueError("缺少 MIMO_API_ENDPOINT 环境变量")
if not WS_URL:
    raise ValueError("缺少 WS_URL 环境变量 (应为 wss://your-worker.workers.dev/ws)")


async def safe_send(ws, lock, data):
    """线程安全发送"""
    async with lock:
        await ws.send(json.dumps(data))


async def handle_request(ws, req, client, lock):
    """处理单个请求"""
    req_id = req.get("req_id")
    path = req.get("path", "")

    try:
        # 根据路径选择 API 端点
        if "/anthropic/" in path:
            target_url = f"{BASE}/anthropic/v1/messages"
        elif "/v1/audio/speech" in path:
            target_url = f"{BASE}/v1/audio/speech"
        elif "/v1/responses" in path:
            target_url = f"{BASE}/v1/responses"
        else:
            target_url = URL

        async with client.stream(
            method=req.get("method", "GET"),
            url=target_url,
            headers={"api-key": KEY, "Content-Type": "application/json"},
            content=req.get("body", "")
        ) as r:
            # 发送响应头
            await safe_send(ws, lock, {
                "req_id": req_id,
                "type": "start",
                "status": r.status_code,
                "headers": dict(r.headers)
            })

            # 流式传输响应体
            async for chunk in r.aiter_text():
                if chunk:
                    await safe_send(ws, lock, {
                        "req_id": req_id,
                        "type": "chunk",
                        "body": chunk
                    })

            # 发送结束标记
            await safe_send(ws, lock, {"req_id": req_id, "type": "finish"})

    except Exception as e:
        await safe_send(ws, lock, {
            "req_id": req_id,
            "type": "error",
            "body": str(e)
        })


async def main():
    """主循环：连接网关并处理请求"""
    print(f"🔗 正在连接 Cloudflare Worker: {WS_URL}")

    async with httpx.AsyncClient(timeout=None) as client:
        while True:
            try:
                async with websockets.connect(WS_URL, max_size=10**8) as ws:
                    send_lock = asyncio.Lock()

                    # 发送注册消息
                    if USER_ID:
                        await safe_send(ws, send_lock, {
                            "type": "register",
                            "user_id": USER_ID
                        })
                        print(f"📋 已注册为用户: {USER_ID}")

                    print("✅ 已连接到网关，等待请求...")

                    # 处理请求
                    async for msg in ws:
                        req = json.loads(msg)
                        asyncio.create_task(handle_request(ws, req, client, send_lock))

            except websockets.exceptions.ConnectionClosed:
                print("⚠️ 连接断开，3秒后重连...")
            except Exception as e:
                print(f"❌ 连接错误: {e}，3秒后重连...")

            await asyncio.sleep(3)


if __name__ == "__main__":
    print("🚀 mimo2api Bridge (Cloudflare Worker 版)")
    print(f"📡 MIMO API: {URL}")
    print(f"🌐 Gateway: {WS_URL}")
    asyncio.run(main())
