import asyncio, websockets, httpx, json, os, signal, time

KEY = os.getenv("MIMO_API_KEY")
URL = os.getenv("MIMO_API_ENDPOINT")
BASE = URL.split("/v1/")[0] if "/v1/" in URL else URL.rstrip("/")
WS_URL = "__WS_URL__"
USER_ID = "__USER_ID__"
PID_FILE = f"/tmp/mimo2api_bridge_{USER_ID or 'default'}.pid"


def _iter_proc_cmdlines():
    proc = "/proc"
    if not os.path.isdir(proc):
        return
    self_pid = os.getpid()
    for name in os.listdir(proc):
        if not name.isdigit():
            continue
        pid = int(name)
        if pid == self_pid:
            continue
        try:
            with open(os.path.join(proc, name, "cmdline"), "rb") as f:
                cmd = f.read().replace(b"\x00", b" ").decode("utf-8", "ignore")
        except Exception:
            continue
        yield pid, cmd


def cleanup_old_bridge_processes():
    """同一个 Claw 容器内只保留一个 mimo2api bridge。"""
    old_pids = set()

    try:
        with open(PID_FILE, "r", encoding="utf-8") as f:
            old_pid = int((f.read() or "0").strip())
        if old_pid and old_pid != os.getpid():
            old_pids.add(old_pid)
    except Exception:
        pass

    for pid, cmd in list(_iter_proc_cmdlines() or []):
        if "python" not in cmd:
            continue
        if any(name in cmd for name in ("bridge.py", "mimo_bridge.py", "mimo2api_bridge")):
            old_pids.add(pid)

    for pid in old_pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:
            pass
    if old_pids:
        time.sleep(0.5)

    try:
        with open(PID_FILE, "w", encoding="utf-8") as f:
            f.write(str(os.getpid()))
    except Exception:
        pass


async def safe_send(ws, lock, data):
    async with lock:
        await ws.send(json.dumps(data))


def upstream_url(path: str) -> str:
    if "/anthropic/" in path:
        return f"{BASE}/anthropic/v1/messages"
    if "/v1/audio/speech" in path:
        return f"{BASE}/v1/audio/speech"
    if "/v1/responses" in path:
        return f"{BASE}/v1/responses"
    return URL


async def handle_request(ws, req, client, lock):
    req_id = req.get("req_id")
    try:
        async with client.stream(
            method=req.get("method", "GET"),
            url=upstream_url(req.get("path", "")),
            headers={"api-key": KEY, "Content-Type": "application/json"},
            content=req.get("body", "")
        ) as r:
            await safe_send(ws, lock, {
                "req_id": req_id, "type": "start",
                "status": r.status_code, "headers": dict(r.headers)
            })
            async for chunk in r.aiter_text():
                if chunk:
                    await safe_send(ws, lock, {
                        "req_id": req_id, "type": "chunk", "body": chunk
                    })
            await safe_send(ws, lock, {"req_id": req_id, "type": "finish"})

    except Exception as e:
        await safe_send(ws, lock, {"req_id": req_id, "type": "error", "body": str(e)})

async def main():
    cleanup_old_bridge_processes()
    async with httpx.AsyncClient(timeout=None) as client:
        while True:
            try:
                async with websockets.connect(WS_URL, max_size=10**8) as ws:
                    send_lock = asyncio.Lock()
                    # 发送注册消息，包含 user_id
                    if USER_ID:
                        await safe_send(ws, send_lock, {"type": "register", "user_id": USER_ID})
                    async for msg in ws:
                        asyncio.create_task(handle_request(ws, json.loads(msg), client, send_lock))
            except Exception:
                await asyncio.sleep(3)

if __name__ == "__main__":
    asyncio.run(main())
