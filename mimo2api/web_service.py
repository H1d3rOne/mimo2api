import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, TextIO
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Response
from fastapi.responses import StreamingResponse, JSONResponse
import uvicorn
import os
from pathlib import Path

try:
    import fcntl
except ImportError:
    fcntl = None

try:
    import msvcrt
except ImportError:
    msvcrt = None

MODEL_MAPPING_FILE = Path(__file__).parent.parent / "model_mapping.json"

from .manager import start_manager_tasks, trigger_rebuild
from .auth import (
    get_webui_username,
    is_ai_auth_enabled,
    is_web_auth_enabled,
    require_ai_request,
    require_webui_request,
)
from .metrics_store import (
    METRICS_BUCKET_SECONDS,
    METRICS_RETENTION_DAYS,
    build_gateway_stats,
    extract_usage_from_sse_chunk,
    init_metrics_db,
    load_status_history,
    metrics_history_worker,
    node_label,
    reclassify_history,
    record_attempt_finished,
    record_attempt_started,
    record_request_finished,
    record_request_started,
)

# 配置基础日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

manager_bg_task = None
metrics_persist_task = None
sweeper_bg_task = None
single_process_lock_file = None
STALE_QUEUE_TTL = 300
SHUTDOWN_TASK_TIMEOUT = float(os.getenv("MIMO_SHUTDOWN_TASK_TIMEOUT", "5"))

def sweep_stale_queues_once(now: float | None = None) -> int:
    now = time.time() if now is None else now
    stale_count = 0
    for req_id, last_activity_at in list(state.req_id_timestamps.items()):
        if now - last_activity_at > STALE_QUEUE_TTL:
            logger.error(f"💀 发现长时间无活动的悬挂队列，强制回收: [{req_id[:8]}]")
            cleanup_pending_request(req_id)
            stale_count += 1
    if stale_count > 0:
        logger.info(f"🧹 垃圾回收周期结束，共清理了 {stale_count} 个泄露队列。当前活跃队列数: {len(state.pending_queues)}")
    return stale_count

async def sweep_stale_queues():
    """后台巡检任务，清理长时间无活动的悬挂请求队列。"""
    while True:
        try:
            await asyncio.sleep(60)
            sweep_stale_queues_once()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"清理死锁队列任务发生异常: {e}")


async def close_active_clients() -> None:
    clients = list(state.active_clients)
    if not clients:
        return

    logger.info(f"🛑 正在关闭 {len(clients)} 个内网节点连接...")
    for client in clients:
        try:
            await client.close()
        except Exception as exc:
            logger.debug(f"关闭内网节点连接失败: {exc}")


async def cancel_and_wait_tasks(tasks: list[asyncio.Task | None], *, label: str) -> None:
    pending = [task for task in tasks if task is not None and not task.done()]
    if not pending:
        return

    for task in pending:
        task.cancel()

    try:
        await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=SHUTDOWN_TASK_TIMEOUT)
    except asyncio.TimeoutError:
        still_running = [task for task in pending if not task.done()]
        logger.warning(
            f"⚠️ 关闭 {label} 超时，{len(still_running)} 个任务在 {SHUTDOWN_TASK_TIMEOUT}s 内未退出"
        )

@asynccontextmanager
async def lifespan(app: FastAPI):
    global manager_bg_task, metrics_persist_task, sweeper_bg_task
    logger.info("🚀 正在拉起挂后台的 Claw 账号守护线程...")
    acquire_single_process_lock()

    await asyncio.to_thread(init_metrics_db)
    fixed = await asyncio.to_thread(reclassify_history)
    if fixed:
        logger.info(f"🔧 重新分类了 {fixed} 条历史状态记录")
        
    manager_bg_task = asyncio.create_task(start_manager_tasks(), name="mimo-manager")
    metrics_persist_task = asyncio.create_task(metrics_history_worker(), name="mimo-metrics")
    sweeper_bg_task = asyncio.create_task(sweep_stale_queues(), name="mimo-sweeper") # 启动巡检死神
    
    yield

    try:
        await close_active_clients()
        await cancel_and_wait_tasks(
            [manager_bg_task, metrics_persist_task, sweeper_bg_task],
            label="核心后台任务",
        )
        await cancel_and_wait_tasks(list(_background_tasks), label="转发清理任务")
    finally:
        manager_bg_task = None
        metrics_persist_task = None
        sweeper_bg_task = None
        release_single_process_lock()

app = FastAPI(lifespan=lifespan)

# 全局状态从 gateway_state 引入
from .gateway_state import state

# 注入前面拆分出的 WebUI 独立路由
from .ui_router import router as ui_router
app.include_router(ui_router)

RETRYABLE_STATUS_CODES = {401, 403, 429}
NODE_RESPONSE_TIMEOUT = 30
MAX_RETRIES = 3
MAX_PENDING_QUEUES = 2000
AI_ROUTE_PREFIXES = ("/v1/", "/anthropic/v1/")
WEBUI_PUBLIC_PATHS = {"/", "/api/auth/session", "/api/auth/login", "/api/auth/logout", "/api/stats", "/api/status/history", "/webui"}

if is_ai_auth_enabled():
    logger.info("🔐 AI API 鉴权已启用")
if is_web_auth_enabled():
    logger.info(f"🔐 WebUI 鉴权已启用，登录用户: {get_webui_username()}")


def is_ai_route(path: str) -> bool:
    return path.startswith(AI_ROUTE_PREFIXES)


def is_webui_route(path: str) -> bool:
    return path.startswith("/api/") and path not in WEBUI_PUBLIC_PATHS


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path

    if is_ai_route(path):
        auth_error = require_ai_request(request)
        if auth_error is not None:
            return auth_error

    if is_webui_route(path):
        auth_error = require_webui_request(request)
        if auth_error is not None:
            return auth_error

    return await call_next(request)


def diagnose_request(body_text: str) -> str:
    """从请求体中提取关键诊断信息，用于 400 错误追踪"""
    try:
        req = json.loads(body_text)
    except Exception:
        return "body=非法JSON"
    msgs = req.get("messages", [])
    model = req.get("model", "未指定")
    stream = req.get("stream", False)
    total_chars = sum(len(str(m.get("content", ""))) for m in msgs)
    est_tokens = total_chars // 3
    tools = req.get("tools", [])
    return (
        f"model={model}, stream={stream}, msgs={len(msgs)}, "
        f"est_tokens≈{est_tokens}, chars={total_chars}, tools={len(tools)}"
    )


def record_error(route: str, status_code: int, reason: str, model: str = "", detail: str = "", request_body: str = ""):
    """记录错误到环形缓冲区，可通过 /api/errors 查询"""
    state.recent_errors.append({
        "ts": int(time.time()),
        "route": route,
        "status": status_code,
        "reason": reason[:200],
        "model": model,
        "detail": detail[:500],
        "request": request_body[:2000] if request_body else "",
    })

STREAM_CHUNK_TIMEOUT = 60
STREAM_KEEPALIVE_INTERVAL = 25  # 秒，需小于 Cloudflare 超时 (~100s)
QUEUE_DRAIN_TIMEOUT = 5
DEFAULT_GATEWAY_ERROR = "Gateway Error: 所有节点请求失败"
NODE_401_COOLDOWN_SECONDS = int(os.getenv("MIMO_NODE_401_COOLDOWN_SECONDS", "900"))
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESS_LOCK_PATH = os.getenv("MIMO_PROCESS_LOCK_PATH", os.path.join(ROOT_DIR, "mimo2api.lock"))

# 后台 fire-and-forget 任务集合
_background_tasks: set[asyncio.Task] = set()
PROCESS_LOCK_SIZE = 1

def _track_task(task: asyncio.Task) -> None:
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)

def _lock_file_nonblocking(lock_file: TextIO) -> None:
    if os.name == "nt":
        if msvcrt is None:
            raise OSError("当前平台缺少 msvcrt，无法加锁。")
        lock_file.seek(0)
        msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, PROCESS_LOCK_SIZE)
        return

    if fcntl is None:
        raise OSError("当前平台缺少 fcntl，无法加锁。")
    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

def _unlock_file(lock_file: TextIO) -> None:
    if os.name == "nt":
        if msvcrt is None:
            raise OSError("当前平台缺少 msvcrt，无法解锁。")
        lock_file.seek(0)
        msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, PROCESS_LOCK_SIZE)
        return

    if fcntl is None:
        raise OSError("当前平台缺少 fcntl，无法解锁。")
    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

def acquire_single_process_lock() -> None:
    global single_process_lock_file
    if single_process_lock_file is not None:
        return

    try:
        lock_path = Path(PROCESS_LOCK_PATH)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path.touch(exist_ok=True)
        lock_file = lock_path.open("r+", encoding="utf-8")
        if lock_path.stat().st_size < PROCESS_LOCK_SIZE:
            lock_file.write("\n")
            lock_file.flush()
        _lock_file_nonblocking(lock_file)
    except (BlockingIOError, OSError) as exc:
        if 'lock_file' in locals():
            lock_file.close()
        raise RuntimeError("当前进程锁被占用。") from exc

    lock_file.seek(0)
    lock_file.truncate()
    lock_file.write(str(os.getpid()))
    lock_file.flush()
    single_process_lock_file = lock_file

def release_single_process_lock() -> None:
    global single_process_lock_file
    if single_process_lock_file is None:
        return
    try:
        _unlock_file(single_process_lock_file)
    finally:
        single_process_lock_file.close()
        single_process_lock_file = None

@dataclass(slots=True)
class RetryState:
    status_code: int = 502
    response_text: str = DEFAULT_GATEWAY_ERROR

@dataclass(slots=True)
class ForwardAttempt:
    req_id: str
    queue: asyncio.Queue
    target_ws: WebSocket
    first_msg: dict[str, Any]
    attempt_number: int

@app.post("/api/rebuild")
async def api_rebuild():
    trigger_rebuild()
    return JSONResponse(content={"ok": True, "message": "重建信号已发送，所有节点将在当前循环结束后立即重建"})

@app.get("/api/stats")
async def api_stats():
    return JSONResponse(content=build_gateway_stats(len(_background_tasks)))

@app.get("/api/status/history")
async def api_status_history(hours: int = 24):
    hours = max(1, min(hours, 24 * METRICS_RETENTION_DAYS))
    return JSONResponse(content=await asyncio.to_thread(load_status_history, hours))

@app.get("/api/errors")
async def api_errors(limit: int = 50):
    limit = max(1, min(limit, 200))
    errors = list(state.recent_errors)[-limit:]
    errors.reverse()  # 最新的在前
    return JSONResponse(content={"count": len(errors), "errors": errors})

def load_model_mapping() -> dict[str, str]:
    if not MODEL_MAPPING_FILE.exists():
        return {}
    try:
        return json.loads(MODEL_MAPPING_FILE.read_text("utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}

def save_model_mapping(mapping: dict[str, str]) -> None:
    tmp = MODEL_MAPPING_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(mapping, ensure_ascii=False, indent=2), "utf-8")
    tmp.rename(MODEL_MAPPING_FILE)

def apply_model_mapping(body_text: str) -> str:
    mapping = load_model_mapping()
    if not mapping:
        return body_text
    try:
        data = json.loads(body_text)
    except (json.JSONDecodeError, AttributeError):
        return body_text
    original_model = data.get("model")
    if original_model and original_model in mapping:
        data["model"] = mapping[original_model]
        logger.info(f"🔀 模型映射: {original_model} → {data['model']}")
        return json.dumps(data, ensure_ascii=False)
    return body_text

@app.get("/api/model_mapping")
async def api_get_model_mapping():
    return JSONResponse(content=load_model_mapping())

@app.put("/api/model_mapping")
async def api_put_model_mapping(request: Request):
    body = await request.body()
    try:
        new_mapping = json.loads(body.decode("utf-8", "ignore").lstrip("\ufeff"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return JSONResponse({"error": "请求体不是合法 JSON"}, status_code=400)
    if not isinstance(new_mapping, dict):
        return JSONResponse({"error": "映射必须是 JSON 对象"}, status_code=400)
    save_model_mapping(new_mapping)
    return JSONResponse(content=new_mapping)

@app.delete("/api/model_mapping/{model_name:path}")
async def api_delete_model_mapping(model_name: str):
    mapping = load_model_mapping()
    if model_name in mapping:
        del mapping[model_name]
        save_model_mapping(mapping)
        return JSONResponse({"ok": True, "deleted": model_name})
    return JSONResponse({"error": f"模型 {model_name} 不在映射中"}, status_code=404)

@app.websocket("/ws")
async def ws_tunnel(ws: WebSocket):
    await ws.accept()
    client_addr = f"{ws.client.host}:{ws.client.port}" if ws.client else "Unknown"
    state.active_clients.append(ws)
    state.client_cooldowns.pop(id(ws), None)
    logger.info(f"✅ 内网节点已接入: {client_addr}。当前在线节点数: {len(state.active_clients)}")
    
    try:
        while True:
            msg = await ws.receive_text()
            data = json.loads(msg)
            req_id = data.get("req_id")
            if req_id and req_id in state.pending_queues:
                touch_pending_request(req_id)
                state.pending_queues[req_id].put_nowait(data)
    except WebSocketDisconnect:
        logger.warning(f"❌ 内网节点主动断开: {client_addr}")
    except Exception as e:
        logger.error(f"❌ 内网节点异常断开: {client_addr}, 错误: {e}")
    finally:
        if ws in state.active_clients:
            state.active_clients.remove(ws)
        state.client_cooldowns.pop(id(ws), None)
        
        # 清理该节点的所有孤儿队列
        orphan_ids = state.ws_to_req_ids.pop(id(ws), set())
        for orphan_id in orphan_ids:
            q = state.pending_queues.pop(orphan_id, None)
            state.req_id_to_ws_id.pop(orphan_id, None)
            state.req_id_timestamps.pop(orphan_id, None)
            if q is not None:
                try:
                    q.put_nowait({"type": "error", "body": "节点断开连接"})
                except asyncio.QueueFull:
                    pass
        if orphan_ids:
            logger.warning(f"🧹 节点断开，已清理 {len(orphan_ids)} 个孤儿请求队列")
            
        if state.current_client_index >= len(state.active_clients):
            state.current_client_index = 0
        logger.info(f"当前在线节点数: {len(state.active_clients)}")


def get_next_client() -> WebSocket | None:
    if not state.active_clients:
        return None
    now = time.time()
    available_clients: list[WebSocket] = []
    for client in state.active_clients:
        if state.client_cooldowns.get(id(client), 0) <= now:
            available_clients.append(client)
    if not available_clients:
        return None
    if state.current_client_index >= len(available_clients):
        state.current_client_index = 0
    client = available_clients[state.current_client_index]
    state.current_client_index = (state.current_client_index + 1) % len(available_clients)
    return client


def get_available_client_count() -> int:
    now = time.time()
    return sum(1 for c in state.active_clients if state.client_cooldowns.get(id(c), 0) <= now)


def touch_pending_request(req_id: str) -> None:
    if req_id in state.pending_queues:
        state.req_id_timestamps[req_id] = time.time()


def create_pending_request() -> tuple[str, asyncio.Queue]:
    if len(state.pending_queues) >= MAX_PENDING_QUEUES:
        raise RuntimeError("pending queue 已满")
    req_id = str(uuid.uuid4())
    queue: asyncio.Queue = asyncio.Queue()
    state.pending_queues[req_id] = queue
    state.req_id_timestamps[req_id] = time.time()
    return req_id, queue


def cleanup_pending_request(req_id: str) -> None:
    state.pending_queues.pop(req_id, None)
    state.req_id_timestamps.pop(req_id, None)
    ws_id = state.req_id_to_ws_id.pop(req_id, None)
    if ws_id is not None:
        req_ids = state.ws_to_req_ids.get(ws_id)
        if req_ids is not None:
            req_ids.discard(req_id)
            if not req_ids:
                state.ws_to_req_ids.pop(ws_id, None)


def cooldown_client(ws: WebSocket, seconds: int, reason: str) -> None:
    cooldown_until = time.time() + max(seconds, 0)
    state.client_cooldowns[id(ws)] = cooldown_until
    logger.warning(
        f"⛔ 节点 {node_label(ws)} 因 {reason} 进入冷却 {seconds}s，"
        f"冷却结束时间戳: {int(cooldown_until)}"
    )

async def drain_and_close(req_id: str, queue: asyncio.Queue) -> None:
    try:
        while True:
            msg = await asyncio.wait_for(queue.get(), timeout=QUEUE_DRAIN_TIMEOUT)
            if msg.get("type") in ["finish", "error"]:
                break
    except Exception:
        pass
    finally:
        cleanup_pending_request(req_id)

def should_retry_status(status_code: int) -> bool:
    return status_code in RETRYABLE_STATUS_CODES or status_code >= 500

def build_ws_payload(req_id: str, method: str, path: str, body: str) -> str:
    return json.dumps({"req_id": req_id, "method": method, "path": path, "body": body})

async def dispatch_to_node(*, method: str, path: str, body: str, log_label: str, attempt_number: int) -> ForwardAttempt | None:
    try:
        req_id, queue = create_pending_request()
    except RuntimeError:
        logger.warning("⚠️ pending queue 已满，拒绝新请求")
        return None
        
    target_ws = get_next_client()
    if not target_ws:
        cleanup_pending_request(req_id)
        return None

    # 🌟 修复内存泄漏的双向绑定：既知道 WS 管哪些 req_id，也知道 req_id 归属于哪个 WS
    state.req_id_to_ws_id[req_id] = id(target_ws)
    state.ws_to_req_ids.setdefault(id(target_ws), set()).add(req_id)

    ws_payload = build_ws_payload(req_id, method, path, body)
    attempt_started_at = time.monotonic()
    record_attempt_started(target_ws)

    try:
        await target_ws.send_text(ws_payload)
        logger.debug(f"👉 {log_label} [{req_id[:8]}] ({method} {path}) -> 节点: {node_label(target_ws)} (尝试 {attempt_number})")
    except RuntimeError:
        record_attempt_finished(target_ws=target_ws, status_code=0, first_byte_latency_ms=(time.monotonic() - attempt_started_at) * 1000, success=False)
        logger.warning(f"⚠️ {log_label} 转发失败，节点状态异常，尝试切换...")
        cleanup_pending_request(req_id) # 内部会自动解绑 target_ws
        if target_ws in state.active_clients:
            state.active_clients.remove(target_ws)
        state.client_cooldowns.pop(id(target_ws), None)
        return None

    try:
        first_msg = await asyncio.wait_for(queue.get(), timeout=NODE_RESPONSE_TIMEOUT)
    except asyncio.TimeoutError:
        record_attempt_finished(target_ws=target_ws, status_code=504, first_byte_latency_ms=(time.monotonic() - attempt_started_at) * 1000, success=False)
        raise

    record_attempt_finished(
        target_ws=target_ws,
        status_code=int(first_msg.get("status", 200)),
        first_byte_latency_ms=(time.monotonic() - attempt_started_at) * 1000,
        success=first_msg.get("type") != "error" and not should_retry_status(int(first_msg.get("status", 200))),
    )
    return ForwardAttempt(req_id=req_id, queue=queue, target_ws=target_ws, first_msg=first_msg, attempt_number=attempt_number)


async def prepare_forward_attempt(*, method: str, path: str, body: str, log_label: str, retry_state: RetryState, attempt_number: int) -> ForwardAttempt | None:
    attempt = await dispatch_to_node(method=method, path=path, body=body, log_label=log_label, attempt_number=attempt_number)
    if attempt is None:
        return None

    first_msg = attempt.first_msg
    if first_msg.get("type") == "error":
        error_text = first_msg.get("body") or "节点返回错误"
        logger.warning(f"⚠️ {log_label} 节点返回内部错误: {error_text}，尝试切换...")
        retry_state.response_text = f"Gateway Error: {error_text}"
        cleanup_pending_request(attempt.req_id)
        return None

    status_code = first_msg.get("status", 200)
    if status_code == 401:
        cooldown_client(attempt.target_ws, NODE_401_COOLDOWN_SECONDS, "401 Unauthorized")
        retry_state.status_code = 401
        retry_state.response_text = "Gateway Error: 节点鉴权失败 (401)，已临时跳过该节点"

    if should_retry_status(status_code):
        logger.warning(f"⚠️ {log_label} 节点返回状态码 {status_code}，触发自动重试 (当前 attempt={attempt_number})...")
        retry_state.status_code = status_code
        _track_task(asyncio.create_task(drain_and_close(attempt.req_id, attempt.queue)))
        return None

    return attempt


def normalize_response_headers(headers: dict | None) -> tuple[str, dict]:
    response_headers = dict(headers or {})
    content_type = response_headers.pop("content-type", "application/json")
    for key in ["content-length", "transfer-encoding", "content-encoding", "connection"]:
        response_headers.pop(key, None)
    return content_type, response_headers


async def collect_response_body(current_req_id: str, current_queue: asyncio.Queue, timeout: int = 120) -> str:
    chunks: list[str] = []
    try:
        while True:
            msg = await asyncio.wait_for(current_queue.get(), timeout=timeout)
            if msg.get("type") == "finish":
                break
            if msg.get("type") == "error":
                raise RuntimeError(msg.get("body") or "节点返回错误")
            if msg.get("type") == "chunk":
                chunks.append(msg.get("body", ""))
    finally:
        cleanup_pending_request(current_req_id)
    return "".join(chunks)

# -------------- API 路由定义 --------------

@app.post("/v1/audio/speech")
async def audio_speech_handler(request: Request):
    return await _forward_request(request, "/v1/audio/speech")

@app.post("/v1/responses")
async def responses_handler(request: Request):
    return await _forward_request(request, "/v1/responses")

_MODELS = [
    ("mimo-v2.5-pro", "MiMo V2.5 Pro", 1048576, 131072),
    ("mimo-v2.5", "MiMo V2.5", 1048576, 131072),
    ("mimo-v2.5-tts", "MiMo V2.5 TTS", 8192, 8192),
    ("mimo-v2-pro", "MiMo V2 Pro", 1048576, 131072),
    ("mimo-v2-flash", "MiMo V2 Flash", 256000, 131072),
    ("mimo-v2-omni", "MiMo V2 Omni", 256000, 131072),
    ("mimo-v2.5-tts-voicedesign", "MiMo V2.5 TTS VoiceDesign", 8192, 8192),
    ("mimo-v2.5-tts-voiceclone", "MiMo V2.5 TTS VoiceClone", 8192, 8192),
    ("mimo-v2-tts", "MiMo V2 TTS", 8192, 8192),
]


@app.get("/v1/models")
async def get_models():
    data = [{"id": m[0], "object": "model", "created": 1700000000, "owned_by": "mimo", "context_length": m[2], "max_tokens": m[2]} for m in _MODELS]
    return JSONResponse(content={"object": "list", "data": data})

@app.get("/anthropic/v1/models")
async def get_anthropic_models():
    data = [
        {
            "id": model_id,
            "display_name": display_name,
            "created_at": "2025-01-01T00:00:00Z",
            "type": "model",
            "max_input_tokens": context_length,
            "max_tokens": max_output_tokens,
        }
        for model_id, display_name, context_length, max_output_tokens in _MODELS
    ]
    return JSONResponse(content={"data": data, "has_more": False, "first_id": data[0]["id"], "last_id": data[-1]["id"]})

@app.post("/api/test_model")
async def test_model_handler(request: Request):
    body = await request.body()
    try:
        data = json.loads(body.decode("utf-8", "ignore").lstrip("\ufeff"))
    except json.JSONDecodeError:
        return JSONResponse({"ok": False, "error": "请求体不是合法 JSON"}, status_code=400)

    model = data.get("model", "mimo-v2-flash")
    test_payload = json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 16,
        "stream": False,
    }, ensure_ascii=False)

    if not state.active_clients:
        return JSONResponse({"ok": False, "model": model, "error": "没有可用的内网节点"})

    max_retries = min(MAX_RETRIES, get_available_client_count())
    retry_state = RetryState()
    route_key = "/api/test_model"
    request_started_at = time.monotonic()

    for attempt in range(max_retries):
        req_id = "unknown"
        try:
            prepared = await prepare_forward_attempt(
                method="POST", path="/v1/chat/completions", body=test_payload,
                log_label="模型测试", retry_state=retry_state, attempt_number=attempt + 1,
            )
            if prepared is None:
                continue
            req_id = prepared.req_id
            queue = prepared.queue
            first_msg = prepared.first_msg
            status_code = first_msg.get("status", 200)
            first_byte_at = time.monotonic()

            if status_code >= 400:
                raw_body = await collect_response_body(req_id, queue)
                record_error(route_key, status_code, f"上游返回 {status_code}", detail=raw_body[:500])
                record_request_finished(route_key=route_key, status_code=status_code, started_at=request_started_at, first_byte_at=first_byte_at, success=False)
                return JSONResponse({"ok": False, "model": model, "error": f"上游返回 {status_code}"})

            raw_body = await collect_response_body(req_id, queue)
            record_request_finished(route_key=route_key, status_code=status_code, started_at=request_started_at, first_byte_at=first_byte_at, success=True)

            try:
                resp_json = json.loads(raw_body)
                content = ""
                choices = resp_json.get("choices", [])
                if choices:
                    content = choices[0].get("message", {}).get("content", "")
                return JSONResponse({"ok": True, "model": model, "response": content})
            except json.JSONDecodeError:
                return JSONResponse({"ok": True, "model": model, "response": "(响应解析失败)"})

        except asyncio.TimeoutError:
            retry_state.status_code = 504
            retry_state.response_text = "请求超时"
            cleanup_pending_request(req_id)
            continue
        except RuntimeError as exc:
            retry_state.status_code = 502
            retry_state.response_text = str(exc)
            cleanup_pending_request(req_id)
            continue
        except Exception as e:
            cleanup_pending_request(req_id)
            raise e

    return JSONResponse({"ok": False, "model": model, "error": retry_state.response_text})

@app.post("/v1/chat/completions")
async def chat_completions_handler(request: Request):
    return await _forward_request(request, "/v1/chat/completions")

@app.post("/anthropic/v1/messages")
async def anthropic_messages_handler(request: Request):
    return await _forward_request(request, "/anthropic/v1/messages")

async def _forward_request(request: Request, path: str):
    if not state.active_clients:
        return Response("Gateway Error: 没有可用的内网节点", status_code=503)

    body = await request.body()
    method = request.method
    max_retries = min(MAX_RETRIES, get_available_client_count())
    if max_retries == 0:
        return Response("Gateway Error: 没有可用的内网节点", status_code=503)

    retry_state = RetryState()
    body_text = body.decode("utf-8", "ignore").lstrip("\ufeff")
    body_text = apply_model_mapping(body_text)
    route_key = path
    request_started_at = time.monotonic()

    is_streaming = False
    try:
        is_streaming = json.loads(body_text).get("stream", False) is True
    except (json.JSONDecodeError, AttributeError):
        pass
    record_request_started(route_key, is_streaming=is_streaming)

    for attempt in range(max_retries):
        req_id = "unknown"
        try:
            prepared = await prepare_forward_attempt(method=method, path=path, body=body_text, log_label="转发请求", retry_state=retry_state, attempt_number=attempt + 1)
            if prepared is None:
                continue
            req_id = prepared.req_id
            queue = prepared.queue
            first_msg = prepared.first_msg
            status_code = first_msg.get("status", 200)
            first_byte_at = time.monotonic()
            content_type, response_headers = normalize_response_headers(first_msg.get("headers", {}))

            async def stream_generator(current_req_id, current_queue, use_keepalive):
                last_data_time = time.monotonic()
                data_task = asyncio.ensure_future(current_queue.get())
                keepalive_task = None
                stream_succeeded = False
                usage_data = None

                async def _do_keepalive():
                    await asyncio.sleep(STREAM_KEEPALIVE_INTERVAL)
                    return b": keep-alive\n\n"
                if use_keepalive:
                    keepalive_task = asyncio.ensure_future(_do_keepalive())

                try:
                    while True:
                        pending = {data_task}
                        if keepalive_task is not None:
                            pending.add(keepalive_task)
                        done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

                        if keepalive_task is not None and keepalive_task in done:
                            elapsed = time.monotonic() - last_data_time
                            if elapsed > STREAM_CHUNK_TIMEOUT:
                                logger.warning(f"⚠️ 流式 {elapsed:.0f}s 无数据，节点可能已断开 [{current_req_id[:8]}]")
                                break
                            yield keepalive_task.result()
                            keepalive_task = asyncio.ensure_future(_do_keepalive())
                            continue

                        last_data_time = time.monotonic()
                        data_task = asyncio.ensure_future(current_queue.get())
                        msg = done.pop().result()
                        if msg.get("type") == "finish":
                            stream_succeeded = True
                            break
                        elif msg.get("type") == "chunk":
                            chunk_body = msg.get("body", "")
                            if usage_data is None:
                                usage_data = extract_usage_from_sse_chunk(chunk_body)
                            yield chunk_body.encode("utf-8")
                finally:
                    data_task.cancel()
                    if keepalive_task is not None:
                        keepalive_task.cancel()
                    await asyncio.gather(*[t for t in (data_task, keepalive_task) if t is not None], return_exceptions=True)
                    cleanup_pending_request(current_req_id)
                    record_request_finished(route_key=route_key, status_code=status_code if stream_succeeded else 502, started_at=request_started_at, first_byte_at=first_byte_at, success=stream_succeeded and status_code < 400, usage=usage_data)

            if status_code >= 400:
                record_error(route_key, status_code, f"上游返回 {status_code}", detail=first_msg.get("body", "")[:300])

            return StreamingResponse(stream_generator(req_id, queue, use_keepalive=is_streaming), status_code=status_code, media_type=content_type, headers=response_headers)

        except asyncio.TimeoutError:
            retry_state.status_code = 504
            retry_state.response_text = "Gateway Error: 请求所有节点超时 (30s)"
            cleanup_pending_request(req_id)
            continue
        except Exception as e:
            cleanup_pending_request(req_id)
            raise e

    record_request_finished(route_key=route_key, status_code=retry_state.status_code, started_at=request_started_at, first_byte_at=None, success=False)
    return Response(retry_state.response_text, status_code=retry_state.status_code)

if __name__ == "__main__":
    logger.info("🚀 启动支持多节点的公网网关...")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        ws_max_size=10**8,
        timeout_graceful_shutdown=int(SHUTDOWN_TASK_TIMEOUT),
    )
