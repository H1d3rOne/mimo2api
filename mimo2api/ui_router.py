import os
import json
import re
import time
import asyncio
import httpx
from fastapi import APIRouter, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from .auth import (
    create_webui_session_token,
    get_webui_cookie_name,
    get_webui_session_ttl,
    get_webui_username,
    is_ai_auth_enabled,
    is_web_auth_enabled,
    is_webui_authenticated,
    verify_webui_login,
    webui_cookie_secure,
)
from .gateway_state import state

router = APIRouter()

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
USERS_DIR = os.path.join(ROOT_DIR, "users")


@router.get("/")
async def root_page():
    return RedirectResponse(url="/webui", status_code=307)

@router.get("/webui")
async def webui_page():
    ui_path = os.path.join(os.path.dirname(__file__), "webui.html")
    if os.path.exists(ui_path):
        with open(ui_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return Response("webui.html not found", status_code=404)

@router.get("/api/system/status")
async def api_status():
    now = time.time()
    nodes = []
    available_clients = 0
    for ws in list(state.active_clients):
        info = state.node_info.get(id(ws), {})
        addr = info.get("addr", "Unknown")
        connected_at = info.get("connected_at", 0)
        requests_served = info.get("requests_served", 0)
        user_id = str(info.get("user_id") or "")
        cooldown_until = state.client_cooldowns.get(id(ws), 0)
        registered = bool(user_id)
        disabled = bool(info.get("disabled"))
        available = registered and not disabled and cooldown_until <= now
        if available:
            available_clients += 1
        uptime = int(now - connected_at) if connected_at else 0
        nodes.append({
            "addr": addr,
            "uptime": uptime,
            "requests_served": requests_served,
            "user_id": user_id,
            "registered": registered,
            "available": available,
            "disabled": disabled,
            "disabled_reason": info.get("disabled_reason", ""),
            "cooldown_until": int(cooldown_until) if cooldown_until > now else 0,
            "cooldown_remaining_seconds": max(0, int(cooldown_until - now)),
        })
    return JSONResponse({
        "active_clients": len(state.active_clients),
        "available_clients": available_clients,
        "nodes": nodes,
    })


@router.get("/api/auth/session")
async def api_auth_session(request: Request):
    auth_enabled = is_web_auth_enabled()
    authenticated = is_webui_authenticated(request)
    return JSONResponse({
        "enabled": auth_enabled,
        "authenticated": authenticated,
        "username": get_webui_username(),
        "ai_auth_enabled": is_ai_auth_enabled(),
    })


@router.post("/api/auth/login")
async def api_auth_login(request: Request):
    if not is_web_auth_enabled():
        return JSONResponse({"ok": True, "enabled": False, "username": get_webui_username()})

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"detail": "请求体不是合法 JSON"}, status_code=400)

    username = str(body.get("username", "")).strip()
    password = str(body.get("password", ""))
    if not verify_webui_login(username, password):
        return JSONResponse({"detail": "用户名或密码错误"}, status_code=401)

    response = JSONResponse({"ok": True, "enabled": True, "username": get_webui_username()})
    response.set_cookie(
        key=get_webui_cookie_name(),
        value=create_webui_session_token(get_webui_username()),
        max_age=get_webui_session_ttl(),
        httponly=True,
        samesite="lax",
        secure=webui_cookie_secure(),
        path="/",
    )
    return response


@router.post("/api/auth/logout")
async def api_auth_logout():
    response = JSONResponse({"ok": True})
    response.delete_cookie(key=get_webui_cookie_name(), path="/")
    return response

async def fetch_user_status(data: dict) -> dict:
    uid = data.get("userId")
    cookies = {
        "serviceToken": data.get("serviceToken", ""),
        "userId": uid,
        "xiaomichatbot_ph": data.get("xiaomichatbot_ph", "")
    }
    url = "https://aistudio.xiaomimimo.com/open-apis/user/mimo-claw/status"
    headers = {
        "Accept": "*/*",
        "Content-Type": "application/json",
        "Origin": "https://aistudio.xiaomimimo.com",
        "Referer": "https://aistudio.xiaomimimo.com/",
        "User-Agent": "Mozilla/5.0"
    }
    try:
        async with httpx.AsyncClient() as c:
            r = await c.get(url, cookies=cookies, headers=headers, timeout=5)
            if r.status_code == 401:
                return {**data, "claw_status": "EXPIRED(401)", "remain_sec": 0}
            r_data = r.json()
            st = r_data.get("data", {}).get("status", "UNKNOWN")
            expire_ms = r_data.get("data", {}).get("expireTime")
            remain_sec = max(0, int(int(expire_ms) / 1000 - time.time())) if expire_ms else 0
            return {**data, "claw_status": st, "remain_sec": remain_sec}
    except Exception:
        return {**data, "claw_status": "ERROR", "remain_sec": 0}

@router.get("/api/users/list")
async def api_users_list():
    raw_users = []
    if os.path.exists(USERS_DIR):
        for fn in os.listdir(USERS_DIR):
            if fn.startswith("user_") and fn.endswith(".json"):
                try:
                    with open(os.path.join(USERS_DIR, fn), "r", encoding="utf-8") as f:
                        raw_users.append(json.load(f))
                except:
                    pass

    # 并发查询所有用户的实例状态
    tasks = [fetch_user_status(rd) for rd in raw_users]
    results = await asyncio.gather(*tasks) if raw_users else []

    users = []
    for data in results:
        users.append({
            "userId": data.get("userId"),
            "name": data.get("name"),
            "serviceToken": data.get("serviceToken"),
            "claw_status": data.get("claw_status", "UNKNOWN"),
            "remain_sec": data.get("remain_sec", 0)
        })
    return JSONResponse({"users": users})

@router.post("/api/users/add")
async def api_users_add(request: Request):
    try:
        body = await request.json()
        raw_text = body.get("raw_text", "").strip()

        if not raw_text:
            return JSONResponse({"detail": "输入不能为空"}, status_code=400)

        # 尝试解析为 JSON（支持单条或数组）
        users = []
        try:
            json_obj = json.loads(raw_text)
            if isinstance(json_obj, list):
                users = [u for u in json_obj if u.get("userId") and u.get("serviceToken") and u.get("xiaomichatbot_ph")]
            elif isinstance(json_obj, dict) and json_obj.get("userId") and json_obj.get("serviceToken") and json_obj.get("xiaomichatbot_ph"):
                users = [json_obj]
        except (json.JSONDecodeError, ValueError):
            # 非 JSON，尝试 Cookie 字符串正则解析
            parsed = {}
            for match in re.finditer(r'([a-zA-Z0-9_]+)="?([^;"]+)"?', raw_text):
                parsed[match.group(1)] = match.group(2)
            uid = parsed.get("userId")
            st = parsed.get("serviceToken")
            ph = parsed.get("xiaomichatbot_ph")
            if uid and st and ph:
                users = [{"userId": uid, "serviceToken": st, "xiaomichatbot_ph": ph, "name": f"Imported_{uid}"}]

        if not users:
            return JSONResponse({"detail": "缺少必要字段 userId, serviceToken 或 xiaomichatbot_ph"}, status_code=400)

        os.makedirs(USERS_DIR, exist_ok=True)
        imported_ids = []
        for u in users:
            uid = u["userId"]
            user_data = {
                "userId": uid,
                "serviceToken": u["serviceToken"],
                "xiaomichatbot_ph": u["xiaomichatbot_ph"],
                "name": u.get("name", f"Imported_{uid}"),
            }
            target_file = os.path.join(USERS_DIR, f"user_{uid}.json")
            with open(target_file, "w", encoding="utf-8") as f:
                json.dump(user_data, f, ensure_ascii=False, indent=2)
            imported_ids.append(uid)

        if len(imported_ids) == 1:
            return JSONResponse({"status": "ok", "userId": imported_ids[0]})
        return JSONResponse({"status": "ok", "imported": len(imported_ids), "userIds": imported_ids})
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=500)

@router.delete("/api/users/delete/{uid}")
async def api_users_delete(uid: str):
    target_file = os.path.join(USERS_DIR, f"user_{uid}.json")
    if os.path.exists(target_file):
        os.remove(target_file)
        return JSONResponse({"status": "ok"})
    return JSONResponse({"detail": "User not found"}, status_code=404)

@router.post("/api/users/destroy/{uid}")
async def api_users_destroy(uid: str):
    from .manager import get_account_manager
    mgr = get_account_manager(uid)
    if not mgr:
        return JSONResponse({"ok": False, "error": f"账号 {uid} 未找到"}, status_code=404)
    result = await mgr.destroy_instance()
    return JSONResponse(result)

@router.post("/api/users/create/{uid}")
async def api_users_create(uid: str):
    from .manager import get_account_manager
    mgr = get_account_manager(uid)
    if not mgr:
        return JSONResponse({"ok": False, "error": f"账号 {uid} 未找到"}, status_code=404)
    result = await mgr.create_instance()
    return JSONResponse(result)

@router.post("/api/users/rebuild/{uid}")
async def api_users_rebuild(uid: str):
    from .manager import get_account_manager
    mgr = get_account_manager(uid)
    if not mgr:
        return JSONResponse({"ok": False, "error": f"账号 {uid} 未找到"}, status_code=404)
    result = await mgr.rebuild_instance()
    return JSONResponse(result)
