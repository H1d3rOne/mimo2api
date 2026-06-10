/**
 * WebUI 管理页面模块（无 Durable Objects 版）
 *
 * 网关状态从 gateway 模块获取，生命周期状态从 KV 读取。
 */

import { Env, UserInfo } from "./types";
import { UserStore } from "./user-store";
import { ClawManager } from "./claw-manager";
import { gatewayForward, gatewayStats } from "./gateway-client";
import { getLifecycleState, deleteLifecycleState } from "./lifecycle";
import { loadNetworkConfig, savePreferredBaseUrl } from "./network-config";
import { loadEndpointConversionConfig, saveEndpointConversionEnabled } from "./endpoint-conversion";
import { appendBridgeToken, getBridgeToken } from "./bridge-auth";
import { getBridgeInjectionPrompt, RESET_CMD } from "./claw-ws-client";
import { getControlChannelLabel, getControlChannelMode, getGatewayEgressFetch, getControlProxyUrl } from "./control-channel";

// ─── Cookie 鉴权 ────────────────────────────────────────────────

const SESSION_COOKIE = "mimo2api_session";
const SESSION_TTL = 86400; // 24 小时
const PROXY_HEALTH_CACHE_MS = 60_000;
const MIMO_AISTUDIO_BASE_URL = "https://aistudio.xiaomimimo.com";

let proxyHealthCache: { ts: number; value: Record<string, unknown> } | null = null;

async function checkProxyHealth(env: Env): Promise<Record<string, unknown>> {
  const mode = getControlChannelMode(env);
  const label = getControlChannelLabel(mode);
  const proxyUrl = (env.MIMO_PROXY_URL || "").replace(/\/+$/, "");
  if (mode === "direct") return { status: "not_configured", mode, label };

  const now = Date.now();
  if (proxyHealthCache && now - proxyHealthCache.ts < PROXY_HEALTH_CACHE_MS) {
    return proxyHealthCache.value;
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 5000);
  try {
    const targetUrl = mode === "proxy" ? proxyUrl : MIMO_AISTUDIO_BASE_URL;
    const fetcher = mode === "gateway" ? getGatewayEgressFetch(env) : undefined;
    const resp = await (fetcher || fetch)(targetUrl, {
      method: "GET",
      redirect: "manual",
      signal: controller.signal,
      headers: { Accept: "text/html,*/*" },
    });
    let body = "";
    try {
      body = (await resp.clone().text()).slice(0, 500);
    } catch {}
    const tunnelError = resp.status === 530 || /1033|argo tunnel error|cloudflare tunnel error/i.test(body);
    const value = {
      status: tunnelError ? "unreachable" : "reachable",
      mode,
      label,
      http_status: resp.status,
      checked_at: now,
      note: tunnelError ? `${label} 返回 Cloudflare Tunnel 错误` : `${label} 有 HTTP 响应`,
    };
    proxyHealthCache = { ts: now, value };
    return value;
  } catch (err) {
    const value = {
      status: "unreachable",
      mode,
      label,
      checked_at: now,
      note: `${label} 请求失败: ${String(err).slice(0, 120)}`,
    };
    proxyHealthCache = { ts: now, value };
    return value;
  } finally {
    clearTimeout(timer);
  }
}

function isWebAuthEnabled(env: Env): boolean {
  return !!(env.MIMO_WEBUI_USERNAME && env.MIMO_WEBUI_PASSWORD);
}

function verifyWebAuth(request: Request, env: Env): boolean {
  if (!isWebAuthEnabled(env)) return true;
  const cookie = getCookie(request, SESSION_COOKIE);
  if (!cookie) return false;
  try {
    const decoded = atob(cookie);
    const [user, pass] = decoded.split(":");
    return user === env.MIMO_WEBUI_USERNAME && pass === env.MIMO_WEBUI_PASSWORD;
  } catch {
    return false;
  }
}

function createSessionToken(env: Env): string {
  return btoa(`${env.MIMO_WEBUI_USERNAME}:${env.MIMO_WEBUI_PASSWORD}`);
}

function getCookie(request: Request, name: string): string | null {
  const header = request.headers.get("cookie") || "";
  for (const part of header.split(";")) {
    const [k, ...v] = part.trim().split("=");
    if (k === name) return v.join("=");
  }
  return null;
}

// ─── WebUI 路由 ─────────────────────────────────────────────────

export async function handleWebuiRoute(
  request: Request,
  path: string,
  env: Env
): Promise<Response | null> {
  const method = request.method;

  // ─── 页面 ──────────────────────────────────────────────

  if (path === "/" && method === "GET") {
    return Response.redirect(new URL("/webui", request.url).href, 307);
  }

  if (path === "/webui" && method === "GET") {
    return new Response(WEBUI_HTML, {
      headers: { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" },
    });
  }

  // ─── 鉴权 API ──────────────────────────────────────────

  if (path === "/api/auth/session" && method === "GET") {
    const enabled = isWebAuthEnabled(env);
    const authenticated = enabled ? verifyWebAuth(request, env) : true;
    return json({
      enabled,
      authenticated,
      username: env.MIMO_WEBUI_USERNAME || "admin",
      ai_auth_enabled: !!(env.MIMO_RELAY_OPENAI_KEY),
    });
  }

  if (path === "/api/auth/login" && method === "POST") {
    if (!isWebAuthEnabled(env)) {
      return json({ ok: true, enabled: false, username: env.MIMO_WEBUI_USERNAME || "admin" });
    }
    try {
      const body = await request.json() as { username?: string; password?: string };
      const username = (body.username || "").trim();
      const password = body.password || "";
      if (username !== env.MIMO_WEBUI_USERNAME || password !== env.MIMO_WEBUI_PASSWORD) {
        return json({ detail: "用户名或密码错误" }, 401);
      }
      const token = createSessionToken(env);
      return json({ ok: true, enabled: true, username: env.MIMO_WEBUI_USERNAME }, 200, {
        "Set-Cookie": `${SESSION_COOKIE}=${token}; Max-Age=${SESSION_TTL}; HttpOnly; SameSite=Lax; Path=/`,
      });
    } catch {
      return json({ detail: "请求体不是合法 JSON" }, 400);
    }
  }

  if (path === "/api/auth/logout" && method === "POST") {
    return json({ ok: true }, 200, {
      "Set-Cookie": `${SESSION_COOKIE}=; Max-Age=0; HttpOnly; SameSite=Lax; Path=/`,
    });
  }

  // ─── 系统状态 ──────────────────────────────────────────

  if (path === "/api/system/status" && method === "GET") {
    if (isWebAuthEnabled(env) && !verifyWebAuth(request, env)) return json({ detail: "Unauthorized" }, 401);

    const gatewayData = await gatewayStats(env);

    const nodes = ((gatewayData.nodes as Array<Record<string, unknown>>) || []).map((n) => ({
      node_id: n.nodeId || 0,
      addr: "Cloudflare Worker",
      uptime: n.uptimeSeconds || 0,
      requests_served: n.requestsServed || 0,
      user_id: n.userId || "",
      connected_at: n.connectedAt || 0,
      available: n.available !== false,
      authenticated: n.authenticated === true,
      registered: n.registered === true,
      api_endpoint_present: n.apiEndpointPresent === true,
      cooldown_remaining_seconds: n.cooldownRemainingSeconds || 0,
      remote_addr: n.remoteAddr || "",
      meta: n.meta || {},
    }));

    const activeLifecycleUserIds = new Set<string>();
    try {
      const store = new UserStore(env.MIMO_KV);
      const userIds = await store.listUserIds();
      for (const uid of userIds) {
        const state = await getLifecycleState(env.MIMO_KV, uid);
        if (state.phase === "running" || state.phase === "injecting") activeLifecycleUserIds.add(uid);
      }
    } catch {}
    const managedAvailableClients = nodes.filter((n) => n.available && n.user_id && activeLifecycleUserIds.has(String(n.user_id))).length;

	    return json({
	      active_clients: gatewayData.activeClients || 0,
	      available_clients: managedAvailableClients,
	      gateway_available_clients: gatewayData.availableClients || 0,
	      control_channel_mode: getControlChannelMode(env),
	      control_channel_label: getControlChannelLabel(getControlChannelMode(env)),
	      proxy_url_configured: Boolean(env.MIMO_PROXY_URL),
	      vpc_service_configured: Boolean(env.MIMO_AISTUDIO),
	      egress_binding_configured: Boolean(env.EGRESS),
	      tunnel_token_configured: Boolean(env.MIMO_TUNNEL_TOKEN),
	      proxy_health: await checkProxyHealth(env),
	      nodes,
	    });
	  }

  // ─── 网络优选配置 ───────────────────────────────────────

  if (path === "/api/network_config" && method === "GET") {
    if (isWebAuthEnabled(env) && !verifyWebAuth(request, env)) return json({ detail: "Unauthorized" }, 401);
    return json(await loadNetworkConfig(env, new URL(request.url)));
  }

  if (path === "/api/network_config" && method === "PUT") {
    if (isWebAuthEnabled(env) && !verifyWebAuth(request, env)) return json({ detail: "Unauthorized" }, 401);
    try {
      const body = await request.json() as { preferred_base_url?: string };
      await savePreferredBaseUrl(env, body.preferred_base_url || "");
      return json(await loadNetworkConfig(env, new URL(request.url)));
    } catch {
      return json({ detail: "请求体不是合法 JSON" }, 400);
    }
  }

  // ─── 端点转换配置 ───────────────────────────────────────

  if (path === "/api/endpoint_conversion" && method === "GET") {
    if (isWebAuthEnabled(env) && !verifyWebAuth(request, env)) return json({ detail: "Unauthorized" }, 401);
    return json(await loadEndpointConversionConfig(env));
  }

  if (path === "/api/endpoint_conversion" && method === "PUT") {
    if (isWebAuthEnabled(env) && !verifyWebAuth(request, env)) return json({ detail: "Unauthorized" }, 401);
    try {
      const body = await request.json() as { enabled?: boolean };
      await saveEndpointConversionEnabled(env, body.enabled === true);
      return json(await loadEndpointConversionConfig(env));
    } catch {
      return json({ detail: "请求体不是合法 JSON" }, 400);
    }
  }

  // ─── 手动注入消息 ───────────────────────────────────────

  if (path === "/api/manual_injection_messages" && method === "GET") {
    if (isWebAuthEnabled(env) && !verifyWebAuth(request, env)) return json({ detail: "Unauthorized" }, 401);

    const requestUrl = new URL(request.url);
    const userId = requestUrl.searchParams.get("user_id") || requestUrl.searchParams.get("uid") || "";
    if (!userId) return json({ detail: "缺少 user_id" }, 400);

    const store = new UserStore(env.MIMO_KV);
    const user = await store.getUser(userId);
    if (!user) return json({ detail: `账号 ${userId} 未找到` }, 404);

    const networkConfig = await loadNetworkConfig(env, requestUrl);
    const wsUrl = appendBridgeToken(networkConfig.effective_ws_url, await getBridgeToken(env.MIMO_KV));
    return json({
      user_id: user.userId,
      name: user.name || "",
      reset_message: RESET_CMD,
      bridge_message: getBridgeInjectionPrompt(
        wsUrl,
        user.userId,
        env.MIMO_TUNNEL_TOKEN || "",
        networkConfig.bridge_connect_host || ""
      ),
    });
  }

  // ─── 用户列表（含 Claw 实例状态） ──────────────────────

  if (path === "/api/users/list" && method === "GET") {
    if (isWebAuthEnabled(env) && !verifyWebAuth(request, env)) return json({ detail: "Unauthorized" }, 401);

    const store = new UserStore(env.MIMO_KV);
    const users = await store.listAllUsers();

    // 实例状态以 aistudio 控制面为准；bridge 只表示有 WS 连接。
    // 旧容器的 bridge 可能在新实例 CREATING/DESTROYED 时仍残留在线，不能反推当前实例 AVAILABLE。
    let gatewayNodes: Array<Record<string, unknown>> = [];
    try {
      const gatewayData = await gatewayStats(env);
      gatewayNodes = (gatewayData.nodes as Array<Record<string, unknown>> | undefined) || [];
    } catch {}
    const bridgeByUserId = new Map<string, Record<string, unknown>>();
    for (const node of gatewayNodes) {
      const uid = String(node.userId || "");
      if (uid) bridgeByUserId.set(uid, node);
    }

    const results = await Promise.all(
      users.map(async (u) => {
        const manager = new ClawManager(u, getControlProxyUrl(env), getGatewayEgressFetch(env));
        const status = await manager.getStatus();
        const lifecycleState = await getLifecycleState(env.MIMO_KV, u.userId);
        const bridgeNode = bridgeByUserId.get(u.userId);
        let clawStatus = status.status || "UNKNOWN";
        let remainSec = 0;
        let statusNote = "";
        let statusSource = status.status ? "aistudio" : "unknown";
        let remainEstimated = false;

        const statusIsAvailable = status.status === "AVAILABLE";
        const lifecycleIsRunning = lifecycleState.phase === "running";

        if (statusIsAvailable && status.expireTime) {
          remainSec = Math.max(0, Math.floor((status.expireTime - Date.now()) / 1000));
        } else if (!status.status && lifecycleIsRunning && lifecycleState.clawExpireAt) {
          remainSec = Math.max(0, Math.floor((lifecycleState.clawExpireAt - Date.now()) / 1000));
          remainEstimated = true;
        } else if (!status.status && lifecycleIsRunning && bridgeNode && typeof bridgeNode.connectedAt === "number") {
          // 只有生命周期已经确认 running 时，才允许按 bridge 上线时间估算剩余时间。
          const fallbackExpireAt = Number(bridgeNode.connectedAt) + 55 * 60 * 1000;
          remainSec = Math.max(0, Math.floor((fallbackExpireAt - Date.now()) / 1000));
          remainEstimated = true;
        }

        if (!status.status) {
          clawStatus = "UNKNOWN";
          statusNote = bridgeNode
            ? (lifecycleIsRunning ? "控制面状态查询失败；按运行态 Bridge 辅助展示" : "控制面状态查询失败；Bridge 是旧连接，不代表当前实例可用")
            : "控制面状态查询失败";
        } else if (bridgeNode && status.status === "DESTROYED") {
          statusNote = "控制面已销毁；Bridge 是旧连接";
        } else if (bridgeNode && status.status !== "AVAILABLE") {
          statusNote = `实例 ${status.status}；Bridge 是旧连接，不代表当前实例可用`;
        } else if (remainEstimated) {
          statusNote = "按 Bridge 上线时间估算";
        }

        return {
          userId: u.userId,
          name: u.name,
          serviceToken: u.serviceToken,
          claw_status: clawStatus,
          remain_sec: remainSec,
          status_note: statusNote,
          status_source: statusSource,
          remain_estimated: remainEstimated,
          lifecycle_phase: lifecycleState.phase,
          current_round_start: lifecycleState.currentRoundStart || 0,
          lifecycle_last_error: lifecycleState.lastError || "",
          lifecycle_next_action_at: lifecycleState.nextActionAt || 0,
        };
      })
    );

    return json({ users: results });
  }

  // ─── 用户导入（支持 JSON + Cookie 字符串 + 批量导入） ────────

  if (path === "/api/users/add" && method === "POST") {
    if (isWebAuthEnabled(env) && !verifyWebAuth(request, env)) return json({ detail: "Unauthorized" }, 401);

    try {
      const body = await request.json() as { raw_text?: string };
      const rawText = (body.raw_text || "").trim();

      if (!rawText) {
        return json({ detail: "输入不能为空" }, 400);
      }

      let users: UserInfo[] = [];
      try {
        const jsonObj = JSON.parse(rawText);
        if (Array.isArray(jsonObj)) {
          users = jsonObj.filter((u: Record<string, unknown>) => u.userId && u.serviceToken && u.xiaomichatbot_ph);
        } else if (jsonObj && typeof jsonObj === "object") {
          if (jsonObj.userId && jsonObj.serviceToken && jsonObj.xiaomichatbot_ph) {
            users = [jsonObj as unknown as UserInfo];
          }
        }
      } catch {
        const parsed: Record<string, string> = {};
        const regex = /([a-zA-Z0-9_]+)=?"?([^;"]+)"?/g;
        let match;
        while ((match = regex.exec(rawText)) !== null) {
          parsed[match[1]] = match[2];
        }
        const uid = parsed.userId;
        const st = parsed.serviceToken;
        const ph = parsed.xiaomichatbot_ph;
        if (uid && st && ph) {
          users = [{ userId: uid, serviceToken: st, xiaomichatbot_ph: ph, name: `Imported_${uid}` }];
        }
      }

      if (users.length === 0) {
        return json({ detail: "缺少必要字段 userId, serviceToken 或 xiaomichatbot_ph" }, 400);
      }

      const store = new UserStore(env.MIMO_KV);
      const importedIds: string[] = [];
      for (const u of users) {
        const user: UserInfo = {
          userId: u.userId,
          serviceToken: u.serviceToken,
          xiaomichatbot_ph: u.xiaomichatbot_ph,
          name: u.name || `Imported_${u.userId}`,
        };
        await store.putUser(user);
        importedIds.push(user.userId);
      }

      if (importedIds.length === 1) {
        return json({ status: "ok", userId: importedIds[0] });
      }
      return json({ status: "ok", imported: importedIds.length, userIds: importedIds });
    } catch (e) {
      return json({ detail: String(e) }, 500);
    }
  }

  // ─── 删除用户 ──────────────────────────────────────────

  if (path.startsWith("/api/users/delete/") && method === "DELETE") {
    if (isWebAuthEnabled(env) && !verifyWebAuth(request, env)) return json({ detail: "Unauthorized" }, 401);

    const uid = path.slice("/api/users/delete/".length);
    const store = new UserStore(env.MIMO_KV);
    const deleted = await store.deleteUser(uid);
    if (!deleted) return json({ detail: "User not found" }, 404);

    await deleteLifecycleState(env.MIMO_KV, uid);

    return json({ status: "ok" });
  }

  // ─── 实例操作 ──────────────────────────────────────────

  if (path.startsWith("/api/users/destroy/") && method === "POST") {
    if (isWebAuthEnabled(env) && !verifyWebAuth(request, env)) return json({ detail: "Unauthorized" }, 401);

    const uid = path.slice("/api/users/destroy/".length);
    const user = await getUser(env, uid);
    if (!user) return json({ ok: false, error: `账号 ${uid} 未找到` }, 404);

    try {
      const manager = new ClawManager(user, getControlProxyUrl(env), getGatewayEgressFetch(env));
      await manager.destroy();
      return json({ ok: true, message: "销毁请求已发送" });
    } catch (e) {
      return json({ ok: false, error: `销毁请求异常: ${String(e)}` }, 500);
    }
  }

  if (path.startsWith("/api/users/create/") && method === "POST") {
    if (isWebAuthEnabled(env) && !verifyWebAuth(request, env)) return json({ detail: "Unauthorized" }, 401);

    const uid = path.slice("/api/users/create/".length);
    const user = await getUser(env, uid);
    if (!user) return json({ ok: false, error: `账号 ${uid} 未找到` }, 404);

    try {
      const manager = new ClawManager(user, getControlProxyUrl(env), getGatewayEgressFetch(env));
      const state = await getLifecycleState(env.MIMO_KV, uid);
      state.phase = "creating";
      state.currentRoundStart = Date.now();
      state.clawExpireAt = undefined;
      state.lastError = undefined;
      state.bridgeMissingSince = undefined;
      state.bridgeOnlineAt = undefined;
      state.injectionStage = undefined;
      await env.MIMO_KV.put(`lifecycle:${uid}`, JSON.stringify(state));

      const result = await manager.create();
      if (!result.ok) {
        state.phase = "error";
        state.lastError = result.error;
        await env.MIMO_KV.put(`lifecycle:${uid}`, JSON.stringify(state));
        return json({ ok: false, error: result.error });
      }
      return json({ ok: true, message: "创建请求已发送，Cron tick 会自动完成后续流程" });
    } catch (e) {
      return json({ ok: false, error: `创建请求异常: ${String(e)}` }, 500);
    }
  }

  if (path.startsWith("/api/users/rebuild/") && method === "POST") {
    if (isWebAuthEnabled(env) && !verifyWebAuth(request, env)) return json({ detail: "Unauthorized" }, 401);

    const uid = path.slice("/api/users/rebuild/".length);
    const user = await getUser(env, uid);
    if (!user) return json({ ok: false, error: `账号 ${uid} 未找到` }, 404);

    try {
      const manager = new ClawManager(user, getControlProxyUrl(env), getGatewayEgressFetch(env));
      await manager.destroy();
      // 不等待，直接触发创建请求
      const result = await manager.create();
      if (!result.ok) {
        return json({ ok: false, error: result.error });
      }

      // 重置生命周期状态，让 Cron tick 自动完成后续注入
      const state = await getLifecycleState(env.MIMO_KV, uid);
      state.phase = "creating";
      state.currentRoundStart = Date.now();
      state.clawExpireAt = undefined;
      state.lastError = undefined;
      await env.MIMO_KV.put(`lifecycle:${uid}`, JSON.stringify(state));

      return json({ ok: true, message: "重建请求已发送，Cron tick 会自动完成后续流程" });
    } catch (e) {
      return json({ ok: false, error: `重建请求异常: ${String(e)}` }, 500);
    }
  }

  // ─── 模型测试 ──────────────────────────────────────────

  if (path === "/api/test_model" && method === "POST") {
    if (isWebAuthEnabled(env) && !verifyWebAuth(request, env)) return json({ detail: "Unauthorized" }, 401);

    try {
      const body = await request.json() as { model?: string };
      const model = body.model || "mimo-v2-flash";

      const testPayload = {
        method: "POST",
        path: "/v1/chat/completions",
        body: JSON.stringify({
          model,
          messages: [{ role: "user", content: "hi" }],
          max_tokens: 32,
          stream: false,
        }),
        headers: { "Content-Type": "application/json" },
      };

      const resp = await gatewayForward(env, testPayload);
      const respText = await resp.text();

      if (resp.ok) {
        try {
          const data = JSON.parse(respText);
          const content = data.choices?.[0]?.message?.content || "(无内容)";
          return json({ ok: true, model, response: content.substring(0, 200) });
        } catch {
          return json({ ok: true, model, response: respText.substring(0, 200) });
        }
      } else {
        return json({ ok: false, model, error: `HTTP ${resp.status}: ${respText.substring(0, 200)}` });
      }
    } catch (e) {
      return json({ ok: false, error: String(e) });
    }
  }

  return null; // 未匹配
}

// ─── 辅助 ────────────────────────────────────────────────────────

function json(data: unknown, status = 200, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
}

async function getUser(env: Env, userId: string): Promise<UserInfo | null> {
  const store = new UserStore(env.MIMO_KV);
  return store.getUser(userId);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ─── WebUI HTML ──────────────────────────────────────────────────

const WEBUI_HTML = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Mimo2 控制面板</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
    <style>
        :root {
            --primary: #4F46E5;
            --primary-hover: #4338CA;
            --danger: #EF4444;
            --danger-hover: #DC2626;
            --bg-color: #F3F4F6;
            --card-bg: rgba(255, 255, 255, 0.75);
            --card-border: rgba(255, 255, 255, 0.4);
            --text-main: #111827;
            --text-muted: #6B7280;
            --shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05), 0 2px 4px -1px rgba(0, 0, 0, 0.03);
            --radius-md: 12px;
            --radius-lg: 16px;
        }

        body {
            margin: 0;
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            background: linear-gradient(135deg, #e0e7ff 0%, #f3f4f6 100%);
            color: var(--text-main);
            min-height: 100vh;
            display: flex;
            flex-direction: column;
            align-items: center;
            padding: 40px 20px;
            box-sizing: border-box;
        }

        .container {
            width: 100%;
            max-width: 900px;
            display: flex;
            flex-direction: column;
            gap: 24px;
        }

        .hidden {
            display: none !important;
        }

        .auth-shell {
            width: 100%;
            max-width: 460px;
        }

        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 8px;
            gap: 12px;
        }

        .header h1 {
            margin: 0;
            font-size: 28px;
            font-weight: 600;
            color: #1e1b4b;
        }

        .header-meta {
            display: flex;
            align-items: center;
            gap: 10px;
            flex-wrap: wrap;
            justify-content: flex-end;
        }

        .badge {
            padding: 8px 12px;
            border-radius: 999px;
            font-size: 12px;
            font-weight: 600;
            background: rgba(255, 255, 255, 0.7);
            color: #3730A3;
            border: 1px solid rgba(79, 70, 229, 0.15);
        }

        .badge.warn {
            color: #92400E;
            border-color: rgba(245, 158, 11, 0.25);
        }

        .glass-card {
            background: var(--card-bg);
            backdrop-filter: blur(12px);
            -webkit-backdrop-filter: blur(12px);
            border: 1px solid var(--card-border);
            border-radius: var(--radius-lg);
            padding: 24px;
            box-shadow: var(--shadow);
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }

        .glass-card:hover {
            box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.05), 0 4px 6px -2px rgba(0, 0, 0, 0.025);
        }

        .status-container {
            display: flex;
            align-items: center;
            gap: 16px;
        }

        .status-indicator {
            width: 14px;
            height: 14px;
            background: #10B981;
            border-radius: 50%;
            box-shadow: 0 0 10px #10B981;
            animation: pulse 2s infinite;
        }

        .status-indicator.offline {
            background: #EF4444;
            box-shadow: 0 0 10px #EF4444;
            animation: none;
        }

        .status-indicator.warning {
            background: #F59E0B;
            box-shadow: 0 0 10px #F59E0B;
            animation: none;
        }

        @keyframes pulse {
            0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }
            70% { transform: scale(1); box-shadow: 0 0 0 8px rgba(16, 185, 129, 0); }
            100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
        }

        .status-text {
            font-size: 18px;
            font-weight: 500;
        }

        .status-subtext {
            color: var(--text-muted);
            font-size: 14px;
            margin-top: 4px;
        }

        .status-hint {
            margin-top: 8px;
            color: #92400E;
            font-size: 13px;
            line-height: 1.5;
        }

        .manual-injection-card {
            margin-top: 12px;
            padding: 12px 14px;
            border: 1px solid rgba(245, 158, 11, 0.28);
            border-radius: var(--radius-md);
            background: rgba(255, 251, 235, 0.78);
            color: #78350F;
            font-size: 13px;
            line-height: 1.55;
        }

        .manual-injection-title {
            font-weight: 600;
            margin-bottom: 4px;
        }

        .login-title {
            margin: 0 0 8px 0;
            font-size: 28px;
            font-weight: 600;
            color: #1e1b4b;
        }

        .login-copy {
            margin: 0 0 24px 0;
            color: var(--text-muted);
            line-height: 1.6;
        }

        .auth-form {
            display: flex;
            flex-direction: column;
            gap: 14px;
        }

        .text-input {
            width: 100%;
            padding: 12px 14px;
            border: 1px solid rgba(99, 102, 241, 0.18);
            border-radius: 12px;
            background: rgba(255, 255, 255, 0.9);
            font-size: 14px;
            box-sizing: border-box;
            transition: border-color 0.2s ease, box-shadow 0.2s ease;
        }

        .text-input:focus {
            outline: none;
            border-color: var(--primary);
            box-shadow: 0 0 0 4px rgba(79, 70, 229, 0.08);
        }

        .flex-between {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 16px;
        }

        .btn {
            background: var(--primary);
            color: white;
            border: none;
            padding: 10px 18px;
            border-radius: var(--radius-md);
            font-size: 14px;
            font-weight: 500;
            cursor: pointer;
            transition: all 0.2s ease;
            display: inline-flex;
            align-items: center;
            gap: 6px;
        }

        .btn:hover { background: var(--primary-hover); transform: translateY(-1px); }
        .btn:active { transform: translateY(0); }

        .btn-danger { background: white; color: var(--danger); border: 1px solid #fca5a5; }
        .btn-danger:hover { background: #fef2f2; color: var(--danger-hover); border-color: #f87171; }

        .table-wrapper {
            overflow-x: auto;
            border-radius: 8px;
            border: 1px solid rgba(0,0,0,0.05);
        }

        table {
            width: 100%;
            border-collapse: collapse;
            text-align: left;
            background: rgba(255, 255, 255, 0.5);
        }

        th, td {
            padding: 14px 16px;
            border-bottom: 1px solid rgba(0,0,0,0.05);
        }

        th {
            background: rgba(243, 244, 246, 0.5);
            font-weight: 600;
            color: var(--text-muted);
            font-size: 13px;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        td {
            font-size: 14px;
            color: #374151;
        }

        .empty-state {
            text-align: center;
            padding: 40px;
            color: var(--text-muted);
            font-size: 14px;
        }

        .modal-overlay {
            position: fixed;
            top: 0; left: 0; width: 100%; height: 100%;
            background: rgba(0, 0, 0, 0.3);
            backdrop-filter: blur(4px);
            display: flex;
            align-items: center;
            justify-content: center;
            opacity: 0;
            visibility: hidden;
            transition: all 0.3s ease;
            z-index: 1000;
        }

        .modal-overlay.active {
            opacity: 1;
            visibility: visible;
        }

        .modal {
            background: white;
            width: 100%;
            max-width: 480px;
            border-radius: var(--radius-lg);
            padding: 24px;
            box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.1), 0 10px 10px -5px rgba(0, 0, 0, 0.04);
            transform: scale(0.95) translateY(10px);
            transition: all 0.3s ease;
        }

        .modal-overlay.active .modal {
            transform: scale(1) translateY(0);
        }

        .modal-title { margin: 0 0 16px 0; font-size: 20px; font-weight: 600; }
        .form-group { margin-bottom: 16px; }
        .form-label { display: block; font-size: 14px; font-weight: 500; margin-bottom: 6px; color: #374151; }
        .form-control {
            width: 100%;
            padding: 12px;
            border: 1px solid #D1D5DB;
            border-radius: 8px;
            font-family: monospace;
            font-size: 13px;
            box-sizing: border-box;
            transition: border-color 0.2s;
            resize: vertical;
            min-height: 100px;
        }
        .form-control:focus { outline: none; border-color: var(--primary); box-shadow: 0 0 0 3px rgba(79, 70, 229, 0.1); }
        .form-hint { font-size: 12px; color: var(--text-muted); margin-top: 6px; line-height: 1.4;}

        .modal-actions {
            display: flex;
            justify-content: flex-end;
            gap: 12px;
            margin-top: 24px;
        }

        .btn-outline { background: transparent; border: 1px solid #D1D5DB; color: #4B5563; }
        .btn-outline:hover { background: #F3F4F6; }

        .toast {
            position: fixed;
            bottom: 20px;
            right: 20px;
            background: white;
            border-left: 4px solid var(--primary);
            padding: 12px 20px;
            border-radius: 8px;
            box-shadow: var(--shadow);
            transform: translateX(120%);
            transition: transform 0.3s cubic-bezier(0.68, -0.55, 0.265, 1.55);
            z-index: 2000;
            font-size: 14px;
            font-weight: 500;
        }
        .toast.show { transform: translateX(0); }
        .toast.error { border-left-color: var(--danger); color: var(--danger); }
        .toast.success { border-left-color: #10B981; color: #047857; }

        @media (max-width: 720px) {
            body { padding: 24px 14px; }
            .header { align-items: flex-start; flex-direction: column; }
            .header-meta { justify-content: flex-start; }
            .glass-card, .modal { padding: 18px; }
            th, td { padding: 12px; }
        }

    </style>
</head>
<body>
    <div id="authShell" class="auth-shell hidden">
        <div class="glass-card">
            <h1 class="login-title">Mimo2api</h1>
            <p class="login-copy">这个面板现在需要登录后才能查看和管理节点账号。</p>
            <div class="auth-form">
                <div>
                    <label class="form-label" for="loginUsername">用户名</label>
                    <input id="loginUsername" class="text-input" autocomplete="username" />
                </div>
                <div>
                    <label class="form-label" for="loginPassword">密码</label>
                    <input id="loginPassword" class="text-input" type="password" autocomplete="current-password" />
                </div>
                <button class="btn" onclick="submitLogin()">登录</button>
            </div>
        </div>
    </div>

    <div id="appShell" class="container hidden">
        <div class="header">
            <h1>Mimo2api</h1>
            <div class="header-meta">
                <span id="webAuthBadge" class="badge">WebUI 未鉴权</span>
                <span id="aiAuthBadge" class="badge warn">AI 端点未鉴权</span>
                <button id="endpointConversionBtn" class="btn btn-outline" onclick="openEndpointConversionModal()">端点转换</button>
                <button id="networkConfigBtn" class="btn btn-outline" onclick="openNetworkModal()">优选连接</button>
                <button id="logoutBtn" class="btn btn-outline hidden" onclick="logout()">退出登录</button>
            </div>
        </div>

        <div class="glass-card">
            <div>
                <div class="status-container">
                    <div id="statusIndicator" class="status-indicator"></div>
                    <div>
	                        <div class="status-text">网关运行中</div>
	                        <div class="status-subtext" id="statusText">正在获取实时数据...</div>
	                        <div class="status-hint hidden" id="statusHint"></div>
	                        <div class="manual-injection-card hidden" id="manualInjectionHint">
	                            <div class="manual-injection-title">所有账号当前不可用，需要手动注入</div>
	                            <div>请在对应账号的操作菜单（…）中复制第 1 条 reset 和第 2 条 bridge 注入消息，按顺序发送到 mimoclaw 聊天页面。</div>
	                        </div>
	                    </div>
	                </div>
	            </div>
	        </div>

        <div class="glass-card">
            <div class="flex-between">
                <h2 style="margin: 0; font-size: 18px;">模型连通性测试</h2>
                <div style="display: flex; gap: 8px; align-items: center;">
                    <select id="testModelSelect" class="text-input" style="width: auto; padding: 8px 12px; font-size: 13px;">
                        <option value="mimo-v2-flash">mimo-v2-flash</option>
                        <option value="mimo-v2.5-pro">mimo-v2.5-pro</option>
                        <option value="mimo-v2.5">mimo-v2.5</option>
                        <option value="mimo-v2-pro">mimo-v2-pro</option>
                        <option value="mimo-v2-omni">mimo-v2-omni</option>
                    </select>
                    <button class="btn" id="testModelBtn" onclick="testModel()">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"></path><polyline points="22 4 12 14.01 9 11.01"></polyline></svg>
                        测试
                    </button>
                </div>
            </div>
            <div id="testResult" style="margin-top: 12px; display: none; padding: 12px 16px; border-radius: 8px; font-size: 14px; line-height: 1.6;"></div>
        </div>

        <div class="glass-card">
            <div class="flex-between">
                <h2 style="margin: 0; font-size: 18px;">Claw 运行账号管理<span style="font-size:13px;color:var(--text-muted);font-weight:400;margin-left:8px;">（首次运行需要手动注入消息，请至少保持一个账号在线）</span></h2>
                <div style="display: flex; gap: 8px;">
                    <button class="btn" id="refreshBtn" onclick="refreshAll()">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 4 23 10 17 10"></polyline><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"></path></svg>
                        刷新
                    </button>
                    <button class="btn" onclick="openModal()">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="5" x2="12" y2="19"></line><line x1="5" y1="12" x2="19" y2="12"></line></svg>
                        导入凭证
                    </button>
                </div>
            </div>

            <div class="table-wrapper">
                <table id="usersTable">
                    <thead>
                        <tr>
                            <th>用户 ID</th>
                            <th>备注名</th>
                            <th>实例状态 / 剩余时长</th>
                            <th>桥接状态</th>
                            <th>ServiceToken</th>
                            <th>操作</th>
                        </tr>
                    </thead>
                    <tbody id="usersBody">
                        <tr><td colspan="6" class="empty-state">加载中...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <div class="modal-overlay" id="networkModal">
        <div class="modal">
            <h3 class="modal-title">Cloudflare 优选连接</h3>
            <div class="form-group">
                <label class="form-label">Cloudflare 优选连接地址</label>
                <input id="preferredBaseUrlInput" class="text-input" style="width:100%;" placeholder="例如：cf-best.example.com 或 https://cf-best.example.com；留空则不使用" />
                <div class="form-hint" style="margin-top:8px;line-height:1.7;">
                    这里是 Xray 风格的 <strong>address</strong>：bridge 实际连接该优选地址，
                    但 TLS SNI / HTTP Host 仍使用 Worker 自定义域名。
                    普通浏览器/客户端不能分离 Host/SNI，建议仍直接访问 Worker 自定义域名。
                </div>
                <div id="networkConfigInfo" class="form-hint" style="margin-top:10px;">正在读取网络配置...</div>
            </div>
            <div class="modal-actions">
                <button class="btn btn-outline" onclick="closeNetworkModal()">取消</button>
                <button class="btn btn-outline" onclick="clearNetworkConfig()">清空</button>
                <button class="btn" onclick="saveNetworkConfig()">保存</button>
            </div>
        </div>
    </div>

    <div class="modal-overlay" id="endpointConversionModal">
        <div class="modal">
            <h3 class="modal-title">端点转换</h3>
            <div class="form-group">
                <label class="form-label" style="display:flex;align-items:center;gap:10px;cursor:pointer;">
                    <input id="endpointConversionEnabledInput" type="checkbox" style="width:18px;height:18px;" />
                    开启 Responses API 转换
                </label>
                <div class="form-hint" style="margin-top:10px;line-height:1.7;">
                    开启后，客户端调用 <code>/v1/responses</code> 时会由网关转换为上游 <code>/v1/chat/completions</code> 请求，
                    并把 Chat Completions 响应转换回 Responses API 格式，包含 <code>tool_calls</code> / <code>function_call</code> 转换。
                    <br>不开启时，<code>/v1/responses</code> 将按原样透传到上游。
                </div>
                <div id="endpointConversionInfo" class="form-hint" style="margin-top:10px;">正在读取配置...</div>
            </div>
            <div class="modal-actions">
                <button class="btn btn-outline" onclick="closeEndpointConversionModal()">取消</button>
                <button class="btn" onclick="saveEndpointConversionConfig()">保存</button>
            </div>
        </div>
    </div>

    <div class="modal-overlay" id="importModal">
        <div class="modal">
            <h3 class="modal-title">导入 Xiaomi 凭证</h3>
            <div class="form-group">
                <label class="form-label">Cookie / 凭证数据字符串</label>
                <textarea id="cookieInput" class="form-control" placeholder='支持 JSON 格式:
{
  "userId": "123",
  "serviceToken": "xxx",
  "xiaomichatbot_ph": "yyy"
}

或 Cookie 字符串:
serviceToken="xxx"; userId=123; xiaomichatbot_ph="yyy"

也支持 JSON 数组批量导入'></textarea>
                <div class="form-hint">支持 <strong>JSON 单条/数组</strong> 及 <strong>Cookie 字符串</strong> 格式，需至少包含 <code>userId</code>, <code>serviceToken</code> 及 <code>xiaomichatbot_ph</code>。</div>
            </div>
            <div class="modal-actions">
                <button class="btn btn-outline" onclick="closeModal()">取消</button>
                <button class="btn" onclick="submitImport()">确认导入</button>
            </div>
        </div>
    </div>

    <div id="toast" class="toast"></div>

    <script>
        let statusInterval;
        let webAuthEnabled = false;
        let webAuthReady = false;
        let preferredUsername = 'admin';

        function showToast(msg, type='success') {
            const el = document.getElementById('toast');
            el.textContent = msg;
            el.className = 'toast show ' + type;
            setTimeout(() => { el.className = 'toast ' + type; }, 3000);
        }

        function setShellVisibility(showApp) {
            document.getElementById('appShell').classList.toggle('hidden', !showApp);
            document.getElementById('authShell').classList.toggle('hidden', showApp);
        }

        function applyAuthBadges(session) {
            const webBadge = document.getElementById('webAuthBadge');
            const aiBadge = document.getElementById('aiAuthBadge');
            const logoutBtn = document.getElementById('logoutBtn');

            webBadge.textContent = session.enabled ? 'WebUI 已鉴权 \\u00b7 ' + session.username : 'WebUI 未鉴权';
            webBadge.className = 'badge' + (session.enabled ? '' : ' warn');
            aiBadge.textContent = session.ai_auth_enabled ? 'AI 端点已鉴权' : 'AI 端点未鉴权';
            aiBadge.className = 'badge' + (session.ai_auth_enabled ? '' : ' warn');
            logoutBtn.classList.toggle('hidden', !session.enabled);
        }

        async function fetchSession() {
            const res = await fetch('/api/auth/session', { credentials: 'same-origin' });
            if (!res.ok) throw new Error('获取登录状态失败');
            const session = await res.json();
            webAuthEnabled = !!session.enabled;
            preferredUsername = session.username || 'admin';
            document.getElementById('loginUsername').value = preferredUsername;
            applyAuthBadges(session);
            return session;
        }

        async function apiFetch(url, options) {
            const opts = Object.assign({ credentials: 'same-origin' }, options || {});
            const res = await fetch(url, opts);
            if (res.status === 401) {
                const wasAuthenticated = webAuthReady;
                webAuthReady = false;
                setShellVisibility(false);
                if (wasAuthenticated) document.getElementById('loginPassword').value = '';
                showToast('登录已失效，请重新登录', 'error');
                throw new Error('UNAUTHORIZED');
            }
            return res;
        }

        function startStatusPolling() {
            if (statusInterval) clearInterval(statusInterval);
            statusInterval = setInterval(function() {
                if (webAuthReady || !webAuthEnabled) fetchStatus();
            }, 3000);
        }

        let bridgeNodes = [];
        let networkConfig = null;
        let endpointConversionConfig = null;

        function shouldShowManualInjectionHint() {
            var users = lastUsersData || [];
            if (users.length === 0) return false;
            var allInstancesUnavailable = users.every(function(u) { return u.claw_status !== 'AVAILABLE'; });
            return allInstancesUnavailable;
        }

        function renderManualInjectionHint() {
            var box = document.getElementById('manualInjectionHint');
            if (!box) return;
            if (!shouldShowManualInjectionHint()) {
                box.className = 'manual-injection-card hidden';
                return;
            }
            box.className = 'manual-injection-card';
        }

        async function copyTextToClipboard(text) {
            if (navigator.clipboard && window.isSecureContext) {
                await navigator.clipboard.writeText(text);
                return;
            }
            var ta = document.createElement('textarea');
            ta.value = text;
            ta.style.position = 'fixed';
            ta.style.left = '-9999px';
            ta.style.top = '-9999px';
            document.body.appendChild(ta);
            ta.focus();
            ta.select();
            try {
                document.execCommand('copy');
            } finally {
                document.body.removeChild(ta);
            }
        }

        async function copyManualInjectionMessage(kind, userId) {
            if (!userId) {
                showToast('缺少要手动注入的账号', 'error');
                return;
            }
            try {
                const res = await apiFetch('/api/manual_injection_messages?user_id=' + encodeURIComponent(userId));
                const data = await res.json();
                if (!res.ok) {
                    showToast(data.detail || '读取手动注入消息失败', 'error');
                    return;
                }
                var text = kind === 'reset' ? data.reset_message : data.bridge_message;
                await copyTextToClipboard(text || '');
                var label = kind === 'reset' ? '第 1 条 reset 消息' : '第 2 条 bridge 消息';
                showToast('已复制' + label + '，请发送到 mimoclaw 聊天页面');
            } catch (e) {
                if (e.message !== 'UNAUTHORIZED') showToast('复制手动注入消息失败: ' + e.message, 'error');
            }
        }

        function renderNetworkConfig() {
            var input = document.getElementById('preferredBaseUrlInput');
            var info = document.getElementById('networkConfigInfo');
            if (!input || !info || !networkConfig) return;
            input.value = networkConfig.preferred_base_url || '';
            var html = 'Worker 域名 / Host/SNI: <code>' + (networkConfig.worker_base_url || networkConfig.effective_base_url || location.origin) + '</code><br>' +
                'Worker WS: <code>' + (networkConfig.worker_ws_url || networkConfig.effective_ws_url) + '</code><br>' +
                'Bridge TCP 连接地址: <code>' + (networkConfig.bridge_connect_host || '默认直连 Worker 域名') + '</code>';
            if (networkConfig.bridge_host_header) html += '<br>Bridge Host/SNI: <code>' + networkConfig.bridge_host_header + '</code>';
            if (!networkConfig.configured) html += '<br>未配置优选连接地址，bridge 直接连接 Worker 域名。';
            info.innerHTML = html;
        }

        async function fetchNetworkConfig() {
            try {
                const res = await apiFetch('/api/network_config');
                networkConfig = await res.json();
                renderNetworkConfig();
            } catch (e) {
                if (e.message !== 'UNAUTHORIZED') console.error('读取网络配置失败', e);
            }
        }

        async function openNetworkModal() {
            document.getElementById('networkModal').classList.add('active');
            await fetchNetworkConfig();
        }

        function closeNetworkModal() {
            document.getElementById('networkModal').classList.remove('active');
        }

        async function clearNetworkConfig() {
            var input = document.getElementById('preferredBaseUrlInput');
            if (input) input.value = '';
            await saveNetworkConfig();
        }

        async function saveNetworkConfig() {
            var input = document.getElementById('preferredBaseUrlInput');
            var value = (input && input.value || '').trim();
            try {
                const res = await apiFetch('/api/network_config', {
                    method: 'PUT',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ preferred_base_url: value })
                });
                const data = await res.json();
                if (!res.ok) {
                    showToast(data.detail || '保存失败', 'error');
                    return;
                }
                networkConfig = data;
                renderNetworkConfig();
                showToast(value ? '优选连接地址已保存，后续注入生效' : '已清空优选连接地址');
                closeNetworkModal();
            } catch (e) {
                if (e.message !== 'UNAUTHORIZED') showToast('保存网络配置失败', 'error');
            }
        }

        function renderEndpointConversionConfig() {
            var input = document.getElementById('endpointConversionEnabledInput');
            var info = document.getElementById('endpointConversionInfo');
            var btn = document.getElementById('endpointConversionBtn');
            if (!endpointConversionConfig) return;
            var enabled = endpointConversionConfig.enabled === true;
            if (input) input.checked = enabled;
            if (btn) btn.textContent = enabled ? '端点转换：开' : '端点转换：关';
            if (info) info.innerHTML = enabled
                ? '当前已开启：<code>/v1/responses</code> 会转换到上游 <code>/v1/chat/completions</code>。'
                : '当前未开启：所有端点按原路径透传。';
        }

        async function fetchEndpointConversionConfig() {
            try {
                const res = await apiFetch('/api/endpoint_conversion');
                endpointConversionConfig = await res.json();
                renderEndpointConversionConfig();
            } catch (e) {
                if (e.message !== 'UNAUTHORIZED') console.error('读取端点转换配置失败', e);
            }
        }

        async function openEndpointConversionModal() {
            document.getElementById('endpointConversionModal').classList.add('active');
            await fetchEndpointConversionConfig();
        }

        function closeEndpointConversionModal() {
            document.getElementById('endpointConversionModal').classList.remove('active');
        }

        async function saveEndpointConversionConfig() {
            var input = document.getElementById('endpointConversionEnabledInput');
            var enabled = !!(input && input.checked);
            try {
                const res = await apiFetch('/api/endpoint_conversion', {
                    method: 'PUT',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ enabled: enabled })
                });
                const data = await res.json();
                if (!res.ok) {
                    showToast(data.detail || '保存失败', 'error');
                    return;
                }
                endpointConversionConfig = data;
                renderEndpointConversionConfig();
                showToast(enabled ? '端点转换已开启' : '端点转换已关闭');
                closeEndpointConversionModal();
            } catch (e) {
                if (e.message !== 'UNAUTHORIZED') showToast('保存端点转换配置失败', 'error');
            }
        }

        async function fetchStatus() {
            if (webAuthEnabled && !webAuthReady) return;
            try {
                const res = await apiFetch('/api/system/status');
                const data = await res.json();
	                const indicator = document.getElementById('statusIndicator');
	                const text = document.getElementById('statusText');
	                const hint = document.getElementById('statusHint');
	
	                var activeClients = Number(data.active_clients || 0);
	                var availableClients = Number(data.available_clients || 0);
	                if (availableClients > 0) {
	                    indicator.className = 'status-indicator';
	                    text.textContent = '可调度 Bridge: ' + availableClients + ' 个（WS 连接 ' + activeClients + ' 个）';
	                } else if (activeClients > 0) {
	                    indicator.className = 'status-indicator warning';
                    text.textContent = '有 WS 连接但没有可调度 Bridge（可能未归属/未就绪），API 请求将被退回';
                } else {
	                    indicator.className = 'status-indicator offline';
	                    text.textContent = '危险！没有任何可调度 Bridge，API 请求将被退回';
	                }

		                if (hint) {
		                    var hints = [];
		                    if (activeClients === 0) {
		                        hints.push('未检测到任何 Bridge 连接。桥接状态只表示 bridge.py 是否连上 Worker /ws；如实例已可用，请检查 /tmp/bridge.log 或重新注入 bridge。');
		                    }
		                    var proxyHealth = data.proxy_health || {};
		                    var controlLabel = data.control_channel_label || proxyHealth.label || '管理通道';
		                    if (proxyHealth.status === 'reachable') {
		                        hints.push('管理通道检测：' + controlLabel + ' 可达（辅助判断，不代表每个 connector 在线）。');
		                    } else if (proxyHealth.status === 'unreachable') {
		                        hints.push('管理通道检测：' + controlLabel + ' 不可达' + (proxyHealth.http_status ? '，HTTP ' + proxyHealth.http_status : '') + '。');
		                    }
		                    if (hints.length > 0) {
		                        hint.textContent = hints.join(' ');
		                        hint.className = 'status-hint';
		                    } else {
		                        hint.textContent = '';
		                        hint.className = 'status-hint hidden';
		                    }
		                }
	
	                bridgeNodes = data.nodes || [];
                renderManualInjectionHint();
                renderUsersWithBridge();
            } catch(e) {
                if (e.message === 'UNAUTHORIZED') return;
                renderManualInjectionHint();
                document.getElementById('statusIndicator').className = 'status-indicator offline';
                document.getElementById('statusText').textContent = '后端连接失败';
            }
        }

        let lastUsersData = [];

        function renderUsersWithBridge() {
            const tbody = document.getElementById('usersBody');
            if (!lastUsersData || lastUsersData.length === 0) {
                tbody.innerHTML = '<tr><td colspan="6" class="empty-state">当前未加载任何凭证</td></tr>';
                return;
            }

            var nodesByUserId = {};
            var unassignedNodes = [];
            bridgeNodes.forEach(function(n) {
                if (n.user_id) nodesByUserId[n.user_id] = n;
                else unassignedNodes.push(n);
            });

            tbody.innerHTML = lastUsersData.map(function(u) {
                var matchedNode = nodesByUserId[u.userId];

                var bridgeHtml;
                if (matchedNode) {
                    var uptimeMin = Math.floor(matchedNode.uptime / 60);
                    var uptimeSec = matchedNode.uptime % 60;
                    var uptimeStr = uptimeMin > 0 ? uptimeMin + '分' + uptimeSec + '秒' : uptimeSec + '秒';
                    var nodeConnectedAt = Number(matchedNode.connected_at || 0);
                    var roundStart = Number(u.current_round_start || 0);
                    var staleByRound = !!(roundStart && nodeConnectedAt && nodeConnectedAt < roundStart - 5 * 60 * 1000);
                    var nodeAvailable = matchedNode.available !== false;
                    var apiEndpointPresent = matchedNode.api_endpoint_present === true || (matchedNode.meta && matchedNode.meta.apiEndpointPresent === true);
                    var currentReady = nodeAvailable && u.claw_status === 'AVAILABLE';
                    var injectingCurrent = nodeAvailable && u.lifecycle_phase === 'injecting' && !staleByRound;
                    var dotColor = currentReady ? '#10B981' : '#EF4444';
                    var textColor = currentReady ? '#047857' : '#991B1B';
                    var label = currentReady ? '在线' : '离线';
                    var note = currentReady ? (uptimeStr + ' \u00b7 ' + matchedNode.requests_served + '次') : '';


                    bridgeHtml = '<div style="display:flex;align-items:center;gap:6px;">' +
                        '<div style="width:8px;height:8px;background:' + dotColor + ';border-radius:50%;box-shadow:0 0 4px ' + dotColor + ';flex-shrink:0;"></div>' +
                        '<span style="color:' + textColor + ';font-weight:500;">' + label + '</span>' +
                        '<span style="color:var(--text-muted);font-size:12px;">' + note + '</span></div>';
                } else {
                    bridgeHtml = '<div style="display:flex;align-items:center;gap:6px;">' +
                        '<div style="width:8px;height:8px;background:#EF4444;border-radius:50%;flex-shrink:0;"></div>' +
                        '<span style="color:#991B1B;font-weight:500;">离线</span></div>';
                }

                var statusColor = u.claw_status === 'AVAILABLE' ? '#10B981' : (u.claw_status === 'EXPIRED(401)' ? '#EF4444' : '#F59E0B');
                var remainText = u.remain_sec > 0 ? '剩余: ' + Math.floor(u.remain_sec / 60) + ' 分 ' + (u.remain_sec % 60) + ' 秒' : '无可用实例';
                if (u.status_note) remainText += ' · ' + u.status_note;
                var tokenPreview = u.serviceToken ? u.serviceToken.substring(0,10) + '...' : '无效';

                return '<tr>' +
                    '<td><strong style="color:var(--primary)">' + u.userId + '</strong></td>' +
                    '<td>' + (u.name || '(未命名)') + '</td>' +
                    '<td><div><strong style="color:' + statusColor + '">' + (u.claw_status || '未知') + '</strong></div>' +
                    '<div class="countdown-text" data-remain="' + u.remain_sec + '" style="font-size:12px;color:var(--text-muted);margin-top:4px;">' + remainText + '</div></td>' +
                    '<td>' + bridgeHtml + '</td>' +
                    '<td><span style="display:inline-block;max-width:120px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:var(--text-muted);">' + tokenPreview + '</span></td>' +
                    '<td><button class="btn" onclick="toggleMenu(event,\\'' + u.userId + '\\')" style="padding:4px 10px;font-size:16px;line-height:1;">&#8230;</button></td></tr>';
            }).join('');
        }

        async function fetchUsers() {
            try {
                const res = await apiFetch('/api/users/list');
                const data = await res.json();
                lastUsersData = data.users || [];
                renderUsersWithBridge();
                renderManualInjectionHint();
            } catch(e) {
                if (e.message !== 'UNAUTHORIZED') console.error("加载用户列表失败", e);
            }
        }

        async function deleteUser(uid) {
            if(!confirm('确定要移除账号 ' + uid + ' 吗？')) return;
            try {
                const res = await apiFetch('/api/users/delete/' + uid, {method: 'DELETE'});
                if(res.ok) { showToast('删除成功'); fetchUsers(); }
                else showToast('删除失败: '+ (await res.text()), 'error');
            } catch(e) { showToast('网络请求异常', 'error'); }
        }

        function openModal() {
            document.getElementById('cookieInput').value = '';
            document.getElementById('importModal').classList.add('active');
        }

        function closeModal() {
            document.getElementById('importModal').classList.remove('active');
        }

        async function submitLogin() {
            var username = document.getElementById('loginUsername').value.trim() || preferredUsername;
            var password = document.getElementById('loginPassword').value;
            if (!password) { showToast('密码不能为空', 'error'); return; }
            try {
                const res = await fetch('/api/auth/login', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    credentials: 'same-origin', body: JSON.stringify({ username, password })
                });
                const data = await res.json();
                if (!res.ok) { showToast(data.detail || '登录失败', 'error'); return; }
                webAuthReady = true;
                document.getElementById('loginPassword').value = '';
                setShellVisibility(true);
                showToast('登录成功');
                await initializeDashboard();
            } catch (e) { showToast('登录请求失败', 'error'); }
        }

        async function logout() {
            try { await fetch('/api/auth/logout', { method: 'POST', credentials: 'same-origin' }); }
            finally { webAuthReady = false; setShellVisibility(false); document.getElementById('loginPassword').value = ''; }
        }

        async function testModel() {
            var model = document.getElementById('testModelSelect').value;
            var btn = document.getElementById('testModelBtn');
            var resultDiv = document.getElementById('testResult');
            btn.disabled = true; btn.textContent = '测试中...';
            resultDiv.style.display = 'block';
            resultDiv.style.background = 'rgba(245, 158, 11, 0.1)';
            resultDiv.style.color = '#92400E';
            resultDiv.textContent = '正在测试 ' + model + '...';
            try {
                const res = await apiFetch('/api/test_model', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ model })
                });
                const data = await res.json();
                if (data.ok) {
                    resultDiv.style.background = 'rgba(16, 185, 129, 0.1)';
                    resultDiv.style.color = '#047857';
                    resultDiv.innerHTML = '<strong>\\u2713 ' + data.model + ' 连通正常</strong><br><span style="color:var(--text-muted);">回复: ' + data.response + '</span>';
                } else {
                    resultDiv.style.background = 'rgba(239, 68, 68, 0.1)';
                    resultDiv.style.color = '#991B1B';
                    resultDiv.innerHTML = '<strong>\\u2717 ' + data.model + ' 连通失败</strong><br><span style="color:var(--text-muted);">' + data.error + '</span>';
                }
            } catch (e) {
                resultDiv.style.background = 'rgba(239, 68, 68, 0.1)';
                resultDiv.style.color = '#991B1B';
                resultDiv.innerHTML = '<strong>\\u2717 请求异常</strong><br><span style="color:var(--text-muted);">' + e.message + '</span>';
            } finally {
                btn.disabled = false;
                btn.innerHTML = '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"></path><polyline points="22 4 12 14.01 9 11.01"></polyline></svg> 测试';
            }
        }

        var _activeMenu = null;

        function toggleMenu(event, userId) {
            event.stopPropagation();
            if (_activeMenu) { _activeMenu.remove(); _activeMenu = null; }
            var btn = event.currentTarget;
            var rect = btn.getBoundingClientRect();
            var menu = document.createElement('div');
            menu.style.cssText = 'position:fixed;z-index:10000;background:white;border-radius:8px;box-shadow:0 4px 16px rgba(0,0,0,0.15);min-width:150px;overflow:hidden;';
            var items = [
                { label: '新建实例', color: '#10B981', hover: '#ECFDF5', action: function() { createInstance(userId); } },
                { label: '销毁实例', color: '#F59E0B', hover: '#FFFBEB', action: function() { destroyInstance(userId); } },
                { label: '重建实例', color: '#3B82F6', hover: '#EFF6FF', action: function() { rebuildInstance(userId); } },
                { label: '复制 reset 消息', color: '#4F46E5', hover: '#EEF2FF', action: function() { copyManualInjectionMessage('reset', userId); } },
                { label: '复制 bridge 消息', color: '#4F46E5', hover: '#EEF2FF', action: function() { copyManualInjectionMessage('bridge', userId); } },
                { label: '删除账号', color: '#EF4444', hover: '#FEF2F2', action: function() { deleteUser(userId); } },
            ];
            items.forEach(function(item, i) {
                var div = document.createElement('div');
                div.textContent = item.label;
                div.style.cssText = 'padding:10px 16px;cursor:pointer;font-size:13px;color:' + item.color + ';' + (i < items.length - 1 ? 'border-bottom:1px solid #f0f0f0;' : '');
                div.onmouseover = function() { div.style.background = item.hover; };
                div.onmouseout = function() { div.style.background = 'white'; };
                div.onclick = function() { menu.remove(); _activeMenu = null; item.action(); };
                menu.appendChild(div);
            });
            document.body.appendChild(menu);
            var menuHeight = menu.offsetHeight;
            var menuWidth = menu.offsetWidth;
            var left = rect.right - menuWidth;
            if (left < 8) left = 8;
            var spaceBelow = window.innerHeight - rect.bottom;
            if (spaceBelow < menuHeight && rect.top > menuHeight) menu.style.top = (rect.top - menuHeight - 4) + 'px';
            else menu.style.top = (rect.bottom + 4) + 'px';
            menu.style.left = left + 'px';
            _activeMenu = menu;
        }

        document.addEventListener('click', function(e) {
            if (_activeMenu && !_activeMenu.contains(e.target)) { _activeMenu.remove(); _activeMenu = null; }
        });

        async function refreshAll() {
            var btn = document.getElementById('refreshBtn');
            btn.disabled = true; btn.style.opacity = '0.6';
            try { await Promise.all([fetchStatus(), fetchUsers()]); }
            finally { btn.disabled = false; btn.style.opacity = '1'; }
        }

        async function createInstance(userId) {
            if (!confirm('确定要为账号 ' + userId + ' 新建实例吗？')) return;
            try {
                const res = await apiFetch('/api/users/create/' + userId, { method: 'POST' });
                const data = await res.json();
                alert(data.ok ? '创建成功: ' + data.message : '创建失败: ' + data.error);
                fetchUsers();
            } catch(e) { alert('请求异常: ' + e.message); }
        }

        async function destroyInstance(userId) {
            if (!confirm('确定要销毁账号 ' + userId + ' 的实例吗？')) return;
            try {
                const res = await apiFetch('/api/users/destroy/' + userId, { method: 'POST' });
                const data = await res.json();
                alert(data.ok ? '销毁成功: ' + data.message : '销毁失败: ' + data.error);
                fetchUsers();
            } catch(e) { alert('请求异常: ' + e.message); }
        }

        async function rebuildInstance(userId) {
            if (!confirm('确定要重建账号 ' + userId + ' 的实例吗？')) return;
            try {
                const res = await apiFetch('/api/users/rebuild/' + userId, { method: 'POST' });
                const data = await res.json();
                alert(data.ok ? '重建成功: ' + data.message : '重建失败: ' + data.error);
                fetchUsers();
            } catch(e) { alert('请求异常: ' + e.message); }
        }

        async function submitImport() {
            var val = document.getElementById('cookieInput').value;
            if(!val.trim()) { showToast('输入不能为空', 'error'); return; }
            try {
                const res = await apiFetch('/api/users/add', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ raw_text: val })
                });
                const data = await res.json();
                if(res.ok) {
                    if (data.imported && data.imported > 1) {
                        showToast('成功批量导入 ' + data.imported + ' 个账号');
                    } else {
                        showToast('成功导入 userId: ' + data.userId);
                    }
                    closeModal(); fetchUsers();
                }
                else showToast(data.detail || '解析缺少必要字段', 'error');
            } catch(e) { if (e.message !== 'UNAUTHORIZED') showToast('提交异常', 'error'); }
        }

        async function initializeDashboard() {
            await Promise.all([fetchStatus(), fetchUsers(), fetchNetworkConfig(), fetchEndpointConversionConfig()]);
            startStatusPolling();
        }

        async function bootstrap() {
            try {
                const session = await fetchSession();
                if (session.enabled && !session.authenticated) { setShellVisibility(false); return; }
                webAuthReady = true;
                setShellVisibility(true);
                await initializeDashboard();
            } catch (e) {
                setShellVisibility(false);
                showToast('初始化失败，请刷新重试', 'error');
            }
        }

        setInterval(function() {
            document.querySelectorAll('.countdown-text').forEach(function(el) {
                var remain = parseInt(el.getAttribute('data-remain'));
                if (remain > 0) {
                    remain--;
                    el.setAttribute('data-remain', remain);
                    el.textContent = '剩余: ' + Math.floor(remain / 60) + ' 分 ' + (remain % 60) + ' 秒';
                } else if (remain === 0 && el.textContent.includes('剩余')) {
                    el.textContent = '即将或已过期...';
                }
            });
        }, 1000);

        window.addEventListener('DOMContentLoaded', bootstrap);
    </script>
</body>
</html>`;
