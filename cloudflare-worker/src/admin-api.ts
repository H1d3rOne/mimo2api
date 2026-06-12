/**
 * 管理员 API 模块（无 Durable Objects 版）
 *
 * 生命周期状态通过 KV 存储和读取，不再通过 DO。
 */

import { Env, UserInfo } from "./types";
import { UserStore } from "./user-store";
import { getLifecycleState, deleteLifecycleState, tick as lifecycleTick, type LifecycleSafety } from "./lifecycle";
import { gatewayStats } from "./gateway-client";
import { loadNetworkConfig } from "./network-config";
import { getControlChannelMode, getGatewayEgressFetch, getControlProxyUrl } from "./control-channel";

// ─── WebUI 鉴权 ─────────────────────────────────────────────────

function isWebAuthEnabled(env: Env): boolean {
  return !!(env.MIMO_WEBUI_USERNAME && env.MIMO_WEBUI_PASSWORD);
}

function verifyWebAuth(request: Request, env: Env): boolean {
  if (!isWebAuthEnabled(env)) return true;

  const authHeader = request.headers.get("authorization") || "";
  if (authHeader.startsWith("Basic ")) {
    const decoded = atob(authHeader.slice(6));
    const [username, password] = decoded.split(":");
    return username === env.MIMO_WEBUI_USERNAME && password === env.MIMO_WEBUI_PASSWORD;
  }

  // 兼容 WebUI 登录后的 Cookie，便于同一套 /api/* 管理端点复用。
  const cookie = request.headers.get("cookie") || "";
  const session = cookie
    .split(";")
    .map((part) => part.trim())
    .find((part) => part.startsWith("mimo2api_session="))
    ?.slice("mimo2api_session=".length);
  if (session) {
    try {
      const decoded = atob(session);
      const [username, password] = decoded.split(":");
      return username === env.MIMO_WEBUI_USERNAME && password === env.MIMO_WEBUI_PASSWORD;
    } catch {}
  }

  return false;
}

// ─── 路由处理 ────────────────────────────────────────────────────

export async function handleAdminApi(
  request: Request,
  path: string,
  env: Env
): Promise<Response | null> {
  const method = request.method;

  // ─── 用户管理 API ───────────────────────────────────────────

  // 列出所有用户
  if (path === "/api/users" && method === "GET") {
    if (!verifyWebAuth(request, env)) return unauthorized();
    const store = new UserStore(env.MIMO_KV);
    const users = await store.listAllUsers();
    const safe = users.map((u) => ({
      userId: u.userId,
      name: u.name,
      serviceToken: maskToken(u.serviceToken),
      xiaomichatbot_ph: maskToken(u.xiaomichatbot_ph),
    }));
    return json({ count: safe.length, users: safe });
  }

  // 添加用户
  if (path === "/api/users" && method === "POST") {
    if (!verifyWebAuth(request, env)) return unauthorized();
    const body = await request.json() as Partial<UserInfo>;
    if (!body.userId || !body.serviceToken || !body.xiaomichatbot_ph) {
      return json({ error: "缺少必需字段: userId, serviceToken, xiaomichatbot_ph" }, 400);
    }
    const user: UserInfo = {
      userId: body.userId,
      name: body.name || body.userId,
      serviceToken: body.serviceToken,
      xiaomichatbot_ph: body.xiaomichatbot_ph,
    };
    const store = new UserStore(env.MIMO_KV);
    await store.putUser(user);

    return json({ ok: true, message: `用户 ${user.name} 已添加` });
  }

  // 删除用户
  if (path.startsWith("/api/users/") && method === "DELETE") {
    if (!verifyWebAuth(request, env)) return unauthorized();
    const userId = path.slice("/api/users/".length);
    const store = new UserStore(env.MIMO_KV);
    const deleted = await store.deleteUser(userId);
    if (!deleted) return json({ error: "用户不存在" }, 404);

    // 清理生命周期状态
    await deleteLifecycleState(env.MIMO_KV, userId);

    return json({ ok: true, deleted: userId });
  }

  // 批量导入用户
  if (path === "/api/users/import" && method === "POST") {
    if (!verifyWebAuth(request, env)) return unauthorized();
    const body = await request.json() as UserInfo[];
    if (!Array.isArray(body)) return json({ error: "请求体必须是用户数组" }, 400);
    const store = new UserStore(env.MIMO_KV);
    const result = await store.importUsers(body);
    return json(result);
  }

  // ─── 生命周期管理 API（KV 存储） ────────────────────────────

  // 获取所有用户的生命周期状态
  if (path === "/api/lifecycle" && method === "GET") {
    if (!verifyWebAuth(request, env)) return unauthorized();
    const store = new UserStore(env.MIMO_KV);
    const userIds = await store.listUserIds();
    const states = [];
    for (const uid of userIds) {
      const state = await getLifecycleState(env.MIMO_KV, uid);
      states.push(state);
    }
    return json({ count: states.length, states });
  }

  // 获取单个用户生命周期状态
  if (path.startsWith("/api/lifecycle/") && method === "GET") {
    if (!verifyWebAuth(request, env)) return unauthorized();
    const userId = path.slice("/api/lifecycle/".length);
    const state = await getLifecycleState(env.MIMO_KV, userId);
    return json(state);
  }

  // 强制重建所有用户
  if (path === "/api/rebuild" && method === "POST") {
    if (!verifyWebAuth(request, env)) return unauthorized();
    const store = new UserStore(env.MIMO_KV);
    const userIds = await store.listUserIds();
    const results = [];
    for (const uid of userIds) {
      // 重置状态为 destroying，下次 tick 会处理
      const state = await getLifecycleState(env.MIMO_KV, uid);
      state.phase = "destroying";
      state.lastError = undefined;
      state.bridgeMissingSince = undefined;
      state.bridgeOnlineAt = undefined;
      state.injectionStage = undefined;
      const kv = env.MIMO_KV;
      await kv.put(`lifecycle:${uid}`, JSON.stringify(state));
      results.push({ userId: uid, ok: true, message: "重建信号已发送" });
    }
    return json({ ok: true, message: "重建信号已发送", results });
  }

  // 强制重建单个用户
  if (path.startsWith("/api/rebuild/") && method === "POST") {
    if (!verifyWebAuth(request, env)) return unauthorized();
    const userId = path.slice("/api/rebuild/".length);
    const state = await getLifecycleState(env.MIMO_KV, userId);
    state.phase = "destroying";
    state.lastError = undefined;
    state.bridgeMissingSince = undefined;
    state.bridgeOnlineAt = undefined;
    state.injectionStage = undefined;
    await env.MIMO_KV.put(`lifecycle:${userId}`, JSON.stringify(state));
    return json({ ok: true, message: "重建信号已发送" });
  }

  // 手动触发一轮 tick（所有用户）
  if (path === "/api/tick" && method === "POST") {
    if (!verifyWebAuth(request, env)) return unauthorized();
    const store = new UserStore(env.MIMO_KV);
    const userIds = await store.listUserIds();
    const networkConfig = await loadNetworkConfig(env, new URL(request.url));
    const wsUrl = networkConfig.effective_ws_url;
    const results = [];
    for (const uid of userIds) {
      try {
        const safety = await buildLifecycleSafety(env);
        const result = await lifecycleTick(
          env.MIMO_KV,
          uid,
          wsUrl,
          getControlProxyUrl(env),
          env.MIMO_TUNNEL_TOKEN,
          safety,
          networkConfig.bridge_connect_host || "",
          getGatewayEgressFetch(env),
        );
        results.push({ userId: uid, ...result });
      } catch (err) {
        results.push({ userId: uid, action: "error", error: String(err) });
      }
    }
    return json({ count: results.length, results });
  }

  // 单用户 tick
  if (path.startsWith("/api/tick/") && method === "POST") {
    if (!verifyWebAuth(request, env)) return unauthorized();
    const userId = path.slice("/api/tick/".length);
    const networkConfig = await loadNetworkConfig(env, new URL(request.url));
    const wsUrl = networkConfig.effective_ws_url;
    try {
      const safety = await buildLifecycleSafety(env);
      const result = await lifecycleTick(
        env.MIMO_KV,
        userId,
        wsUrl,
        getControlProxyUrl(env),
        env.MIMO_TUNNEL_TOKEN,
        safety,
        networkConfig.bridge_connect_host || "",
        getGatewayEgressFetch(env),
      );
      return json(result);
    } catch (err) {
      return json({ action: "error", error: String(err) }, 500);
    }
  }

  return null; // 未匹配
}

async function buildLifecycleSafety(env: Env): Promise<LifecycleSafety> {
  let gatewayData: Record<string, unknown> = {};
  try {
    gatewayData = await gatewayStats(env);
  } catch {}

  const nodes = (gatewayData.nodes as Array<Record<string, unknown>> | undefined) || [];
  // lifecycle 只应把“可调度”的 bridge 当作在线 connector。
  // 401/403/超时冷却中的节点虽然 WebSocket 还连着，但继续保护它会卡住轮换。
  const activeNodes = nodes
    .map((node) => ({
      userId: String(node.userId || ""),
      connectedAt: typeof node.connectedAt === "number" ? node.connectedAt : undefined,
      uptimeSeconds: typeof node.uptimeSeconds === "number" ? node.uptimeSeconds : undefined,
      available: node.available !== false,
    }))
    .filter((node) => node.userId && node.available !== false);
  const activeNodeUserIds = activeNodes.map((node) => node.userId);
  const availableClients =
    typeof gatewayData.availableClients === "number"
      ? Number(gatewayData.availableClients)
      : activeNodes.length;

  let destroyingCount = 0;
  try {
    const store = new UserStore(env.MIMO_KV);
    const userIds = await store.listUserIds();
    for (const uid of userIds) {
      const state = await getLifecycleState(env.MIMO_KV, uid);
      if (state.phase === "destroying") destroyingCount++;
    }
  } catch {}

  return {
    activeClients: availableClients,
    activeNodeUserIds,
    activeNodes,
    destroyingCount,
    protectLastConnector: getControlChannelMode(env) === "proxy",
  };
}

// ─── 辅助 ────────────────────────────────────────────────────────

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function unauthorized(): Response {
  return new Response(JSON.stringify({ error: "Unauthorized" }), {
    status: 401,
    headers: { "Content-Type": "application/json", "WWW-Authenticate": "Basic" },
  });
}

function maskToken(token: string): string {
  if (token.length <= 8) return "****";
  return token.substring(0, 4) + "****" + token.substring(token.length - 4);
}
