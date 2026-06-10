/**
 * mimo2api Cloudflare Worker 版
 *
 * 1. Durable Object 网关 /ws — 内网节点隧道连接（bridge.py 连接入口）
 * 2. AI API 转发 — /v1/chat/completions, /v1/responses, /v1/audio/speech, /anthropic/v1/messages
 * 3. 管理员 API — 用户 CRUD、生命周期管理、手动重建
 * 4. Cron 调度 — 定时执行生命周期 tick（创建/注入/轮换 Claw 实例）
 */

import { verifyAiApiKey, extractAiApiKey, isAiAuthEnabled } from "./auth";
import {
  applyModelMapping,
  deleteModelMappingItem,
  loadModelMappingFromEnvAndKv,
  saveModelMappingToKv,
} from "./model_mapping";
import { handleAdminApi } from "./admin-api";
import { handleWebuiRoute } from "./webui";
import { UserStore } from "./user-store";
import { gatewayErrors, gatewayForward, gatewayStats, getGatewayStub } from "./gateway-client";
import { tick as lifecycleTick, getLifecycleState, type LifecycleSafety } from "./lifecycle";
import { getBridgeCode, getSetupScript } from "./claw-ws-client";
import { loadNetworkConfig } from "./network-config";
import { appendBridgeToken, getBridgeToken } from "./bridge-auth";
import { convertResponsesRequestToChat, loadEndpointConversionConfig, transformChatCompletionResponseToResponses } from "./endpoint-conversion";
import { getControlChannelMode, getGatewayEgressFetch, getControlChannelLabel, getControlProxyUrl } from "./control-channel";
import { ClawManager } from "./claw-manager";
import type { Env } from "./types";

export { GatewayDurableObject } from "./gateway-do";

// ─── 模型列表 ───────────────────────────────────────────────────

const MODELS = [
  { id: "mimo-v2.5-pro", display: "MiMo V2.5 Pro", context: 1048576, maxOutput: 131072 },
  { id: "mimo-v2.5", display: "MiMo V2.5", context: 1048576, maxOutput: 131072 },
  { id: "mimo-v2.5-tts", display: "MiMo V2.5 TTS", context: 8192, maxOutput: 8192 },
  { id: "mimo-v2-pro", display: "MiMo V2 Pro", context: 1048576, maxOutput: 131072 },
  { id: "mimo-v2-flash", display: "MiMo V2 Flash", context: 256000, maxOutput: 131072 },
  { id: "mimo-v2-omni", display: "MiMo V2 Omni", context: 256000, maxOutput: 131072 },
  { id: "mimo-v2.5-tts-voicedesign", display: "MiMo V2.5 TTS VoiceDesign", context: 8192, maxOutput: 8192 },
  { id: "mimo-v2.5-tts-voiceclone", display: "MiMo V2.5 TTS VoiceClone", context: 8192, maxOutput: 8192 },
  { id: "mimo-v2-tts", display: "MiMo V2 TTS", context: 8192, maxOutput: 8192 },
];

const AI_ROUTE_PREFIXES = ["/v1/", "/anthropic/v1/"];
const ADMIN_ROUTE_PREFIX = "/api/";

// ─── 工具函数 ───────────────────────────────────────────────────

function isAiRoute(path: string): boolean {
  return AI_ROUTE_PREFIXES.some((p) => path.startsWith(p));
}

function isAdminRoute(path: string): boolean {
  return path.startsWith(ADMIN_ROUTE_PREFIX);
}

function jsonResponse(data: unknown, status = 200, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
}

// 冷启动兜底端点：供外部（GitHub Actions）调用。
// - 鉴权：Authorization: Bearer <MIMO_COLDSTART_KEY>，常量时间比较。
// - 判断依据：aistudio 控制面的真实实例状态（status==AVAILABLE），不看 bridge/WS。
//   bridge 的 WS 可能在实例已销毁后仍残留在线（僵尸连接），不能用来判断实例可用。
//
// 两分支（兼顾风控：Worker 能确认有实例时，GitHub 完全不碰 aistudio）：
//   1) Worker 查到任意 AVAILABLE → {needColdStart:false}，不吐凭据，GitHub 直接退出。
//      覆盖绝大多数时候，避免 GitHub 频繁异地访问 aistudio 触发风控。
//   2) Worker 没查到任何 AVAILABLE（全部非 AVAILABLE，或通道断查不到）→ 吐【全部】凭据，
//      由 GitHub 直连 aistudio 再独立验证一遍真实状态，全部确认非 AVAILABLE 才 create。
//      给全部凭据是为了让 GitHub 全量复核，避免“单凭据离线但其他实例在线”的误判。
async function handleColdStart(request: Request, env: Env): Promise<Response> {
  const expected = (env.MIMO_COLDSTART_KEY || "").trim();
  if (!expected) {
    return jsonResponse({ error: "cold-start disabled: MIMO_COLDSTART_KEY not set" }, 503);
  }
  const auth = request.headers.get("authorization") || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7).trim() : "";
  if (!token || !timingSafeEqual(token, expected)) {
    return jsonResponse({ error: "unauthorized" }, 401);
  }

  const store = new UserStore(env.MIMO_KV);
  const users = await store.listAllUsers();
  const pool = users.filter((u) => u.userId && u.serviceToken && u.xiaomichatbot_ph);
  if (pool.length === 0) {
    return jsonResponse({ needColdStart: false, error: "no usable credentials in pool" }, 409);
  }

  // 逐个查 aistudio 控制面真实状态：任意一个 AVAILABLE 就视为有可用实例，无需冷启动。
  // 注意：通道断时 getStatus 会返回空 status，此时算作“未查到 AVAILABLE”，
  // 走分支 2 交给 GitHub 复核（GitHub 直连不依赖 tunnel）。
  const controlFetch = getGatewayEgressFetch(env);
  const proxyUrl = getControlProxyUrl(env);
  let availableInstances = 0;
  for (const u of pool) {
    const manager = new ClawManager(u, proxyUrl, controlFetch);
    const st = await manager.getStatus();
    if (st.status === "AVAILABLE") availableInstances++;
  }

  // 分支 1：Worker 已确认有可用实例 → 不吐凭据，GitHub 不碰 aistudio。
  if (availableInstances > 0) {
    return jsonResponse({ action: "none", availableInstances });
  }

  // 分支 2：Worker 没查到任何 AVAILABLE → 吐全部凭据，交给 GitHub 直连复核 + 决定 create。
  return jsonResponse({
    action: "verify",
    availableInstances: 0,
    credentials: pool.map((u) => ({
      userId: u.userId,
      serviceToken: u.serviceToken,
      xiaomichatbot_ph: u.xiaomichatbot_ph,
    })),
  });
}

// 常量时间字符串比较，避免对 MIMO_COLDSTART_KEY 的计时侧信道。
function timingSafeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

// ─── AI 鉴权中间件 ──────────────────────────────────────────────

function requireAiAuth(request: Request, env: Env): Response | null {
  if (!isAiAuthEnabled(env)) return null;
  const key = extractAiApiKey(request);
  if (verifyAiApiKey(key, env)) return null;
  return jsonResponse(
    { error: { message: "Unauthorized: missing or invalid API key", type: "invalid_request_error" } },
    401,
    { "WWW-Authenticate": "Bearer" },
  );
}

// ─── API 路由处理 ───────────────────────────────────────────────

async function handleModels(): Promise<Response> {
  const data = MODELS.map((m) => ({
    id: m.id,
    object: "model",
    created: 1700000000,
    owned_by: "mimo",
    context_length: m.context,
    max_tokens: m.context,
  }));
  return jsonResponse({ object: "list", data });
}

async function handleAnthropicModels(): Promise<Response> {
  const data = MODELS.map((m) => ({
    id: m.id,
    display_name: m.display,
    created_at: "2025-01-01T00:00:00Z",
    type: "model",
    max_input_tokens: m.context,
    max_tokens: m.maxOutput,
  }));
  return jsonResponse({ data, has_more: false, first_id: data[0].id, last_id: data[data.length - 1].id });
}

async function handleStatsFull(env: Env): Promise<Response> {
  const gatewayData = await gatewayStats(env);

  const store = new UserStore(env.MIMO_KV);
  const userIds = await store.listUserIds();

  const lifecycleStates = [];
  for (const uid of userIds) {
    const state = await getLifecycleState(env.MIMO_KV, uid);
    lifecycleStates.push(state);
  }

  return jsonResponse({
    ...gatewayData,
    lifecycle: { count: lifecycleStates.length, states: lifecycleStates },
  });
}

async function buildLifecycleSafety(env: Env): Promise<LifecycleSafety> {
  let gatewayData: Record<string, unknown> = {};
  try {
    gatewayData = await gatewayStats(env);
  } catch {}

  const nodes = (gatewayData.nodes as Array<Record<string, unknown>> | undefined) || [];
  // lifecycle 只应把“可调度”的 bridge 当作在线 connector。
  // 401/403/超时冷却中的节点虽然 WebSocket 还连着，但继续保护它会卡住轮换，
  // 导致已失效的 MIMO_API_KEY 节点一直不被销毁/重建。
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
    // 只有使用依赖 Claw 容器内 cloudflared 的 MIMO_PROXY_URL 时才保护最后一个 connector；
    // Gateway EGRESS / direct fetch 不需要。
    protectLastConnector: getControlChannelMode(env) === "proxy",
  };
}

async function handleErrorsApi(env: Env, url: URL): Promise<Response> {
  const limit = Math.max(1, Math.min(parseInt(url.searchParams.get("limit") || "50"), 200));
  return gatewayErrors(env, limit);
}

async function handleModelMappingGet(env: Env): Promise<Response> {
  const mapping = await loadModelMappingFromEnvAndKv(env);
  return jsonResponse(mapping);
}

async function handleModelMappingPut(request: Request, env: Env): Promise<Response> {
  try {
    const newMapping = await request.json();
    if (typeof newMapping !== "object" || newMapping === null || Array.isArray(newMapping)) {
      return jsonResponse({ error: "映射必须是 JSON 对象" }, 400);
    }
    const normalized: Record<string, string> = {};
    for (const [key, value] of Object.entries(newMapping)) {
      normalized[String(key)] = String(value);
    }
    await saveModelMappingToKv(env, normalized);
    return jsonResponse(normalized);
  } catch {
    return jsonResponse({ error: "请求体不是合法 JSON" }, 400);
  }
}

async function handleModelMappingDelete(env: Env, modelName: string): Promise<Response> {
  const deleted = await deleteModelMappingItem(env, decodeURIComponent(modelName));
  if (!deleted) {
    return jsonResponse({ error: `模型 ${decodeURIComponent(modelName)} 不在映射中` }, 404);
  }
  return jsonResponse({ ok: true, deleted: decodeURIComponent(modelName) });
}

// ─── 核心：请求转发 ─────────────────────────────────────────────

async function forwardRequest(request: Request, path: string, env: Env): Promise<Response> {
  const body = await request.text();
  let forwardPath = path;
  let forwardBody = body;
  let convertChatResponseToResponses = false;
  let responseModelFallback = "";

  if (path === "/v1/responses" && (await loadEndpointConversionConfig(env)).enabled) {
    let parsed: Record<string, any>;
    try {
      parsed = JSON.parse(body || "{}");
    } catch {
      return jsonResponse({ error: { message: "Invalid JSON body", type: "invalid_request_error" } }, 400);
    }
    const chatBody = convertResponsesRequestToChat(parsed);
    forwardPath = "/v1/chat/completions";
    forwardBody = JSON.stringify(chatBody);
    convertChatResponseToResponses = true;
    responseModelFallback = String(chatBody.model || parsed.model || "");
  }

  const mappedBody = await applyModelMapping(forwardBody, env);
  if (convertChatResponseToResponses) {
    try {
      const mapped = JSON.parse(mappedBody);
      responseModelFallback = String(mapped.model || responseModelFallback || "");
    } catch {}
  }

  const payload = {
    method: request.method,
    path: forwardPath,
    body: mappedBody,
    headers: Object.fromEntries(request.headers.entries()),
  };

  const response = await gatewayForward(env, payload);
  if (convertChatResponseToResponses) {
    return transformChatCompletionResponseToResponses(response, responseModelFallback);
  }
  return response;
}

// ─── WebSocket 升级处理 ─────────────────────────────────────────

async function handleWebSocketUpgrade(request: Request, env: Env): Promise<Response> {
  const upgradeHeader = request.headers.get("Upgrade");
  if (upgradeHeader !== "websocket") {
    return new Response("Expected WebSocket upgrade", { status: 400 });
  }

  return getGatewayStub(env).fetch(request);
}

function handleStatusHistory(url: URL): Response {
  const hours = Math.max(1, Math.min(parseInt(url.searchParams.get("hours") || "24", 10), 24 * 30));
  const now = Math.floor(Date.now() / 1000);
  return jsonResponse({
    bucket_seconds: 300,
    hours,
    generated_at: now,
    bucket_starts: [],
    components: [],
  });
}

async function buildManualInstallScript(request: Request, requestUrl: URL, env: Env): Promise<Response> {
  const userId = requestUrl.searchParams.get("user_id") || requestUrl.searchParams.get("uid") || "";
  const installToken = requestUrl.searchParams.get("token") || "";
  const tokenOk = installToken && installToken === await getBridgeToken(env.MIMO_KV);
  if (!tokenOk && isBasicWebAuthEnabled(env) && !verifyBasicWebAuth(request, env)) {
    return new Response("Unauthorized", {
      status: 401,
      headers: { "WWW-Authenticate": "Basic", "Content-Type": "text/plain; charset=utf-8" },
    });
  }

  const networkConfig = await loadNetworkConfig(env, requestUrl);
  const publicWsUrl = networkConfig.effective_ws_url;
  const wsUrl = appendBridgeToken(publicWsUrl, await getBridgeToken(env.MIMO_KV));
  const bridgeCode = getBridgeCode(wsUrl, userId, "", networkConfig.bridge_connect_host || "");
  const script = `#!/usr/bin/env bash
set -e

echo "[mimo2api] installing bridge dependencies..."
PY_BIN="$(command -v python3 || command -v python || true)"
if [ -z "$PY_BIN" ]; then
  echo "[mimo2api] ERROR: python3/python not found."
  exit 1
fi

if ! "$PY_BIN" - <<'PYCHK' >/dev/null 2>&1
import websockets, httpx
PYCHK
then
  ("$PY_BIN" -m pip install -q --break-system-packages websockets httpx || "$PY_BIN" -m pip install -q --user websockets httpx || pip3 install -q --break-system-packages websockets httpx || pip3 install -q --user websockets httpx) >/tmp/mimo2api-pip.log 2>&1 || {
  echo "[mimo2api] dependency install failed, see /tmp/mimo2api-pip.log"
  cat /tmp/mimo2api-pip.log || true
  exit 1
  }
fi

if [ -z "$MIMO_API_KEY" ] || [ -z "$MIMO_API_ENDPOINT" ]; then
  echo "[mimo2api] ERROR: MIMO_API_KEY or MIMO_API_ENDPOINT is empty."
  echo "[mimo2api] Please run this inside a live MIMO Claw container."
  exit 1
fi

echo "[mimo2api] writing /tmp/bridge.py ..."
cat > /tmp/bridge.py <<'PYEOF'
${bridgeCode}
PYEOF

echo "[mimo2api] reconciling bridge ..."
# bridge.py 内部会判断当前配置是否已运行；旧版/旧 token bridge 会被替换，cloudflared 不受影响。
nohup "$PY_BIN" -u /tmp/bridge.py >/dev/null 2>&1 &
echo "[mimo2api] bridge reconcile started, pid=$!"
echo "[mimo2api] worker_ws=${networkConfig.worker_ws_url} (token hidden)"
echo "[mimo2api] connect_host=${networkConfig.bridge_connect_host || "default"}"
echo "[mimo2api] host_sni=${networkConfig.bridge_host_header || "default"}"
echo "[mimo2api] user_id=${userId}"
`;

  return new Response(script, {
    headers: {
      "Content-Type": "text/plain; charset=utf-8",
      "Cache-Control": "no-store",
      "Content-Disposition": "inline; filename=\"install.sh\"",
    },
  });
}

function isBasicWebAuthEnabled(env: Env): boolean {
  return !!(env.MIMO_WEBUI_USERNAME && env.MIMO_WEBUI_PASSWORD);
}

function verifyBasicWebAuth(request: Request, env: Env): boolean {
  if (!env.MIMO_WEBUI_USERNAME || !env.MIMO_WEBUI_PASSWORD) return false;
  const authHeader = request.headers.get("authorization") || "";
  if (!authHeader.startsWith("Basic ")) return false;
  try {
    const decoded = atob(authHeader.slice(6));
    const index = decoded.indexOf(":");
    if (index < 0) return false;
    const username = decoded.slice(0, index);
    const password = decoded.slice(index + 1);
    return username === env.MIMO_WEBUI_USERNAME && password === env.MIMO_WEBUI_PASSWORD;
  } catch {
    return false;
  }
}

async function buildBootstrapScript(request: Request, requestUrl: URL, env: Env): Promise<Response> {
  if ((env.MIMO_TUNNEL_TOKEN || isBasicWebAuthEnabled(env)) && !verifyBasicWebAuth(request, env)) {
    return new Response("Unauthorized", {
      status: 401,
      headers: { "WWW-Authenticate": "Basic", "Content-Type": "text/plain; charset=utf-8" },
    });
  }

  const userId = requestUrl.searchParams.get("user_id") || requestUrl.searchParams.get("uid") || "";
  const networkConfig = await loadNetworkConfig(env, requestUrl);
  const wsUrl = appendBridgeToken(networkConfig.effective_ws_url, await getBridgeToken(env.MIMO_KV));
  const script = getSetupScript(wsUrl, userId, env.MIMO_TUNNEL_TOKEN || "", networkConfig.bridge_connect_host || "");
  return new Response(script, {
    headers: {
      "Content-Type": "text/plain; charset=utf-8",
      "Cache-Control": "no-store",
      "Content-Disposition": "inline; filename=\"bootstrap.sh\"",
    },
  });
}

async function buildManualInstallHelp(request: Request, requestUrl: URL, env: Env): Promise<Response> {
  if (isBasicWebAuthEnabled(env) && !verifyBasicWebAuth(request, env)) {
    return new Response("Unauthorized", {
      status: 401,
      headers: { "WWW-Authenticate": "Basic", "Content-Type": "text/plain; charset=utf-8" },
    });
  }

  const base = `${requestUrl.origin}/install`;
  const networkConfig = await loadNetworkConfig(env, requestUrl);
  const wsUrl = networkConfig.effective_ws_url;
  const authPrefix = isBasicWebAuthEnabled(env) ? `-u ${env.MIMO_WEBUI_USERNAME}:<WebUI密码> ` : "";
  const prompt = [
    "请在当前 MIMO Claw 容器里后台执行：",
    "",
    "```bash",
    `curl ${authPrefix}-fsSL '${base}?user_id=6873555017' | bash`,
    "```",
  ].join("\n");
  return jsonResponse({
    ok: true,
    mode: "manual_bridge_injection",
    ws_url: wsUrl,
    usage: [
      `curl ${authPrefix}-fsSL ${base} | bash`,
      `curl ${authPrefix}-fsSL '${base}?user_id=6873555017' | bash`,
      `curl ${authPrefix}-fsSL '${requestUrl.origin}/bootstrap.sh?user_id=6873555017' | bash`,
    ],
    prompt,
  });
}

// ─── Cron 调度处理 ──────────────────────────────────────────────

async function handleScheduled(env: Env): Promise<void> {
  console.log("[Scheduled] 执行生命周期 tick...");
  const store = new UserStore(env.MIMO_KV);
  const userIds = await store.listUserIds();
  const networkConfig = await loadNetworkConfig(env);
  const wsUrl = networkConfig.effective_ws_url;

  for (const userId of userIds) {
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
      if (result.error) {
        console.error(`[Scheduled] 用户 ${userId}: ${result.action} - ${result.error}`);
      } else {
        console.log(`[Scheduled] 用户 ${userId}: ${result.action}`);
      }
    } catch (err) {
      console.error(`[Scheduled] 用户 ${userId}: tick 失败 - ${err}`);
    }
  }
}

// ─── 主入口 ─────────────────────────────────────────────────────

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    // WebSocket 隧道连接（bridge.py 连接入口）
    if (path === "/ws" && request.headers.get("Upgrade") === "websocket") {
      return handleWebSocketUpgrade(request, env);
    }

    // 手动注入 bridge.py：用于 Worker 无法直连 aistudio 管理接口时的当前容器快速接入。
    if ((path === "/install" || path === "/install.sh") && request.method === "GET") {
      return buildManualInstallScript(request, url, env);
    }
    if (path === "/bootstrap.sh" && request.method === "GET") {
      return buildBootstrapScript(request, url, env);
    }
    if (path === "/install-help" && request.method === "GET") {
      return buildManualInstallHelp(request, url, env);
    }

    // WebUI 管理页面路由
    const webuiResult = await handleWebuiRoute(request, path, env);
    if (webuiResult) return webuiResult;

    // 管理员 API 路由
    if (isAdminRoute(path)) {
      const adminResult = await handleAdminApi(request, path, env);
      if (adminResult) return adminResult;
    }

    // AI 路由鉴权
    if (isAiRoute(path)) {
      const authError = requireAiAuth(request, env);
      if (authError) return authError;
    }

    // 模型列表
    if (path === "/v1/models" && request.method === "GET") {
      return handleModels();
    }
    if (path === "/anthropic/v1/models" && request.method === "GET") {
      return handleAnthropicModels();
    }

    // 运行状态
    if (path === "/api/stats" && request.method === "GET") {
      return handleStatsFull(env);
    }
    if (path === "/api/errors" && request.method === "GET") {
      return handleErrorsApi(env, url);
    }
    if (path === "/api/status/history" && request.method === "GET") {
      return handleStatusHistory(url);
    }

    // 模型映射管理
    if (path === "/api/model_mapping" && request.method === "GET") {
      return handleModelMappingGet(env);
    }
    if (path === "/api/model_mapping" && request.method === "PUT") {
      return handleModelMappingPut(request, env);
    }
    if (path.startsWith("/api/model_mapping/") && request.method === "DELETE") {
      return handleModelMappingDelete(env, path.slice("/api/model_mapping/".length));
    }

    // API 转发路由
    if (path === "/v1/chat/completions" && request.method === "POST") {
      return forwardRequest(request, "/v1/chat/completions", env);
    }
    if (path === "/v1/responses" && request.method === "POST") {
      return forwardRequest(request, "/v1/responses", env);
    }
    if (path === "/v1/audio/speech" && request.method === "POST") {
      return forwardRequest(request, "/v1/audio/speech", env);
    }
    if (path === "/anthropic/v1/messages" && request.method === "POST") {
      return forwardRequest(request, "/anthropic/v1/messages", env);
    }
    if (path === "/v1/messages" && request.method === "POST") {
      return forwardRequest(request, "/anthropic/v1/messages", env);
    }

    // 健康检查
    if (path === "/health") {
      const controlChannelMode = getControlChannelMode(env);
      return jsonResponse({
        status: "ok",
        timestamp: Date.now(),
        gateway: "durable_object",
        control_channel: {
          mode: controlChannelMode,
          label: getControlChannelLabel(controlChannelMode),
          proxy_url_configured: Boolean(env.MIMO_PROXY_URL),
          token_configured: Boolean(env.MIMO_TUNNEL_TOKEN),
          vpc_service_configured: Boolean(env.MIMO_AISTUDIO),
          egress_binding_configured: Boolean(env.EGRESS),
        },
        tunnel: {
          proxy_url_configured: Boolean(env.MIMO_PROXY_URL),
          token_configured: Boolean(env.MIMO_TUNNEL_TOKEN),
        },
      });
    }

    // 冷启动兜底：供外部（GitHub Actions）查询是否需要拉起实例，并按需领取一个随机凭据。
    // 仅当全部实例离线时才吐出凭据，平时不暴露任何 cookie。
    if (path === "/api/cold-start" && request.method === "GET") {
      return handleColdStart(request, env);
    }

    return new Response("Not Found", { status: 404 });
  },

  // Cron 定时触发器 — 执行生命周期 tick
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    ctx.waitUntil(handleScheduled(env));
  },
};
