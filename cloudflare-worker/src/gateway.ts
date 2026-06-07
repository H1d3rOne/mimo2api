/**
 * Gateway — mimo2api 的 WebSocket 隧道网关（无 Durable Objects 版）
 *
 * 使用模块级全局状态管理 WebSocket 连接和请求转发。
 * CF Worker 免费计划可用。
 *
 * 限制：
 * - Worker 实例可能在空闲时被回收，节点连接会丢失
 * - 没有持久化状态，重启后节点需重新连接
 */

// ─── 类型 ───────────────────────────────────────────────────────

interface NodeConnection {
  ws: WebSocket;
  userId: string;
  connectedAt: number;
  requestsServed: number;
  cooldownUntil: number;
}

interface PendingRequest {
  reqId: string;
  queue: Array<WsMessage>;
  resolveStart?: (msg: WsMessage) => void;
  resolveChunk?: (msg: WsMessage) => void;
  rejectStart?: (err: Error) => void;
  started: boolean;
  finished: boolean;
}

interface WsMessage {
  req_id: string;
  type: "start" | "chunk" | "finish" | "error";
  status?: number;
  headers?: Record<string, string>;
  body?: string;
}

interface ForwardPayload {
  method: string;
  path: string;
  body: string;
  headers?: Record<string, string>;
}

// ─── 常量 ───────────────────────────────────────────────────────

const RETRYABLE_STATUS_CODES = new Set([401, 403, 429]);
const MAX_RETRIES = 3;
const NODE_RESPONSE_TIMEOUT_MS = 15_000;
const STREAM_CHUNK_TIMEOUT_MS = 60_000;
const MAX_PENDING_QUEUES = 2000;
const NODE_401_COOLDOWN_MS = 900_000; // 15 分钟

// ─── 全局状态（模块级，同一 Worker 实例共享） ──────────────────

const nodes: Map<number, NodeConnection> = new Map();
const pendingRequests: Map<string, PendingRequest> = new Map();
const wsToNodeId: Map<WebSocket, number> = new Map();
const nodeRequestMap: Map<number, Set<string>> = new Map();
let nextNodeId = 1;
let recentErrors: Array<{ ts: number; route: string; status: number; reason: string; detail: string }> = [];

// ─── 节点连接管理 ─────────────────────────────────────────────

export function handleNodeConnect(request: Request): Response {
  const pair = new WebSocketPair();
  const [client, server] = Object.values(pair);
  const nodeId = nextNodeId++;

  server.accept();
  wsToNodeId.set(server, nodeId);

  const conn: NodeConnection = {
    ws: server,
    userId: "",
    connectedAt: Date.now(),
    requestsServed: 0,
    cooldownUntil: 0,
  };
  nodes.set(nodeId, conn);
  nodeRequestMap.set(nodeId, new Set());

  console.log(`✅ 内网节点已接入 #${nodeId}。当前在线节点数: ${nodes.size}`);

  // 监听来自节点的消息
  server.addEventListener("message", (event) => {
    try {
      const data = JSON.parse(event.data as string) as WsMessage;

      // 注册消息
      if ((data as any).type === "register") {
        conn.userId = (data as any).user_id || "";
        console.log(`📋 节点 #${nodeId} 注册为用户 ${conn.userId}`);
        return;
      }

      // 响应消息
      const reqId = data.req_id;
      const pending = pendingRequests.get(reqId);
      if (!pending) return;

      if ((data.type === "start" || data.type === "error") && pending.resolveStart) {
        pending.started = true;
        pending.resolveStart(data);
        pending.resolveStart = undefined;
      } else if (pending.resolveChunk) {
        if (data.type === "finish") pending.finished = true;
        if (data.type === "error") {
          pending.finished = true;
          pending.rejectStart?.(new Error(data.body || "节点返回错误"));
          pending.rejectStart = undefined;
        }
        pending.resolveChunk(data);
        pending.resolveChunk = undefined;
      } else {
        pending.queue.push(data);
      }
    } catch (err) {
      console.error("解析节点消息失败:", err);
    }
  });

  server.addEventListener("close", () => {
    removeNode(nodeId);
  });

  server.addEventListener("error", () => {
    removeNode(nodeId);
  });

  return new Response(null, { status: 101, webSocket: client });
}

function removeNode(nodeId: number): void {
  const conn = nodes.get(nodeId);
  if (!conn) return;

  nodes.delete(nodeId);
  wsToNodeId.delete(conn.ws);

  // 清理该节点的所有孤儿请求
  const orphanIds = nodeRequestMap.get(nodeId) || new Set();
  for (const reqId of orphanIds) {
    const pending = pendingRequests.get(reqId);
    if (pending && !pending.finished) {
      pending.rejectStart?.(new Error("节点断开连接"));
      pending.resolveChunk?.({ req_id: reqId, type: "error", body: "节点断开连接" });
    }
    pendingRequests.delete(reqId);
  }
  nodeRequestMap.delete(nodeId);

  console.log(`❌ 内网节点 #${nodeId} 断开。当前在线节点数: ${nodes.size}`);
}

// ─── 节点选择 ───────────────────────────────────────────────

function getNextNode(): NodeConnection | null {
  const now = Date.now();
  const available: NodeConnection[] = [];

  for (const conn of nodes.values()) {
    if (conn.cooldownUntil <= now) {
      available.push(conn);
    }
  }

  if (available.length === 0) return null;

  // 最少负载优先
  available.sort((a, b) => a.requestsServed - b.requestsServed);
  return available[0];
}

function getAvailableNodeCount(): number {
  const now = Date.now();
  let count = 0;
  for (const conn of nodes.values()) {
    if (conn.cooldownUntil <= now) count++;
  }
  return count;
}

function cooldownNode(nodeId: number, ms: number, reason: string): void {
  const conn = nodes.get(nodeId);
  if (!conn) return;
  conn.cooldownUntil = Date.now() + ms;
  console.log(`⛔ 节点 #${nodeId} 因 ${reason} 进入冷却 ${ms / 1000}s`);
}

// ─── 请求转发核心 ───────────────────────────────────────────

export async function handleForward(payload: ForwardPayload): Promise<Response> {
  if (nodes.size === 0) {
    return new Response("Gateway Error: 没有可用的内网节点", { status: 503 });
  }

  const maxRetries = Math.min(MAX_RETRIES, getAvailableNodeCount());
  if (maxRetries === 0) {
    return new Response("Gateway Error: 没有可用的内网节点", { status: 503 });
  }

  let isStreaming = false;
  try {
    const parsed = JSON.parse(payload.body);
    isStreaming = parsed.stream === true;
  } catch {}

  let lastStatusCode = 502;
  let lastErrorText = "Gateway Error: 所有节点请求失败";

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    const node = getNextNode();
    if (!node) break;

    const nodeId = getNodeId(node);
    const reqId = crypto.randomUUID();

    // 创建 pending 请求
    const pending = createPendingRequest(reqId, nodeId);
    if (!pending) continue;

    // 发送请求到节点
    try {
      node.ws.send(
        JSON.stringify({
          req_id: reqId,
          method: payload.method,
          path: payload.path,
          body: payload.body,
        }),
      );
      node.requestsServed++;
    } catch {
      cleanupPendingRequest(reqId, nodeId);
      removeNode(nodeId);
      continue;
    }

    // 等待首个响应
    try {
      const startMsg = await waitForStart(reqId, pending);

      if (startMsg.type === "error") {
        lastErrorText = `Gateway Error: ${startMsg.body || "节点返回错误"}`;
        cleanupPendingRequest(reqId, nodeId);
        continue;
      }

      const statusCode = startMsg.status || 200;

      // 401 冷却
      if (statusCode === 401) {
        cooldownNode(nodeId, NODE_401_COOLDOWN_MS, "401 Unauthorized");
        lastStatusCode = 401;
        lastErrorText = "Gateway Error: 节点鉴权失败 (401)，已临时跳过该节点";
      }

      // 可重试状态码
      if (RETRYABLE_STATUS_CODES.has(statusCode) || statusCode >= 500) {
        lastStatusCode = statusCode;
        cleanupPendingRequest(reqId, nodeId);
        continue;
      }

      // 成功 — 流式响应
      if (isStreaming || isSseContent(startMsg)) {
        return createStreamingResponse(reqId, pending, startMsg, nodeId, payload.path);
      }

      // 非流式 — 收集完整响应
      return await collectFullResponse(reqId, pending, startMsg, nodeId, payload.path);
    } catch (err) {
      if (err instanceof TimeoutError) {
        cooldownNode(nodeId, 30_000, "响应超时");
        lastStatusCode = 504;
        lastErrorText = "Gateway Error: 请求所有节点超时 (15s)";
      }
      cleanupPendingRequest(reqId, nodeId);
      continue;
    }
  }

  return new Response(lastErrorText, { status: lastStatusCode });
}

function createPendingRequest(reqId: string, nodeId: number): PendingRequest | null {
  if (pendingRequests.size >= MAX_PENDING_QUEUES) return null;

  const pending: PendingRequest = { reqId, queue: [], started: false, finished: false };
  pendingRequests.set(reqId, pending);
  nodeRequestMap.get(nodeId)?.add(reqId);
  return pending;
}

function cleanupPendingRequest(reqId: string, nodeId: number): void {
  pendingRequests.delete(reqId);
  nodeRequestMap.get(nodeId)?.delete(reqId);
}

function waitForStart(reqId: string, pending: PendingRequest): Promise<WsMessage> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new TimeoutError("节点响应超时"));
    }, NODE_RESPONSE_TIMEOUT_MS);

    pending.resolveStart = (msg: WsMessage) => {
      clearTimeout(timer);
      resolve(msg);
    };
    pending.rejectStart = (err: Error) => {
      clearTimeout(timer);
      reject(err);
    };

    if (pending.queue.length > 0) {
      const msg = pending.queue.shift()!;
      clearTimeout(timer);
      resolve(msg);
    }
  });
}

function isSseContent(startMsg: WsMessage): boolean {
  const ct = startMsg.headers?.["content-type"] || "";
  return ct.includes("text/event-stream");
}

function createStreamingResponse(
  reqId: string,
  pending: PendingRequest,
  startMsg: WsMessage,
  nodeId: number,
  route: string,
): Response {
  const statusCode = startMsg.status || 200;
  const contentType = startMsg.headers?.["content-type"] || "text/event-stream";
  const responseHeaders: Record<string, string> = { ...startMsg.headers };
  delete responseHeaders["content-type"];
  delete responseHeaders["content-length"];
  delete responseHeaders["transfer-encoding"];
  delete responseHeaders["content-encoding"];
  delete responseHeaders["connection"];

  let lastDataTime = Date.now();

  const stream = new ReadableStream({
    async start(controller) {
      if (startMsg.body) {
        controller.enqueue(new TextEncoder().encode(startMsg.body));
      }

      const enqueue = (data: string) => {
        lastDataTime = Date.now();
        controller.enqueue(new TextEncoder().encode(data));
      };

      while (!pending.finished) {
        try {
          const msg = await waitForNextChunk(reqId, pending);

          if (msg.type === "finish") {
            pending.finished = true;
            break;
          }
          if (msg.type === "error") {
            recordError(route, 502, "节点返回错误", msg.body || "");
            break;
          }
          if (msg.type === "chunk" && msg.body) {
            enqueue(msg.body);
          }
        } catch {
          if (Date.now() - lastDataTime > STREAM_CHUNK_TIMEOUT_MS) {
            console.warn(`⚠️ 流式超时 ${reqId.slice(0, 8)}`);
            break;
          }
        }
      }

      cleanupPendingRequest(reqId, nodeId);
      try {
        controller.close();
      } catch {}
    },
  });

  return new Response(stream, {
    status: statusCode,
    headers: { "Content-Type": contentType, ...responseHeaders },
  });
}

async function collectFullResponse(
  reqId: string,
  pending: PendingRequest,
  startMsg: WsMessage,
  nodeId: number,
  route: string,
): Promise<Response> {
  const statusCode = startMsg.status || 200;
  const contentType = startMsg.headers?.["content-type"] || "application/json";
  const responseHeaders: Record<string, string> = { ...startMsg.headers };
  delete responseHeaders["content-type"];
  delete responseHeaders["content-length"];
  delete responseHeaders["transfer-encoding"];
  delete responseHeaders["content-encoding"];
  delete responseHeaders["connection"];

  const chunks: string[] = [];
  if (startMsg.body) chunks.push(startMsg.body);

  while (!pending.finished) {
    try {
      const msg = await waitForNextChunk(reqId, pending);
      if (msg.type === "finish") {
        pending.finished = true;
        break;
      }
      if (msg.type === "error") {
        recordError(route, 502, "节点返回错误", msg.body || "");
        break;
      }
      if (msg.type === "chunk" && msg.body) {
        chunks.push(msg.body);
      }
    } catch {
      break;
    }
  }

  cleanupPendingRequest(reqId, nodeId);
  const fullBody = chunks.join("");

  if (statusCode >= 400) {
    recordError(route, statusCode, `上游返回 ${statusCode}`, fullBody.slice(0, 300));
  }

  return new Response(fullBody, {
    status: statusCode,
    headers: { "Content-Type": contentType, ...responseHeaders },
  });
}

function waitForNextChunk(reqId: string, pending: PendingRequest): Promise<WsMessage> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new TimeoutError("等待数据块超时"));
    }, STREAM_CHUNK_TIMEOUT_MS);

    pending.resolveChunk = (msg: WsMessage) => {
      clearTimeout(timer);
      resolve(msg);
    };

    if (pending.queue.length > 0) {
      const msg = pending.queue.shift()!;
      clearTimeout(timer);
      resolve(msg);
    }
  });
}

function getNodeId(conn: NodeConnection): number {
  for (const [id, c] of nodes.entries()) {
    if (c === conn) return id;
  }
  return -1;
}

// ─── 状态 API ───────────────────────────────────────────────

export function handleStats(): Response {
  const now = Date.now();
  const nodeList: Array<Record<string, unknown>> = [];

  for (const [nodeId, conn] of nodes.entries()) {
    const isAvailable = conn.cooldownUntil <= now;
    const pendingReqs = nodeRequestMap.get(nodeId)?.size || 0;
    nodeList.push({
      nodeId,
      userId: conn.userId,
      available: isAvailable,
      cooldownRemainingSeconds: isAvailable ? 0 : Math.max(0, Math.round((conn.cooldownUntil - now) / 1000)),
      pendingRequests: pendingReqs,
      requestsServed: conn.requestsServed,
      uptimeSeconds: Math.round((now - conn.connectedAt) / 1000),
    });
  }

  return new Response(
    JSON.stringify({
      activeClients: nodes.size,
      availableClients: getAvailableNodeCount(),
      pendingRequests: pendingRequests.size,
      recentErrorsCount: recentErrors.length,
      nodes: nodeList,
    }),
    { headers: { "Content-Type": "application/json" } },
  );
}

export function handleErrors(limit: number): Response {
  const errors = recentErrors.slice(-limit).reverse();
  return new Response(JSON.stringify({ count: errors.length, errors }), {
    headers: { "Content-Type": "application/json" },
  });
}

function recordError(route: string, status: number, reason: string, detail: string): void {
  recentErrors.push({
    ts: Math.floor(Date.now() / 1000),
    route,
    status,
    reason: reason.slice(0, 200),
    detail: detail.slice(0, 500),
  });
  if (recentErrors.length > 500) {
    recentErrors = recentErrors.slice(-400);
  }
}

// ─── 辅助 ───────────────────────────────────────────────────────

class TimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "TimeoutError";
  }
}
