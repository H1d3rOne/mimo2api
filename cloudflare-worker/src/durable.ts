/**
 * GatewayDurableObject — mimo2api 的 WebSocket 隧道网关核心
 *
 * 职责：
 * 1. 管理内网节点的 WebSocket 连接（注册、心跳、断开清理）
 * 2. 将 API 请求通过 WebSocket 转发给内网节点
 * 3. 收集节点响应（流式/非流式）并返回给客户端
 * 4. 多节点负载均衡和自动重试
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

interface Env {
  GATEWAY: DurableObjectNamespace;
  LIFECYCLE: DurableObjectNamespace;
  MIMO_KV: KVNamespace;
  MIMO_RELAY_OPENAI_KEY?: string;
  MIMO_WEBUI_USERNAME?: string;
  MIMO_WEBUI_PASSWORD?: string;
  MODEL_MAPPING_JSON?: string;
  MIMO2API_WS_URL?: string;
}

// ─── 常量 ───────────────────────────────────────────────────────

const RETRYABLE_STATUS_CODES = new Set([401, 403, 429]);
const MAX_RETRIES = 3;
const NODE_RESPONSE_TIMEOUT_MS = 15_000;
const STREAM_CHUNK_TIMEOUT_MS = 60_000;
const STREAM_KEEPALIVE_INTERVAL_MS = 25_000;
const MAX_PENDING_QUEUES = 2000;
const NODE_401_COOLDOWN_MS = 900_000; // 15 分钟

// ─── Durable Object ─────────────────────────────────────────────

export class GatewayDurableObject implements DurableObject {
  private state: DurableObjectState;
  private nodes: Map<number, NodeConnection> = new Map();
  private pendingRequests: Map<string, PendingRequest> = new Map();
  private wsToNodeId: Map<WebSocket, number> = new Map();
  private nodeRequestMap: Map<number, Set<string>> = new Map(); // nodeId -> reqIds
  private recentErrors: Array<{ ts: number; route: string; status: number; reason: string; detail: string }> = [];
  private nextNodeId = 1;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    // WebSocket 升级 — 内网节点连接
    if (request.headers.get("Upgrade") === "websocket") {
      return this.handleNodeConnect(request);
    }

    // 内部 API
    if (path === "/api/stats") return this.handleStats();
    if (path === "/api/errors") return this.handleErrors(url);

    // 请求转发
    if (path === "/forward") return this.handleForward(request);

    return new Response("Not Found", { status: 404 });
  }

  // ─── 节点连接管理 ───────────────────────────────────────────

  private handleNodeConnect(request: Request): Response {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    const nodeId = this.nextNodeId++;

    this.state.acceptWebSocket(server);
    this.wsToNodeId.set(server, nodeId);

    const conn: NodeConnection = {
      ws: server,
      userId: "",
      connectedAt: Date.now(),
      requestsServed: 0,
      cooldownUntil: 0,
    };
    this.nodes.set(nodeId, conn);
    this.nodeRequestMap.set(nodeId, new Set());

    console.log(`✅ 内网节点已接入 #${nodeId}。当前在线节点数: ${this.nodes.size}`);

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
        const pending = this.pendingRequests.get(reqId);
        if (!pending) return;

        if (data.type === "start") {
          pending.started = true;
          pending.resolveStart?.(data);
        } else if (data.type === "chunk") {
          pending.resolveChunk?.(data);
        } else if (data.type === "finish") {
          pending.finished = true;
          pending.resolveChunk?.(data);
        } else if (data.type === "error") {
          pending.finished = true;
          pending.rejectStart?.(new Error(data.body || "节点返回错误"));
          pending.resolveChunk?.(data);
        }
      } catch (err) {
        console.error("解析节点消息失败:", err);
      }
    });

    server.addEventListener("close", () => {
      this.removeNode(nodeId);
    });

    server.addEventListener("error", () => {
      this.removeNode(nodeId);
    });

    return new Response(null, { status: 101, webSocket: client });
  }

  private removeNode(nodeId: number): void {
    const conn = this.nodes.get(nodeId);
    if (!conn) return;

    this.nodes.delete(nodeId);
    this.wsToNodeId.delete(conn.ws);

    // 清理该节点的所有孤儿请求
    const orphanIds = this.nodeRequestMap.get(nodeId) || new Set();
    for (const reqId of orphanIds) {
      const pending = this.pendingRequests.get(reqId);
      if (pending && !pending.finished) {
        pending.rejectStart?.(new Error("节点断开连接"));
        pending.resolveChunk?.({ req_id: reqId, type: "error", body: "节点断开连接" });
      }
      this.pendingRequests.delete(reqId);
    }
    this.nodeRequestMap.delete(nodeId);

    console.log(`❌ 内网节点 #${nodeId} 断开。当前在线节点数: ${this.nodes.size}`);
  }

  // ─── 节点选择 ───────────────────────────────────────────────

  private getNextNode(): NodeConnection | null {
    const now = Date.now();
    const available: NodeConnection[] = [];

    for (const conn of this.nodes.values()) {
      if (conn.cooldownUntil <= now) {
        available.push(conn);
      }
    }

    if (available.length === 0) return null;

    // 最少负载优先
    available.sort((a, b) => a.requestsServed - b.requestsServed);
    return available[0];
  }

  private getAvailableNodeCount(): number {
    const now = Date.now();
    let count = 0;
    for (const conn of this.nodes.values()) {
      if (conn.cooldownUntil <= now) count++;
    }
    return count;
  }

  private cooldownNode(nodeId: number, ms: number, reason: string): void {
    const conn = this.nodes.get(nodeId);
    if (!conn) return;
    conn.cooldownUntil = Date.now() + ms;
    console.log(`⛔ 节点 #${nodeId} 因 ${reason} 进入冷却 ${ms / 1000}s`);
  }

  // ─── 请求转发核心 ───────────────────────────────────────────

  private async handleForward(request: Request): Promise<Response> {
    if (this.nodes.size === 0) {
      return new Response("Gateway Error: 没有可用的内网节点", { status: 503 });
    }

    const payload: ForwardPayload = await request.json();
    const maxRetries = Math.min(MAX_RETRIES, this.getAvailableNodeCount());
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
      const node = this.getNextNode();
      if (!node) break;

      const nodeId = this.getNodeId(node);
      const reqId = crypto.randomUUID();

      // 创建 pending 请求
      const pending = this.createPendingRequest(reqId, nodeId);
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
        this.cleanupPendingRequest(reqId, nodeId);
        this.removeNode(nodeId);
        continue;
      }

      // 等待首个响应
      try {
        const startMsg = await this.waitForStart(reqId, pending);

        if (startMsg.type === "error") {
          lastErrorText = `Gateway Error: ${startMsg.body || "节点返回错误"}`;
          this.cleanupPendingRequest(reqId, nodeId);
          continue;
        }

        const statusCode = startMsg.status || 200;

        // 401 冷却
        if (statusCode === 401) {
          this.cooldownNode(nodeId, NODE_401_COOLDOWN_MS, "401 Unauthorized");
          lastStatusCode = 401;
          lastErrorText = "Gateway Error: 节点鉴权失败 (401)，已临时跳过该节点";
        }

        // 可重试状态码
        if (RETRYABLE_STATUS_CODES.has(statusCode) || statusCode >= 500) {
          lastStatusCode = statusCode;
          this.cleanupPendingRequest(reqId, nodeId);
          continue;
        }

        // 成功 — 流式响应
        if (isStreaming || this.isSseContent(startMsg)) {
          return this.createStreamingResponse(reqId, pending, startMsg, nodeId, payload.path);
        }

        // 非流式 — 收集完整响应
        return await this.collectFullResponse(reqId, pending, startMsg, nodeId, payload.path);
      } catch (err) {
        if (err instanceof TimeoutError) {
          this.cooldownNode(nodeId, 30_000, "响应超时");
          lastStatusCode = 504;
          lastErrorText = "Gateway Error: 请求所有节点超时 (15s)";
        }
        this.cleanupPendingRequest(reqId, nodeId);
        continue;
      }
    }

    return new Response(lastErrorText, { status: lastStatusCode });
  }

  private createPendingRequest(reqId: string, nodeId: number): PendingRequest | null {
    if (this.pendingRequests.size >= MAX_PENDING_QUEUES) return null;

    const pending: PendingRequest = { reqId, queue: [], started: false, finished: false };
    this.pendingRequests.set(reqId, pending);
    this.nodeRequestMap.get(nodeId)?.add(reqId);
    return pending;
  }

  private cleanupPendingRequest(reqId: string, nodeId: number): void {
    this.pendingRequests.delete(reqId);
    this.nodeRequestMap.get(nodeId)?.delete(reqId);
  }

  private waitForStart(reqId: string, pending: PendingRequest): Promise<WsMessage> {
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

      // 检查是否已有消息在队列中
      if (pending.queue.length > 0) {
        const msg = pending.queue.shift()!;
        clearTimeout(timer);
        resolve(msg);
      }
    });
  }

  private isSseContent(startMsg: WsMessage): boolean {
    const ct = startMsg.headers?.["content-type"] || "";
    return ct.includes("text/event-stream");
  }

  private createStreamingResponse(
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

    const self = this;
    let lastDataTime = Date.now();

    const stream = new ReadableStream({
      async start(controller) {
        // 发送首条消息中可能包含的 body
        if (startMsg.body) {
          controller.enqueue(new TextEncoder().encode(startMsg.body));
        }

        const enqueue = (data: string) => {
          lastDataTime = Date.now();
          controller.enqueue(new TextEncoder().encode(data));
        };

        // 持续读取后续消息
        while (!pending.finished) {
          try {
            const msg = await self.waitForNextChunk(reqId, pending);

            if (msg.type === "finish") {
              pending.finished = true;
              break;
            }
            if (msg.type === "error") {
              self.recordError(route, 502, "节点返回错误", msg.body || "");
              break;
            }
            if (msg.type === "chunk" && msg.body) {
              enqueue(msg.body);
            }
          } catch {
            // 超时或中断
            if (Date.now() - lastDataTime > STREAM_CHUNK_TIMEOUT_MS) {
              console.warn(`⚠️ 流式超时 ${reqId.slice(0, 8)}`);
              break;
            }
          }
        }

        self.cleanupPendingRequest(reqId, nodeId);
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

  private async collectFullResponse(
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
        const msg = await this.waitForNextChunk(reqId, pending);
        if (msg.type === "finish") {
          pending.finished = true;
          break;
        }
        if (msg.type === "error") {
          this.recordError(route, 502, "节点返回错误", msg.body || "");
          break;
        }
        if (msg.type === "chunk" && msg.body) {
          chunks.push(msg.body);
        }
      } catch {
        break;
      }
    }

    this.cleanupPendingRequest(reqId, nodeId);
    const fullBody = chunks.join("");

    if (statusCode >= 400) {
      this.recordError(route, statusCode, `上游返回 ${statusCode}`, fullBody.slice(0, 300));
    }

    return new Response(fullBody, {
      status: statusCode,
      headers: { "Content-Type": contentType, ...responseHeaders },
    });
  }

  private waitForNextChunk(reqId: string, pending: PendingRequest): Promise<WsMessage> {
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

  private getNodeId(conn: NodeConnection): number {
    for (const [id, c] of this.nodes.entries()) {
      if (c === conn) return id;
    }
    return -1;
  }

  // ─── 状态 API ───────────────────────────────────────────────

  private handleStats(): Response {
    const now = Date.now();
    const nodes: Array<Record<string, unknown>> = [];

    for (const [nodeId, conn] of this.nodes.entries()) {
      const isAvailable = conn.cooldownUntil <= now;
      const pendingReqs = this.nodeRequestMap.get(nodeId)?.size || 0;
      nodes.push({
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
        activeClients: this.nodes.size,
        availableClients: this.getAvailableNodeCount(),
        pendingRequests: this.pendingRequests.size,
        recentErrorsCount: this.recentErrors.length,
        nodes,
      }),
      { headers: { "Content-Type": "application/json" } },
    );
  }

  private handleErrors(url: URL): Response {
    const limit = Math.max(1, Math.min(parseInt(url.searchParams.get("limit") || "50"), 200));
    const errors = this.recentErrors.slice(-limit).reverse();
    return new Response(JSON.stringify({ count: errors.length, errors }), {
      headers: { "Content-Type": "application/json" },
    });
  }

  private recordError(route: string, status: number, reason: string, detail: string): void {
    this.recentErrors.push({
      ts: Math.floor(Date.now() / 1000),
      route,
      status,
      reason: reason.slice(0, 200),
      detail: detail.slice(0, 500),
    });
    if (this.recentErrors.length > 500) {
      this.recentErrors = this.recentErrors.slice(-400);
    }
  }

  // ─── WebSocket 消息处理（hibernation 兼容） ────────────────

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    const nodeId = this.wsToNodeId.get(ws);
    if (!nodeId) return;

    try {
      const data = JSON.parse(typeof message === "string" ? message : new TextDecoder().decode(message)) as WsMessage;
      const conn = this.nodes.get(nodeId);
      if (!conn) return;

      // 注册消息
      if ((data as any).type === "register") {
        conn.userId = (data as any).user_id || "";
        return;
      }

      // 响应消息 -> 路由到 pending 请求
      const reqId = data.req_id;
      const pending = this.pendingRequests.get(reqId);
      if (!pending) return;

      if (data.type === "start" && pending.resolveStart) {
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
        // 没有等待中的 resolve，放入队列
        pending.queue.push(data);
      }
    } catch (err) {
      console.error("解析节点消息失败:", err);
    }
  }

  async webSocketClose(ws: WebSocket, code: number, reason: string): Promise<void> {
    const nodeId = this.wsToNodeId.get(ws);
    if (nodeId !== undefined) {
      this.removeNode(nodeId);
    }
  }

  async webSocketError(ws: WebSocket, error: unknown): Promise<void> {
    const nodeId = this.wsToNodeId.get(ws);
    if (nodeId !== undefined) {
      this.removeNode(nodeId);
    }
  }
}

// ─── 辅助 ───────────────────────────────────────────────────────

class TimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "TimeoutError";
  }
}
