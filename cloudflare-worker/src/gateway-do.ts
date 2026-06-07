/**
 * GatewayDurableObject — hibernation 版 WebSocket 网关。
 *
 * 只使用 Cloudflare 自身能力：Worker + KV + Durable Object。
 * DO 使用 WebSocket Hibernation：空闲长连接不持续消耗 duration，适合免费额度。
 */

import type { ForwardPayload } from "./gateway-client";
import type { Env } from "./types";
import { verifyBridgeToken } from "./bridge-auth";

interface NodeAttachment {
  nodeId: number;
  userId: string;
  connectedAt: number;
  requestsServed: number;
  cooldownUntil: number;
  remoteAddr?: string;
  colo?: string;
  userAgent?: string;
  meta?: Record<string, unknown>;
  authenticated?: boolean;
}

interface NodeConnection extends NodeAttachment {
  ws: WebSocket;
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

const RETRYABLE_STATUS_CODES = new Set([401, 403, 429]);
const MAX_RETRIES = 3;
const NODE_RESPONSE_TIMEOUT_MS = 15_000;
const STREAM_CHUNK_TIMEOUT_MS = 60_000;
const MAX_PENDING_QUEUES = 2000;
const NODE_AUTH_FAILURE_COOLDOWN_MS = 24 * 60 * 60 * 1000;
const ERROR_RING_MAX = 500;

export class GatewayDurableObject implements DurableObject {
  private nodes: Map<number, NodeConnection> = new Map();
  private wsToNodeId: Map<WebSocket, number> = new Map();
  private nodeRequestMap: Map<number, Set<string>> = new Map();
  private pendingRequests: Map<string, PendingRequest> = new Map();
  private recentErrors: Array<{ ts: number; route: string; status: number; reason: string; detail: string }> = [];
  private nextNodeId = 1;
  private initialized = false;

  constructor(private state: DurableObjectState, private env: Env) {
    // 每个事件最多保持 2 分钟活跃；之后仍未结束则让请求自身超时处理。
    this.state.setHibernatableWebSocketEventTimeout(120_000);
  }

  async fetch(request: Request): Promise<Response> {
    this.restoreSockets();
    const url = new URL(request.url);
    const path = url.pathname;

    if (path === "/ws" && request.headers.get("Upgrade") === "websocket") {
      return this.handleNodeConnect(request);
    }

    if (path === "/forward" && request.method === "POST") {
      const payload = (await request.json()) as ForwardPayload;
      return this.handleForward(payload);
    }

    if (path === "/api/stats" && request.method === "GET") {
      return this.handleStats();
    }

    if (path === "/api/errors" && request.method === "GET") {
      const limit = Math.max(1, Math.min(parseInt(url.searchParams.get("limit") || "50", 10), 200));
      return this.handleErrors(limit);
    }

    return new Response("Not Found", { status: 404 });
  }

  private restoreSockets(): void {
    if (this.initialized) return;
    this.initialized = true;

    for (const ws of this.state.getWebSockets()) {
      const attachment = ws.deserializeAttachment() as NodeAttachment | null;
      if (!attachment || typeof attachment.nodeId !== "number") continue;
      if (attachment.authenticated !== true) {
        try {
          ws.close(1008, "bridge auth required");
        } catch {}
        continue;
      }
      const conn: NodeConnection = { ...attachment, ws };
      this.nodes.set(conn.nodeId, conn);
      this.wsToNodeId.set(ws, conn.nodeId);
      this.nodeRequestMap.set(conn.nodeId, new Set());
      this.nextNodeId = Math.max(this.nextNodeId, conn.nodeId + 1);
    }
  }

  private saveNodeAttachment(conn: NodeConnection): void {
    const attachment: NodeAttachment = {
      nodeId: conn.nodeId,
      userId: conn.userId,
      connectedAt: conn.connectedAt,
      requestsServed: conn.requestsServed,
      cooldownUntil: conn.cooldownUntil,
      remoteAddr: conn.remoteAddr,
      colo: conn.colo,
      userAgent: conn.userAgent,
      meta: conn.meta,
      authenticated: conn.authenticated === true,
    };
    conn.ws.serializeAttachment(attachment);
  }

  private async handleNodeConnect(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const token = url.searchParams.get("token") || request.headers.get("x-mimo-bridge-token") || "";
    if (!await verifyBridgeToken(this.env.MIMO_KV, token)) {
      return new Response("Unauthorized bridge", { status: 401 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    const nodeId = this.nextNodeId++;

    const conn: NodeConnection = {
      ws: server,
      nodeId,
      userId: "",
      connectedAt: Date.now(),
      requestsServed: 0,
      cooldownUntil: 0,
      remoteAddr: request.headers.get("cf-connecting-ip") || "",
      colo: request.cf?.colo ? String(request.cf.colo) : "",
      userAgent: request.headers.get("user-agent") || "",
      meta: {},
      authenticated: true,
    };

    this.nodes.set(nodeId, conn);
    this.wsToNodeId.set(server, nodeId);
    this.nodeRequestMap.set(nodeId, new Set());
    this.saveNodeAttachment(conn);
    this.state.acceptWebSocket(server, [`node:${nodeId}`]);

    console.log(`✅ 内网节点已接入 #${nodeId}。当前在线节点数: ${this.nodes.size}`);
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    this.restoreSockets();
    const nodeId = this.wsToNodeId.get(ws);
    if (nodeId === undefined) return;

    const conn = this.nodes.get(nodeId);
    if (!conn) return;

    try {
      const data = JSON.parse(typeof message === "string" ? message : new TextDecoder().decode(message)) as any;

      if (data.type === "register") {
        conn.userId = data.user_id || "";
        conn.meta = {
          hostname: data.hostname || "",
          pid: data.pid || 0,
          startedAt: data.started_at || 0,
          bridgeMarker: data.bridge_marker || "",
          apiEndpointPresent: data.api_endpoint_present === true,
        };
        this.saveNodeAttachment(conn);

        if (!conn.userId || conn.meta.apiEndpointPresent !== true) {
          console.warn(
            `⚠️ 节点 #${nodeId} 注册信息无效，关闭连接: userId=${conn.userId || "empty"}, apiEndpointPresent=${String(conn.meta.apiEndpointPresent)}`,
          );
          try {
            ws.close(1008, "invalid bridge register");
          } catch {}
          this.removeNode(nodeId);
          return;
        }

        console.log(`📋 节点 #${nodeId} 注册为用户 ${conn.userId}`);
        return;
      }

      const pending = this.pendingRequests.get(data.req_id);
      if (!pending) return;

      if ((data.type === "start" || data.type === "error") && pending.resolveStart) {
        pending.started = true;
        pending.resolveStart(data as WsMessage);
        pending.resolveStart = undefined;
      } else if (pending.resolveChunk) {
        if (data.type === "finish") pending.finished = true;
        if (data.type === "error") {
          pending.finished = true;
          pending.rejectStart?.(new Error(data.body || "节点返回错误"));
          pending.rejectStart = undefined;
        }
        pending.resolveChunk(data as WsMessage);
        pending.resolveChunk = undefined;
      } else {
        pending.queue.push(data as WsMessage);
      }
    } catch (err) {
      console.error("解析节点消息失败:", err);
    }
  }

  async webSocketClose(ws: WebSocket): Promise<void> {
    this.restoreSockets();
    const nodeId = this.wsToNodeId.get(ws);
    if (nodeId !== undefined) this.removeNode(nodeId);
  }

  async webSocketError(ws: WebSocket): Promise<void> {
    this.restoreSockets();
    const nodeId = this.wsToNodeId.get(ws);
    if (nodeId !== undefined) this.removeNode(nodeId);
  }

  private removeNode(nodeId: number): void {
    const conn = this.nodes.get(nodeId);
    if (!conn) return;

    this.nodes.delete(nodeId);
    this.wsToNodeId.delete(conn.ws);

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

  private isDispatchableNode(conn: NodeConnection, now = Date.now()): boolean {
    return (
      conn.authenticated === true &&
      !!conn.userId &&
      conn.meta?.apiEndpointPresent === true &&
      conn.cooldownUntil <= now
    );
  }

  private getNextNode(): NodeConnection | null {
    const now = Date.now();
    const available = [...this.nodes.values()].filter((conn) => this.isDispatchableNode(conn, now));
    if (available.length === 0) return null;
    available.sort((a, b) => a.requestsServed - b.requestsServed);
    return available[0];
  }

  private getAvailableNodeCount(): number {
    const now = Date.now();
    let count = 0;
    for (const conn of this.nodes.values()) {
      if (this.isDispatchableNode(conn, now)) count++;
    }
    return count;
  }

  private cooldownNode(nodeId: number, ms: number, reason: string): void {
    const conn = this.nodes.get(nodeId);
    if (!conn) return;
    conn.cooldownUntil = Date.now() + ms;
    this.saveNodeAttachment(conn);
    console.log(`⛔ 节点 #${nodeId} 因 ${reason} 进入冷却 ${ms / 1000}s`);
  }

  private async handleForward(payload: ForwardPayload): Promise<Response> {
    if (this.nodes.size === 0) {
      return new Response("Gateway Error: 没有可用的内网节点", { status: 503 });
    }

    const maxRetries = Math.min(MAX_RETRIES, this.getAvailableNodeCount());
    if (maxRetries === 0) {
      return new Response("Gateway Error: 没有可用的内网节点", { status: 503 });
    }

    let isStreaming = false;
    try {
      isStreaming = JSON.parse(payload.body).stream === true;
    } catch {}

    let lastStatusCode = 502;
    let lastErrorText = "Gateway Error: 所有节点请求失败";

    for (let attempt = 0; attempt < maxRetries; attempt++) {
      const node = this.getNextNode();
      if (!node) break;

      const reqId = crypto.randomUUID();
      const pending = this.createPendingRequest(reqId, node.nodeId);
      if (!pending) continue;

      try {
        node.ws.send(JSON.stringify({ req_id: reqId, method: payload.method, path: payload.path, body: payload.body }));
        node.requestsServed++;
        this.saveNodeAttachment(node);
      } catch {
        this.cleanupPendingRequest(reqId, node.nodeId);
        this.removeNode(node.nodeId);
        continue;
      }

      try {
        const startMsg = await this.waitForStart(reqId, pending);

        if (startMsg.type === "error") {
          lastErrorText = `Gateway Error: ${startMsg.body || "节点返回错误"}`;
          this.cleanupPendingRequest(reqId, node.nodeId);
          continue;
        }

        const statusCode = startMsg.status || 200;
        if (statusCode === 401 || statusCode === 403) {
          const reason = statusCode === 401 ? "401 Unauthorized" : "403 Forbidden";
          this.cooldownNode(node.nodeId, NODE_AUTH_FAILURE_COOLDOWN_MS, reason);
          lastStatusCode = statusCode;
          lastErrorText =
            statusCode === 401
              ? "Gateway Error: 节点鉴权失败 (401)，已临时跳过该节点"
              : "Gateway Error: 节点无访问权限 (403)，已临时跳过该节点";
          this.recordError(
            payload.path,
            statusCode,
            reason,
            `nodeId=${node.nodeId}, userId=${node.userId || "unknown"}`,
          );
        }

        if (RETRYABLE_STATUS_CODES.has(statusCode) || statusCode >= 500) {
          lastStatusCode = statusCode;
          this.cleanupPendingRequest(reqId, node.nodeId);
          continue;
        }

        if (isStreaming || this.isSseContent(startMsg)) {
          return this.createStreamingResponse(reqId, pending, startMsg, node.nodeId, payload.path);
        }

        return await this.collectFullResponse(reqId, pending, startMsg, node.nodeId, payload.path);
      } catch (err) {
        if (err instanceof TimeoutError) {
          this.cooldownNode(node.nodeId, 30_000, "响应超时");
          lastStatusCode = 504;
          lastErrorText = "Gateway Error: 请求所有节点超时 (15s)";
        }
        this.cleanupPendingRequest(reqId, node.nodeId);
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
      const timer = setTimeout(() => reject(new TimeoutError("节点响应超时")), NODE_RESPONSE_TIMEOUT_MS);
      pending.resolveStart = (msg) => {
        clearTimeout(timer);
        resolve(msg);
      };
      pending.rejectStart = (err) => {
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

  private waitForNextChunk(reqId: string, pending: PendingRequest): Promise<WsMessage> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new TimeoutError("等待数据块超时")), STREAM_CHUNK_TIMEOUT_MS);
      pending.resolveChunk = (msg) => {
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

  private isSseContent(startMsg: WsMessage): boolean {
    const ct = startMsg.headers?.["content-type"] || startMsg.headers?.["Content-Type"] || "";
    return ct.includes("text/event-stream");
  }

  private createStreamingResponse(reqId: string, pending: PendingRequest, startMsg: WsMessage, nodeId: number, route: string): Response {
    const statusCode = startMsg.status || 200;
    const contentType = startMsg.headers?.["content-type"] || startMsg.headers?.["Content-Type"] || "text/event-stream";
    const responseHeaders = normalizeHeaders(startMsg.headers);
    let lastDataTime = Date.now();

    const stream = new ReadableStream({
      start: async (controller) => {
        if (startMsg.body) controller.enqueue(new TextEncoder().encode(startMsg.body));

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
              lastDataTime = Date.now();
              controller.enqueue(new TextEncoder().encode(msg.body));
            }
          } catch {
            if (Date.now() - lastDataTime > STREAM_CHUNK_TIMEOUT_MS) break;
          }
        }

        this.cleanupPendingRequest(reqId, nodeId);
        try {
          controller.close();
        } catch {}
      },
      cancel: () => this.cleanupPendingRequest(reqId, nodeId),
    });

    return new Response(stream, { status: statusCode, headers: { "Content-Type": contentType, ...responseHeaders } });
  }

  private async collectFullResponse(reqId: string, pending: PendingRequest, startMsg: WsMessage, nodeId: number, route: string): Promise<Response> {
    const statusCode = startMsg.status || 200;
    const contentType = startMsg.headers?.["content-type"] || startMsg.headers?.["Content-Type"] || "application/json";
    const responseHeaders = normalizeHeaders(startMsg.headers);
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
        if (msg.type === "chunk" && msg.body) chunks.push(msg.body);
      } catch {
        break;
      }
    }

    this.cleanupPendingRequest(reqId, nodeId);
    const fullBody = chunks.join("");
    if (statusCode >= 400) this.recordError(route, statusCode, `上游返回 ${statusCode}`, fullBody.slice(0, 300));

    return new Response(fullBody, { status: statusCode, headers: { "Content-Type": contentType, ...responseHeaders } });
  }

  private handleStats(): Response {
    const now = Date.now();
    const nodes = [...this.nodes.values()].map((conn) => {
      const dispatchable = this.isDispatchableNode(conn, now);
      const cooldownRemainingSeconds = conn.cooldownUntil > now ? Math.max(0, Math.round((conn.cooldownUntil - now) / 1000)) : 0;
      return {
        nodeId: conn.nodeId,
        userId: conn.userId,
        available: dispatchable,
        authenticated: conn.authenticated === true,
        registered: Boolean(conn.userId),
        apiEndpointPresent: conn.meta?.apiEndpointPresent === true,
        connectedAt: conn.connectedAt,
        remoteAddr: conn.remoteAddr || "",
        colo: conn.colo || "",
        userAgent: conn.userAgent || "",
        meta: conn.meta || {},
        cooldownRemainingSeconds,
        pendingRequests: this.nodeRequestMap.get(conn.nodeId)?.size || 0,
        requestsServed: conn.requestsServed,
        uptimeSeconds: Math.round((now - conn.connectedAt) / 1000),
      };
    });

    return json({
      activeClients: this.nodes.size,
      availableClients: this.getAvailableNodeCount(),
      pendingRequests: this.pendingRequests.size,
      recentErrorsCount: this.recentErrors.length,
      nodes,
      websocketMode: "hibernation",
    });
  }

  private handleErrors(limit: number): Response {
    const errors = this.recentErrors.slice(-limit).reverse();
    return json({ count: errors.length, errors });
  }

  private recordError(route: string, status: number, reason: string, detail: string): void {
    this.recentErrors.push({ ts: Math.floor(Date.now() / 1000), route, status, reason: reason.slice(0, 200), detail: detail.slice(0, 500) });
    if (this.recentErrors.length > ERROR_RING_MAX) this.recentErrors = this.recentErrors.slice(-400);
  }
}

function normalizeHeaders(headers: Record<string, string> | undefined): Record<string, string> {
  const responseHeaders = { ...(headers || {}) };
  for (const key of ["content-type", "Content-Type", "content-length", "Content-Length", "transfer-encoding", "Transfer-Encoding", "content-encoding", "Content-Encoding", "connection", "Connection"]) {
    delete responseHeaders[key];
  }
  return responseHeaders;
}

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), { status, headers: { "Content-Type": "application/json" } });
}

class TimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "TimeoutError";
  }
}
