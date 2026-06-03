/**
 * mimo2api Cloudflare Worker Gateway
 *
 * 基于 WebSocket 隧道的 API 网关，将请求转发到内网节点
 */

import { DurableObject } from 'cloudflare:workers';

export interface Env {
  GATEWAY_STATE: DurableObjectNamespace;
  AI_AUTH_KEY?: string;
}

// 模型列表
const MODELS = [
  { id: 'mimo-v2.5-pro', display_name: 'MiMo V2.5 Pro', context_length: 1048576, max_tokens: 131072 },
  { id: 'mimo-v2.5', display_name: 'MiMo V2.5', context_length: 1048576, max_tokens: 131072 },
  { id: 'mimo-v2.5-tts', display_name: 'MiMo V2.5 TTS', context_length: 8192, max_tokens: 8192 },
  { id: 'mimo-v2-pro', display_name: 'MiMo V2 Pro', context_length: 1048576, max_tokens: 131072 },
  { id: 'mimo-v2-flash', display_name: 'MiMo V2 Flash', context_length: 256000, max_tokens: 131072 },
  { id: 'mimo-v2-omni', display_name: 'MiMo V2 Omni', context_length: 256000, max_tokens: 131072 },
  { id: 'mimo-v2.5-tts-voicedesign', display_name: 'MiMo V2.5 TTS VoiceDesign', context_length: 8192, max_tokens: 8192 },
  { id: 'mimo-v2.5-tts-voiceclone', display_name: 'MiMo V2.5 TTS VoiceClone', context_length: 8192, max_tokens: 8192 },
  { id: 'mimo-v2-tts', display_name: 'MiMo V2 TTS', context_length: 8192, max_tokens: 8192 },
];

// AI 路由前缀
const AI_ROUTE_PREFIXES = ['/v1/', '/anthropic/v1/'];

function isAiRoute(path: string): boolean {
  return AI_ROUTE_PREFIXES.some(prefix => path.startsWith(prefix));
}

// 生成请求 ID
function generateReqId(): string {
  return crypto.randomUUID();
}

export class GatewayState extends DurableObject<Env> {
  private nodes: Map<string, WebSocket> = new Map();
  private nodeInfo: Map<string, { addr: string; connectedAt: number; requestsServed: number; userId?: string }> = new Map();
  private pendingRequests: Map<string, { resolve: (value: any) => void; reject: (reason: any) => void; queue: any[] }> = new Map();
  private clientCooldowns: Map<string, number> = new Map();

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    // WebSocket 升级 - 内网节点连接
    if (request.headers.get('Upgrade') === 'websocket') {
      return this.handleNodeConnection(request);
    }

    // 内部 API - 获取状态
    if (url.pathname === '/internal/stats') {
      return Response.json(this.getStats());
    }

    // 内部 API - 转发请求
    if (url.pathname === '/internal/forward') {
      return this.handleForward(request);
    }

    return new Response('Not Found', { status: 404 });
  }

  private async handleNodeConnection(request: Request): Promise<Response> {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    server.accept();
    const nodeId = generateReqId();
    const addr = request.headers.get('CF-Connecting-IP') || 'unknown';

    this.nodes.set(nodeId, server);
    this.nodeInfo.set(nodeId, {
      addr,
      connectedAt: Date.now(),
      requestsServed: 0,
    });

    console.log(`✅ 内网节点已接入: ${addr} (nodeId: ${nodeId.slice(0, 8)})`);

    server.addEventListener('message', (event: MessageEvent) => {
      try {
        const data = JSON.parse(event.data as string);

        // 处理注册消息
        if (data.type === 'register') {
          const info = this.nodeInfo.get(nodeId);
          if (info) {
            info.userId = data.user_id;
            console.log(`📋 节点 ${addr} 注册为用户 ${data.user_id}`);
          }
          return;
        }

        // 处理请求响应
        const reqId = data.req_id;
        const pending = this.pendingRequests.get(reqId);
        if (pending) {
          pending.queue.push(data);
        }
      } catch (e) {
        console.error('解析消息失败:', e);
      }
    });

    server.addEventListener('close', () => {
      this.nodes.delete(nodeId);
      this.nodeInfo.delete(nodeId);
      this.clientCooldowns.delete(nodeId);
      console.log(`❌ 内网节点断开: ${addr}`);
    });

    server.addEventListener('error', (e) => {
      console.error(`节点错误: ${addr}`, e);
    });

    return new Response(null, { status: 101, webSocket: client });
  }

  private async handleForward(request: Request): Promise<Response> {
    const { method, path, body } = await request.json();

    // 选择可用节点
    const nodeId = this.selectNode();
    if (!nodeId) {
      return new Response('Gateway Error: 没有可用的内网节点', { status: 503 });
    }

    const ws = this.nodes.get(nodeId);
    if (!ws) {
      return new Response('Gateway Error: 节点不可用', { status: 503 });
    }

    const reqId = generateReqId();
    const pending = { queue: [] as any[], resolve: () => {}, reject: () => {} };
    this.pendingRequests.set(reqId, pending);

    // 发送请求到节点
    ws.send(JSON.stringify({ req_id: reqId, method, path, body }));

    // 更新节点请求计数
    const info = this.nodeInfo.get(nodeId);
    if (info) info.requestsServed++;

    // 等待响应
    try {
      const result = await this.collectResponse(reqId, pending);
      return result;
    } finally {
      this.pendingRequests.delete(reqId);
    }
  }

  private selectNode(): string | null {
    const now = Date.now();
    const available: string[] = [];

    for (const [nodeId] of this.nodes) {
      const cooldown = this.clientCooldowns.get(nodeId) || 0;
      if (cooldown <= now) {
        available.push(nodeId);
      }
    }

    if (available.length === 0) return null;

    // 选择负载最低的节点
    available.sort((a, b) => {
      const infoA = this.nodeInfo.get(a);
      const infoB = this.nodeInfo.get(b);
      return (infoA?.requestsServed || 0) - (infoB?.requestsServed || 0);
    });

    return available[0];
  }

  private async collectResponse(reqId: string, pending: { queue: any[] }): Promise<Response> {
    // 等待第一个消息
    const firstMsg = await this.waitForMessage(pending, 15000);
    if (!firstMsg) {
      return new Response('Gateway Error: 节点响应超时', { status: 504 });
    }

    if (firstMsg.type === 'error') {
      return new Response(`Gateway Error: ${firstMsg.body}`, { status: 502 });
    }

    const status = firstMsg.status || 200;
    const headers = firstMsg.headers || {};
    const contentType = headers['content-type'] || 'application/json';

    // 收集完整响应体
    const chunks: string[] = [];
    while (true) {
      const msg = await this.waitForMessage(pending, 120000);
      if (!msg || msg.type === 'finish') break;
      if (msg.type === 'error') {
        return new Response(`Gateway Error: ${msg.body}`, { status: 502 });
      }
      if (msg.type === 'chunk') {
        chunks.push(msg.body || '');
      }
    }

    const body = chunks.join('');

    // 检查是否是流式响应
    if (contentType.includes('text/event-stream')) {
      return new Response(body, {
        status,
        headers: { 'content-type': contentType },
      });
    }

    return new Response(body, {
      status,
      headers: { 'content-type': contentType },
    });
  }

  private async waitForMessage(pending: { queue: any[] }, timeout: number): Promise<any> {
    const start = Date.now();
    while (pending.queue.length === 0 && Date.now() - start < timeout) {
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    return pending.queue.shift();
  }

  private getStats() {
    return {
      nodes: this.nodes.size,
      nodeInfo: Object.fromEntries(
        Array.from(this.nodeInfo.entries()).map(([id, info]) => [id.slice(0, 8), info])
      ),
      pendingRequests: this.pendingRequests.size,
    };
  }
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    // AI 鉴权检查
    if (isAiRoute(url.pathname) && env.AI_AUTH_KEY) {
      const authHeader = request.headers.get('Authorization');
      if (!authHeader || !authHeader.startsWith('Bearer ') || authHeader.slice(7) !== env.AI_AUTH_KEY) {
        return new Response(JSON.stringify({ error: 'Unauthorized' }), {
          status: 401,
          headers: { 'content-type': 'application/json' },
        });
      }
    }

    // 获取 Durable Object 实例
    const id = env.GATEWAY_STATE.idFromName('default');
    const stub = env.GATEWAY_STATE.get(id);

    // WebSocket 隧道端点
    if (url.pathname === '/ws') {
      return stub.fetch(request);
    }

    // 统计信息
    if (url.pathname === '/api/stats') {
      return stub.fetch(new Request('http://internal/internal/stats'));
    }

    // 模型列表
    if (url.pathname === '/v1/models') {
      const data = MODELS.map(m => ({
        id: m.id,
        object: 'model',
        created: 1700000000,
        owned_by: 'mimo',
        context_length: m.context_length,
        max_tokens: m.max_tokens,
      }));
      return Response.json({ object: 'list', data });
    }

    if (url.pathname === '/anthropic/v1/models') {
      const data = MODELS.map(m => ({
        id: m.id,
        display_name: m.display_name,
        created_at: '2025-01-01T00:00:00Z',
        type: 'model',
        max_input_tokens: m.context_length,
        max_tokens: m.max_tokens,
      }));
      return Response.json({ data, has_more: false, first_id: data[0]?.id, last_id: data[data.length - 1]?.id });
    }

    // AI API 转发
    if (url.pathname.startsWith('/v1/') || url.pathname.startsWith('/anthropic/v1/')) {
      const method = request.method;
      const path = url.pathname;
      const body = await request.text();

      const forwardRequest = new Request('http://internal/internal/forward', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ method, path, body }),
      });

      return stub.fetch(forwardRequest);
    }

    // 健康检查
    if (url.pathname === '/health') {
      return Response.json({ status: 'ok' });
    }

    // 根路径
    if (url.pathname === '/') {
      return new Response(`
<!DOCTYPE html>
<html>
<head>
  <title>mimo2api Gateway</title>
  <style>
    body { font-family: system-ui; max-width: 800px; margin: 50px auto; padding: 20px; }
    h1 { color: #333; }
    code { background: #f4f4f4; padding: 2px 6px; border-radius: 4px; }
  </style>
</head>
<body>
  <h1>🚀 mimo2api Gateway (Cloudflare Worker)</h1>
  <p>基于 WebSocket 隧道的 API 网关</p>
  <h2>端点</h2>
  <ul>
    <li><code>GET /v1/models</code> - 模型列表</li>
    <li><code>POST /v1/chat/completions</code> - Chat Completions</li>
    <li><code>POST /v1/responses</code> - Responses API</li>
    <li><code>POST /v1/audio/speech</code> - TTS</li>
    <li><code>POST /anthropic/v1/messages</code> - Anthropic Messages</li>
    <li><code>WS /ws</code> - WebSocket 隧道</li>
  </ul>
</body>
</html>
      `, { headers: { 'content-type': 'text/html' } });
    }

    return new Response('Not Found', { status: 404 });
  },
};
