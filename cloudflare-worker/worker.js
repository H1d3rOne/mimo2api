/**
 * mimo2api Cloudflare Worker Gateway
 *
 * 单文件版本，可直接复制到 Cloudflare Dashboard 运行
 *
 * 使用说明：
 * 1. 登录 Cloudflare Dashboard -> Workers & Pages
 * 2. 创建新 Worker，将此文件内容粘贴到编辑器
 * 3. 部署后获得 *.workers.dev 域名
 * 4. 内网节点连接: wss://your-worker.workers.dev/ws
 */

// ============== 配置 ==============
const AI_AUTH_KEY = ''; // 可选：设置 API 鉴权密钥

// ============== 模型列表 ==============
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

// ============== 全局状态 ==============
// 注意：Worker 是无状态的，这些变量在每个隔离中独立
// WebSocket 连接会绑定到特定隔离，适合单节点场景
let nodes = new Map(); // nodeId -> { ws, info }
let pendingRequests = new Map(); // reqId -> { queue, resolve, reject }

// ============== 工具函数 ==============
function generateId() {
  return crypto.randomUUID();
}

function isAiRoute(path) {
  return path.startsWith('/v1/') || path.startsWith('/anthropic/v1/');
}

function selectNode() {
  if (nodes.size === 0) return null;
  // 选择负载最低的节点
  let selected = null;
  let minRequests = Infinity;
  for (const [nodeId, node] of nodes) {
    const requests = node.info?.requestsServed || 0;
    if (requests < minRequests) {
      minRequests = requests;
      selected = nodeId;
    }
  }
  return selected;
}

// ============== 主处理函数 ==============
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;

    // AI 鉴权检查
    if (isAiRoute(path) && AI_AUTH_KEY) {
      const auth = request.headers.get('Authorization');
      if (!auth || !auth.startsWith('Bearer ') || auth.slice(7) !== AI_AUTH_KEY) {
        return new Response(JSON.stringify({ error: 'Unauthorized' }), {
          status: 401,
          headers: { 'content-type': 'application/json' }
        });
      }
    }

    // WebSocket 隧道端点 - 内网节点连接
    if (path === '/ws') {
      return handleWebSocket(request, ctx);
    }

    // 健康检查
    if (path === '/health') {
      return Response.json({ status: 'ok', nodes: nodes.size });
    }

    // 统计信息
    if (path === '/api/stats') {
      const nodeStats = [];
      for (const [nodeId, node] of nodes) {
        nodeStats.push({
          id: nodeId.slice(0, 8),
          addr: node.info?.addr || 'unknown',
          requestsServed: node.info?.requestsServed || 0,
          connectedAt: node.info?.connectedAt,
        });
      }
      return Response.json({
        nodes: nodes.size,
        pendingRequests: pendingRequests.size,
        nodeDetails: nodeStats,
      });
    }

    // 模型列表
    if (path === '/v1/models') {
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

    if (path === '/anthropic/v1/models') {
      const data = MODELS.map(m => ({
        id: m.id,
        display_name: m.display_name,
        created_at: '2025-01-01T00:00:00Z',
        type: 'model',
        max_input_tokens: m.context_length,
        max_tokens: m.max_tokens,
      }));
      return Response.json({
        data,
        has_more: false,
        first_id: data[0]?.id,
        last_id: data[data.length - 1]?.id
      });
    }

    // AI API 转发
    if (isAiRoute(path)) {
      return handleForward(request, path, ctx);
    }

    // 根路径 - 简单状态页
    if (path === '/') {
      return new Response(getIndexHtml(), {
        headers: { 'content-type': 'text/html; charset=utf-8' }
      });
    }

    return new Response('Not Found', { status: 404 });
  }
};

// ============== WebSocket 处理 ==============
async function handleWebSocket(request, ctx) {
  const pair = new WebSocketPair();
  const [client, server] = Object.values(pair);

  server.accept();
  const nodeId = generateId();
  const addr = request.headers.get('CF-Connecting-IP') || 'unknown';

  nodes.set(nodeId, {
    ws: server,
    info: {
      addr,
      connectedAt: Date.now(),
      requestsServed: 0,
    }
  });

  console.log(`✅ 内网节点已接入: ${addr} (nodeId: ${nodeId.slice(0, 8)})`);

  // 消息处理
  server.addEventListener('message', (event) => {
    try {
      const data = JSON.parse(event.data);

      // 注册消息
      if (data.type === 'register') {
        const node = nodes.get(nodeId);
        if (node) {
          node.info.userId = data.user_id;
          console.log(`📋 节点 ${addr} 注册为用户 ${data.user_id}`);
        }
        return;
      }

      // 响应消息
      const reqId = data.req_id;
      const pending = pendingRequests.get(reqId);
      if (pending) {
        pending.queue.push(data);
      }
    } catch (e) {
      console.error('解析消息失败:', e);
    }
  });

  // 关闭处理
  server.addEventListener('close', () => {
    nodes.delete(nodeId);
    console.log(`❌ 内网节点断开: ${addr}`);
  });

  server.addEventListener('error', (e) => {
    console.error(`节点错误: ${addr}`, e);
    nodes.delete(nodeId);
  });

  return new Response(null, { status: 101, webSocket: client });
}

// ============== 请求转发 ==============
async function handleForward(request, path, ctx) {
  const nodeId = selectNode();
  if (!nodeId) {
    return new Response('Gateway Error: 没有可用的内网节点', { status: 503 });
  }

  const node = nodes.get(nodeId);
  if (!node || node.ws.readyState !== 1) {
    nodes.delete(nodeId);
    return new Response('Gateway Error: 节点不可用', { status: 503 });
  }

  const method = request.method;
  const body = await request.text();
  const reqId = generateId();

  // 创建待处理请求
  const pending = { queue: [] };
  pendingRequests.set(reqId, pending);

  // 发送请求到节点
  try {
    node.ws.send(JSON.stringify({
      req_id: reqId,
      method,
      path,
      body
    }));

    // 更新请求计数
    node.info.requestsServed++;
  } catch (e) {
    pendingRequests.delete(reqId);
    return new Response('Gateway Error: 发送失败', { status: 502 });
  }

  // 等待响应
  try {
    // 等待第一个消息（超时 15s）
    const firstMsg = await waitForMessage(pending, 15000);
    if (!firstMsg) {
      return new Response('Gateway Error: 节点响应超时', { status: 504 });
    }

    if (firstMsg.type === 'error') {
      return new Response(`Gateway Error: ${firstMsg.body || '节点返回错误'}`, { status: 502 });
    }

    const status = firstMsg.status || 200;
    const headers = firstMsg.headers || {};
    const contentType = headers['content-type'] || 'application/json';

    // 收集完整响应
    const chunks = [];
    while (true) {
      const msg = await waitForMessage(pending, 120000);
      if (!msg || msg.type === 'finish') break;
      if (msg.type === 'error') {
        return new Response(`Gateway Error: ${msg.body}`, { status: 502 });
      }
      if (msg.type === 'chunk') {
        chunks.push(msg.body || '');
      }
    }

    const responseBody = chunks.join('');

    // 构建响应头
    const responseHeaders = new Headers();
    responseHeaders.set('content-type', contentType);
    for (const [key, value] of Object.entries(headers)) {
      if (!['content-length', 'transfer-encoding', 'content-encoding', 'connection'].includes(key.toLowerCase())) {
        responseHeaders.set(key, value);
      }
    }

    return new Response(responseBody, { status, headers: responseHeaders });

  } finally {
    pendingRequests.delete(reqId);
  }
}

// 等待消息
function waitForMessage(pending, timeout) {
  return new Promise((resolve) => {
    const start = Date.now();
    const check = () => {
      if (pending.queue.length > 0) {
        resolve(pending.queue.shift());
        return;
      }
      if (Date.now() - start >= timeout) {
        resolve(null);
        return;
      }
      setTimeout(check, 10);
    };
    check();
  });
}

// ============== 首页 HTML ==============
function getIndexHtml() {
  return `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>mimo2api Gateway</title>
  <style>
    * { box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      max-width: 900px;
      margin: 0 auto;
      padding: 40px 20px;
      background: #f5f5f5;
    }
    .card {
      background: white;
      border-radius: 8px;
      padding: 24px;
      margin-bottom: 20px;
      box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    h1 { margin: 0 0 8px; color: #1a1a1a; }
    h2 { margin: 0 0 16px; color: #333; font-size: 18px; }
    p { margin: 0 0 16px; color: #666; }
    code {
      background: #f4f4f4;
      padding: 2px 8px;
      border-radius: 4px;
      font-family: 'SF Mono', Monaco, monospace;
      font-size: 14px;
    }
    pre {
      background: #2d2d2d;
      color: #f8f8f2;
      padding: 16px;
      border-radius: 6px;
      overflow-x: auto;
    }
    pre code { background: none; padding: 0; color: inherit; }
    .endpoint {
      display: flex;
      align-items: center;
      padding: 8px 0;
      border-bottom: 1px solid #eee;
    }
    .endpoint:last-child { border-bottom: none; }
    .method {
      font-weight: 600;
      width: 60px;
      color: #0066cc;
    }
    .path { color: #333; }
    .status { color: #22c55e; font-weight: 600; }
    .stats { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; }
    .stat { text-align: center; }
    .stat-value { font-size: 32px; font-weight: 700; color: #1a1a1a; }
    .stat-label { font-size: 14px; color: #666; }
  </style>
</head>
<body>
  <div class="card">
    <h1>🚀 mimo2api Gateway</h1>
    <p>Cloudflare Worker 版本 - 基于 WebSocket 隧道的 API 网关</p>
  </div>

  <div class="card">
    <h2>API 端点</h2>
    <div class="endpoint"><span class="method">GET</span><span class="path"><code>/v1/models</code></span></div>
    <div class="endpoint"><span class="method">POST</span><span class="path"><code>/v1/chat/completions</code></span></div>
    <div class="endpoint"><span class="method">POST</span><span class="path"><code>/v1/responses</code></span></div>
    <div class="endpoint"><span class="method">POST</span><span class="path"><code>/v1/audio/speech</code></span></div>
    <div class="endpoint"><span class="method">POST</span><span class="path"><code>/anthropic/v1/messages</code></span></div>
    <div class="endpoint"><span class="method">WS</span><span class="path"><code>/ws</code></span></div>
  </div>

  <div class="card">
    <h2>使用说明</h2>
    <p>内网节点连接 WebSocket 隧道：</p>
    <pre><code>WS_URL=wss://your-worker.workers.dev/ws</code></pre>
  </div>
</body>
</html>`;
}
