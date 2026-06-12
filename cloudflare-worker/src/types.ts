/**
 * 全局类型定义（无 Durable Objects 版）
 */

// ─── Env 接口（所有模块共享） ────────────────────────────────────

export interface Env {
  MIMO_KV: KVNamespace;
  GATEWAY: DurableObjectNamespace;

  // AI API 鉴权密钥
  MIMO_RELAY_OPENAI_KEY?: string;
  // WebUI 鉴权
  MIMO_WEBUI_USERNAME?: string;
  MIMO_WEBUI_PASSWORD?: string;
  // 模型映射
  MODEL_MAPPING_JSON?: string;
  // bridge.py 连接的 WS 地址（Worker 自身的 /ws 端点）
  MIMO2API_WS_URL?: string;
  // 可选：优选域名/API Base，用于派生 bridge 回连 WS，加速 MIMO 容器访问 Cloudflare
  MIMO2API_PREFERRED_BASE_URL?: string;
  // 可选：显式指定优选 WS URL，优先级高于 MIMO2API_PREFERRED_BASE_URL
  MIMO2API_PREFERRED_WS_URL?: string;
  // 可选：默认是否开启 Responses API -> Chat Completions 端点转换
  MIMO_ENDPOINT_CONVERSION_ENABLED?: string;
  // Tunnel 代理 URL（解决 CF Worker 无法直连受 CF 保护的站点）
  // 通过 Cloudflare Tunnel 将 aistudio.xiaomimimo.com 映射到你的子域名
  // Tunnel 在 Mimo 容器中运行，CF Worker 通过此域名访问 aistudio API
  // 格式：https://mimo-tunnel.your-domain.com（不带尾部斜杠）
  MIMO_PROXY_URL?: string;
  // Cloudflare Tunnel Token（注入到容器中，让 cloudflared 自动连接）
  // 在 Cloudflare Dashboard → Zero Trust → Networks → Tunnels 创建后获取
  MIMO_TUNNEL_TOKEN?: string;
}

// ─── 用户信息 ────────────────────────────────────────────────────

export interface UserInfo {
  userId: string;
  name: string;
  serviceToken: string;
  xiaomichatbot_ph: string;
}

// ─── Claw 实例状态 ──────────────────────────────────────────────

export type ClawStatus =
  | "CREATING"
  | "AVAILABLE"
  | "DESTROYED"
  | "DESTROYING"
  | "ERROR"
  | "CREATE_FAILED"
  | "DESTROY_FAILED"
  | "";

export interface ClawStatusResponse {
  status: ClawStatus;
  expireTime?: number;
  message?: string;
}

// ─── 生命周期状态 ───────────────────────────────────────────────

export type LifecyclePhase =
  | "idle"          // 空闲，无实例
  | "creating"      // 创建中
  | "injecting"     // 注入 bridge 中
  | "running"       // 正常运行
  | "destroying"    // 销毁中
  | "error";        // 出错

export interface LifecycleState {
  userId: string;
  phase: LifecyclePhase;
  lastUpdate: number;        // 时间戳 ms
  lastError?: string;
  clawExpireAt?: number;     // Claw 到期时间戳 ms
  nextActionAt?: number;     // 下次动作时间戳 ms
  servedCount: number;       // 已服务请求次数
  currentRoundStart?: number; // 本轮开始时间
  bridgeMissingSince?: number; // running 状态下首次发现可调度 bridge 丢失的时间
  bridgeOnlineAt?: number;   // 当前实例周期内首次/最近一次确认 bridge 可调度在线的时间
  injectionStage?: "reset_sent"; // 新建实例注入分两轮：已发送 reset，等待下一轮注入 bridge
}
