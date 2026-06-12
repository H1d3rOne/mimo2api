/**
 * Claw 实例管理模块
 *
 * 对接 MIMO AI Studio API，管理 Claw 容器的创建、销毁、状态查询等。
 * 对应原 Python 版 manager.py 中的 NativeClawClient 和 AccountManager 的 API 调用部分。
 */

import { UserInfo, ClawStatus, ClawStatusResponse } from "./types";

const BASE_URL = "https://aistudio.xiaomimimo.com";

function aistudioHeaders(): Record<string, string> {
  return {
    Accept: "*/*",
    "Content-Type": "application/json",
    Origin: BASE_URL,
    Referer: `${BASE_URL}/`,
    "x-timezone": "Asia/Shanghai",
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
  };
}

function buildCookies(user: UserInfo): Record<string, string> {
  return {
    serviceToken: user.serviceToken,
    userId: user.userId,
    xiaomichatbot_ph: user.xiaomichatbot_ph,
  };
}

/** 构建 Cookie header 字符串 */
function cookieString(cookies: Record<string, string>): string {
  return Object.entries(cookies)
    .map(([k, v]) =>
      v.includes(" ") || v.includes("=")
        ? `${k}="${v}"`
        : `${k}=${v}`
    )
    .join("; ");
}

// ─── API 响应解析 ────────────────────────────────────────────────

interface ApiResponse {
  code?: number;
  message?: string;
  msg?: string;
  error?: string;
  data?: {
    status?: string;
    expireTime?: number;
    ticket?: string;
    message?: string;
    [key: string]: unknown;
  };
}

function parseResponse(raw: unknown): ApiResponse {
  if (typeof raw === "object" && raw !== null) return raw as ApiResponse;
  return {};
}

function isSuccess(data: ApiResponse): boolean {
  return data.code === undefined || data.code === 0;
}

// ─── Claw Manager ────────────────────────────────────────────────

export class ClawManager {
  private user: UserInfo;
  private cookies: Record<string, string>;
  private tunnelDomain: string | undefined;

  constructor(user: UserInfo, proxyUrl?: string) {
    this.user = user;
    this.cookies = buildCookies(user);
    // proxyUrl 现在是 Tunnel 域名（如 https://mimo-tunnel.your-domain.com）
    // 用于替换 aistudio.xiaomimimo.com，路径保持不变
    this.tunnelDomain = proxyUrl;
  }

  get userId(): string {
    return this.user.userId;
  }

  get userName(): string {
    return this.user.name;
  }

  /** 通过 Tunnel 域名替换发起 fetch 请求 */
  private async proxiedFetch(url: string, init: RequestInit): Promise<Response> {
    if (this.tunnelDomain) {
      // Tunnel 模式：仅替换域名，路径保持不变
      // https://aistudio.xiaomimimo.com/open-apis/... → https://mimo-tunnel.your-domain.com/open-apis/...
      const proxyTarget = url.replace(BASE_URL, this.tunnelDomain);
      return fetch(proxyTarget, init);
    }
    return fetch(url, init);
  }

  /** 查询 Claw 实例状态 */
  async getStatus(): Promise<ClawStatusResponse> {
    const url = `${BASE_URL}/open-apis/user/mimo-claw/status`;
    try {
      const resp = await this.proxiedFetch(url, {
        method: "GET",
        headers: { ...aistudioHeaders(), Cookie: cookieString(this.cookies) },
      });
      const raw = await resp.json();
      const data = parseResponse(raw);

      if (!isSuccess(data) || !data.data) {
        return { status: "", message: data.message || data.msg || data.error || "" };
      }

      const status = (data.data.status || "").trim() as ClawStatus;
      const expireTime = data.data.expireTime;
      const message = String(data.data.message || data.message || data.msg || data.error || "");
      return { status, expireTime, message };
    } catch (err) {
      console.error(`[ClawManager] getStatus 失败: ${err}`);
      return { status: "", message: String(err) };
    }
  }

  /** 签署 agreement */
  async signAgreement(): Promise<boolean> {
    const ph = encodeURIComponent(this.user.xiaomichatbot_ph);
    const url = `${BASE_URL}/open-apis/agreement/user/mimo-claw?xiaomichatbot_ph=${ph}`;
    try {
      const resp = await this.proxiedFetch(url, {
        method: "POST",
        headers: { ...aistudioHeaders(), Cookie: cookieString(this.cookies) },
      });
      const contentType = resp.headers.get("content-type") || "";
      if (!contentType.includes("application/json")) {
        console.warn(`[ClawManager] 签署 agreement 非 JSON 响应: HTTP ${resp.status}`);
        return false;
      }
      const raw = await resp.json();
      const data = parseResponse(raw);
      if (resp.status >= 400 || !isSuccess(data)) {
        console.warn(`[ClawManager] 签署 agreement 异常: code=${data.code}, msg=${data.message || data.msg}`);
      }
      return true;
    } catch (err) {
      console.warn(`[ClawManager] 签署 agreement 异常: ${err}`);
      return false;
    }
  }

  /** 创建 Claw 实例（仅发起创建请求，不轮询等待） */
  async create(): Promise<{ ok: boolean; error?: string }> {
    // 先签署 agreement
    await this.signAgreement();

    const ph = encodeURIComponent(this.user.xiaomichatbot_ph);
    const url = `${BASE_URL}/open-apis/user/mimo-claw/create?xiaomichatbot_ph=${ph}`;

    try {
      const resp = await this.proxiedFetch(url, {
        method: "POST",
        headers: { ...aistudioHeaders(), Cookie: cookieString(this.cookies) },
      });

      // 处理非 JSON 响应（如 Cloudflare 1002 错误）
      const contentType = resp.headers.get("content-type") || "";
      if (!contentType.includes("application/json")) {
        const text = await resp.text();
        const cfMatch = text.match(/error code:\s*(\d+)/);
        if (cfMatch) {
          const cfCode = cfMatch[1];
          const cfErrors: Record<string, string> = {
            "1002": "DNS 解析受限（CF Worker 无法直连受 CF 保护的站点），请在容器中配置 Cloudflare Tunnel（设置 MIMO_TUNNEL_TOKEN 和 MIMO_PROXY_URL）",
            "1042": "Worker 子请求命中了同 zone 的另一个 Worker。通常是 MIMO_PROXY_URL 配错了：未配置 Tunnel 时请删除 MIMO_PROXY_URL；如确实要走同 zone Tunnel/Worker，请使用独立 Tunnel hostname 或启用 global_fetch_strictly_public。",
            "1015": "请求频率过高，被 Cloudflare 限流",
            "1020": "访问被 Cloudflare 安全规则拦截",
          };
          return { ok: false, error: cfErrors[cfCode] || `Cloudflare 错误 ${cfCode}` };
        }
        return { ok: false, error: `非 JSON 响应: HTTP ${resp.status}` };
      }

      const raw = await resp.json();
      const data = parseResponse(raw);

      if (resp.status === 401) {
        return { ok: false, error: "账户已过期失效" };
      }
      if (resp.status === 429) {
        return { ok: false, error: "当前 Claw 实例负载过高" };
      }
      if (resp.status >= 400) {
        return { ok: false, error: `创建请求失败: HTTP ${resp.status}` };
      }
      if (!isSuccess(data)) {
        return { ok: false, error: `创建接口返回异常: code=${data.code}, msg=${data.message || data.msg}` };
      }

      // 不轮询等待，由 Cron tick 生命周期管理自动处理后续状态
      return { ok: true };
    } catch (err) {
      return { ok: false, error: `创建请求异常: ${String(err)}` };
    }
  }

  /** 创建 Claw 实例并轮询等待（仅供生命周期 tick 使用，Worker 内会受 CPU 时间限制） */
  async createWithPolling(timeoutMs = 120_000): Promise<{ ok: boolean; error?: string }> {
    const result = await this.create();
    if (!result.ok) return result;

    const deadline = Date.now() + timeoutMs;
    let lastStatus = "";
    while (Date.now() < deadline) {
      const status = await this.getStatus();
      if (status.status && status.status !== lastStatus) {
        console.log(`[ClawManager] 用户 ${this.userName} Claw 创建状态: ${status.status}`);
        lastStatus = status.status;
      }
      if (status.status === "AVAILABLE") {
        return { ok: true };
      }
      if (
        status.status.endsWith("FAILED") ||
        status.status === "DESTROYED" ||
        status.status === "ERROR"
      ) {
        return { ok: false, error: `创建失败，状态: ${status.status}` };
      }
      await sleep(2000);
    }
    return { ok: false, error: "创建等待超时" };
  }

  /** 销毁 Claw 实例 */
  async destroy(): Promise<boolean> {
    const ph = encodeURIComponent(this.user.xiaomichatbot_ph);
    const url = `${BASE_URL}/open-apis/user/mimo-claw/destroy?xiaomichatbot_ph=${ph}`;

    const cookiesWithPh = { ...this.cookies, xiaomichatbot_ph: this.user.xiaomichatbot_ph };
    try {
      await this.proxiedFetch(url, {
        method: "POST",
        headers: { ...aistudioHeaders(), Cookie: cookieString(cookiesWithPh) },
      });
    } catch (err) {
      console.error(`[ClawManager] 销毁请求异常: ${err}`);
    }

    // 不 sleep 等待，由 Cron tick 检查状态
    return true;
  }

  /** 获取 WS ticket */
  async getTicket(maxRetries = 5): Promise<string | null> {
    const ph = encodeURIComponent(this.user.xiaomichatbot_ph);
    const url = `${BASE_URL}/open-apis/user/ws/ticket?xiaomichatbot_ph=${ph}`;

    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        const resp = await this.proxiedFetch(url, {
          method: "GET",
          headers: { ...aistudioHeaders(), Cookie: cookieString(this.cookies) },
        });
        const raw = await resp.json();
        const data = parseResponse(raw);

        if (resp.status === 200 && data.data?.ticket) {
          return data.data.ticket;
        }

        if (attempt < maxRetries - 1) {
          console.warn(`[ClawManager] 获取 Ticket 失败，3秒后重试...`);
          await sleep(3000);
        }
      } catch (err) {
        console.warn(`[ClawManager] 获取 Ticket 异常: ${err}`);
        if (attempt < maxRetries - 1) await sleep(3000);
      }
    }
    return null;
  }
}

// ─── 辅助 ────────────────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
