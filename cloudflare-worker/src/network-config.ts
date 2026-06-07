import type { Env } from "./types";

const PREFERRED_BASE_URL_KV_KEY = "config:preferred_base_url";

export interface NetworkConfig {
  /** 优选连接地址（Xray 风格 address），可为空。 */
  preferred_base_url: string;
  /** 客户端/WebUI 推荐访问的 Worker 真实 Base，不强行改成优选连接地址。 */
  effective_base_url: string;
  /** bridge 实际连接的 WS 地址；配置优选地址时为优选地址。 */
  effective_ws_url: string;
  /** Worker 真实 WS 地址，用作 Host/SNI 来源。 */
  worker_ws_url: string;
  worker_base_url: string;
  /** bridge 使用的 TCP 连接地址（Xray 风格 address），例如 saas.sin.fan。 */
  bridge_connect_host: string;
  /** bridge 的 Host/SNI，即 Worker 真实域名，例如 mimo.rushai.us.ci。 */
  bridge_host_header: string;
  configured: boolean;
}

export function normalizeBaseUrl(value: string | undefined | null): string {
  let raw = (value || "").trim().replace(/\/+$/, "");
  if (raw && !/^https?:\/\//i.test(raw)) raw = `https://${raw}`;
  if (!raw) return "";
  try {
    const url = new URL(raw);
    if (url.protocol !== "https:" && url.protocol !== "http:") return "";
    return `${url.protocol}//${url.host}`;
  } catch {
    return "";
  }
}

function normalizeWsUrl(value: string | undefined | null): string {
  const raw = (value || "").trim();
  if (!raw) return "";
  try {
    const url = new URL(raw);
    if (url.protocol !== "wss:" && url.protocol !== "ws:") return "";
    if (!url.pathname || url.pathname === "/") url.pathname = "/ws";
    return url.toString();
  } catch {
    return "";
  }
}

function baseToWsUrl(baseUrl: string): string {
  if (!baseUrl) return "";
  const url = new URL(baseUrl);
  url.protocol = url.protocol === "http:" ? "ws:" : "wss:";
  url.pathname = "/ws";
  url.search = "";
  url.hash = "";
  return url.toString();
}

function wsToBaseUrl(wsUrl: string): string {
  if (!wsUrl) return "";
  try {
    const url = new URL(wsUrl);
    url.protocol = url.protocol === "ws:" ? "http:" : "https:";
    url.pathname = "";
    url.search = "";
    url.hash = "";
    return url.toString().replace(/\/+$/, "");
  } catch {
    return "";
  }
}

export async function loadNetworkConfig(env: Env, requestUrl?: URL): Promise<NetworkConfig> {
  const kvPreferred = (await env.MIMO_KV.get(PREFERRED_BASE_URL_KV_KEY, "text")) || "";
  const preferredBase = normalizeBaseUrl(kvPreferred || env.MIMO2API_PREFERRED_BASE_URL || "");
  const requestBase = requestUrl ? normalizeBaseUrl(requestUrl.origin) : "";
  const workerWs = normalizeWsUrl(env.MIMO2API_WS_URL || "") || (requestBase ? baseToWsUrl(requestBase) : "") || "wss://placeholder.workers.dev/ws";
  const workerBase = wsToBaseUrl(workerWs) || requestBase;

  const preferredWs = normalizeWsUrl(env.MIMO2API_PREFERRED_WS_URL || "") || (preferredBase ? baseToWsUrl(preferredBase) : "");

  let bridgeConnectHost = "";
  let bridgeHostHeader = "";
  if (preferredWs) {
    try {
      bridgeConnectHost = new URL(preferredWs).host;
      bridgeHostHeader = new URL(workerWs).host;
    } catch {}
  }

  return {
    preferred_base_url: preferredBase,
    effective_base_url: workerBase || preferredBase,
    // WS_URL 保持 Worker 真实域名；bridge 通过 connect host 覆盖 TCP 连接地址，实现 SNI/Host 与连接地址分离。
    effective_ws_url: workerWs,
    worker_ws_url: workerWs,
    worker_base_url: workerBase,
    bridge_connect_host: bridgeConnectHost,
    bridge_host_header: bridgeHostHeader,
    configured: Boolean(preferredBase || env.MIMO2API_PREFERRED_WS_URL),
  };
}

export async function savePreferredBaseUrl(env: Env, value: string): Promise<void> {
  const normalized = normalizeBaseUrl(value);
  if (!normalized) {
    await env.MIMO_KV.delete(PREFERRED_BASE_URL_KV_KEY);
    return;
  }
  await env.MIMO_KV.put(PREFERRED_BASE_URL_KV_KEY, normalized);
}
