import type { Env } from "./types";

export type ControlChannelMode = "proxy" | "gateway" | "direct";

export type ControlFetch = (input: string | Request | URL, init?: RequestInit) => Promise<Response>;

function truthy(value: string | undefined): boolean {
  return /^(1|true|yes|on)$/i.test((value || "").trim());
}

function configuredMode(env: Env): ControlChannelMode | "" {
  const raw = (env.MIMO_CONTROL_CHANNEL || "").trim().toLowerCase();
  if (raw === "proxy" || raw === "tunnel") return "proxy";
  if (raw === "gateway" || raw === "egress" || raw === "zero-trust" || raw === "zero_trust") return "gateway";
  if (raw === "direct" || raw === "none") return "direct";
  if (truthy(env.USE_VPC_EGRESS)) return "gateway";
  return "";
}

/**
 * MIMO AI Studio 管理接口出站通道：
 * - MIMO_PROXY_URL：传统 Tunnel/反代，替换 aistudio host 后走普通 fetch。
 * - MIMO_AISTUDIO：Cloudflare Zero Trust / Workers VPC Service，专门路由到 MIMO AI Studio。
 * - EGRESS：Cloudflare Zero Trust Gateway / Workers VPC egress binding，直连原始 aistudio URL。
 * - direct：普通 Worker fetch。
 */
export function getControlChannelMode(env: Env): ControlChannelMode {
  const forced = configuredMode(env);
  if (forced) return forced;
  if ((env.MIMO_PROXY_URL || "").trim()) return "proxy";
  if (env.MIMO_AISTUDIO && typeof env.MIMO_AISTUDIO.fetch === "function") return "gateway";
  if (env.EGRESS && typeof env.EGRESS.fetch === "function") return "gateway";
  return "direct";
}

export function getGatewayEgressFetch(env: Env): ControlFetch | undefined {
  if (getControlChannelMode(env) !== "gateway") return undefined;

  // 优先走专用 MIMO_AISTUDIO VPC Service。
  // 该 service 固定到 MIMO AI Studio 公网 ALB IP，能保持请求 URL 的 Host/SNI，
  // 避免 Worker direct fetch 的 1002 以及 EGRESS 对该目标的 destination_ip_prohibited。
  if (env.MIMO_AISTUDIO && typeof env.MIMO_AISTUDIO.fetch === "function") {
    return async (input, init) => {
      let serviceError: unknown;
      try {
        return await env.MIMO_AISTUDIO!.fetch(input, init);
      } catch (err) {
        serviceError = err;
        if (env.EGRESS && typeof env.EGRESS.fetch === "function") {
          try {
            console.warn(`[ControlChannel] MIMO_AISTUDIO VPC service 失败，回退 EGRESS: ${String(err)}`);
            return await env.EGRESS.fetch(input, init);
          } catch (egressErr) {
            console.warn(`[ControlChannel] EGRESS 也失败，回退 Worker direct fetch: ${String(egressErr)}`);
          }
        }
        console.warn(`[ControlChannel] Gateway VPC service 失败，回退 Worker direct fetch: ${String(serviceError)}`);
        return fetch(input, init);
      }
    };
  }

  if (env.EGRESS && typeof env.EGRESS.fetch === "function") {
    return async (input, init) => {
      try {
        return await env.EGRESS!.fetch(input, init);
      } catch (err) {
        console.warn(`[ControlChannel] Gateway EGRESS 失败，回退 Worker direct fetch: ${String(err)}`);
        return fetch(input, init);
      }
    };
  }

  if (!env.EGRESS || typeof env.EGRESS.fetch !== "function") {
    return async () => {
      throw new Error("MIMO_CONTROL_CHANNEL=gateway / USE_VPC_EGRESS=true，但 MIMO_AISTUDIO VPC service 或 EGRESS VPC network binding 未配置");
    };
  }
}

export function getControlProxyUrl(env: Env): string | undefined {
  if (getControlChannelMode(env) !== "proxy") return undefined;
  return (env.MIMO_PROXY_URL || "").replace(/\/+$/, "") || undefined;
}

export function getControlChannelLabel(mode: ControlChannelMode): string {
  if (mode === "proxy") return "MIMO_PROXY_URL 代理/Tunnel";
  if (mode === "gateway") return "Cloudflare Zero Trust VPC/Gateway";
  return "Worker direct fetch";
}
