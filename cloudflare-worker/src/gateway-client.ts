import type { Env } from "./types";

export interface ForwardPayload {
  method: string;
  path: string;
  body: string;
  headers?: Record<string, string>;
}

const GATEWAY_DO_NAME = "mimo2api-gateway";
const GATEWAY_INTERNAL_ORIGIN = "https://mimo2api-gateway.internal";

export function getGatewayStub(env: Env): DurableObjectStub {
  const id = env.GATEWAY.idFromName(GATEWAY_DO_NAME);
  return env.GATEWAY.get(id);
}

export function gatewayRequest(path: string, init?: RequestInit): Request {
  return new Request(`${GATEWAY_INTERNAL_ORIGIN}${path}`, init);
}

export async function gatewayStats(env: Env): Promise<Record<string, unknown>> {
  const resp = await getGatewayStub(env).fetch(gatewayRequest("/api/stats"));
  return resp.json() as Promise<Record<string, unknown>>;
}

export async function gatewayErrors(env: Env, limit: number): Promise<Response> {
  return getGatewayStub(env).fetch(gatewayRequest(`/api/errors?limit=${encodeURIComponent(String(limit))}`));
}

export async function gatewayForward(env: Env, payload: ForwardPayload): Promise<Response> {
  return getGatewayStub(env).fetch(
    gatewayRequest("/forward", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }),
  );
}

