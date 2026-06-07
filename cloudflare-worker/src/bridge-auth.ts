const BRIDGE_TOKEN_KV_KEY = "config:bridge_token";

function randomToken(): string {
  const bytes = new Uint8Array(32);
  crypto.getRandomValues(bytes);
  return [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
}

export async function getBridgeToken(kv: KVNamespace): Promise<string> {
  const existing = await kv.get(BRIDGE_TOKEN_KV_KEY, "text");
  if (existing) return existing;
  const token = randomToken();
  await kv.put(BRIDGE_TOKEN_KV_KEY, token);
  return token;
}

export async function verifyBridgeToken(kv: KVNamespace, candidate: string | null): Promise<boolean> {
  if (!candidate) return false;
  const expected = await getBridgeToken(kv);
  return candidate === expected;
}

export function appendBridgeToken(wsUrl: string, token: string): string {
  const url = new URL(wsUrl);
  url.searchParams.set("token", token);
  return url.toString();
}
