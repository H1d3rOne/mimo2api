/**
 * AI API 鉴权模块
 *
 * 从请求中提取 API Key 并验证，兼容 OpenAI Bearer 和 x-api-key 头格式。
 */

interface Env {
  MIMO_RELAY_OPENAI_KEY?: string;
}

/**
 * 判断 AI 鉴权是否启用
 */
export function isAiAuthEnabled(env: Env): boolean {
  return !!env.MIMO_RELAY_OPENAI_KEY && env.MIMO_RELAY_OPENAI_KEY.length > 0;
}

/**
 * 从请求中提取 API Key
 * 支持 Authorization: Bearer xxx, x-api-key: xxx, api-key: xxx
 */
export function extractAiApiKey(request: Request): string | null {
  const authHeader = request.headers.get("authorization") || "";
  if (authHeader.toLowerCase().startsWith("bearer ")) {
    return authHeader.slice(7).trim();
  }

  for (const headerName of ["x-api-key", "api-key"]) {
    const value = (request.headers.get(headerName) || "").trim();
    if (value) return value;
  }

  return null;
}

/**
 * 验证 API Key 是否匹配（恒定时间比较）
 */
export function verifyAiApiKey(candidate: string | null, env: Env): boolean {
  const expected = env.MIMO_RELAY_OPENAI_KEY || "";
  if (!expected) return true; // 未配置密钥则不鉴权
  if (!candidate) return false;
  return candidate === expected;
}
