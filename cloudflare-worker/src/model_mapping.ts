/**
 * 模型映射模块
 *
 * 读取模型映射配置，将客户端请求中的模型名映射为内网模型名。
 * 在 CF Worker 版本中，映射关系通过环境变量 MODEL_MAPPING_JSON 传入。
 */

interface Env {
  MIMO_KV?: KVNamespace;
  MODEL_MAPPING_JSON?: string;
}

const MODEL_MAPPING_KV_KEY = "config:model_mapping";

// 默认映射（与项目根目录 model_mapping.json 保持一致）
const DEFAULT_MODEL_MAPPING: Record<string, string> = {
  "claude-haiku-4-5-20251001": "mimo-v2-flash",
  "claude-opus-4-7": "mimo-v2.5-pro",
  "claude-opus-4-6": "mimo-v2.5-pro",
  "sonnet-4.6": "mimo-v2.5",
  "gpt-5.5": "mimo-v2.5-pro",
  "gpt-5.4": "mimo-v2.5-pro",
  "gpt-5.4-mini": "mimo-v2-flash",
};

/**
 * 加载模型映射（从环境变量解析，fallback 到默认映射）
 */
export function loadModelMapping(): Record<string, string> {
  // CF Worker 环境中通过 env.MODEL_MAPPING_JSON 传入
  // 在不依赖 env 的场景下使用默认映射
  return DEFAULT_MODEL_MAPPING;
}

/**
 * 加载模型映射（带环境变量支持）
 */
export function loadModelMappingFromEnv(env: { MODEL_MAPPING_JSON?: string }): Record<string, string> {
  const raw = env.MODEL_MAPPING_JSON;
  if (!raw) return DEFAULT_MODEL_MAPPING;

  try {
    const parsed = JSON.parse(raw);
    if (typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)) {
      return { ...DEFAULT_MODEL_MAPPING, ...parsed };
    }
  } catch {}

  return DEFAULT_MODEL_MAPPING;
}

/**
 * Worker 版没有本地 model_mapping.json 可写，因此把 WebUI/API 修改后的映射持久化到 KV。
 * 优先级：默认映射 < MODEL_MAPPING_JSON < KV 中保存的映射。
 */
export async function loadModelMappingFromEnvAndKv(env: Env): Promise<Record<string, string>> {
  const envMapping = loadModelMappingFromEnv(env);
  if (!env.MIMO_KV) return envMapping;

  const raw = await env.MIMO_KV.get(MODEL_MAPPING_KV_KEY, "text");
  if (!raw) return envMapping;

  try {
    const parsed = JSON.parse(raw);
    if (typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)) {
      return { ...envMapping, ...(parsed as Record<string, string>) };
    }
  } catch {}

  return envMapping;
}

export async function saveModelMappingToKv(env: Env, mapping: Record<string, string>): Promise<void> {
  if (!env.MIMO_KV) {
    throw new Error("MIMO_KV 未绑定，无法持久化模型映射");
  }
  await env.MIMO_KV.put(MODEL_MAPPING_KV_KEY, JSON.stringify(mapping));
}

export async function deleteModelMappingItem(env: Env, modelName: string): Promise<boolean> {
  const mapping = await loadModelMappingFromEnvAndKv(env);
  if (!(modelName in mapping)) return false;
  delete mapping[modelName];
  await saveModelMappingToKv(env, mapping);
  return true;
}

/**
 * 对请求体应用模型映射
 * 如果请求体中的 model 字段在映射表中存在，则替换为映射后的模型名
 */
export async function applyModelMapping(bodyText: string, env?: { MODEL_MAPPING_JSON?: string }): Promise<string> {
  const mapping = env ? await loadModelMappingFromEnvAndKv(env as Env) : loadModelMapping();
  if (!mapping || Object.keys(mapping).length === 0) return bodyText;

  try {
    const data = JSON.parse(bodyText);
    const originalModel = data.model;
    if (originalModel && originalModel in mapping) {
      data.model = mapping[originalModel];
      console.log(`🔀 模型映射: ${originalModel} → ${data.model}`);
      return JSON.stringify(data);
    }
    return bodyText;
  } catch {
    return bodyText;
  }
}
