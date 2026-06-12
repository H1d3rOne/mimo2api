/**
 * Lifecycle — 每用户生命周期管理（无 Durable Objects 版）
 *
 * 使用 KV 存储生命周期状态，Cron tick 直接执行状态机逻辑。
 * CF Worker 免费计划可用。
 *
 * 状态机：idle → creating → injecting → running → destroying → idle
 *
 * 限制：
 * - 无 Alarm API，依赖 Cron（每 5 分钟）或手动触发 tick
 * - 无持久化 WebSocket 连接（注入用的 ClawWsClient 为短连接，无影响）
 * - 注入阶段可能因 Worker CPU 时间限制（免费 10ms）而超时
 *   → 通过 ctx.waitUntil() 延长执行时间
 */

import { ClawManager } from "./claw-manager";
import { appendBridgeToken, getBridgeToken } from "./bridge-auth";
import { ClawWsClient, getBridgeInjectionPrompt, RESET_CMD, SHUTDOWN_PROMPT, SHUTDOWN_CONFIRM_PROMPT } from "./claw-ws-client";
import type { UserInfo, LifecycleState, LifecyclePhase } from "./types";

// ─── 常量 ────────────────────────────────────────────────────────

const CLAW_REUSE_MIN_REMAIN_MS = 3 * 60 * 1000;  // 3 分钟最低复用寿命
const CLAW_REUSE_BUFFER_MS = 15 * 60 * 1000;      // 双账号接力模式提前 15 分钟轮换，避免 60 分钟硬销毁导致断链
const CLAW_BRIDGE_FALLBACK_LIFETIME_MS = 55 * 60 * 1000; // 控制面查不到 expireTime 时，用 bridge 上线时间保守估算
const RECONNECT_WAIT_MS = 15 * 1000;               // 重启后等待 15s
const MAX_CONNECT_RETRIES = 5;
const MAX_RECONNECT_RETRIES = 10;
const MAX_SHUTDOWN_REPLY_SECONDS = 90;
const INJECTION_LEASE_MS = 12 * 60 * 1000;         // 注入会跨多轮 cron，租约内不重复发送
const INJECTION_STEP_LEASE_MS = 4 * 60 * 1000;     // 单步注入最长等待 180s，租约略长一点，避免 cron 重叠重复发送
const CREATE_FAILURE_BACKOFF_MS = 2 * 60 * 1000;   // 创建失败后退避，避免平台故障时每轮 cron 硬刷

// KV storage keys
const LIFECYCLE_KEY_PREFIX = "lifecycle:";

export interface LifecycleSafety {
  activeClients?: number;
  activeNodeUserIds?: string[];
  activeNodes?: Array<{ userId: string; connectedAt?: number; uptimeSeconds?: number; available?: boolean }>;
  destroyingCount?: number;
  protectLastConnector?: boolean;
}

// ─── 状态持久化（KV） ──────────────────────────────────────────

function lifecycleKey(userId: string): string {
  return `${LIFECYCLE_KEY_PREFIX}${userId}`;
}

export async function getLifecycleState(kv: KVNamespace, userId: string): Promise<LifecycleState> {
  const raw = await kv.get(lifecycleKey(userId), "text");
  if (raw) {
    try {
      return JSON.parse(raw);
    } catch {}
  }
  return {
    userId,
    phase: "idle",
    lastUpdate: Date.now(),
    servedCount: 0,
  };
}

async function saveLifecycleState(kv: KVNamespace, state: LifecycleState): Promise<void> {
  state.lastUpdate = Date.now();
  await kv.put(lifecycleKey(state.userId), JSON.stringify(state));
}

export async function deleteLifecycleState(kv: KVNamespace, userId: string): Promise<void> {
  await kv.delete(lifecycleKey(userId));
}

// ─── 核心：生命周期 tick ──────────────────────────────────────

export async function tick(
  kv: KVNamespace,
  userId: string,
  wsUrl: string,
  proxyUrl?: string,
  tunnelToken?: string,
  safety?: LifecycleSafety,
  bridgeHostHeader = "",
): Promise<{ action: string; error?: string; phase?: string }> {
  const state = await getLifecycleState(kv, userId);

  const store = new UserStoreKV(kv);
  const user = await store.getUser(userId);

  if (!user) {
    return { action: "skip", error: "用户不存在", phase: state.phase };
  }

  const manager = new ClawManager(user, proxyUrl);

  let result: { action: string; error?: string };

  switch (state.phase) {
    case "idle":
      result = await phaseIdle(state, manager, user, kv, wsUrl, proxyUrl, safety);
      break;
    case "creating":
      result = await phaseCreating(state, manager, user, kv, wsUrl, proxyUrl, safety);
      break;
    case "injecting": {
      const bridgeWsUrl = appendBridgeToken(wsUrl, await getBridgeToken(kv));
      result = await phaseInjecting(state, manager, user, kv, bridgeWsUrl, proxyUrl, tunnelToken, bridgeHostHeader);
      break;
    }
    case "running":
      result = await phaseRunning(state, manager, user, kv, safety, wsUrl, proxyUrl, tunnelToken, bridgeHostHeader);
      break;
    case "destroying":
      result = await phaseDestroying(state, manager, user, kv, proxyUrl, safety);
      break;
    case "error":
      result = await phaseError(state, manager, user, kv);
      break;
    default:
      result = { action: "skip", error: `未知状态: ${state.phase}` };
  }

  return { ...result, phase: state.phase };
}

// ─── 状态机各阶段 ────────────────────────────────────────────

async function phaseIdle(state: LifecycleState, manager: ClawManager, user: UserInfo, kv: KVNamespace, wsUrl: string, proxyUrl?: string, safety?: LifecycleSafety): Promise<{ action: string; error?: string }> {
  console.log(`[Lifecycle] 用户 ${user.name}: 空闲状态，检查可复用实例...`);

  const now = Date.now();
  if (state.nextActionAt && state.nextActionAt > now) {
    const cappedNextActionAt = (state.lastUpdate || now) + CREATE_FAILURE_BACKOFF_MS;
    if (cappedNextActionAt > now) {
      const waitSeconds = Math.ceil((cappedNextActionAt - now) / 1000);
      return { action: "wait_backoff", error: `上次创建失败，退避中，${waitSeconds}s 后重试` };
    }
    state.nextActionAt = undefined;
    await saveLifecycleState(kv, state);
  }

  const status = await manager.getStatus();

  // live bridge 已在线且控制面状态未知时，以运行时为准接管状态。
  // 明确 DESTROYED 不再接管：旧 bridge 可能还保持 WS 连接，但内部 API key 已失效，
  // 继续接管会导致“看似在线、实际没有实例创建”。
  if (!status.status && isUserActiveConnector(user, safety)) {
    console.log(`[Lifecycle] 用户 ${user.name}: 检测到 bridge 在线，接管为 running（控制面状态=${status.status || "UNKNOWN"}）`);
    state.phase = "running";
    state.lastError = undefined;
    state.currentRoundStart = getUserActiveNode(user, safety)?.connectedAt || state.currentRoundStart || Date.now();
    state.clawExpireAt = status.expireTime || estimateBridgeExpireAt(state, user, safety);
    state.bridgeMissingSince = undefined;
    state.bridgeOnlineAt = getUserActiveNode(user, safety)?.connectedAt || Date.now();
    await saveLifecycleState(kv, state);
    return { action: "adopt_running_bridge" };
  }

  // 有可复用实例
  if (status.status === "AVAILABLE" && status.expireTime) {
    const remainMs = status.expireTime - Date.now();
    if (remainMs > CLAW_REUSE_MIN_REMAIN_MS) {
      console.log(`[Lifecycle] 用户 ${user.name}: 发现可复用实例，剩余 ${Math.round(remainMs / 1000)}s`);
      state.phase = "injecting";
      state.clawExpireAt = status.expireTime;
      await saveLifecycleState(kv, state);
      return { action: "reuse" };
    }
  }

  // 创建新实例
  console.log(`[Lifecycle] 用户 ${user.name}: 开始创建新实例...`);
  state.phase = "creating";
  state.currentRoundStart = Date.now();
  state.lastError = undefined;
  state.nextActionAt = undefined;
  state.bridgeMissingSince = undefined;
  state.bridgeOnlineAt = undefined;
  state.injectionStage = undefined;
  await saveLifecycleState(kv, state);
  return phaseCreating(state, manager, user, kv, wsUrl, proxyUrl, safety);
}

async function phaseCreating(state: LifecycleState, manager: ClawManager, user: UserInfo, kv: KVNamespace, wsUrl: string, proxyUrl?: string, safety?: LifecycleSafety): Promise<{ action: string; error?: string }> {
  const status = await manager.getStatus();

  if (!status.status && isUserCurrentActiveConnector(user, safety, state)) {
    console.log(`[Lifecycle] 用户 ${user.name}: 创建阶段检测到本轮 bridge 已在线，接管为 running`);
    state.phase = "running";
    state.lastError = undefined;
    state.currentRoundStart = getUserActiveNode(user, safety)?.connectedAt || state.currentRoundStart || Date.now();
    state.clawExpireAt = status.expireTime || estimateBridgeExpireAt(state, user, safety);
    state.bridgeMissingSince = undefined;
    state.bridgeOnlineAt = getUserActiveNode(user, safety)?.connectedAt || Date.now();
    await saveLifecycleState(kv, state);
    return { action: "adopt_running_bridge" };
  }

  if (status.status === "AVAILABLE") {
    console.log(`[Lifecycle] 用户 ${user.name}: 实例已可用，进入注入阶段`);
    state.phase = "injecting";
    state.clawExpireAt = status.expireTime;
    await saveLifecycleState(kv, state);
    return { action: "inject" };
  }

  if (isTerminalFailedStatus(status.status)) {
    const err = formatStatusError("创建进入失败终态", status.status, status.message);
    console.warn(`[Lifecycle] 用户 ${user.name}: ${err}，清理失败态后重新发起创建`);
    await manager.destroy();

    const recreate = await manager.create();
    if (!recreate.ok) {
      const recreateErr = `${err}; 重建请求失败: ${recreate.error || "unknown"}`;
      state.phase = "idle";
      state.lastError = recreateErr;
      state.clawExpireAt = undefined;
      state.currentRoundStart = undefined;
      state.nextActionAt = Date.now() + CREATE_FAILURE_BACKOFF_MS;
      state.bridgeMissingSince = undefined;
      state.bridgeOnlineAt = undefined;
      state.injectionStage = undefined;
      await saveLifecycleState(kv, state);
      return { action: "recover_failed_create", error: recreateErr };
    }

    const newStatus = await manager.getStatus();
    if (newStatus.status === "AVAILABLE") {
      state.phase = "injecting";
      state.lastError = undefined;
      state.clawExpireAt = newStatus.expireTime;
      state.nextActionAt = undefined;
      await saveLifecycleState(kv, state);
      return { action: "inject" };
    }

    if (isTerminalFailedStatus(newStatus.status)) {
      const recreateErr = formatStatusError("重建后仍失败", newStatus.status, newStatus.message);
      state.phase = "idle";
      state.lastError = recreateErr;
      state.clawExpireAt = undefined;
      state.currentRoundStart = undefined;
      state.nextActionAt = Date.now() + CREATE_FAILURE_BACKOFF_MS;
      state.bridgeMissingSince = undefined;
      state.bridgeOnlineAt = undefined;
      state.injectionStage = undefined;
      await saveLifecycleState(kv, state);
      return { action: "recover_failed_create", error: recreateErr };
    }

    state.phase = "creating";
    state.lastError = undefined;
    state.clawExpireAt = newStatus.expireTime;
    state.nextActionAt = undefined;
    await saveLifecycleState(kv, state);
    return { action: "recreate_after_failed_status" };
  }

  // 还没有实例，发起创建
  if (!status.status || status.status === "DESTROYED" || status.status === "ERROR") {
    if (status.status && status.status !== "DESTROYED") {
      await manager.destroy();
    }

    const result = await manager.create();
    if (!result.ok) {
      console.error(`[Lifecycle] 用户 ${user.name}: 创建失败 - ${result.error}`);
      state.phase = "error";
      state.lastError = result.error;
      await saveLifecycleState(kv, state);
      return { action: "error", error: result.error };
    }

    const newStatus = await manager.getStatus();
    if (newStatus.status === "AVAILABLE") {
      state.phase = "injecting";
      state.clawExpireAt = newStatus.expireTime;
      await saveLifecycleState(kv, state);
      return { action: "inject" };
    }

    if (isTerminalFailedStatus(newStatus.status)) {
      const err = formatStatusError("创建后状态失败", newStatus.status, newStatus.message);
      console.warn(`[Lifecycle] 用户 ${user.name}: ${err}`);
      await manager.destroy();
      state.phase = "idle";
      state.lastError = err;
      state.clawExpireAt = undefined;
      state.currentRoundStart = undefined;
      state.nextActionAt = Date.now() + CREATE_FAILURE_BACKOFF_MS;
      state.bridgeMissingSince = undefined;
      state.bridgeOnlineAt = undefined;
      state.injectionStage = undefined;
      await saveLifecycleState(kv, state);
      return { action: "recover_failed_create", error: err };
    }

    state.phase = "creating";
    state.clawExpireAt = newStatus.expireTime;
    await saveLifecycleState(kv, state);
    return { action: "waiting_create" };
  }

  // 还在创建中
  return { action: "waiting_create" };
}

async function phaseInjecting(state: LifecycleState, manager: ClawManager, user: UserInfo, kv: KVNamespace, wsUrl: string, proxyUrl?: string, tunnelToken?: string, bridgeHostHeader = ""): Promise<{ action: string; error?: string }> {
  console.log(`[Lifecycle] 用户 ${user.name}: 准备注入 bridge.py...`);

  // 顺序必须严格：只有控制面确认实例 AVAILABLE 后，才允许获取 ticket 和注入 bridge。
  // 不能用“某个 WS 自报在线”反推实例创建成功。
  const status = await manager.getStatus();
  if (status.status !== "AVAILABLE") {
    const err = formatStatusError("注入前实例未就绪", status.status, status.message);

    if (isTerminalFailedStatus(status.status)) {
      console.warn(`[Lifecycle] 用户 ${user.name}: ${err}，销毁后回到 idle 等待重建`);
      await manager.destroy();
      state.phase = "idle";
      state.lastError = err;
      state.clawExpireAt = undefined;
      state.currentRoundStart = undefined;
      state.nextActionAt = Date.now() + CREATE_FAILURE_BACKOFF_MS;
      state.bridgeMissingSince = undefined;
      state.bridgeOnlineAt = undefined;
      state.injectionStage = undefined;
      await saveLifecycleState(kv, state);
      return { action: "recover_failed_create", error: err };
    }

    if (status.status === "CREATING" || !status.status) {
      state.phase = "creating";
      state.lastError = err;
      state.nextActionAt = undefined;
      await saveLifecycleState(kv, state);
      return { action: "wait_instance_ready", error: err };
    }

    if (status.status === "DESTROYING") {
      state.phase = "destroying";
      state.lastError = err;
      state.nextActionAt = undefined;
      await saveLifecycleState(kv, state);
      return { action: "wait_destroying", error: err };
    }

    // DESTROYED / ERROR 等都说明当前没有可注入实例，回到 idle 后按创建流程重来。
    if (status.status && status.status !== "DESTROYED") {
      await manager.destroy();
    }
    state.phase = "idle";
    state.lastError = err;
    state.clawExpireAt = undefined;
    state.currentRoundStart = undefined;
    state.nextActionAt = status.status === "DESTROYED" ? undefined : Date.now() + CREATE_FAILURE_BACKOFF_MS;
    state.bridgeMissingSince = undefined;
    state.bridgeOnlineAt = undefined;
    state.injectionStage = undefined;
    await saveLifecycleState(kv, state);
    return { action: "recover_instance_not_ready", error: err };
  }

  state.clawExpireAt = status.expireTime || state.clawExpireAt;

  const now = Date.now();
  const resetAlreadySent = state.injectionStage === "reset_sent";
  const needsResetBeforeBridge = state.currentRoundStart !== undefined && !resetAlreadySent;

  if (state.nextActionAt && state.nextActionAt > now) {
    const waitSeconds = Math.ceil((state.nextActionAt - now) / 1000);
    const action = resetAlreadySent ? "wait_after_reset" : "wait_injection_lease";
    const reason = resetAlreadySent
      ? `环境重置已发送，等待容器重启，${waitSeconds}s 后注入 bridge`
      : `已有注入任务运行中，等待 ${waitSeconds}s 后可重试`;
    return { action, error: reason };
  }

  state.nextActionAt = now + INJECTION_STEP_LEASE_MS;
  await saveLifecycleState(kv, state);

  const client = new ClawWsClient(user, proxyUrl);

  // 新建实例分两轮注入：第一轮只发送 reset，然后保存阶段并退出。
  // 避免在同一次 Worker scheduled 事件里 reset + 等重启 + 第二条注入，导致运行时间过长时只发出第一条消息。
  if (needsResetBeforeBridge) {
    const ticket = await manager.getTicket();
    if (!ticket) {
      const err = "获取 WS ticket 失败";
      state.phase = "error";
      state.lastError = err;
      state.nextActionAt = undefined;
      state.injectionStage = undefined;
      await saveLifecycleState(kv, state);
      return { action: "error", error: err };
    }

    if (!await connectWithRetry(client, ticket, MAX_CONNECT_RETRIES, 5000)) {
      const detail = client.getLastError();
      const err = `首次连接 Claw 失败${detail ? `: ${detail}` : ""}`;
      state.phase = "error";
      state.lastError = err;
      state.nextActionAt = undefined;
      state.injectionStage = undefined;
      await saveLifecycleState(kv, state);
      return { action: "error", error: err };
    }

    console.log(`[Lifecycle] 用户 ${user.name}: 发送环境重置指令...`);
    const resetReply = await client.sendMessage(RESET_CMD, 120);
    console.log(`[Lifecycle] 用户 ${user.name}: 环境重置反馈: ${resetReply?.substring(0, 200)}`);
    client.close();

    state.injectionStage = "reset_sent";
    state.nextActionAt = Date.now() + RECONNECT_WAIT_MS;
    await saveLifecycleState(kv, state);
    return { action: "reset_sent", error: "已发送环境重置，等待容器重启后下一轮注入 bridge" };
  }

  // 第二轮：新建实例 reset 后重新获取 ticket 连接；复用实例则直接连接并注入。
  const ticket = await manager.getTicket();
  if (!ticket) {
    const err = resetAlreadySent ? "重启后获取 WS ticket 失败" : "获取 WS ticket 失败";
    state.phase = "error";
    state.lastError = err;
    state.nextActionAt = undefined;
    state.injectionStage = undefined;
    await saveLifecycleState(kv, state);
    return { action: "error", error: err };
  }

  const connected = resetAlreadySent
    ? await connectWithRetry(client, ticket, MAX_RECONNECT_RETRIES, 8000)
    : await connectWithRetry(client, ticket, 3, 5000);
  if (!connected) {
    const detail = client.getLastError();
    const prefix = resetAlreadySent ? "重启后重连失败" : "复用连接失败";
    const err = `${prefix}${detail ? `: ${detail}` : ""}`;
    state.phase = "error";
    state.lastError = err;
    state.nextActionAt = undefined;
    state.injectionStage = undefined;
    await saveLifecycleState(kv, state);
    return { action: "error", error: err };
  }

  // 第二条业务消息：对齐原 Python 项目的 bridge 注入话术。
  // Cloudflare Tunnel 启动逻辑放在 bridge.py 内部，避免额外增加聊天消息。
  const injectPrompt = getBridgeInjectionPrompt(wsUrl, user.userId, tunnelToken || "", bridgeHostHeader);

  console.log(`[Lifecycle] 用户 ${user.name}: 发送 bridge 注入指令...`);
  const reply = await client.sendMessage(injectPrompt, 180);
  console.log(`[Lifecycle] 用户 ${user.name}: 注入反馈: ${reply?.substring(0, 200)}`);
  client.close();

  if (reply && reply.startsWith("(发送失败")) {
    const err = `bridge 注入消息未确认送达: ${reply}`;
    state.phase = "injecting";
    state.lastError = err;
    state.nextActionAt = Date.now() + INJECTION_STEP_LEASE_MS;
    await saveLifecycleState(kv, state);
    return { action: "retry_bridge_inject", error: err };
  }

  // 更新状态为运行中
  state.phase = "running";
  if (!state.currentRoundStart) state.currentRoundStart = Date.now();
  state.lastError = undefined;
  state.nextActionAt = undefined;
  state.injectionStage = undefined;
  state.bridgeMissingSince = undefined;
  await saveLifecycleState(kv, state);
  return { action: "running" };
}

function isUserActiveConnector(user: UserInfo, safety?: LifecycleSafety): boolean {
  return (safety?.activeNodes || []).some((node) => node.userId === user.userId && node.available !== false);
}

function isUserCurrentActiveConnector(user: UserInfo, safety: LifecycleSafety | undefined, state: LifecycleState): boolean {
  const node = getUserActiveNode(user, safety);
  if (!node) return false;
  if (state.currentRoundStart && node.connectedAt && node.connectedAt < state.currentRoundStart - 5 * 60 * 1000) return false;
  return node.available !== false;
}

function isTerminalFailedStatus(status: string | undefined): boolean {
  return !!status && status.endsWith("FAILED");
}

function formatStatusError(prefix: string, status: string | undefined, message: string | undefined): string {
  return `${prefix}: ${status || "UNKNOWN"}${message ? ` - ${message}` : ""}`;
}

function getUserActiveNode(user: UserInfo, safety?: LifecycleSafety): { userId: string; connectedAt?: number; uptimeSeconds?: number; available?: boolean } | undefined {
  return (safety?.activeNodes || []).find((node) => node.userId === user.userId && node.available !== false);
}

function estimateBridgeExpireAt(state: LifecycleState, user: UserInfo, safety?: LifecycleSafety): number | undefined {
  if (state.currentRoundStart) return state.currentRoundStart + CLAW_BRIDGE_FALLBACK_LIFETIME_MS;
  const node = getUserActiveNode(user, safety);
  const connectedAt = typeof node?.connectedAt === "number" ? node.connectedAt : undefined;
  if (connectedAt && connectedAt > 0) return connectedAt + CLAW_BRIDGE_FALLBACK_LIFETIME_MS;
  return undefined;
}

function isProtectedLastConnector(user: UserInfo, safety?: LifecycleSafety, state?: LifecycleState, now = Date.now()): boolean {
  if (!safety?.protectLastConnector) return false;
  if ((safety.activeClients || 0) > 1) return false;
  if (!isUserActiveConnector(user, safety)) return false;

  // 不保护已经过期/超过保守生命周期的最后 connector。
  // 否则旧 bridge 即使仍保持 WS 连接，也会阻止真正的实例创建。
  if (state?.clawExpireAt && state.clawExpireAt <= now) return false;
  if (state?.currentRoundStart && state.currentRoundStart + CLAW_BRIDGE_FALLBACK_LIFETIME_MS <= now) return false;

  return true;
}

async function phaseRunning(
  state: LifecycleState,
  manager: ClawManager,
  user: UserInfo,
  kv: KVNamespace,
  safety?: LifecycleSafety,
  wsUrl = "",
  proxyUrl?: string,
  tunnelToken?: string,
  bridgeHostHeader = "",
): Promise<{ action: string; error?: string }> {
  const now = Date.now();

  const status = await manager.getStatus();
  if (status.expireTime) {
    state.clawExpireAt = status.expireTime;
    if (!state.currentRoundStart) state.currentRoundStart = now;
    await saveLifecycleState(kv, state);
  }

  if (!status.status && isUserActiveConnector(user, safety)) {
    const estimatedExpireAt = estimateBridgeExpireAt(state, user, safety);
    if (estimatedExpireAt && (!state.clawExpireAt || state.clawExpireAt !== estimatedExpireAt)) {
      state.clawExpireAt = estimatedExpireAt;
      if (!state.currentRoundStart) {
        const node = getUserActiveNode(user, safety);
        state.currentRoundStart = typeof node?.connectedAt === "number" ? node.connectedAt : now;
      }
      await saveLifecycleState(kv, state);
    }
  }

  const expireAt = state.clawExpireAt || 0;
  const remainMs = expireAt - now;
  const hasActiveBridge = !safety || isUserActiveConnector(user, safety);

  if (hasActiveBridge) {
    const node = getUserActiveNode(user, safety);
    const bridgeOnlineAt = typeof node?.connectedAt === "number" ? node.connectedAt : (state.bridgeOnlineAt || now);
    if (state.bridgeMissingSince || state.bridgeOnlineAt !== bridgeOnlineAt) {
      state.bridgeMissingSince = undefined;
      state.bridgeOnlineAt = bridgeOnlineAt;
      await saveLifecycleState(kv, state);
    }
  }

  // 运行态 UI/调度需要可用 bridge，但 bridge WS 丢失不等价于容器里 bridge.py 没安装/没运行。
  // 网络、Cloudflare Tunnel、优选连接、WS 抖动都可能导致短暂断开；bridge.py 自身会重连。
  // 如果当前实例周期内已经见过 bridge 在线，说明容器内 bridge.py 和 cloudflared 已经正常跑过；
  // 后续离线只按“重新建立 bridge 连接”处理，不再重发注入消息。
  if (!hasActiveBridge) {
    if (status.status === "AVAILABLE" && remainMs > CLAW_REUSE_MIN_REMAIN_MS) {
      if (state.bridgeOnlineAt) {
        if (!state.bridgeMissingSince) {
          state.bridgeMissingSince = now;
          await saveLifecycleState(kv, state);
          console.warn(`[Lifecycle] 用户 ${user.name}: bridge 曾在线但当前离线，实例仍 AVAILABLE，等待重新建立连接`);
        }
        return { action: "wait_bridge_reconnect", error: "当前实例的 bridge 曾经在线，离线后只等待重新建立连接，不重发注入消息" };
      }

      console.warn(`[Lifecycle] 用户 ${user.name}: 实例 AVAILABLE 但 bridge 从未在线，重新注入第二条 bridge 消息`);
      state.phase = "injecting";
      state.currentRoundStart = undefined; // 实例已可用，只重发第二条 bridge 注入消息，不发送 reset
      state.bridgeMissingSince = undefined;
      state.nextActionAt = undefined;
      state.injectionStage = undefined;
      await saveLifecycleState(kv, state);
      const bridgeWsUrl = appendBridgeToken(wsUrl, await getBridgeToken(kv));
      return phaseInjecting(state, manager, user, kv, bridgeWsUrl, proxyUrl, tunnelToken, bridgeHostHeader);
    }

    console.warn(`[Lifecycle] 用户 ${user.name}: 运行态但无可用 bridge，回到 idle 重新创建`);
    state.phase = "idle";
    state.clawExpireAt = undefined;
    state.currentRoundStart = undefined;
    state.bridgeMissingSince = undefined;
    state.bridgeOnlineAt = undefined;
    state.injectionStage = undefined;
    await saveLifecycleState(kv, state);
    return { action: "recover_missing_bridge", error: "运行态无可用 bridge，已回到 idle" };
  }

  if (remainMs > CLAW_REUSE_BUFFER_MS) {
    return { action: "keep_running" };
  }

  if ((safety?.destroyingCount || 0) > 0) {
    return { action: "wait_existing_rotation", error: "已有账号正在轮换，暂缓本账号销毁以避免所有 connector 同时断开" };
  }

  if (isProtectedLastConnector(user, safety, state, now)) {
    return { action: "hold_last_connector", error: "当前账号是最后一个在线 connector，暂缓销毁，等待另一个账号接管" };
  }

  console.log(`[Lifecycle] 用户 ${user.name}: Claw 即将过期（剩余 ${Math.round(remainMs / 1000)}s），开始轮换...`);
  state.phase = "destroying";
  await saveLifecycleState(kv, state);
  return { action: "rotate" };
}

async function phaseDestroying(state: LifecycleState, manager: ClawManager, user: UserInfo, kv: KVNamespace, proxyUrl?: string, safety?: LifecycleSafety): Promise<{ action: string; error?: string }> {
  console.log(`[Lifecycle] 用户 ${user.name}: 销毁旧实例...`);

  if (isProtectedLastConnector(user, safety, state)) {
    state.phase = "running";
    await saveLifecycleState(kv, state);
    return { action: "hold_last_connector", error: "当前账号是最后一个在线 connector，取消本次销毁" };
  }

  // 尝试通过 AI 指令关机
  const status = await manager.getStatus();
  if (status.status === "AVAILABLE") {
    const client = new ClawWsClient(user, proxyUrl);
    const ticket = await manager.getTicket();
    if (ticket && await connectWithRetry(client, ticket, 3, 3000)) {
      const reply = await client.sendMessage(SHUTDOWN_PROMPT, MAX_SHUTDOWN_REPLY_SECONDS);
      console.log(`[Lifecycle] 用户 ${user.name}: 关机反馈: ${reply?.substring(0, 100)}`);

      if (reply && looksLikeShutdownConfirmation(reply)) {
        await client.sendMessage(SHUTDOWN_CONFIRM_PROMPT, 45);
      }
      client.close();
      await sleep(8000);
    }
  }

  // API 销毁
  await manager.destroy();

  // 回到空闲
  state.phase = "idle";
  state.clawExpireAt = undefined;
  state.currentRoundStart = undefined;
  state.bridgeMissingSince = undefined;
  state.bridgeOnlineAt = undefined;
  state.injectionStage = undefined;
  await saveLifecycleState(kv, state);
  return { action: "destroyed" };
}

async function phaseError(state: LifecycleState, manager: ClawManager, user: UserInfo, kv: KVNamespace): Promise<{ action: string; error?: string }> {
  console.warn(`[Lifecycle] 用户 ${user.name}: 错误状态 (${state.lastError})，尝试重置...`);

  await manager.destroy();

  state.phase = "idle";
  state.lastError = undefined;
  state.clawExpireAt = undefined;
  state.currentRoundStart = undefined;
  state.bridgeMissingSince = undefined;
  state.bridgeOnlineAt = undefined;
  state.injectionStage = undefined;
  await saveLifecycleState(kv, state);
  return { action: "reset" };
}

// ─── 辅助 ────────────────────────────────────────────────────

async function connectWithRetry(
  client: ClawWsClient,
  ticket: string,
  maxRetries: number,
  delayMs: number
): Promise<boolean> {
  for (let i = 0; i < maxRetries; i++) {
    if (await client.connect(ticket)) {
      return true;
    }
    if (i < maxRetries - 1) {
      await sleep(delayMs);
    }
  }
  return false;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function looksLikeShutdownConfirmation(reply: string): boolean {
  const text = reply.trim().toLowerCase();
  const keywords = [
    "确认", "请确认", "确认一下", "确定", "是否继续", "是否确认",
    "are you sure", "confirm", "确认关机", "确定要", "do you want",
  ];
  return keywords.some((kw) => text.includes(kw));
}

// ─── 轻量 UserStore（复用 KV，避免循环依赖 user-store） ──────

import type { Env } from "./types";

class UserStoreKV {
  constructor(private kv: KVNamespace) {}

  async getUser(userId: string): Promise<UserInfo | null> {
    const raw = await this.kv.get(`user:${userId}`, "text");
    if (!raw) return null;
    try {
      return JSON.parse(raw);
    } catch {
      return null;
    }
  }
}
