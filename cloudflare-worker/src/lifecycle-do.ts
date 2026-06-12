/**
 * LifecycleDurableObject — 每用户一个实例的生命周期管理
 *
 * 优势（相比普通 Worker handler）：
 * 1. 无 CPU 时间硬限制 — 不会因注入 bridge.py 耗时过长被杀
 * 2. 支持 WebSocket Hibernation — 可保持与 Claw 容器的长连接
 * 3. Alarm API — 精确计时，替代 setTimeout 轮询
 * 4. 持久化状态 — DO storage 存储生命周期状态，不依赖 KV 一致性
 *
 * 状态机：idle → creating → injecting → running → destroying → idle
 */

import { ClawManager } from "./claw-manager";
import { ClawWsClient, getBridgeInjectionPrompt, RESET_CMD, SHUTDOWN_PROMPT, SHUTDOWN_CONFIRM_PROMPT } from "./claw-ws-client";
import type { UserInfo, LifecycleState, LifecyclePhase } from "./types";
import { getGatewayEgressFetch, getControlProxyUrl } from "./control-channel";

// ─── 常量 ────────────────────────────────────────────────────────

const CLAW_REUSE_MIN_REMAIN_MS = 3 * 60 * 1000;  // 3 分钟最低复用寿命
const CLAW_REUSE_BUFFER_MS = 15 * 60 * 1000;      // 双账号接力模式提前 15 分钟轮换，避免 60 分钟硬销毁导致断链
const RECONNECT_WAIT_MS = 15 * 1000;               // 重启后等待 15s
const MAX_CONNECT_RETRIES = 5;
const MAX_RECONNECT_RETRIES = 10;
const MAX_SHUTDOWN_REPLY_SECONDS = 90;

// Alarm 间隔
const ALARM_INTERVAL_IDLE_MS = 60 * 1000;           // 空闲时 1 分钟后重试
const ALARM_INTERVAL_CREATING_MS = 10 * 1000;       // 创建中 10 秒检查一次
const ALARM_INTERVAL_INJECTING_MS = 5 * 1000;       // 注入中 5 秒
const ALARM_INTERVAL_RUNNING_MS = 2 * 60 * 1000;    // 运行中 2 分钟检查一次
const ALARM_INTERVAL_DESTROYING_MS = 10 * 1000;     // 销毁中 10 秒
const ALARM_INTERVAL_ERROR_MS = 30 * 1000;          // 错误 30 秒后重试

// DO storage keys
const STATE_KEY = "lifecycle_state";
const USER_KEY = "user_info";

// ─── LifecycleDurableObject ───────────────────────────────────────

export class LifecycleDurableObject implements DurableObject {
  private state: DurableObjectState;
  private env: Env;

  // 缓存的运行时状态
  private cachedState: LifecycleState | null = null;
  private cachedUser: UserInfo | null = null;
  private clawWs: ClawWsClient | null = null;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    // 触发 tick
    if (path === "/tick" && request.method === "POST") {
      const result = await this.tick();
      return Response.json(result);
    }

    // 获取状态
    if (path === "/state" && request.method === "GET") {
      const state = await this.getOrCreateState();
      return Response.json(state);
    }

    // 强制重建
    if (path === "/rebuild" && request.method === "POST") {
      const state = await this.getOrCreateState();
      state.phase = "destroying";
      state.lastError = undefined;
      await this.saveState(state);
      // 设置 alarm 立即触发
      await this.state.storage.setAlarm(Date.now());
      return Response.json({ ok: true, message: "重建信号已发送" });
    }

    // 更新用户信息
    if (path === "/user" && request.method === "PUT") {
      const user = await request.json() as UserInfo;
      await this.state.storage.put(USER_KEY, JSON.stringify(user));
      this.cachedUser = user;
      return Response.json({ ok: true });
    }

    // 删除（清理）
    if (path === "/purge" && request.method === "POST") {
      this.closeClawWs();
      await this.state.storage.deleteAll();
      this.cachedState = null;
      this.cachedUser = null;
      return Response.json({ ok: true });
    }

    return new Response("Not Found", { status: 404 });
  }

  // ─── Alarm handler ────────────────────────────────────────────

  async alarm(): Promise<void> {
    console.log(`[LifecycleDO] alarm 触发, userId=${await this.getUserId()}`);
    await this.tick();
  }

  // ─── 核心：生命周期 tick ──────────────────────────────────────

  async tick(): Promise<{ action: string; error?: string; phase?: string }> {
    const state = await this.getOrCreateState();
    const user = await this.getUser();

    if (!user) {
      return { action: "skip", error: "用户不存在", phase: state.phase };
    }

    const manager = new ClawManager(user, getControlProxyUrl(this.env), getGatewayEgressFetch(this.env));

    let result: { action: string; error?: string };

    switch (state.phase) {
      case "idle":
        result = await this.phaseIdle(state, manager, user);
        break;
      case "creating":
        result = await this.phaseCreating(state, manager, user);
        break;
      case "injecting":
        result = await this.phaseInjecting(state, manager, user);
        break;
      case "running":
        result = await this.phaseRunning(state, manager, user);
        break;
      case "destroying":
        result = await this.phaseDestroying(state, manager, user);
        break;
      case "error":
        result = await this.phaseError(state, manager, user);
        break;
      default:
        result = { action: "skip", error: `未知状态: ${state.phase}` };
    }

    // 根据 phase 设置下一次 alarm
    await this.scheduleNextAlarm(state.phase);

    return { ...result, phase: state.phase };
  }

  // ─── 状态机各阶段 ────────────────────────────────────────────

  /** 空闲 → 检查可复用实例 or 创建新实例 */
  private async phaseIdle(state: LifecycleState, manager: ClawManager, user: UserInfo): Promise<{ action: string; error?: string }> {
    console.log(`[LifecycleDO] 用户 ${user.name}: 空闲状态，检查可复用实例...`);

    const status = await manager.getStatus();

    // 有可复用实例
    if (status.status === "AVAILABLE" && status.expireTime) {
      const remainMs = status.expireTime - Date.now();
      if (remainMs > CLAW_REUSE_MIN_REMAIN_MS) {
        console.log(`[LifecycleDO] 用户 ${user.name}: 发现可复用实例，剩余 ${Math.round(remainMs / 1000)}s`);
        state.phase = "injecting";
        state.clawExpireAt = status.expireTime;
        await this.saveState(state);
        return { action: "reuse" };
      }
    }

    // 创建新实例
    console.log(`[LifecycleDO] 用户 ${user.name}: 开始创建新实例...`);
    state.phase = "creating";
    state.currentRoundStart = Date.now();
    await this.saveState(state);
    return { action: "create" };
  }

  /** 创建中 → 等待创建完成 → 注入 */
  private async phaseCreating(state: LifecycleState, manager: ClawManager, user: UserInfo): Promise<{ action: string; error?: string }> {
    const status = await manager.getStatus();

    if (status.status === "AVAILABLE") {
      console.log(`[LifecycleDO] 用户 ${user.name}: 实例已可用，进入注入阶段`);
      state.phase = "injecting";
      state.clawExpireAt = status.expireTime;
      await this.saveState(state);
      return { action: "inject" };
    }

    // 还没有实例，发起创建
    if (!status.status || status.status === "DESTROYED" || status.status === "ERROR") {
      if (status.status && status.status !== "DESTROYED") {
        await manager.destroy();
      }

      const result = await manager.create();
      if (!result.ok) {
        console.error(`[LifecycleDO] 用户 ${user.name}: 创建失败 - ${result.error}`);
        state.phase = "error";
        state.lastError = result.error;
        await this.saveState(state);
        return { action: "error", error: result.error };
      }

      const newStatus = await manager.getStatus();
      state.phase = "injecting";
      state.clawExpireAt = newStatus.expireTime;
      await this.saveState(state);
      return { action: "inject" };
    }

    // 还在创建中
    return { action: "waiting_create" };
  }

  /** 注入阶段 → 连接 Claw WS → 注入 bridge.py */
  private async phaseInjecting(state: LifecycleState, manager: ClawManager, user: UserInfo): Promise<{ action: string; error?: string }> {
    console.log(`[LifecycleDO] 用户 ${user.name}: 开始注入 bridge.py...`);

    const client = new ClawWsClient(user, getControlProxyUrl(this.env), getGatewayEgressFetch(this.env));

    // 获取 ticket
    const ticket = await manager.getTicket();
    if (!ticket) {
      const err = "获取 WS ticket 失败";
      state.phase = "error";
      state.lastError = err;
      await this.saveState(state);
      return { action: "error", error: err };
    }

    // 判断是否是复用场景
    const isReuse = state.currentRoundStart === undefined ||
      (Date.now() - (state.currentRoundStart || 0)) > 60000;

    // 发送环境重置（仅新建时）
    if (!isReuse) {
      if (!await this.connectWithRetry(client, ticket, MAX_CONNECT_RETRIES, 5000)) {
        const err = "首次连接 Claw 失败";
        state.phase = "error";
        state.lastError = err;
        await this.saveState(state);
        return { action: "error", error: err };
      }

      // 发送重置指令
      console.log(`[LifecycleDO] 用户 ${user.name}: 发送环境重置指令...`);
      await client.sendMessage(RESET_CMD, 120);
      client.close();

      // 等待重启
      await sleep(RECONNECT_WAIT_MS);

      // 重新获取 ticket 并连接
      const ticket2 = await manager.getTicket();
      if (!ticket2) {
        const err = "重启后获取 WS ticket 失败";
        state.phase = "error";
        state.lastError = err;
        await this.saveState(state);
        return { action: "error", error: err };
      }

      if (!await this.connectWithRetry(client, ticket2, MAX_RECONNECT_RETRIES, 8000)) {
        const err = "重启后重连失败";
        state.phase = "error";
        state.lastError = err;
        await this.saveState(state);
        return { action: "error", error: err };
      }
    } else {
      // 复用场景，直接连接
      if (!await this.connectWithRetry(client, ticket, 3, 5000)) {
        const err = "复用连接失败";
        state.phase = "error";
        state.lastError = err;
        await this.saveState(state);
        return { action: "error", error: err };
      }
    }

    // 第二条业务消息：对齐原 Python 项目的 bridge 注入话术。
    // Cloudflare Tunnel 启动逻辑放在 bridge.py 内部，避免额外增加聊天消息。
    const wsUrl = this.env.MIMO2API_WS_URL || "wss://placeholder.workers.dev/ws";
    const tunnelToken = this.env.MIMO_TUNNEL_TOKEN || "";
    const injectPrompt = getBridgeInjectionPrompt(wsUrl, user.userId, tunnelToken);

    console.log(`[LifecycleDO] 用户 ${user.name}: 发送 bridge 注入指令...`);
    const reply = await client.sendMessage(injectPrompt, 180);
    console.log(`[LifecycleDO] 用户 ${user.name}: 注入反馈: ${reply?.substring(0, 200)}`);
    client.close();

    // 保存 WS 连接备用
    this.clawWs = client;

    // 更新状态为运行中
    state.phase = "running";
    if (!state.currentRoundStart) state.currentRoundStart = Date.now();
    state.lastError = undefined;
    await this.saveState(state);
    return { action: "running" };
  }

  /** 运行中 → 检查是否需要轮换 */
  private async phaseRunning(state: LifecycleState, manager: ClawManager, user: UserInfo): Promise<{ action: string; error?: string }> {
    const now = Date.now();

    const status = await manager.getStatus();
    if (status.expireTime) {
      state.clawExpireAt = status.expireTime;
    }

    const expireAt = state.clawExpireAt || 0;
    const remainMs = expireAt - now;

    if (remainMs > CLAW_REUSE_BUFFER_MS) {
      return { action: "keep_running" };
    }

    console.log(`[LifecycleDO] 用户 ${user.name}: Claw 即将过期（剩余 ${Math.round(remainMs / 1000)}s），开始轮换...`);
    state.phase = "destroying";
    await this.saveState(state);
    return { action: "rotate" };
  }

  /** 销毁中 → 销毁旧实例 → 回到空闲 */
  private async phaseDestroying(state: LifecycleState, manager: ClawManager, user: UserInfo): Promise<{ action: string; error?: string }> {
    console.log(`[LifecycleDO] 用户 ${user.name}: 销毁旧实例...`);

    // 关闭已持有的 WS
    this.closeClawWs();

    // 尝试通过 AI 指令关机
    const status = await manager.getStatus();
    if (status.status === "AVAILABLE") {
      const client = new ClawWsClient(user, getControlProxyUrl(this.env), getGatewayEgressFetch(this.env));
      const ticket = await manager.getTicket();
      if (ticket && await this.connectWithRetry(client, ticket, 3, 3000)) {
        const reply = await client.sendMessage(SHUTDOWN_PROMPT, MAX_SHUTDOWN_REPLY_SECONDS);
        console.log(`[LifecycleDO] 用户 ${user.name}: 关机反馈: ${reply?.substring(0, 100)}`);

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
    await this.saveState(state);
    return { action: "destroyed" };
  }

  /** 错误状态 → 尝试销毁 → 回到空闲 */
  private async phaseError(state: LifecycleState, manager: ClawManager, user: UserInfo): Promise<{ action: string; error?: string }> {
    console.warn(`[LifecycleDO] 用户 ${user.name}: 错误状态 (${state.lastError})，尝试重置...`);

    this.closeClawWs();
    await manager.destroy();

    state.phase = "idle";
    state.lastError = undefined;
    state.clawExpireAt = undefined;
    state.currentRoundStart = undefined;
    await this.saveState(state);
    return { action: "reset" };
  }

  // ─── Alarm 调度 ──────────────────────────────────────────────

  private async scheduleNextAlarm(phase: LifecyclePhase): Promise<void> {
    let delayMs: number;

    switch (phase) {
      case "idle":
        delayMs = ALARM_INTERVAL_IDLE_MS;
        break;
      case "creating":
        delayMs = ALARM_INTERVAL_CREATING_MS;
        break;
      case "injecting":
        delayMs = ALARM_INTERVAL_INJECTING_MS;
        break;
      case "running":
        delayMs = ALARM_INTERVAL_RUNNING_MS;
        break;
      case "destroying":
        delayMs = ALARM_INTERVAL_DESTROYING_MS;
        break;
      case "error":
        delayMs = ALARM_INTERVAL_ERROR_MS;
        break;
      default:
        delayMs = ALARM_INTERVAL_IDLE_MS;
    }

    await this.state.storage.setAlarm(Date.now() + delayMs);
  }

  // ─── 状态持久化 ──────────────────────────────────────────────

  private async getOrCreateState(): Promise<LifecycleState> {
    if (this.cachedState) return this.cachedState;

    const raw = await this.state.storage.get<string>(STATE_KEY);
    if (raw) {
      try {
        this.cachedState = JSON.parse(raw);
        return this.cachedState!;
      } catch {}
    }

    const userId = await this.getUserId();
    this.cachedState = {
      userId,
      phase: "idle",
      lastUpdate: Date.now(),
      servedCount: 0,
    };
    return this.cachedState;
  }

  private async saveState(state: LifecycleState): Promise<void> {
    state.lastUpdate = Date.now();
    this.cachedState = state;
    await this.state.storage.put(STATE_KEY, JSON.stringify(state));
  }

  private async getUser(): Promise<UserInfo | null> {
    if (this.cachedUser) return this.cachedUser;

    const raw = await this.state.storage.get<string>(USER_KEY);
    if (!raw) return null;
    try {
      this.cachedUser = JSON.parse(raw);
      return this.cachedUser;
    } catch {
      return null;
    }
  }

  private async getUserId(): Promise<string> {
    const user = await this.getUser();
    return user?.userId || "unknown";
  }

  // ─── 辅助 ────────────────────────────────────────────────────

  private async connectWithRetry(
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

  private closeClawWs(): void {
    if (this.clawWs) {
      try {
        this.clawWs.close();
      } catch {}
      this.clawWs = null;
    }
  }
}

// ─── Env 接口（与 types.ts 保持一致） ─────────────────────────────

interface Env {
  GATEWAY: DurableObjectNamespace;
  LIFECYCLE: DurableObjectNamespace;
  MIMO_KV: KVNamespace;
  MIMO_RELAY_OPENAI_KEY?: string;
  MIMO_WEBUI_USERNAME?: string;
  MIMO_WEBUI_PASSWORD?: string;
  MODEL_MAPPING_JSON?: string;
  MIMO2API_WS_URL?: string;
  EGRESS?: { fetch(input: string | Request | URL, init?: RequestInit): Promise<Response> };
  MIMO_CONTROL_CHANNEL?: string;
  USE_VPC_EGRESS?: string;
  MIMO_PROXY_URL?: string;  // Tunnel 代理 URL（替换 aistudio.xiaomimimo.com 域名）
  MIMO_TUNNEL_TOKEN?: string;  // Tunnel Token（注入到容器中）
}

// ─── 辅助函数 ────────────────────────────────────────────────────

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
