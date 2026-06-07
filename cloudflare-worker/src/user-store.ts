/**
 * 用户配置存储模块
 *
 * 使用 Cloudflare KV 替代原 Python 版的 users/ 目录。
 * 每个 userId 对应一条 KV 记录，值是 JSON 格式的用户信息。
 *
 * KV Key 规范：
 *   user:{userId}  →  UserInfo JSON
 *   user:list      →  userId[] JSON (用户 ID 列表索引)
 */

import { UserInfo } from "./types";

const KEY_PREFIX = "user:";
const KEY_LIST = "user:list";

export class UserStore {
  constructor(private kv: KVNamespace) {}

  /** 获取所有用户 ID 列表 */
  async listUserIds(): Promise<string[]> {
    const raw = await this.kv.get(KEY_LIST, "text");
    if (!raw) return [];
    try {
      return JSON.parse(raw);
    } catch {
      return [];
    }
  }

  /** 获取所有用户 */
  async listAllUsers(): Promise<UserInfo[]> {
    const ids = await this.listUserIds();
    const users: UserInfo[] = [];
    for (const id of ids) {
      const u = await this.getUser(id);
      if (u) users.push(u);
    }
    return users;
  }

  /** 获取单个用户 */
  async getUser(userId: string): Promise<UserInfo | null> {
    const raw = await this.kv.get(`${KEY_PREFIX}${userId}`, "text");
    if (!raw) return null;
    try {
      return JSON.parse(raw);
    } catch {
      return null;
    }
  }

  /** 添加或更新用户 */
  async putUser(user: UserInfo): Promise<void> {
    await this.kv.put(`${KEY_PREFIX}${user.userId}`, JSON.stringify(user));

    // 更新索引
    const ids = await this.listUserIds();
    if (!ids.includes(user.userId)) {
      ids.push(user.userId);
      await this.kv.put(KEY_LIST, JSON.stringify(ids));
    }
  }

  /** 删除用户 */
  async deleteUser(userId: string): Promise<boolean> {
    const existing = await this.getUser(userId);
    if (!existing) return false;

    await this.kv.delete(`${KEY_PREFIX}${userId}`);

    // 更新索引
    const ids = await this.listUserIds();
    const newIds = ids.filter((id) => id !== userId);
    await this.kv.put(KEY_LIST, JSON.stringify(newIds));
    return true;
  }

  /** 批量导入用户（从原 Python 版 users/*.json 格式） */
  async importUsers(users: UserInfo[]): Promise<{ imported: number; skipped: number }> {
    let imported = 0;
    let skipped = 0;
    const existingIds = await this.listUserIds();

    for (const user of users) {
      if (!user.userId || !user.serviceToken || !user.xiaomichatbot_ph) {
        skipped++;
        continue;
      }
      await this.kv.put(`${KEY_PREFIX}${user.userId}`, JSON.stringify(user));
      if (!existingIds.includes(user.userId)) {
        existingIds.push(user.userId);
      }
      imported++;
    }

    await this.kv.put(KEY_LIST, JSON.stringify(existingIds));
    return { imported, skipped };
  }
}
