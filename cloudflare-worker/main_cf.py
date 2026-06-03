#!/usr/bin/env python3
"""
mimo2api Cloudflare Worker 版本 - 统一启动入口

启动 Manager，自动管理 Claw 容器并注入 bridge_cf.py

使用方法：
    1. 复制 env.example 为 .env 并配置
    2. 运行: python main_cf.py
"""
import os
import sys
import logging
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 配置检查
CF_WS_URL = os.getenv("CF_WS_URL")
MIMO_API_KEY = os.getenv("MIMO_API_KEY")
MIMO_API_ENDPOINT = os.getenv("MIMO_API_ENDPOINT")

print("=" * 60)
print("🚀 mimo2api Cloudflare Worker 版本")
print("=" * 60)

if not CF_WS_URL:
    print("❌ 错误: CF_WS_URL 未配置")
    print("   请在 .env 中设置: CF_WS_URL=wss://your-worker.workers.dev/ws")
    sys.exit(1)

if not MIMO_API_KEY:
    print("⚠️  警告: MIMO_API_KEY 未配置，将从容器内环境变量读取")

if not MIMO_API_ENDPOINT:
    print("⚠️  警告: MIMO_API_ENDPOINT 未配置，将从容器内环境变量读取")

print(f"📡 Cloudflare Worker: {CF_WS_URL}")
print(f"🔑 MIMO API Key: {'已配置' if MIMO_API_KEY else '未配置'}")
print(f"🌐 MIMO API Endpoint: {MIMO_API_ENDPOINT or '未配置'}")
print("=" * 60)

# 配置日志
log_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "manager_cf.log")

from logging.handlers import RotatingFileHandler

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.handlers.clear()

fmt = logging.Formatter("%(asctime)s - [%(name)s] - %(levelname)s - %(message)s")

sh = logging.StreamHandler()
sh.setFormatter(fmt)
root_logger.addHandler(sh)

fh = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8")
fh.setFormatter(fmt)
root_logger.addHandler(fh)

# 启动 Manager
from manager_cf import start_manager_tasks
import asyncio

if __name__ == "__main__":
    asyncio.run(start_manager_tasks())
