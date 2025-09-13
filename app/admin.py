"""Адмін-команди для DataStore / процесу через Redis Pub/Sub.

Шлях: ``app/admin.py``

Підтримувані операції (JSON payload → ``op``):
    • dump_status      — знімок стану процесу та DataStore
    • set_profile      — гаряче оновлення полів профілю (ram_limit_mb, тощо)
    • set_priority     — встановлення пріоритету символу (евікшен у RAM)
    • flush_now        — примусовий дренаж write-behind черги
"""

import json
import asyncio
import logging
import os
import time
from typing import Any, Dict

import psutil
from rich.console import Console
from rich.logging import RichHandler

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("app.admin")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False


class DataStoreAdmin:
    def __init__(self, store, redis, cfg):
        self.store = store
        self.redis = redis
        self.cfg = cfg

    async def dump_status(self) -> Dict[str, Any]:
        p = psutil.Process(os.getpid())
        rss = p.memory_info().rss
        cpu = p.cpu_percent(interval=0.1)
        open_fds = p.num_fds() if hasattr(p, "num_fds") else None

        ram = self.store.ram.stats
        backlog = len(self.store._flush_q)

        # опціонально: порахувати приблизно ключі Redis із namespace
        redis_keys = None
        try:
            redis_keys = await self.redis.r.dbsize()
        except Exception:
            pass

        return {
            "ts": time.time(),
            "process": {"rss": rss, "cpu_percent": cpu, "open_fds": open_fds},
            "datastore": {
                "ram": ram,
                "flush_backlog": backlog,
                "ram_hits": self.store._ram_hits,
                "ram_miss": self.store._ram_miss,
                "redis_hits": self.store._redis_hits,
                "redis_miss": self.store._redis_miss,
            },
            "redis": {"dbsize": redis_keys},
        }


async def admin_command_loop(admin: DataStoreAdmin):
    ch = admin.cfg.admin.commands_channel
    pubsub = admin.redis.r.pubsub()
    try:
        await pubsub.subscribe(ch)
        logger.info(f"🛠  Admin commands listening on {ch}")
        async for msg in pubsub.listen():
            if msg.get("type") != "message":
                continue
            try:
                cmd = json.loads(msg["data"])
            except Exception:
                logger.error("Invalid admin command payload")
                continue

            op = (cmd.get("op") or "").lower()
            if op == "dump_status":
                status = await admin.dump_status()
                logger.info(
                    "[ADMIN] dump_status → %s", json.dumps(status, ensure_ascii=False)
                )
            elif op == "set_profile":
                prof = cmd.get("profile") or {}
                for k, v in prof.items():
                    setattr(admin.store.cfg.profile, k, v)
                logger.warning("[ADMIN] set_profile → %s", prof)
            elif op == "set_priority":
                sym = cmd.get("symbol")
                lvl = int(cmd.get("level", 1))
                admin.store.set_priority(sym, lvl)
                logger.info("[ADMIN] set_priority %s → %s", sym, lvl)
            elif op == "flush_now":
                await admin.store._drain_flush_queue(force=True)
                logger.info("[ADMIN] flush_now executed")
            else:
                logger.warning("Unknown admin op: %s", op)
    except asyncio.CancelledError:  # graceful cancellation
        logger.info("Admin command loop cancelled")
    finally:
        try:
            await pubsub.unsubscribe(ch)
        except Exception:
            pass
        try:
            await pubsub.close()
        except Exception:
            pass
