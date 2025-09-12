# data/ws_worker.py
# -*- coding: utf-8 -*-
"""
Binance Futures WebSocket → Redis bridge для AiOne_t
===================================================

Приймає 1m-бар з Binance, пише в Redis (TTL=90с), при закритті bar[x] — ресемпл у 1h,
публікує через Pub/Sub. Символи — з Redis selector (prefilter) або з ENV.
"""

import asyncio
import json
import logging
import os
from typing import Any, Dict, List, Optional, Set

import pandas as pd
import websockets
from rich.console import Console
from rich.logging import RichHandler

from .cache_handler import SimpleCacheHandler
from .raw_data import _df_to_bytes, _bytes_to_df
from .ram_buffer import RAMBuffer

# --- Налаштування/константи ---
CACHE = SimpleCacheHandler(redis_url=os.getenv("REDIS_URL"))
PARTIAL_CHANNEL = "klines.1m.partial"
FINAL_CHANNEL = "klines.1h.update"
TTL_1M = 90
TTL_1H = 65 * 60
SELECTOR_KEY = "selector:active:stream"
SELECTOR_REFRESH_S = 30

STATIC_SYMBOLS = os.getenv("STREAM_SYMBOLS", "")
DEFAULT_SYMBOLS = [s.lower() for s in STATIC_SYMBOLS.split(",") if s] or ["btcusdt"]

# --- Логування ---
logger = logging.getLogger("ws_worker")
logger.setLevel(logging.WARNING)
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False

logging.getLogger("websockets").setLevel(logging.WARNING)
logging.getLogger("websockets.client.protocol").setLevel(logging.WARNING)

# ────────────────────── WS Worker ──────────────────────


class WSWorker:
    """
    WebSocket-воркер: стрім 1m-барів Binance → RAMBuffer (для stage1) і Redis (fallback).
    """

    def __init__(
        self,
        symbols: Optional[List[str]] = None,
        ram_buffer: Optional[RAMBuffer] = None,
        redis_cache: Optional[Any] = None,
    ):
        self._symbols: Set[str] = set(
            [s.lower() for s in symbols] if symbols else DEFAULT_SYMBOLS
        )
        self._ws_url: Optional[str] = None
        self._backoff: int = 3
        self._refresh_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self.buffer = ram_buffer if ram_buffer is not None else RAMBuffer(max_bars=120)
        self.cache = redis_cache if redis_cache is not None else CACHE

    async def _get_live_symbols(self) -> List[str]:
        """
        Отримує whitelist символів (prefilter із Redis або ENV).
        """
        data = await self.cache.get_fast_symbols()  # ← використовує self.cache!
        # Дефолт — якщо нічого не прийшло, або тип невідомий
        syms = []
        if isinstance(data, dict):
            syms = list(data.keys())
        elif isinstance(data, list):
            syms = data
        # Fallback якщо порожній
        if not syms:
            logger.warning(
                "[WSWorker] selector:active:stream пустий або невалидний, fallback btcusdt"
            )
            syms = DEFAULT_SYMBOLS
        if len(syms) < 3:
            logger.warning(
                "[WSWorker] ВАЖЛИВО: Кількість symbols у стрімі підозріло мала: %d (%s)",
                len(syms),
                syms,
            )
        logger.debug(
            "[WSWorker][_get_live_symbols] data type: %s, value: %s",
            type(data),
            str(data)[:200],
        )
        logger.debug("[WSWorker] Символи для стріму: %d (%s...)", len(syms), syms[:10])
        return syms

    def _build_ws_url(self, symbols: Set[str]) -> str:
        streams = "/".join(f"{s}@kline_1m" for s in sorted(symbols))
        return f"wss://fstream.binance.com/stream?streams={streams}"

    async def _store_minute(self, sym: str, ts: int, k: Dict[str, Any]) -> pd.DataFrame:
        """Оновлює 1m DataFrame в Redis."""
        raw = await CACHE.fetch_from_cache(sym, "1m", prefix="candles", raw=True)
        df = (
            _bytes_to_df(raw)
            if raw
            else pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
        )
        dt = pd.to_datetime(ts, unit="ms", utc=True)
        df.loc[dt] = [
            float(k["o"]),
            float(k["h"]),
            float(k["l"]),
            float(k["c"]),
            float(k["v"]),
        ]
        await CACHE.store_in_cache(
            sym, "1m", _df_to_bytes(df), ttl=TTL_1M, prefix="candles", raw=True
        )
        return df

    async def _on_final_candle(self, sym: str, df_1m: pd.DataFrame) -> None:
        """Агрегує 1h-бар, зберігає у Redis, публікує подію."""
        df_1h = (
            df_1m.resample("1h", label="right", closed="right")
            .agg(
                {
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "volume": "sum",
                }
            )
            .dropna()
        )
        await CACHE.store_in_cache(
            sym, "1h", _df_to_bytes(df_1h), ttl=TTL_1H, prefix="candles", raw=True
        )
        await CACHE.client.publish(FINAL_CHANNEL, sym)
        logger.debug("[%s] 1h closed → published %s", sym, FINAL_CHANNEL)

    async def _handle_kline(self, k: Dict[str, Any]) -> None:
        """
        Обробка WS kline:
        - зберігає 1m в RAMBuffer (оновлення bar по timestamp, без дублювання),
        - при закритті bar[x] — агрегує у 1h.
        """
        sym = k["s"].lower()
        ts = int(k["t"])
        tf = "1m"
        bar = {
            "open": float(k["o"]),
            "high": float(k["h"]),
            "low": float(k["l"]),
            "close": float(k["c"]),
            "volume": float(k["v"]),
            "timestamp": ts,
        }
        # Додаємо/оновлюємо бар у RAMBuffer (тільки якщо новий timestamp або partial update)
        self.buffer.add(sym, tf, bar)
        # Логувати тільки при новому bar (x==True) або зміні timestamp
        if k.get("x", False):
            logger.debug(
                "[WSWorker][heartbeat] FINAL bar у RAMBuffer: %s (%d)", sym, ts
            )
        elif (
            self.buffer.data[sym][tf]
            and self.buffer.data[sym][tf][-1]["timestamp"] == ts
        ):
            logger.debug("[WSWorker][heartbeat] partial update %s (%d)", sym, ts)

        # Запис у Redis (як fallback і для stage2+)
        df_1m = await self._store_minute(sym, ts, k)
        await CACHE.client.publish(PARTIAL_CHANNEL, sym)
        if k.get("x"):
            await self._on_final_candle(sym, df_1m)

    async def _refresh_symbols(self, ws: websockets.WebSocketClientProtocol) -> None:
        """Фоновий таск: refresh whitelist і ресабскрайб."""
        while not self._stop_event.is_set():
            await asyncio.sleep(SELECTOR_REFRESH_S)
            try:
                new_syms = set(await self._get_live_symbols())
                if new_syms and new_syms != self._symbols:
                    await self._resubscribe(ws, new_syms)
            except Exception as e:
                logger.warning("Refresh symbols error: %s", e)

    async def _resubscribe(
        self, ws: websockets.WebSocketClientProtocol, new_syms: Set[str]
    ) -> None:
        """UNSUBSCRIBE/SUBSCRIBE WS-канали без reconnect."""
        old_syms = self._symbols
        to_unsub = [f"{s}@kline_1m" for s in old_syms - new_syms]
        to_sub = [f"{s}@kline_1m" for s in new_syms - old_syms]
        rid = 1
        if to_unsub:
            await ws.send(
                json.dumps({"method": "UNSUBSCRIBE", "params": to_unsub, "id": rid})
            )
            rid += 1
        if to_sub:
            await ws.send(
                json.dumps({"method": "SUBSCRIBE", "params": to_sub, "id": rid})
            )
            logger.debug(
                "WS re-subscribed +%d -%d total=%d",
                len(to_sub),
                len(to_unsub),
                len(new_syms),
            )
        self._symbols = new_syms

    async def consume(self) -> None:
        """Головний цикл: підключення, обробка WS, reconnect/backoff."""
        while not self._stop_event.is_set():
            try:
                # 1. Завжди визначаємо syms, fallback на {"btcusdt"}
                syms = set(await self._get_live_symbols() or [])
                if not syms:
                    syms = {"btcusdt"}
                # 2. Оновлюємо ws_url лише якщо символи змінились
                if syms != self._symbols or not self._ws_url:
                    self._symbols = syms
                    self._ws_url = self._build_ws_url(self._symbols)

                logger.info(
                    "🔄 Запуск WS (%d symbols): %s",
                    len(self._symbols),
                    list(self._symbols)[:5],
                )
                async with websockets.connect(self._ws_url, ping_interval=20) as ws:
                    logger.debug("WS connected (%d streams)…", len(self._symbols))
                    self._backoff = 3
                    self._stop_event.clear()
                    self._refresh_task = asyncio.create_task(self._refresh_symbols(ws))
                    async for msg in ws:
                        try:
                            data = json.loads(msg).get("data", {}).get("k")
                            if data:
                                await self._handle_kline(data)
                        except Exception as e:
                            try:
                                msg_for_log = msg
                                if isinstance(msg, dict):
                                    msg_for_log = json.dumps(msg, default=str)[:80]
                                logger.debug(
                                    "Bad WS message: %s… (%s)", str(msg)[:200], e
                                )
                            except Exception as e2:
                                logger.debug("Bad WS message: <unserializable> (%s)", e)
            except Exception as exc:
                logger.warning("WS error: %s → reconnect in %ds", exc, self._backoff)
                await asyncio.sleep(self._backoff)
                self._backoff = min(self._backoff * 2, 30)
            finally:
                if self._refresh_task:
                    self._refresh_task.cancel()

    async def stop(self) -> None:
        """Зупиняє воркер і всі фонові таски."""
        self._stop_event.set()
        if self._refresh_task:
            self._refresh_task.cancel()


# ──────────────── Запуск модуля ────────────────

if __name__ == "__main__":
    worker = WSWorker()
    try:
        asyncio.run(worker.consume())
    except KeyboardInterrupt:
        logger.info("WSWorker stopped by user")
    except Exception as e:
        logger.error("Fatal error: %s", e, exc_info=True)
