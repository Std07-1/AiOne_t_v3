"""Binance Futures WebSocket воркер → UnifiedDataStore / Redis.

Шлях: ``data/ws_worker.py``

Призначення:
    • отримує потік 1m kline через Binance Futures WS;
    • оновлює хвилинну історію (legacy blob для швидкого зчитування + UnifiedDataStore.put_bars);
    • при фіналізації хвилини агрегує 1h бар і публікує оновлення (``klines.1h.update``);
    • підтримує динамічний whitelist символів (prefilter через Redis селектор).

Особливості:
    • reconnect із експоненційним backoff;
    • періодичний refresh символів без повного reconnect (UNSUBSCRIBE/SUBSCRIBE);
    • fallback на дефолтні символи, якщо селектор порожній чи некоректний.
"""

import asyncio
import json
import logging
import os
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
import websockets
from rich.console import Console
from rich.logging import RichHandler
import orjson
from lz4.frame import compress, decompress


# ── Вбудовані (мінімальні) серіалізатори DataFrame (видалено raw_data.py) ──
def _df_to_bytes(df: pd.DataFrame, *, compress_lz4: bool = True) -> bytes:
    """Серіалізація DataFrame → bytes (orjson + опційно LZ4).

    Тільки колонки та індекс: використовує формат orient="split".
    Якщо є datetime колонка `timestamp`, перетворюємо у int64 ms для JS.
    """
    df_out = df.copy()
    if "timestamp" in df_out.columns and pd.api.types.is_datetime64_any_dtype(
        df_out["timestamp"]
    ):
        df_out["timestamp"] = (df_out["timestamp"].astype("int64") // 1_000_000).astype(
            "int64"
        )
    raw_json = orjson.dumps(df_out.to_dict(orient="split"))
    return compress(raw_json) if compress_lz4 else raw_json


def _bytes_to_df(buf: bytes | str, *, compressed: bool = True) -> pd.DataFrame:
    """Десеріалізація bytes → DataFrame (reverse _df_to_bytes)."""
    raw = buf.encode() if isinstance(buf, str) else buf
    if compressed:
        raw = decompress(raw)
    obj = orjson.loads(raw)
    df = pd.DataFrame(**obj)
    if "timestamp" in df.columns and pd.api.types.is_integer_dtype(df["timestamp"]):
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df


from data.unified_store import (
    UnifiedDataStore,
)  # unified store (single source of truth)

# --- Налаштування/константи ---
PARTIAL_CHANNEL = "klines.1m.partial"
FINAL_CHANNEL = "klines.1h.update"
# Default TTLs for legacy blob snapshot (NOT the canonical ds.cfg.intervals_ttl)
DEFAULT_INTERVALS_TTL: Dict[str, int] = {"1m": 90, "1h": 65 * 60}
SELECTOR_REFRESH_S = 30  # how often to refresh symbol whitelist

STATIC_SYMBOLS = os.getenv("STREAM_SYMBOLS", "")
DEFAULT_SYMBOLS = [s.lower() for s in STATIC_SYMBOLS.split(",") if s] or ["btcusdt"]

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("app.data.ws_worker")
if not logger.handlers:
    logger.setLevel(logging.WARNING)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False

logging.getLogger("websockets").setLevel(logging.WARNING)
logging.getLogger("websockets.client.protocol").setLevel(logging.WARNING)

# ────────────────────── WS Worker ──────────────────────


class WSWorker:
    """WebSocket worker streaming 1m Binance klines directly into UnifiedDataStore.

    Legacy SimpleCacheHandler / RAMBuffer are removed. Minute bars persisted via
    store.put_bars; last bar & fast symbols served from Redis through the store.
    """

    def __init__(
        self,
        symbols: Optional[List[str]] = None,
        *,
        store: UnifiedDataStore,
        selectors_key: Optional[str] = None,
        intervals_ttl: Optional[Dict[str, int]] = None,
    ):
        if store is None:
            raise ValueError("WSWorker requires a UnifiedDataStore instance")
        self.store = store
        self._symbols: Set[str] = set(
            [s.lower() for s in symbols] if symbols else DEFAULT_SYMBOLS
        )
        # optional override for selector redis key ("part1:part2:..")
        self._selectors_key: Optional[Tuple[str, ...]] = (
            tuple(p for p in selectors_key.split(":") if p) if selectors_key else None
        )
        # TTLs for legacy blob snapshots; fall back to defaults above
        mapping = DEFAULT_INTERVALS_TTL.copy()
        if intervals_ttl:
            mapping.update(intervals_ttl)
        self._ttl_1m = mapping.get("1m", DEFAULT_INTERVALS_TTL["1m"])
        self._ttl_1h = mapping.get("1h", DEFAULT_INTERVALS_TTL["1h"])
        self._ws_url: Optional[str] = None
        self._backoff: int = 3
        self._refresh_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    async def _get_live_symbols(self) -> List[str]:
        """Fetch whitelist symbols either via custom selectors_key or store helper.

        selectors_key (if provided) is a colon-delimited path inside the store namespace
        e.g. "selectors:fast_symbols" -> ai_one:selectors:fast_symbols
        """
        if self._selectors_key:
            try:
                data = await self.store.redis.jget(*self._selectors_key, default=[])
            except Exception as e:  # pragma: no cover
                logger.warning("[WSWorker] selectors_key fetch failed: %s", e)
                data = []
        else:
            data = await self.store.get_fast_symbols()
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
        """Incrementally update 1m history snapshot blob (legacy format) for quick replay.

        This keeps backward compatibility for any code still reading the old serialized
        frame while the canonical last bar lives in store.redis (jset candles...).
        """
        raw = await self.store.fetch_from_cache(sym, "1m", prefix="candles", raw=True)
        if raw:
            try:
                df = _bytes_to_df(raw)
            except Exception:
                df = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
        else:
            df = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
        dt = pd.to_datetime(ts, unit="ms", utc=True)
        df.loc[dt] = [
            float(k["o"]),
            float(k["h"]),
            float(k["l"]),
            float(k["c"]),
            float(k["v"]),
        ]
        await self.store.store_in_cache(
            sym, "1m", _df_to_bytes(df), ttl=self._ttl_1m, prefix="candles", raw=True
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
        await self.store.store_in_cache(
            sym, "1h", _df_to_bytes(df_1h), ttl=self._ttl_1h, prefix="candles", raw=True
        )
        await self.store.redis.r.publish(FINAL_CHANNEL, sym)
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
        # Write via UnifiedDataStore (single row DataFrame)
        df_row = pd.DataFrame(
            [
                {
                    "open_time": ts,
                    "open": bar["open"],
                    "high": bar["high"],
                    "low": bar["low"],
                    "close": bar["close"],
                    "volume": bar["volume"],
                    "close_time": ts + 60_000,
                }
            ]
        )
        try:
            await self.store.put_bars(sym, tf, df_row)
        except Exception as e:
            logger.warning("Failed to put bars into UnifiedDataStore: %s", e)
        # Лічильник отриманих повідомлень WS
        try:
            self.store.metrics.errors.inc(stage="ws_msg")  # reuse counter namespace
        except Exception:
            pass

        # Запис у Redis (як fallback і для stage2+)
        df_1m = await self._store_minute(sym, ts, k)
        await self.store.redis.r.publish(PARTIAL_CHANNEL, sym)
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
