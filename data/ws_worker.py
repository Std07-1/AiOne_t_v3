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
from typing import Any, cast

import aiohttp
import orjson
import pandas as pd
import websockets
from lz4.frame import compress, decompress
from rich.console import Console
from rich.logging import RichHandler

# unified store (single source of truth)
from data.unified_store import UnifiedDataStore

# Аудит: жодної нормалізації часу — працюємо із сирими значеннями як приходять


# ── Вбудовані (мінімальні) серіалізатори DataFrame (видалено raw_data.py) ──
def _df_to_bytes(df: pd.DataFrame, *, compress_lz4: bool = True) -> bytes:
    """Серіалізація DataFrame → bytes (orjson + опційно LZ4).

    Тільки колонки та індекс: використовує формат orient="split".
    Якщо є datetime колонка `timestamp`, перетворюємо у int64 ms для JS.
    """
    df_out = df.copy()
    # 1) Конвертуємо datetime-колонку 'timestamp' у мс (int64) для сумісності з JS
    if "timestamp" in df_out.columns and pd.api.types.is_datetime64_any_dtype(
        df_out["timestamp"]
    ):
        df_out["timestamp"] = (df_out["timestamp"].astype("int64") // 1_000_000).astype(
            "int64"
        )
    # 2) Якщо індекс datetime-подібний — теж конвертуємо у мс (int64),
    #    інакше orjson не зможе серіалізувати pandas.Timestamp у split['index']
    try:
        if isinstance(
            df_out.index, pd.DatetimeIndex
        ) or pd.api.types.is_datetime64_any_dtype(df_out.index):
            idx_ms = (df_out.index.view("int64") // 1_000_000).astype("int64")
            df_out.index = idx_ms
    except Exception:
        # індекс не datetime або не вдалося — залишаємо як є
        pass
    raw_json = orjson.dumps(df_out.to_dict(orient="split"))
    if compress_lz4:
        return cast(bytes, compress(raw_json))
    # raw_json is bytes from orjson.dumps
    return raw_json


def _bytes_to_df(buf: bytes | str, *, compressed: bool = True) -> pd.DataFrame:
    """Десеріалізація bytes → DataFrame (reverse _df_to_bytes)."""
    raw = buf.encode() if isinstance(buf, str) else buf
    if compressed:
        raw = decompress(raw)
    obj = orjson.loads(raw)
    df = pd.DataFrame(**obj)
    # Відновлюємо datetime-індекс, якщо індекс цілий (мс)
    try:
        if getattr(
            df.index, "dtype", None
        ) is not None and pd.api.types.is_integer_dtype(df.index):
            df.index = pd.to_datetime(df.index.astype("int64"), unit="ms", utc=True)
    except Exception:
        pass
    # Відновлюємо колонку 'timestamp', якщо вона збережена як цілі числа (мс)
    if "timestamp" in df.columns and pd.api.types.is_integer_dtype(df["timestamp"]):
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df


# (moved UnifiedDataStore import to top for ruff E402 compliance)

# ── Налаштування / константи ───────────────────────────────────────────────
PARTIAL_CHANNEL = "klines.1m.partial"
FINAL_CHANNEL = "klines.1h.update"
# Default TTLs for legacy blob snapshot (NOT the canonical ds.cfg.intervals_ttl)
DEFAULT_INTERVALS_TTL: dict[str, int] = {"1m": 90, "1h": 65 * 60}
SELECTOR_REFRESH_S = 30  # how often to refresh symbol whitelist

STATIC_SYMBOLS = os.getenv("STREAM_SYMBOLS", "")
DEFAULT_SYMBOLS = [s.lower() for s in STATIC_SYMBOLS.split(",") if s] or ["btcusdt"]

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("app.data.ws_worker")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    # show_path=True для точного місця походження WARNING/ERROR
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=True))
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
        symbols: list[str] | None = None,
        *,
        store: UnifiedDataStore,
        selectors_key: str | None = None,
        intervals_ttl: dict[str, int] | None = None,
    ):
        if store is None:
            raise ValueError("WSWorker requires a UnifiedDataStore instance")
        self.store = store
        self._symbols: set[str] = set(
            [s.lower() for s in symbols] if symbols else DEFAULT_SYMBOLS
        )
        # optional override for selector redis key ("part1:part2:..")
        self._selectors_key: tuple[str, ...] | None = (
            tuple(p for p in selectors_key.split(":") if p) if selectors_key else None
        )
        # TTLs for legacy blob snapshots; fall back to defaults above
        mapping = DEFAULT_INTERVALS_TTL.copy()
        if intervals_ttl:
            mapping.update(intervals_ttl)
        self._ttl_1m = mapping.get("1m", DEFAULT_INTERVALS_TTL["1m"])
        self._ttl_1h = mapping.get("1h", DEFAULT_INTERVALS_TTL["1h"])
        self._ws_url: str | None = None
        self._backoff: int = 3
        self._refresh_task: asyncio.Task[Any] | None = None
        self._hb_task: asyncio.Task[Any] | None = None
        self._stop_event = asyncio.Event()

    async def _get_live_symbols(self) -> list[str]:
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
        # Нормалізуємо до нижнього регістру для сумісності з Binance streams
        syms = [s.lower() for s in syms]
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

    def _build_ws_url(self, symbols: set[str]) -> str:
        streams = "/".join(f"{s}@kline_1m" for s in sorted(symbols))
        return f"wss://fstream.binance.com/stream?streams={streams}"

    async def _store_minute(self, sym: str, ts: int, k: dict[str, Any]) -> pd.DataFrame:
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
        df.at[dt, "open"] = float(k["o"])  # set by label to avoid dtype issues
        df.at[dt, "high"] = float(k["h"])
        df.at[dt, "low"] = float(k["l"])
        df.at[dt, "close"] = float(k["c"])
        df.at[dt, "volume"] = float(k["v"])
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

    async def _handle_kline(self, k: dict[str, Any]) -> None:
        """
        Обробка WS kline:
        - зберігає 1m в RAMBuffer (оновлення bar по timestamp, без дублювання),
        - при закритті bar[x] — агрегує у 1h.
        """
        sym = k["s"].lower()
        ts = int(k["t"])

        if ts < 1_000_000_000_000:  # < 1e12 → це секунди
            ts *= 1_000

        tf = "1m"
        bar = {
            "open": float(k["o"]),
            "high": float(k["h"]),
            "low": float(k["l"]),
            "close": float(k["c"]),
            "volume": float(k["v"]),
            "timestamp": ts,  # має бути ms
        }
        # Аудит: лог сирих значень часу/бару (перші повідомлення по символу)
        try:
            logger.debug(
                "[WS RECEIVE] %s | t=%s x=%s",
                sym,
                k.get("t"),
                k.get("x"),
            )
            logger.debug(
                "[WS RAW] %s t=%s o=%s h=%s l=%s c=%s v=%s x=%s",
                sym,
                k.get("t"),
                k.get("o"),
                k.get("h"),
                k.get("l"),
                k.get("c"),
                k.get("v"),
                k.get("x"),
            )
        except Exception:
            pass

        # Write via UnifiedDataStore (single row DataFrame) — без конвертації часу
        is_closed = bool(k.get("x", False))
        close_time_val = ts + 60_000 - 1
        df_row = pd.DataFrame(
            [
                {
                    "open_time": ts,
                    "open": bar["open"],
                    "high": bar["high"],
                    "low": bar["low"],
                    "close": bar["close"],
                    "volume": bar["volume"],
                    "close_time": close_time_val,
                    "is_closed": is_closed,
                }
            ]
        )
        try:
            logger.debug(
                "[WS PASS] %s | put_bars %s rows=1 open_time=%s close_time=%s",
                sym,
                tf,
                df_row["open_time"].iloc[0],
                df_row["close_time"].iloc[0],
            )
            await self.store.put_bars(sym, tf, df_row)
        except Exception as e:
            logger.warning("Failed to put bars into UnifiedDataStore: %s", e)
        # Лічильник отриманих повідомлень WS
        try:
            self.store.metrics.errors.labels(stage="ws_msg").inc()
        except Exception:
            try:
                self.store.metrics.errors.inc()
            except Exception:
                pass

        # Запис у Redis (як fallback і для stage2+)
        df_1m = await self._store_minute(sym, ts, k)
        await self.store.redis.r.publish(PARTIAL_CHANNEL, sym)
        if is_closed:
            # Gap-detector: очікуємо, що цей open_time на 60_000 більший за попередній закритий
            try:
                prev = await self.store.get_df(sym, tf, limit=2)
                if prev is not None and len(prev) >= 2:
                    last_two = prev.tail(2)
                    ot_prev = int(last_two["open_time"].iloc[-2])
                    ot_cur = int(last_two["open_time"].iloc[-1])
                    if (ot_cur - ot_prev) != 60_000:
                        logger.warning(
                            "[WS GAP] %s %s gap detected: prev=%s cur=%s delta=%s",
                            sym,
                            tf,
                            ot_prev,
                            ot_cur,
                            ot_cur - ot_prev,
                        )
                        # Мінімальний auto-heal: backfill пропущених хвилин через REST
                        missing = (ot_cur - ot_prev) // 60_000 - 1
                        if missing > 0:
                            # Запускаємо у фоні, щоб не блокувати WS-цикл
                            asyncio.create_task(
                                self._safe_backfill(
                                    sym=sym,
                                    start_open_time=ot_prev + 60_000,
                                    end_open_time=ot_cur - 60_000,
                                    max_bars=missing,
                                )
                            )
            except Exception:
                pass
            # Публікуємо 1h лише на межі години
            if (close_time_val % 3_600_000) == (3_600_000 - 1):
                await self._on_final_candle(sym, df_1m)

    async def _backfill_gap_1m(
        self,
        sym: str,
        *,
        start_open_time: int,
        end_open_time: int,
        max_bars: int,
    ) -> None:
        """Backfill пропущених 1m барів через Binance Futures REST.

        Args:
            sym: символ у lower (напр., "btcusdt").
            start_open_time: перший пропущений open_time (ms, UTC).
            end_open_time: останній пропущений open_time (ms, UTC).
            max_bars: верхня межа кількості барів (зазвичай невеликий).
        """
        url = "https://fapi.binance.com/fapi/v1/klines"
        interval = "1m"
        # Binance дозволяє limit до ~1500; для безпеки візьмемо 1000
        remaining = max_bars
        start = start_open_time
        async with aiohttp.ClientSession() as sess:
            while remaining > 0 and start <= end_open_time:
                limit = min(1000, remaining)
                params: dict[str, str | int] = {
                    "symbol": sym.upper(),
                    "interval": interval,
                    "startTime": int(start),
                    # endTime інколи корисний, але можна не ставити, щоб брати limit від start
                    "limit": int(limit),
                }
                async with sess.get(
                    url, params=params, timeout=aiohttp.ClientTimeout(total=5.0)
                ) as resp:
                    if resp.status != 200:
                        txt = await resp.text()
                        raise RuntimeError(
                            f"REST backfill HTTP {resp.status}: {txt[:200]}"
                        )
                    data = await resp.json()
                if not data:
                    break
                # Побудуємо DataFrame з потрібними колонками
                rows = []
                for it in data:
                    # формат: [open_time, o, h, l, c, v, close_time, ... , trades, ...]
                    ot = int(it[0])
                    if ot > end_open_time:
                        break
                    rows.append(
                        {
                            "open_time": ot,
                            "open": float(it[1]),
                            "high": float(it[2]),
                            "low": float(it[3]),
                            "close": float(it[4]),
                            "volume": float(it[5]),
                            "close_time": int(it[6]),
                            "is_closed": True,
                        }
                    )
                if not rows:
                    break
                df = pd.DataFrame(rows)
                await self.store.put_bars(sym, "1m", df)
                # Рухаємося далі
                last_ot = int(df["open_time"].iloc[-1])
                if last_ot >= end_open_time:
                    break
                start = last_ot + 60_000
                remaining -= len(df)
            # Стенограма видалена
            # Опціональний reactive Stage1 hook: викликати монітор одразу після закриття бару
            try:
                import os

                reactive = os.getenv("REACTIVE_STAGE1", "0") in ("1", "true", "True")
                monitor = getattr(self.store, "stage1_monitor", None)
                if reactive and monitor is not None:
                    # не блокуємо WS – запускаємо як окрему задачу
                    import asyncio as _asyncio

                    _asyncio.create_task(self._reactive_stage1_call(monitor, sym, None))
            except Exception:  # pragma: no cover
                pass

    async def _safe_backfill(
        self,
        *,
        sym: str,
        start_open_time: int,
        end_open_time: int,
        max_bars: int,
    ) -> None:
        """Безпечний обгортник backfill з перехопленням винятків."""
        try:
            await self._backfill_gap_1m(
                sym,
                start_open_time=start_open_time,
                end_open_time=end_open_time,
                max_bars=max_bars,
            )
            logger.info(
                "[WS GAP] backfill done %s 1m: %s→%s (%s bars)",
                sym,
                start_open_time,
                end_open_time,
                max_bars,
            )
        except Exception as e:
            logger.warning("[WS GAP] backfill failed %s 1m: %s", sym, e, exc_info=True)

    async def _refresh_symbols(self, ws: Any) -> None:
        """Фоновий таск: refresh whitelist і ресабскрайб."""
        while not self._stop_event.is_set():
            await asyncio.sleep(SELECTOR_REFRESH_S)
            try:
                new_syms = set(await self._get_live_symbols())
                if new_syms and new_syms != self._symbols:
                    await self._resubscribe(ws, new_syms)
            except Exception as e:
                logger.warning("Refresh symbols error: %s", e)

    async def _resubscribe(self, ws: Any, new_syms: set[str]) -> None:
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
                # Стенограма видалена
                async with websockets.connect(self._ws_url, ping_interval=20) as ws:
                    logger.debug("WS connected (%d streams)…", len(self._symbols))
                    self._backoff = 3
                    self._stop_event.clear()
                    self._refresh_task = asyncio.create_task(self._refresh_symbols(ws))
                    # Стенограма видалена
                    async for msg in ws:
                        try:
                            data = json.loads(msg).get("data", {}).get("k")
                            if data:
                                await self._handle_kline(data)
                        except Exception as e:
                            try:
                                if isinstance(msg, dict):
                                    _ = json.dumps(msg, default=str)[:80]
                                logger.debug(
                                    "Bad WS message: %s… (%s)", str(msg)[:200], e
                                )
                            except Exception:
                                logger.debug("Bad WS message: <unserializable> (%s)", e)
            except Exception as exc:
                logger.warning("WS error: %s → reconnect in %ds", exc, self._backoff)
                # Стенограма видалена
                await asyncio.sleep(self._backoff)
                self._backoff = min(self._backoff * 2, 30)
            finally:
                if self._refresh_task:
                    self._refresh_task.cancel()
                if self._hb_task:
                    self._hb_task.cancel()

    async def stop(self) -> None:
        """Зупиняє воркер і всі фонові таски."""
        self._stop_event.set()
        if self._refresh_task:
            self._refresh_task.cancel()

    # ──────────────────────────────────────────────────────────────────────
    # Сумісність із тестами: _reactive_stage1_call(mon, symbol, payload)
    # Якщо у монітора є update_and_check → використовуємо його, інакше process_new_bar
    async def _reactive_stage1_call(
        self, monitor: Any, symbol: str, payload: Any
    ) -> None:
        # Якщо немає payload — одразу process_new_bar
        if payload is None:
            try:
                maybe2 = monitor.process_new_bar(symbol)
                if asyncio.iscoroutine(maybe2):
                    await maybe2
            except Exception:
                pass
            return
        # Інакше — спробувати update_and_check, якщо є
        try:
            fn = getattr(monitor, "update_and_check", None)
            if callable(fn):
                maybe = fn(symbol, payload)
                if asyncio.iscoroutine(maybe):
                    await maybe
                return
        except Exception:
            pass
        # Fallback
        try:
            maybe2 = monitor.process_new_bar(symbol)
            if asyncio.iscoroutine(maybe2):
                await maybe2
        except Exception:
            pass


# ──────────────── Запуск модуля ────────────────

if __name__ == "__main__":
    # Minimal bootstrap for manual run: create Redis-less store if available
    try:
        from redis.asyncio import Redis as _Redis

        from data.unified_store import StoreConfig, StoreProfile, UnifiedDataStore

        # default ephemeral config
        _redis = _Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
        )
        _cfg = StoreConfig(
            namespace=os.getenv("AI_ONE_NS", "ai_one"),
            base_dir=os.getcwd(),
            profile=StoreProfile(),
            intervals_ttl={"1m": 90, "1h": 65 * 60},
            write_behind=False,
            validate_on_read=False,
            validate_on_write=False,
            io_retry_attempts=2,
            io_retry_backoff=0.5,
        )
        _store = UnifiedDataStore(redis=_redis, cfg=_cfg)
    except Exception as _e:  # pragma: no cover
        logger.error("Failed to init UnifiedDataStore: %s", _e)
        raise

    worker = WSWorker(store=_store)
    try:
        asyncio.run(worker.consume())
    except KeyboardInterrupt:
        logger.info("WSWorker stopped by user")
    except Exception as e:
        logger.error("Fatal error: %s", e, exc_info=True)
