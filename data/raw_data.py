# raw_data.py
# -*- coding: utf-8 -*-
"""
AiOne_t • Data Fetcher (RAW)
============================

Асинхронний модуль для масового завантаження та кешування історичних свічок
з Binance Futures API. Розроблено спеціально під AiOne_t з урахуванням:
  • висока паралельність і back‑off‑повторення;
  • спільний CacheHandler (Redis + файловий фелбек);
  • ефективна серіалізація (JSON / LZ4 + orjson);
  • детальне логування у форматі head:3\tail:3 на DEBUG‑рівні;
  • готовність до інтеграції в pipeline «збір → валідація → підготовка».

Швидкий огляд API
-----------------
>>> async with aiohttp.ClientSession() as sess:
...     cache = CacheHandler(redis_url="rediss://…", ttl_default=3600)
...     fetcher = OptimizedDataFetcher(cache_handler=cache, session=sess)
...     data = await fetcher.get_data_batch(["BTCUSDT", "ETHUSDT"],
...                                         interval="1h", limit=720)
"""

from __future__ import annotations
import sys
import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import aiohttp
import orjson
import pandas as pd
import rich.logging
from lz4.frame import compress, decompress
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from .utils import get_ttl_for_interval

# ───────────────────────────── ЛОГУВАННЯ ─────────────────────────────
logger = logging.getLogger("raw_data")
logger.setLevel(logging.INFO)
progress_console = Console(file=sys.stderr)
_handler = rich.logging.RichHandler(
    console=progress_console,
    show_level=True,
    show_path=False,
    markup=False,  # ← НЕ парсить розмітку!
    rich_tracebacks=True,
)
if not logger.handlers:
    logger.addHandler(_handler)
logger.propagate = False

# ──────────────────────────────── КОНСТАНТИ ───────────────────────────────
BINANCE_FUTURES_KLINES = "https://fapi.binance.com/fapi/v1/klines"
BINANCE_FUTURES_EXINFO = "https://fapi.binance.com/fapi/v1/exchangeInfo"

MAX_PARALLEL_KLINES = 10  # макс. одночасно REST‑запитів за свічками
_KLINE_SEM = asyncio.Semaphore(MAX_PARALLEL_KLINES)

# глобальний семафор для усіх інших REST‑викликів
GLOBAL_SEMAPHORE = asyncio.Semaphore(25)


# ───────────────────────────── допоміжні функції ────────────────────────────
def _log_dataframe(df: pd.DataFrame, label: str) -> None:
    """DEBUG‑знімок у форматі head:3\\tail:3 (економить простір у логах)."""
    if logger.isEnabledFor(logging.DEBUG) and not df.empty:
        snippet = pd.concat([df.head(3), df.tail(3)]).to_dict("records")
        logger.debug("%s  head:3\\tail:3=%s", label, snippet)


def _prepare_kline(df: pd.DataFrame, *, resample_to: str | None = None) -> pd.DataFrame:
    """
    Підготовка DataFrame:
      • конвертація OHLCV → float32 (‑50% RAM),
      • timestamp → UTC tz‑aware,
      • за потреби ресемпл (наприклад, "1m" → "1h").
    """
    if df.empty:
        return df

    df = df.astype(
        {
            "open": "float32",
            "high": "float32",
            "low": "float32",
            "close": "float32",
            "volume": "float32",
        }
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)

    if resample_to:
        df = (
            df.set_index("timestamp")
            .resample(resample_to, label="right", closed="right")
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
            .reset_index()
        )
    return df


def _df_to_bytes(df: pd.DataFrame, *, compress_lz4: bool = True) -> bytes:
    """
    Серіалізація DataFrame → bytes через orjson (з опційним LZ4‑стисненням).
    Timestamp (datetime) → int64 ms для JS‑дружнього формату.
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
    """
    Десеріалізація bytes/str → DataFrame.
    Якщо compressed=True, виконуємо LZ4‑декомпресію.
    Перетворюємо timestamp (int64 ms) → datetime UTC.
    """
    try:
        raw = buf.encode() if isinstance(buf, str) else buf
        if compressed:
            raw = decompress(raw)
        obj = orjson.loads(raw)
        df = pd.DataFrame(**obj)
        if "timestamp" in df.columns and pd.api.types.is_integer_dtype(df["timestamp"]):
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        return df
    except Exception as e:
        raise ValueError(f"Не вдалося розпакувати DataFrame: {e}") from e


# ───────────────────────── network primitives ──────────────────────────
async def fetch_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    *,
    params: Optional[Dict[str, str]] = None,
    max_retries: int = 3,
    backoff_sec: float = 2.0,
    timeout_sec: float = 10.0,
) -> str:
    """
    GET із back‑off‑повторенням та логуванням. Після max_retries кидає ClientError.
    """
    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=timeout_sec)
            ) as resp:
                text = await resp.text()
                if resp.status == 200:
                    return text
                logger.warning(
                    "[fetch_with_retry] %s (спроба=%d) => HTTP %d, body=%s",
                    url,
                    attempt,
                    resp.status,
                    text[:200],
                )
        except (
            aiohttp.ClientConnectorError,
            aiohttp.ClientPayloadError,
            asyncio.TimeoutError,
        ) as err:
            logger.warning("[fetch_with_retry] %s (спроба=%d) => %s", url, attempt, err)
        if attempt < max_retries:
            await asyncio.sleep(backoff_sec)
    raise aiohttp.ClientError(f"Не вдалося отримати {url} після {max_retries} спроб.")


def parse_futures_exchange_info(text: str) -> Dict[str, List[Dict[str, str]]]:
    """
    Парсер JSON для exchangeInfo → {"symbols":[...]}.
    Навіть якщо поле відсутнє, повертає {"symbols":[]}.
    """
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as e:
        logger.error("[FUTURES EXCHANGE] Помилка JSON: %s", e)
        return {"symbols": []}

    symbols = parsed.get("symbols")
    if not isinstance(symbols, list):
        logger.warning("[FUTURES EXCHANGE] Поле 'symbols' відсутнє або некоректне.")
        return {"symbols": []}

    df = pd.DataFrame(symbols)
    logger.debug("[FUTURES EXCHANGE] Знайдено %d символів.", len(df))
    return {"symbols": df.to_dict("records")}


# ────────────────────── Основний клас ─────────────────────────────
class OptimizedDataFetcher:
    """
    Аcинхронне вантаження OHLCV з кешем і інкрементом.

    Параметри:
      • cache_handler : Redis + fallback
      • session       : aiohttp.ClientSession
      • compress_cache: чи стискати LZ4 (за замовчуванням True)
    """

    def __init__(
        self,
        *,
        cache_handler,
        session: aiohttp.ClientSession,
        compress_cache: bool = True,
    ):
        self.cache_handler = cache_handler
        self.session = session
        self.compress_cache = compress_cache

    # ───────────────────── Пакетне отримання ───────────────────────────
    async def get_data_batch(
        self,
        symbols: List[str],
        *,
        interval: str = "1d",
        limit: int = 500,
        min_candles: int = 24,
        read_cache: bool = True,
        write_cache: bool = True,
        show_progress: bool = False,  # ← новий параметр
    ) -> Dict[str, pd.DataFrame]:
        """
        Паралельне завантаження до MAX_PARALLEL_KLINES символів.
        Якщо show_progress=True — рендерить плавний Rich Progress.
        Повертає dict{symbol: DataFrame}.
        """
        total = len(symbols)
        results: Dict[str, pd.DataFrame] = {}
        start = time.perf_counter()

        # Вибір контексту для прогрес‑бару
        if show_progress:
            prog_ctx = Progress(
                SpinnerColumn(),  # ⠙ спінер
                BarColumn(bar_width=30),  # ━╸━━━━
                TextColumn("[cyan]Завантаження даних… {task.completed}/{task.total}"),
                TimeElapsedColumn(),  # 0:00:03
                console=progress_console,
                transient=True,
                refresh_per_second=4,
            )
        else:
            # "Пустий" прогрес без візуалізації
            class DummyProgress:
                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    pass

                def add_task(self, *a, **k):
                    return None

                def advance(self, *a, **k):
                    pass

                def update(self, *a, **k):
                    pass

            prog_ctx = DummyProgress()

        # Виконуємо пакетне завантаження
        with prog_ctx as prog:
            task_id = prog.add_task("Завантаження даних…", total=total)
            tasks = [
                asyncio.create_task(
                    self._worker_for_symbol(
                        sym,
                        interval,
                        limit,
                        min_candles,
                        read_cache=read_cache,
                        write_cache=write_cache,
                    )
                )
                for sym in symbols
            ]
            for fut in asyncio.as_completed(tasks):
                sym, df = await fut
                prog.update(task_id, description=f"Завантаження [{sym}]")
                prog.advance(task_id, 1)
                if df is not None and not df.empty:
                    results[sym] = df

        elapsed = time.perf_counter() - start
        logger.info(
            "[BATCH] %d/%d символів готово за %.2f с.", len(results), total, elapsed
        )
        return results

    async def _worker_for_symbol(
        self,
        symbol: str,
        interval: str,
        limit: int,
        min_candles: int,
        *,
        read_cache: bool,
        write_cache: bool,
    ) -> Tuple[str, Optional[pd.DataFrame]]:
        async with _KLINE_SEM:
            df = await self.get_data(
                symbol,
                interval,
                limit=limit,
                min_candles=min_candles,
                read_cache=read_cache,
                write_cache=write_cache,
            )
        return symbol, df

    # ───────────────────── Односерійне отримання ───────────────────────
    async def get_data(
        self,
        symbol: str,
        interval: str,
        *,
        limit: int = 1000,
        min_candles: int = 24,
        read_cache: bool = True,
        write_cache: bool = True,
    ) -> Optional[pd.DataFrame]:
        """
        Основний метод: кеш → інкремент → повне REST.
        Після повернення гарантує len(df) >= min_candles.
        """
        ttl = get_ttl_for_interval(interval)
        prefix = "candles"

        # 1) Спроба зчитати з кешу
        df_cached: Optional[pd.DataFrame] = None
        if read_cache:
            raw = await self.cache_handler.fetch_from_cache(
                symbol, interval, prefix=prefix, raw=True
            )
            if raw is not None:
                try:
                    df_cached = _bytes_to_df(raw, compressed=self.compress_cache)
                    df_cached = _prepare_kline(df_cached)
                except ValueError:
                    logger.warning(
                        "[CACHE] Пошкоджений кеш %s:%s — ігноруємо.", symbol, interval
                    )

        # 2) Якщо кеш актуальний → інкрементальний апдейт
        if df_cached is not None and self._is_data_actual(df_cached, ttl, interval):
            updated = await self._incremental_update(
                symbol,
                interval,
                df_cached,
                ttl=ttl,
                cache_prefix=prefix,
                write_cache=write_cache,
            )
            if len(updated) >= min_candles:
                return updated

        # 3) Інакше — повне REST‑завантаження
        df_full = await self._fetch_binance_data(symbol, interval, limit=limit)
        if df_full.empty or len(df_full) < min_candles:
            logger.warning(
                f"[{symbol}][{interval}] недостатньо свічок ({len(df_full)})."
            )

            return None

        df_full = _prepare_kline(df_full)
        # _log_dataframe(df_full, f"[FULL]{symbol}:{interval}")

        if write_cache:
            await self.cache_handler.store_in_cache(
                symbol,
                interval,
                _df_to_bytes(df_full, compress_lz4=self.compress_cache),
                ttl=ttl,
                prefix=prefix,
                raw=True,
            )
        return df_full

    # ─────────────────── Інкрементальний апдейт ───────────────────────
    async def _incremental_update(
        self,
        symbol: str,
        interval: str,
        df_cached: pd.DataFrame,
        *,
        ttl: int,
        cache_prefix: str,
        write_cache: bool,
    ) -> pd.DataFrame:
        # Останній відомий timestamp + 1 мс
        last_ts_ms = int(df_cached["timestamp"].max().timestamp() * 1000) + 1
        # ① отримуємо «чистий» DataFrame з int‑timestamp
        df_new = await self._fetch_binance_data(
            symbol, interval, limit=1000, start_time=last_ts_ms
        )
        if df_new.empty:
            # немає нових свічок
            return df_cached

        # ② Приводимо нові свічки у той же формат, що й full‑завантаження:
        #    конвертує _timestamp_→datetime, float32, тощо
        df_new = _prepare_kline(df_new)

        df_all = (
            pd.concat([df_cached, df_new])
            .drop_duplicates(subset="timestamp")
            .sort_values("timestamp")
            .reset_index(drop=True)
        )
        # _log_dataframe(df_all, f"[INCR]{symbol}:{interval}")

        if write_cache:
            await self.cache_handler.store_in_cache(
                symbol,
                interval,
                _df_to_bytes(df_all, compress_lz4=self.compress_cache),
                ttl=ttl,
                prefix=cache_prefix,
                raw=True,
            )
        return df_all

    # ───────────────────── REST‑запит до Binance ─────────────────────────
    async def _fetch_binance_data(
        self,
        symbol: str,
        interval: str,
        *,
        limit: int,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Безпосередньо йде на /fapi/v1/klines.
        Додано перевірку символу: тільки USDT‑контракти.
        """
        # Підтримуємо і lower-, і UPPER-case: для API потрібен uppercase
        sym_api = symbol.upper()
        if not sym_api.endswith("USDT"):
            logger.error(
                "[FETCH] Невалідний символ: %s — можливо, не USDT‑Futures", symbol
            )
            return pd.DataFrame()

        # Формуємо запит до Binance із UPPER‑case
        params: Dict[str, str | int] = {
            "symbol": sym_api,
            "interval": interval,
            "limit": limit,
        }
        if startTime:
            params["startTime"] = startTime
        if endTime:
            params["endTime"] = endTime
        logger.debug("[FETCH] %s %s — запит %s", symbol, interval, params)

        async with GLOBAL_SEMAPHORE:
            text = await fetch_with_retry(
                self.session, BINANCE_FUTURES_KLINES, params=params
            )
        try:
            parsed: List[List] = json.loads(text)
        except json.JSONDecodeError:
            logger.error("[FETCH] Помилка JSON для %s %s.", symbol, interval)
            return pd.DataFrame()
        if not parsed:
            return pd.DataFrame()

        columns = [
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_asset_volume",
            "trades",
            "taker_buy_base",
            "taker_buy_quote",
            "ignore",
        ]
        df = pd.DataFrame(parsed, columns=columns)
        df = df[["timestamp", "open", "high", "low", "close", "volume"]]
        df[["open", "high", "low", "close", "volume"]] = df[
            ["open", "high", "low", "close", "volume"]
        ].astype(float)

        logger.debug("[FETCH] %s %s — отримано %d рядків.", symbol, interval, len(df))
        return df

    # ───────────────────── перевірка «свіжості» кеша ──────────────────────
    @staticmethod
    def _is_data_actual(df: pd.DataFrame, max_age_sec: int, interval: str) -> bool:
        """
        Перевіряє, наскільки давня остання свічка.
        Для '1d' після півночі UTC вимагає повного оновлення.
        Інакше max_age_sec.
        """
        now = datetime.now(timezone.utc)
        last_ts = df["timestamp"].max()
        if interval == "1d" and OptimizedDataFetcher._after_midnight_utc():
            return False
        age = (now - last_ts).total_seconds()
        status = "АКТУАЛЬНИЙ ✅" if age < max_age_sec else "ЗАСТАРІЛИЙ ❌"
        logger.debug(
            "[TTL] %s last=%s age=%.0fs max=%ds %s",
            interval,
            last_ts.strftime("%Y-%m-%d %H:%M:%S"),
            age,
            max_age_sec,
            status,
        )
        return age < max_age_sec

    @staticmethod
    def _after_midnight_utc() -> bool:
        """True, якщо зараз 00:00–00:05 UTC (після півночі)."""
        now = datetime.now(timezone.utc)
        return now.hour == 0 and now.minute < 5

    # ───────────────────────── exchangeInfo ─────────────────────────────
    async def get_futures_exchange_info(self) -> Dict[str, List[Dict[str, str]]]:
        """
        Повертає справжню інформацію про ф’ючерсні контракти (USDT‑M) з кешу або REST.
        """
        txt = await fetch_with_retry(
            self.session, BINANCE_FUTURES_EXINFO, max_retries=2, timeout_sec=5.0
        )
        return parse_futures_exchange_info(txt)

    async def get_data_for_calibration(
        self,
        symbol: str,
        interval: str,
        *,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        limit: int = 1000,
    ) -> Optional[pd.DataFrame]:
        """Спеціалізований метод для калібрування з підтримкою часових діапазонів"""
        params = {"limit": limit}
        if startTime:
            params["startTime"] = startTime
        if endTime:
            params["endTime"] = endTime

        return await self._fetch_binance_data(symbol, interval, **params)


"""
 Поточний шлях отримання даних (історія + інкременти)
                               

            ┌───────────────┐
request →   │  DataFetcher  │   get_data(...)        (async/await)
            └───────────────┘
                     │
                     ▼
          ┌────────────────────┐   ①   cache_handler.fetch_from_cache(raw=True)
          │      Redis         │───────────────┐
          └────────────────────┘               │
                     │                         │  LZ4‑bytes  (90 % випадків)
     cache miss / expired                      │
                     │                         │
                     ▼                         │
            ②  REST‑API  (fapi/binance)        │
                     │                         │
                     ▼                         │
          ┌────────────────────┐   ③   cache_handler.store_in_cache(..., raw=True)
          │      Redis         │◀──────────────┘
          └────────────────────┘
                     │
                     ▼
               DataFrame → скринер / індикатори



 Архітектура з урахуванням нового cache_handler

┌───────────────┐  wss://…@kline_1m  ┌───────────────────────────────┐
│ Binance WS    │──────────────────▶│  stream_worker.py (task)      │
└───────────────┘                   └────────────┬───────────────────┘
                                                ①│  CacheHandler.store_in_cache(
                                                 │     symbol, "1m",
                                                 │     raw_bytes, ttl=90,
                                                 │     prefix="candles",
                                                 │     publish_channel="klines.1m.tick"
                                                 │ )
                                                ②│  if kline["x"]:  # final 1m
                                                 │        aggregate_hourly() →
                                                 │        store "1h" (ttl=3900)
                                                 │        publish "klines.1h.update"
                                                 ▼
                               ┌───────────────────────────────┐
                               │              Redis            │
                               └───────────────────────────────┘
                                                ▲
                                                │
                DataFetcher.get_data(read_cache=True) ── ③ fetch_from_cache(raw=True)


 Поточний data‑flow у AiOne_t (після всіх патчів)

REST‑коли потрібно        ┌──────────────────────────┐
┌─────────────┐            │  OptimizedDataFetcher   │
│ Binance REST│───┐        └────────────┬───────────┘
└─────────────┘   │ ③ full / incr JSON/LZ4          ▲
                  ▼                                 │
   ② SETEX (raw) + PUBLISH    ① GET (raw)           │
┌────────────────────────────────────────────────────┼──────────┐
│                     Redis                          │          │
│  key = candles:{symbol}:{tf}  |  TTL  90 s (1m)    │          │
│                               |       3900 s (1h)  │          │
└────────────────────────────────────────────────────┴──────────┘
                  ▲                                 │
                  │  publish "klines.1h.update"     │
┌──────────────┐  │                                 │
│ stream_worker│──┘                                 ▼
│  (Binance WS)│          скринер / індикатори / trigger‑logic
└──────────────┘
                
                
"""
