# raw_data.py
"""
AiOne_t • Data Fetcher (RAW)
============================

Асинхронний модуль для масового завантаження історичних свічок
з Binance Futures API. Спрощена версія без зовнішнього CacheHandler.
Залишено:
    • паралельність і back‑off‑повторення;
    • (опц.) внутрішня памʼятева міні‑кеш мапа (symbol,interval) → DataFrame;
    • ефективна серіалізація (JSON / LZ4 + orjson) — залишено helper-и
        на випадок подальшої інтеграції;
    • детальне логування (DEBUG: head:3/tail:3).

Швидкий огляд API
-----------------
>>> async with aiohttp.ClientSession() as sess:
...     fetcher = OptimizedDataFetcher(session=sess)
...     data = await fetcher.get_data_batch(["BTCUSDT", "ETHUSDT"],
...                                         interval="1h", limit=720)
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

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
)

from utils.utils import get_ttl_for_interval

# ───────────────────────────── ЛОГУВАННЯ ─────────────────────────────
logger = logging.getLogger("raw_data")
logger.setLevel(logging.DEBUG)
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
        # dict records: list of row dicts with mixed types
        snippet: list[dict[str, Any]] = pd.concat([df.head(3), df.tail(3)]).to_dict(
            "records"
        )
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
    # Робимо перетворення timestamp тільки якщо ще не datetime64[ns, UTC]
    if "timestamp" in df.columns:
        if pd.api.types.is_integer_dtype(
            df["timestamp"]
        ) or pd.api.types.is_float_dtype(df["timestamp"]):
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        elif pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            # переконуємось, що tz-aware UTC
            if df["timestamp"].dt.tz is None:
                df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
            else:
                df["timestamp"] = df["timestamp"].dt.tz_convert("UTC")

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
    params: aiohttp.typedefs.Query | None = None,
    max_retries: int = 3,
    backoff_sec: float = 1.5,
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
            TimeoutError,
            aiohttp.ClientConnectorError,
            aiohttp.ClientPayloadError,
        ) as err:
            logger.warning("[fetch_with_retry] %s (спроба=%d) => %s", url, attempt, err)
        if attempt < max_retries:
            await asyncio.sleep(backoff_sec)
    raise aiohttp.ClientError(f"Не вдалося отримати {url} після {max_retries} спроб.")


def parse_futures_exchange_info(text: str) -> dict[str, list[dict[str, Any]]]:
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
    records: list[dict[str, Any]] = df.to_dict("records")
    return {"symbols": records}


# ────────────────────── Основний клас ─────────────────────────────
class OptimizedDataFetcher:
    """Асинхронне вантаження OHLCV (спрощено).

    Прибрано зовнішній CacheHandler. Додається легкий in‑memory кеш
    (волатильний на час життя процесу) для зменшення кількості REST викликів
    під час повторних звернень у межах одного запуску.
    """

    def __init__(self, *, session: aiohttp.ClientSession, compress_cache: bool = True):
        self.session = session
        self.compress_cache = compress_cache
        # in‑memory cache: (symbol, interval) -> (df, ts_fetch)
        self._mem_cache: dict[tuple[str, str], tuple[pd.DataFrame, float]] = {}
        # директорія снапшотів (можна змінити ззовні до першого виклику)
        self.snapshot_dir = Path("data_snapshots")
        self.snapshot_dir.mkdir(exist_ok=True)

    # ───────────────────── Snapshot helpers (on-disk) ─────────────────────
    def _snapshot_path(self, symbol: str, interval: str) -> Path:
        return self.snapshot_dir / f"{symbol.upper()}_{interval}_snapshot.lz4"

    def _load_snapshot(
        self,
        symbol: str,
        interval: str,
        *,
        max_age_sec: int,
        min_candles: int,
    ) -> pd.DataFrame | None:
        path = self._snapshot_path(symbol, interval)
        if not path.exists():
            return None
        try:
            age = time.time() - path.stat().st_mtime
            if age > max_age_sec:
                logger.info(
                    "[SNAPSHOT] %s %s застарілий (%.0fs > %ds) — ігнорую",
                    symbol,
                    interval,
                    age,
                    max_age_sec,
                )
                return None
            raw = path.read_bytes()
            df = _bytes_to_df(raw, compressed=True)
            if df.empty or len(df) < min_candles:
                return None
            df = _prepare_kline(df)
            logger.debug(
                "[SNAPSHOT] Завантажено %s %s rows=%d mtime_age=%.0fs",
                symbol,
                interval,
                len(df),
                age,
            )
            return df
        except Exception as e:  # pragma: no cover
            logger.warning("[SNAPSHOT] Неможливо прочитати %s (%s) — ігнорую", path, e)
            return None

    def _save_snapshot(self, symbol: str, interval: str, df: pd.DataFrame) -> None:
        try:
            path = self._snapshot_path(symbol, interval)
            buf = _df_to_bytes(df, compress_lz4=True)
            path.write_bytes(buf)
            logger.debug(
                "[SNAPSHOT] Збережено %s %s rows=%d → %s",
                symbol,
                interval,
                len(df),
                path.name,
            )
        except Exception as e:  # pragma: no cover
            logger.warning(
                "[SNAPSHOT] Помилка збереження snapshot %s %s: %s",
                symbol,
                interval,
                e,
            )

    async def _extend_snapshot_backward(
        self,
        symbol: str,
        interval: str,
        existing: pd.DataFrame,
        need_rows: int,
    ) -> pd.DataFrame:
        """Довантажує історію «в минуле» (backward) до existing.

        Повертає об'єднаний DataFrame (може бути менше need_rows якщо історія закінчилась).
        existing очікується вже підготовленим (_prepare_kline).
        """
        if need_rows <= 0:
            return existing
        earliest = existing["timestamp"].min()
        end_time = int(earliest.value // 1_000_000) - 1  # ns → ms
        remaining = need_rows
        parts: list[pd.DataFrame] = []
        safety = 0
        while remaining > 0:
            safety += 1
            if safety > 200:
                logger.warning(
                    "[SNAPSHOT] backward safety break %s %s", symbol, interval
                )
                break
            batch = await self._fetch_binance_data(
                symbol, interval, limit=min(1000, remaining), end_time=end_time
            )
            if batch.empty:
                break
            batch = _prepare_kline(batch)
            parts.append(batch)
            remaining -= len(batch)
            # move further back
            new_earliest = batch["timestamp"].min()
            end_time = int(new_earliest.value // 1_000_000) - 1
            if len(batch) < min(1000, remaining + len(batch)):
                # ранній розрив (менше ніж просили) — історія закінчилась
                pass
        if parts:
            parts.reverse()
            older = pd.concat(parts, ignore_index=True)
            combined = pd.concat([older, existing], ignore_index=True)
            combined = combined.drop_duplicates(subset=["timestamp"], keep="first")
            combined = combined.sort_values("timestamp")
            return combined
        return existing

    # ───────────────────── Пакетне отримання ───────────────────────────
    async def get_data_batch(
        self,
        symbols: list[str],
        *,
        interval: str = "1d",
        limit: int = 500,
        min_candles: int = 24,
        read_cache: bool = True,
        write_cache: bool = True,
        show_progress: bool = False,  # ← новий параметр
    ) -> dict[str, pd.DataFrame]:
        """Паралельне завантаження OHLCV для кількох символів.

        Parameters
        ----------
        symbols : list[str]
            Перелік символів (USDT‑фʼючерси).
        interval : str
            Тф Binance ("1m", "5m", "1h", "4h", "1d" ...).
        limit : int
            Кількість свічок на символ (при потребі далі усередині викликається
            пагінація в get_data).
        min_candles : int
            Мінімально прийнятна довжина ряду; якщо менше — символ пропускається.
        read_cache / write_cache : bool
            Контроль використання in‑memory кешу.
        show_progress : bool
            Візуалізація прогресу через Rich.

        Returns
        -------
        dict[str, DataFrame]
            Успішно завантажені ряди (може бути менше за кількість вхідних символів).
        """
        total = len(symbols)
        results: dict[str, pd.DataFrame] = {}
        start = time.perf_counter()

        # Вибір контексту для прогрес‑бару
        if show_progress:
            prog_ctx: Progress | DummyProgress
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
    ) -> tuple[str, pd.DataFrame | None]:
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
        use_snapshot: bool = True,
        snapshot_max_age_sec: int = 24 * 3600,
        snapshot_max_store: int | None = None,
    ) -> pd.DataFrame | None:
        """
        Основний метод отримання історії.

        Якщо limit <= MAX_SINGLE_FETCH (≈1500) — одноразове звернення.
        Якщо limit > MAX_SINGLE_FETCH — поетапно завантажує батчами (пагінація по startTime)
        доки не збере потрібно барів або дані закінчаться.
        Повернення гарантує len(df)>=min_candles або None.
        """
        ttl = get_ttl_for_interval(interval)

        # 1) In-memory cache
        key = (symbol.upper(), interval)
        now = time.time()
        if read_cache and key in self._mem_cache:
            df_cached, ts_fetch = self._mem_cache[key]
            if (now - ts_fetch) < ttl and len(df_cached) >= min_candles:
                return df_cached.tail(limit) if len(df_cached) > limit else df_cached

        # 1.5) Snapshot (on-disk) — тільки якщо дозволено
        snapshot_df: pd.DataFrame | None = None
        if use_snapshot and read_cache:
            snapshot_df = self._load_snapshot(
                symbol,
                interval,
                max_age_sec=snapshot_max_age_sec,
                min_candles=min_candles,
            )
        if snapshot_df is not None:
            # Якщо snapshot вже містить достатньо свічок — віддаємо
            if len(snapshot_df) >= limit:
                out = snapshot_df.tail(limit)
                if write_cache:
                    self._mem_cache[key] = (snapshot_df.copy(), now)
                return out
            # Інакше — довантажимо «назад» від найстарішої точки
            need_extra = limit - len(snapshot_df)
            logger.info(
                "[SNAPSHOT] %s %s need extend by %d (have %d < %d)",
                symbol,
                interval,
                need_extra,
                len(snapshot_df),
                limit,
            )
            extended = await self._extend_snapshot_backward(
                symbol, interval, snapshot_df, need_extra
            )
            if snapshot_max_store and len(extended) > snapshot_max_store:
                extended = extended.tail(snapshot_max_store)
            # зберігаємо оновлений снапшот
            if write_cache:
                self._save_snapshot(symbol, interval, extended)
                self._mem_cache[key] = (extended.copy(), now)
            return extended.tail(limit)

        max_single = 1500  # хард-ліміт Binance (перестраховка)
        if limit <= max_single:
            df_full = await self._fetch_binance_data(symbol, interval, limit=limit)
        else:
            # Backward pagination: беремо останній блок, потім рухаємося у минуле через endTime
            remaining = limit
            batch_limit = min(1000, max_single)
            parts: list[pd.DataFrame] = []
            end_time: int | None = None
            safety_iter = 0
            while remaining > 0:
                safety_iter += 1
                if safety_iter > 300:
                    logger.warning(
                        f"[{symbol}][{interval}] safety break (backward pagination)"
                    )
                    break
                current_limit = min(batch_limit, remaining)
                part = await self._fetch_binance_data(
                    symbol, interval, limit=current_limit, end_time=end_time
                )
                if part.empty:
                    # немає старіших даних
                    break
                # prepend logic: ми рухаємось назад, тому додаємо ліворуч
                parts.append(part)
                remaining -= len(part)
                # Наступний endTime = (перший timestamp поточного блоку - 1 ms)
                first_ts = int(part["timestamp"].iloc[0])
                end_time = first_ts - 1
                if len(part) < current_limit:  # дійшли до початку історії
                    break
                await asyncio.sleep(0)  # кооперативна уступка
            if not parts:
                df_full = pd.DataFrame()
            else:
                # parts зібрані від новішого до старішого, треба інвертувати порядок конкатенації
                parts.reverse()
                df_full = pd.concat(parts, ignore_index=True)
                df_full = df_full.drop_duplicates(subset=["timestamp"], keep="first")
                # Беремо тільки останні (найновіші) limit, якщо перевищили
                if len(df_full) > limit:
                    df_full = df_full.iloc[-limit:]
        if df_full.empty or len(df_full) < min_candles:
            logger.warning(
                f"[{symbol}][{interval}] недостатньо свічок ({len(df_full)})."
            )

            return None

        df_full = _prepare_kline(df_full)
        # _log_dataframe(df_full, f"[FULL]{symbol}:{interval}")
        if write_cache:
            self._mem_cache[key] = (df_full.copy(), now)
            if use_snapshot:
                try:
                    self._save_snapshot(symbol, interval, df_full)
                except Exception:
                    pass
        return df_full.tail(limit) if len(df_full) > limit else df_full

    # ─────────────────── Інкрементальний апдейт ───────────────────────
    # incremental update видалено у спрощеній версії
    # (залишено можливість відновити при потребі)

    # ───────────────────── REST‑запит до Binance ─────────────────────────
    async def _fetch_binance_data(
        self,
        symbol: str,
        interval: str,
        *,
        limit: int,
        start_time: int | None = None,
        end_time: int | None = None,
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

        # Binance Futures API обмежує limit для klines (наразі 1500). Якщо запит
        # приходить із вищим значенням (наприклад, 2000 у конфізі) — обрізаємо,
        # щоб уникнути HTTP 400 (code -1130).
        max_limit = 1500
        if limit > max_limit:
            logger.debug(
                "[FETCH] Запитаний limit=%d > %d, буде використано %d.",
                limit,
                max_limit,
                max_limit,
            )
            limit = max_limit

        # Формуємо запит до Binance із UPPER‑case
        params_raw: dict[str, object] = {
            "symbol": sym_api,
            "interval": interval,
            "limit": limit,
        }
        if start_time is not None:
            params_raw["startTime"] = start_time
        if end_time is not None:
            params_raw["endTime"] = end_time
        # Перетворюємо у послідовність пар (str, str) для узгодженості з aiohttp
        params: list[tuple[str, str]] = [(k, str(v)) for k, v in params_raw.items()]
        logger.debug("[FETCH] %s %s — запит %s", symbol, interval, params)

        async with GLOBAL_SEMAPHORE:
            text = await fetch_with_retry(
                self.session, BINANCE_FUTURES_KLINES, params=params
            )
        try:
            parsed: list[list] = json.loads(text)
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
        now = dt.datetime.now(dt.UTC)
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
        try:
            return bool(age < float(max_age_sec))
        except Exception:
            return False

    @staticmethod
    def _after_midnight_utc() -> bool:
        """True, якщо зараз 00:00–00:05 UTC (після півночі)."""
        now = dt.datetime.now(dt.UTC)
        return now.hour == 0 and now.minute < 5

    # ───────────────────────── exchangeInfo ─────────────────────────────
    async def get_futures_exchange_info(self) -> dict[str, list[dict[str, str]]]:
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
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
    ) -> pd.DataFrame | None:
        """Спеціалізований метод для калібрування з підтримкою часових діапазонів"""
        params = {"limit": limit}
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time

        return await self._fetch_binance_data(symbol, interval, **params)

    # ───────────────────── сервісні методи ────────────────────────────
    def reset_cache(self) -> None:
        """Очищує внутрішній in‑memory кеш (symbol, interval) → DataFrame.

        Корисно при довготривалих інтерактивних сесіях або при зміні політики TTL.
        """
        before = len(self._mem_cache)
        self._mem_cache.clear()
        logger.info("[CACHE] Очищено in‑memory кеш (було записів: %d).", before)
