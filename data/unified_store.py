"""UnifiedDataStore — центральне шарувате сховище (RAM ↔ Redis ↔ Disk).

Шлях: ``data/unified_store.py``

Призначення:
    • швидкий RAM‑кеш (TTL, LRU, пріоритет активів, квоти профілю);
    • Redis як шар спільного стану (namespace ``ai_one:``) та останні бари;
    • write‑behind збереження на диск (Parquet | JSONL) зі згладженим тиском;
    • метрики (optionally Prometheus), евікшен та перевірки валідності (схема, NaT, монотонність);
    • уніфіковане API для Stage1/Stage2/WebSocket/UI компонентів.

Ключові методи:
        get_df / get_last / put_bars / warmup / set_priority / metrics_snapshot.

Особливості реалізації:
    • write-behind черга з адаптивним backpressure (soft/hard пороги);
    • sum‑тип TTL для інтервалів (cfg.intervals_ttl) + профіль гарячості;
    • агрегація/валідація не виконується тут — лише зберігання та читання.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Deque, Dict, List, Optional, Tuple
from collections import OrderedDict, deque
import pandas as pd

try:
    import pyarrow  # noqa: F401

    _HAS_PARQUET = True
except Exception:  # pragma: no cover
    _HAS_PARQUET = False

from redis.asyncio import Redis  # type: ignore

from rich.console import Console
from rich.logging import RichHandler

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("app.data.unified_store")
if not logger.handlers:  # guard проти повторної ініціалізації
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False

# ---- Стандарти й константи --------------------------------------------------

DEFAULT_NAMESPACE = "ai_one"

REQUIRED_OHLCV_COLS = (
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
)
MIN_COLUMNS = set(REQUIRED_OHLCV_COLS)

# ---- Допоміжні структури ----------------------------------------------------


@dataclass
class StoreProfile:
    """Профіль використання ресурсів."""

    name: str = "small"
    ram_limit_mb: int = 512
    max_symbols_hot: int = 96
    hot_ttl_sec: int = 6 * 3600  # 1m гарячий
    warm_ttl_sec: int = 24 * 3600  # 15m-1h теплий
    flush_batch_max: int = 8
    flush_queue_soft: int = 200
    flush_queue_hard: int = 1000


@dataclass
class StoreConfig:
    """Базова конфігурація сховища."""

    namespace: str = DEFAULT_NAMESPACE
    intervals_ttl: Dict[str, int] = field(
        default_factory=lambda: {
            "1m": 6 * 3600,
            "5m": 12 * 3600,
            "15m": 24 * 3600,
            "1h": 3 * 24 * 3600,
            "4h": 7 * 24 * 3600,
            "1d": 30 * 24 * 3600,
        }
    )
    profile: StoreProfile = field(default_factory=StoreProfile)
    write_behind: bool = True
    base_dir: str = "./datastore"
    validate_on_write: bool = True
    validate_on_read: bool = True
    # retry для Redis/диска
    io_retry_attempts: int = 3
    io_retry_backoff: float = 0.25  # секунди, експоненційно


class Priority:
    """Пріоритети активів для політик евікшену/утримання в RAM."""

    ALERT = 3
    STAGE2 = 2
    NORMAL = 1
    COLD = 0


# ---- Ключі/імена ------------------------------------------------------------


def k(namespace: str, *parts: str) -> str:
    """Будує стабільний Redis-ключ: ai_one:part1:part2..."""
    sane = [p.strip(":") for p in parts if p]
    return ":".join([namespace, *sane])


def file_name(symbol: str, context: str, event: str, ext: str = "parquet") -> str:
    """Ім'я файла у форматі: SYMBOL_context_event.ext"""
    return f"{symbol}_{context}_{event}.{ext}"


# ---- Метрики ----------------------------------------------------------------


class _Noop:
    def inc(self, *_, **__):
        pass

    def set(self, *_, **__):
        pass

    def observe(self, *_, **__):
        pass

    def labels(self, *_, **__):  # mimic prometheus client chaining
        return self


class Metrics:
    """Проста метрика з optional Prometheus-інтеграцією."""

    def __init__(self) -> None:
        try:
            from prometheus_client import Counter, Gauge, Histogram  # type: ignore

            self._counter = Counter
            self._gauge = Gauge
            self._hist = Histogram
            self.enabled = True
        except Exception:  # pragma: no cover
            self.enabled = False

        if self.enabled:
            self.get_latency = self._hist(
                "ds_get_latency_seconds", "Latency of get_df/get_last", ["layer"]
            )
            self.put_latency = self._hist(
                "ds_put_latency_seconds", "Latency of put_bars", ["layer"]
            )
            self.ram_hit_ratio = self._gauge("ds_ram_hit_ratio", "RAM hit ratio (0..1)")
            self.redis_hit_ratio = self._gauge(
                "ds_redis_hit_ratio", "Redis hit ratio (0..1)"
            )
            self.bytes_in_ram = self._gauge("ds_bytes_in_ram", "Bytes in RAM")
            self.flush_backlog = self._gauge(
                "ds_flush_backlog", "Write-behind backlog size"
            )
            self.evictions = self._counter(
                "ds_evictions_total", "RAM evictions", ["reason"]
            )
            self.errors = self._counter("ds_errors_total", "Errors", ["stage"])
            self.last_put_ts = self._gauge(
                "ds_last_put_timestamp",
                "Unix timestamp (s) of last successful put_bars",
            )
        else:
            self.get_latency = _Noop()
            self.put_latency = _Noop()
            self.ram_hit_ratio = _Noop()
            self.redis_hit_ratio = _Noop()
            self.bytes_in_ram = _Noop()
            self.flush_backlog = _Noop()
            self.evictions = _Noop()
            self.errors = _Noop()
            self.last_put_ts = _Noop()


# ---- RAM Layer --------------------------------------------------------------


class RamLayer:
    """RAM-кеш з TTL, LRU, квотами, пріоритетами й приблизною оцінкою пам'яті."""

    def __init__(self, profile: StoreProfile) -> None:
        self._store: Dict[Tuple[str, str], Tuple[pd.DataFrame, float, int]] = {}
        self._lru: "OrderedDict[Tuple[str, str], None]" = OrderedDict()
        self._prio: Dict[str, int] = {}  # symbol -> Priority
        self._profile = profile
        self._bytes_in_ram: int = 0

    # --- утиліти -------------------------------------------------------------

    @staticmethod
    def _estimate_bytes(df: pd.DataFrame) -> int:
        try:
            return int(df.memory_usage(index=True, deep=True).sum())
        except Exception:
            return max(1024, len(df) * 128)

    def _ttl_for(self, interval: str) -> int:
        # hot vs warm залежно від інтервалу
        if interval in ("1m", "5m"):
            return self._profile.hot_ttl_sec
        return self._profile.warm_ttl_sec

    # --- API -----------------------------------------------------------------

    def set_priority(self, symbol: str, level: int) -> None:
        self._prio[symbol] = level

    def get_priority(self, symbol: str) -> int:
        return self._prio.get(symbol, Priority.NORMAL)

    def get(self, symbol: str, interval: str) -> Optional[pd.DataFrame]:
        key = (symbol, interval)
        item = self._store.get(key)
        if not item:
            return None
        df, ts, ttl = item
        if time.time() - ts > ttl:
            self.delete(key, reason="ttl_expired")
            return None
        # LRU touch
        self._lru.move_to_end(key, last=True)
        return df

    def put(self, symbol: str, interval: str, df: pd.DataFrame) -> None:
        key = (symbol, interval)
        ttl = self._ttl_for(interval)
        now = time.time()

        old = self._store.get(key)
        if old:
            old_df, _, _ = old
            self._bytes_in_ram -= self._estimate_bytes(old_df)

        self._store[key] = (df, now, ttl)
        self._lru[key] = None
        self._lru.move_to_end(key, last=True)
        self._bytes_in_ram += self._estimate_bytes(df)

        self._enforce_quotas()

    def delete(self, key: Tuple[str, str], *, reason: str = "evict") -> None:
        item = self._store.pop(key, None)
        if item:
            df, _, _ = item
            self._bytes_in_ram -= self._estimate_bytes(df)
        if key in self._lru:
            del self._lru[key]

    def sweep(self, metrics: Metrics) -> None:
        """Прибрати протухлі ключі/зайві записи."""
        now = time.time()
        expired: List[Tuple[str, str]] = []
        for key, (df, ts, ttl) in list(self._store.items()):
            if now - ts > ttl:
                expired.append(key)
        for key in expired:
            self.delete(key, reason="ttl_expired")
            metrics.evictions.inc(reason="ttl_expired")

        self._enforce_quotas()

        metrics.bytes_in_ram.set(self._bytes_in_ram)

    # --- внутрішнє -----------------------------------------------------------

    def _enforce_quotas(self) -> None:
        """Квоти: обмеження символів у hot та за RAM-обсягом."""
        # ліміт по кількості гарячих символів
        symbols_in_lru = list(
            OrderedDict(((s, None) for s, _ in self._lru.keys())).keys()
        )
        if len(symbols_in_lru) > self._profile.max_symbols_hot:
            # евікшн менш пріоритетних і найстаріших
            to_drop = len(symbols_in_lru) - self._profile.max_symbols_hot
            self._evict_by_priority(to_drop)

        # грубий ліміт по байтах RAM
        ram_limit_bytes = self._profile.ram_limit_mb * 1024 * 1024
        while self._bytes_in_ram > ram_limit_bytes and self._lru:
            key, _ = self._lru.popitem(last=False)  # найстаріший
            self.delete(key, reason="ram_quota")

    def _evict_by_priority(self, count: int) -> None:
        # будуємо список (prio, age_index, key)
        ranked: List[Tuple[int, int, Tuple[str, str]]] = []
        for idx, key in enumerate(self._lru.keys()):
            sym, _ = key
            prio = self.get_priority(sym)
            ranked.append((prio, idx, key))
        ranked.sort(
            key=lambda x: (x[0], x[1])
        )  # пріоритет зростає -> першим викидаємо найнижчий

        removed = 0
        for _, _, key in ranked:
            sym, _ = key
            # не чіпаємо ALERT
            if self.get_priority(sym) >= Priority.ALERT:
                continue
            self.delete(key, reason="hot_quota")
            removed += 1
            if removed >= count:
                break

    # --- інспектори ----------------------------------------------------------

    @property
    def stats(self) -> Dict[str, Any]:
        return {
            "entries": len(self._store),
            "bytes_in_ram": self._bytes_in_ram,
            "lru_len": len(self._lru),
        }


# ---- Redis Adapter ----------------------------------------------------------


class RedisAdapter:
    """Обгортка над redis.asyncio.Redis з JSON-нормалізацією та retry."""

    def __init__(self, redis: Redis, cfg: StoreConfig) -> None:
        self.r = redis
        self.cfg = cfg

    async def jget(self, *parts: str, default: Any = None) -> Any:
        key = k(self.cfg.namespace, *parts)
        for attempt in range(self.cfg.io_retry_attempts):
            try:
                raw = await self.r.get(key)
                return default if raw is None else json.loads(raw)
            except Exception as e:
                await asyncio.sleep(self.cfg.io_retry_backoff * (2**attempt))
                if attempt == self.cfg.io_retry_attempts - 1:
                    logger.error(f"Redis GET failed for {key}: {e}")
                    return default

    async def jset(self, *parts: str, value: Any, ttl: Optional[int] = None) -> None:
        key = k(self.cfg.namespace, *parts)
        data = json.dumps(value, ensure_ascii=False)
        for attempt in range(self.cfg.io_retry_attempts):
            try:
                if ttl:
                    await self.r.set(key, data, ex=ttl)
                else:
                    await self.r.set(key, data)
                return
            except Exception as e:
                await asyncio.sleep(self.cfg.io_retry_backoff * (2**attempt))
                if attempt == self.cfg.io_retry_attempts - 1:
                    logger.error(f"Redis SET failed for {key}: {e}")


# ---- Disk Adapter -----------------------------------------------------------


class StorageAdapter:
    """Збереження на диск: Parquet (якщо доступний) або JSON. Async через виконавця."""

    def __init__(self, base_dir: str, cfg: StoreConfig) -> None:
        self.base_dir = base_dir
        self.cfg = cfg
        os.makedirs(self.base_dir, exist_ok=True)

    async def save_bars(self, symbol: str, interval: str, df: pd.DataFrame) -> str:
        """Зберігає історію барів. Контекст=f"bars_{interval}", event="snapshot"."""
        context = f"bars_{interval}"
        # Використовуємо pathlib для побудови шляху + атомічний запис
        from pathlib import Path

        path = Path(self.base_dir) / file_name(
            symbol, context, "snapshot", ("parquet" if _HAS_PARQUET else "jsonl")
        )
        path.parent.mkdir(parents=True, exist_ok=True)

        loop = asyncio.get_running_loop()

        def _write_parquet(p: Path, frame: pd.DataFrame) -> None:
            # Використовуємо тимчасовий файл для атомічності
            tmp = p.with_suffix(p.suffix + ".tmp")
            frame.to_parquet(tmp, index=False)
            tmp.replace(p)

        def _write_jsonl(p: Path, frame: pd.DataFrame) -> None:
            tmp = p.with_suffix(p.suffix + ".tmp")
            # Використовуємо keyword-only аргументи to_json (сумісно з pandas >=2.2/3.0)
            frame.to_json(
                path_or_buf=tmp,
                orient="records",
                lines=True,
                date_format="iso",
                date_unit="ms",
                force_ascii=False,
                compression=None,
                index=False,
                indent=None,
            )
            tmp.replace(p)

        try:
            if _HAS_PARQUET:
                await loop.run_in_executor(None, _write_parquet, path, df)
            else:
                await loop.run_in_executor(None, _write_jsonl, path, df)
            return str(path)
        except Exception:  # pragma: no cover - деталізований лог + traceback
            logger.exception("Disk flush failed for %s %s", symbol, interval)
            raise

    async def load_bars(self, symbol: str, interval: str) -> Optional[pd.DataFrame]:
        """Завантажує історію барів, якщо файл існує."""
        context = f"bars_{interval}"
        parquet = os.path.join(
            self.base_dir, file_name(symbol, context, "snapshot", "parquet")
        )
        jsonl = os.path.join(
            self.base_dir, file_name(symbol, context, "snapshot", "jsonl")
        )
        legacy_json = os.path.join(
            self.base_dir, file_name(symbol, context, "snapshot", "json")
        )
        loop = asyncio.get_running_loop()
        if _HAS_PARQUET and os.path.exists(parquet):
            return await loop.run_in_executor(None, pd.read_parquet, parquet)
        # Спочатку читаємо новий jsonl формат
        if os.path.exists(jsonl):
            return await loop.run_in_executor(
                None, lambda: pd.read_json(jsonl, orient="records", lines=True)
            )
        # Fallback на старий json (без lines)
        if os.path.exists(legacy_json):
            return await loop.run_in_executor(None, pd.read_json, legacy_json)
        return None


# ---- Unified DataStore ------------------------------------------------------


class UnifiedDataStore:
    """
    Єдиний DataStore для всієї системи.

    Методи:
        get_df(symbol, interval, limit) — отримати DF (read-through RAM→Redis→Disk).
        put_bars(symbol, interval, bars) — записати нові бари (write-through RAM→Redis, write-behind Disk).
        get_last(symbol, interval) — повернути останній бар (з RAM або Redis).
        warmup(symbols, interval, bars_needed) — попередньо прогріти RAM.
        set_priority(symbol, level) — встановити пріоритет активу (ALERT/STAGE2/...).
        maintenance_task() — фонове обслуговування: sweep, flush queue, backpressure.

    Примітка:
        - Усі дані в Redis лежать у стабільному неймспейсі: ai_one:candles:{symbol}:{interval}
        - JSON-нормалізація — в RedisAdapter.
        - На диск — snapshot-и історії, агрегація виконується вище (Stage/DataManager v2).
    """

    def __init__(self, *, redis: Redis, cfg: Optional[StoreConfig] = None) -> None:
        self.cfg = cfg or StoreConfig()
        self.ram = RamLayer(self.cfg.profile)
        self.redis = RedisAdapter(redis, self.cfg)
        self.disk = StorageAdapter(self.cfg.base_dir, self.cfg)
        self.metrics = Metrics()

        # write-behind черга для диска
        self._flush_q: "Deque[Tuple[str, str, pd.DataFrame]]" = deque()
        self._ram_hits = 0
        self._ram_miss = 0
        self._redis_hits = 0
        self._redis_miss = 0

        self._mtx = asyncio.Lock()
        self._maint_task: Optional[asyncio.Task] = None

    # ---- Публічний API ------------------------------------------------------

    async def start_maintenance(self) -> None:
        """Запустити фонову задачку обслуговування."""
        if not self._maint_task:
            self._maint_task = asyncio.create_task(self._maintenance_loop())

    async def stop_maintenance(self) -> None:
        if self._maint_task:
            self._maint_task.cancel()
            try:
                await self._maint_task
            except asyncio.CancelledError:
                pass
            self._maint_task = None

    def set_priority(self, symbol: str, level: int) -> None:
        """Встановити пріоритет для активу (впливає на евікшен)."""
        self.ram.set_priority(symbol, level)

    # ---- Symbol selection helpers (prefilter integration) -----------------

    async def set_fast_symbols(self, symbols: List[str], ttl: int = 600) -> None:
        """Зберігає список активних (prefiltered) символів у Redis.

        Args:
            symbols: перелік символів у нижньому регістрі.
            ttl: час життя запису (секунди).
        """
        await self.redis.jset("selectors", "fast_symbols", value=symbols, ttl=ttl)

    async def get_fast_symbols(self) -> List[str]:
        """Повертає перелік символів із префільтра, або порожній список."""
        res = await self.redis.jget("selectors", "fast_symbols", default=[])
        return res or []

    async def get_last(self, symbol: str, interval: str) -> Optional[Dict[str, Any]]:
        """
        Повертає останній бар (словник), якщо він є в RAM/Redis.

        Args:
            symbol: Напр. "BTCUSDT".
            interval: "1m"|"5m"|...

        Returns:
            Останній бар або None.
        """
        t0 = time.perf_counter()

        # 1) RAM (спробуємо DF і візьмемо останній рядок)
        df = self.ram.get(symbol, interval)
        if df is not None and len(df):
            self._ram_hits += 1
            self.metrics.get_latency.labels(layer="ram").observe(
                time.perf_counter() - t0
            )
            return df.iloc[-1].to_dict()

        self._ram_miss += 1

        # 2) Redis
        last = await self.redis.jget("candles", symbol, interval, default=None)
        if last:
            self._redis_hits += 1
            self.metrics.get_latency.labels(layer="redis").observe(
                time.perf_counter() - t0
            )
            return last

        self._redis_miss += 1
        self.metrics.get_latency.labels(layer="miss").observe(time.perf_counter() - t0)
        return None

    # ---- Legacy cache compatibility (for raw_data & transitional code) ------

    async def fetch_from_cache(
        self,
        symbol: str,
        interval: str,
        *,
        prefix: str = "candles",
        raw: bool = True,
    ) -> Any:
        """Mimics old cache_handler.fetch_from_cache returning raw bytes.

        Stored under namespaced key: <namespace>:blob:<prefix>:<symbol>:<interval>
        so it doesn't collide with structured JSON keys used elsewhere.
        """
        key = k(self.cfg.namespace, "blob", prefix, symbol, interval)
        try:
            return await self.redis.r.get(key)  # type: ignore[attr-defined]
        except Exception as e:  # pragma: no cover
            logger.warning("fetch_from_cache failed %s: %s", key, e)
            return None

    async def store_in_cache(
        self,
        symbol: str,
        interval: str,
        payload: Any,
        *,
        ttl: Optional[int] = None,
        prefix: str = "candles",
        raw: bool = True,
    ) -> None:
        """Mimics old cache_handler.store_in_cache.

        Expects payload already serialized (bytes) when raw=True.
        """
        key = k(self.cfg.namespace, "blob", prefix, symbol, interval)
        try:
            if ttl:
                await self.redis.r.set(key, payload, ex=ttl)  # type: ignore[attr-defined]
            else:
                await self.redis.r.set(key, payload)  # type: ignore[attr-defined]
        except Exception as e:  # pragma: no cover
            logger.error("store_in_cache failed %s: %s", key, e)

    async def get_df(
        self, symbol: str, interval: str, *, limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Повертає DataFrame барів. Read-through RAM → Redis → Disk.

        Якщо знайдено тільки частину (наприклад, в Redis лише останній бар) — модуль НЕ агрегує історію;
        історію тримаємо батчами у RAM і snapshot-ами на диску.

        Args:
            symbol: Символ (напр. "BTCUSDT").
            interval: Таймфрейм (напр. "1m").
            limit: Обмеження рядків на повернення (опційно).

        Returns:
            DataFrame із стовпцями OHLCV.
        """
        t0 = time.perf_counter()

        # 1) RAM
        df = self.ram.get(symbol, interval)
        if df is not None:
            self._ram_hits += 1
            self.metrics.get_latency.labels(layer="ram").observe(
                time.perf_counter() - t0
            )
            return df.tail(limit) if limit else df

        self._ram_miss += 1

        # 2) Redis (останній бар) — як доповнення
        last = await self.redis.jget("candles", symbol, interval, default=None)
        if last:
            self._redis_hits += 1
            last_df = pd.DataFrame([last])
        else:
            self._redis_miss += 1
            last_df = pd.DataFrame(columns=list(MIN_COLUMNS))

        # 3) Disk snapshot
        disk_df = await self.disk.load_bars(symbol, interval)
        if disk_df is None or disk_df.empty:
            out = last_df
        else:
            out = pd.concat([disk_df, last_df], ignore_index=True)
            out = self._dedup_sort(out)

        # кешуємо назад у RAM
        if len(out):
            self.ram.put(symbol, interval, out)

        self._publish_hit_ratios()
        self.metrics.get_latency.labels(layer="disk").observe(time.perf_counter() - t0)
        return out.tail(limit) if limit else out

    async def put_bars(self, symbol: str, interval: str, bars: pd.DataFrame) -> None:
        """
        Записує нові бари: RAM → Redis (write-through), Disk (write-behind).

        Args:
            symbol: Символ.
            interval: Інтервал (напр. "1m").
            bars: DataFrame барів (OHLCV), можна інкрементальні.
        """
        t0 = time.perf_counter()

        # Normalize open_time dtype early (ms int) to avoid Timestamp/int compare issues
        if "open_time" in bars.columns:
            try:
                if not pd.api.types.is_integer_dtype(bars["open_time"]):
                    bars = bars.copy()
                    bars["open_time"] = (
                        pd.to_datetime(
                            bars["open_time"], unit="ms", errors="coerce"
                        ).astype("int64")
                        // 10**6
                    )
            except Exception:
                pass

        if self.cfg.validate_on_write:
            self._validate_bars(bars, stage="put_bars")

        async with self._mtx:
            # 1) змерджити з RAM
            current = self.ram.get(symbol, interval)
            merged = self._merge_bars(current, bars)
            self.ram.put(symbol, interval, merged)

            # 2) останній бар у Redis
            ttl = self.cfg.intervals_ttl.get(interval, self.cfg.profile.warm_ttl_sec)
            last_bar = merged.iloc[-1].to_dict()
            await self.redis.jset("candles", symbol, interval, value=last_bar, ttl=ttl)

            # 3) write-behind на диск
            if self.cfg.write_behind:
                self._flush_q.append((symbol, interval, merged))
                self.metrics.flush_backlog.set(len(self._flush_q))
            else:
                await self.disk.save_bars(symbol, interval, merged)

        self.metrics.put_latency.labels(layer="ram+redis").observe(
            time.perf_counter() - t0
        )
        try:
            self.metrics.last_put_ts.set(int(time.time()))
        except Exception:
            pass

    async def warmup(self, symbols: List[str], interval: str, bars_needed: int) -> None:
        """
        Прогріває RAM із диска (якщо є snapshot-и), встановлює TTL/пріоритети.
        """
        for s in symbols:
            df = await self.disk.load_bars(s, interval)
            if df is None or df.empty:
                continue
            if self.cfg.validate_on_read:
                self._validate_bars(df, stage="warmup_read")
            if bars_needed > 0:
                df = df.tail(bars_needed)
            self.ram.put(s, interval, self._dedup_sort(df))

    # ---- Фонова обслуга -----------------------------------------------------

    async def _maintenance_loop(self) -> None:
        """
        Фонова задачка: sweep RAM, скидання write-behind, контроль backpressure.
        """
        try:
            while True:
                await asyncio.sleep(1.0)
                # RAM sweep
                self.ram.sweep(self.metrics)

                # Flush queue
                await self._drain_flush_queue()

                # Оновити метрики
                self._publish_hit_ratios()
        except asyncio.CancelledError:
            # фінальний дренаж
            await self._drain_flush_queue(force=True)
            raise

    async def _drain_flush_queue(self, *, force: bool = False) -> None:
        """Скидання write-behind черги з backpressure."""
        limit = self.cfg.profile.flush_batch_max
        size = len(self._flush_q)

        # м'який/жорсткий тиск
        if size > self.cfg.profile.flush_queue_soft and not force:
            limit = max(1, limit // 2)
            logger.warning(
                f"[DataStore] Backpressure: backlog={size}, batch_limit={limit}"
            )
        if size > self.cfg.profile.flush_queue_hard and not force:
            # аварійний режим — агресивно ріжемо batch
            limit = 1
            logger.error(
                f"[DataStore] Severe backpressure: backlog={size}, forcing batch_limit={limit}"
            )

        for _ in range(min(limit, size) if not force else size):
            symbol, interval, df = self._flush_q.popleft()
            try:
                await self.disk.save_bars(symbol, interval, df)
            except Exception as e:
                # якщо не вдалось — повертаємо в хвіст і почекаємо
                logger.error(f"Disk flush failed for {symbol} {interval}: {e}")
                self._flush_q.append((symbol, interval, df))
                await asyncio.sleep(self.cfg.io_retry_backoff)

        self.metrics.flush_backlog.set(len(self._flush_q))

    # ---- Внутрішні перевірки/злиття -----------------------------------------

    @staticmethod
    def _dedup_sort(df: pd.DataFrame) -> pd.DataFrame:
        if "open_time" in df.columns:
            df = df.drop_duplicates(subset=["open_time"]).sort_values("open_time")
        return df.reset_index(drop=True)

    def _merge_bars(
        self, current: Optional[pd.DataFrame], new: pd.DataFrame
    ) -> pd.DataFrame:
        # Coerce open_time in both frames to homogeneous int64 ms
        def _coerce(df: pd.DataFrame) -> pd.DataFrame:
            if "open_time" in df.columns and not pd.api.types.is_integer_dtype(
                df["open_time"]
            ):
                try:
                    df = df.copy()
                    df["open_time"] = (
                        pd.to_datetime(
                            df["open_time"], unit="ms", errors="coerce"
                        ).astype("int64")
                        // 10**6
                    )
                except Exception:
                    pass
            return df

        new = _coerce(new)
        if current is None or current.empty:
            return self._dedup_sort(new.copy())
        current = _coerce(current)
        # Early append optimization: if new strictly after current
        try:
            if (
                "open_time" in current.columns
                and "open_time" in new.columns
                and len(current)
                and len(new)
            ):
                last_cur = int(current["open_time"].iloc[-1])
                first_new = int(new["open_time"].iloc[0])
                if first_new > last_cur:
                    # fast path: just concatenate (already monotonic)
                    parts = [df for df in (current, new) if df is not None and len(df)]
                    if len(parts) == 1:
                        return self._dedup_sort(parts[0].copy())
                    return self._dedup_sort(pd.concat(parts, ignore_index=True))
        except Exception:
            pass
        # fallback merge + dedup
        parts = [df for df in (current, new) if df is not None and len(df)]
        if not parts:
            return pd.DataFrame(
                columns=new.columns if isinstance(new, pd.DataFrame) else []
            )
        if len(parts) == 1:
            return self._dedup_sort(parts[0].copy())
        cat = pd.concat(parts, ignore_index=True)
        return self._dedup_sort(cat)

    def _validate_bars(self, df: pd.DataFrame, *, stage: str) -> None:
        cols = set(df.columns)
        missing = MIN_COLUMNS - cols
        if missing:
            logger.error(f"[validate:{stage}] Missing columns: {missing}")
            self.metrics.errors.inc(stage=f"validate_{stage}")
        # простий детектор гепів (по open_time)
        if "open_time" in cols:
            s = pd.to_datetime(df["open_time"], unit="ms", errors="coerce")
            gaps = s.isna().sum()
            if gaps:
                logger.warning(f"[validate:{stage}] NaT in open_time: {gaps}")
        # монотонність часу
        if "open_time" in cols and len(df) > 1:
            s = pd.to_numeric(df["open_time"], errors="coerce")
            if not pd.Series(s).is_monotonic_increasing:
                logger.warning(f"[validate:{stage}] Non-monotonic open_time detected")

    def _publish_hit_ratios(self) -> None:
        total_ram = self._ram_hits + self._ram_miss
        total_redis = self._redis_hits + self._redis_miss
        if total_ram:
            self.metrics.ram_hit_ratio.set(self._ram_hits / total_ram)
        if total_redis:
            self.metrics.redis_hit_ratio.set(self._redis_hits / total_redis)

    # ---- Інспектори ----------------------------------------------------------

    def debug_stats(self) -> Dict[str, Any]:
        st = self.ram.stats
        st.update(
            {
                "flush_backlog": len(self._flush_q),
                "ram_hits": self._ram_hits,
                "ram_miss": self._ram_miss,
                "redis_hits": self._redis_hits,
                "redis_miss": self._redis_miss,
            }
        )
        return st

    # --- Metrics snapshot for UI / external publishing ---------------------
    def metrics_snapshot(self) -> Dict[str, Any]:
        """Lightweight snapshot of key metrics for UI publisher.

        Prometheus client already holds internal time-series; this is for
        lightweight Redis pub/sub (UI) without scraping HTTP endpoint.
        """
        try:
            ram_ratio = (
                self._ram_hits / (self._ram_hits + self._ram_miss)
                if (self._ram_hits + self._ram_miss)
                else 0.0
            )
            redis_ratio = (
                self._redis_hits / (self._redis_hits + self._redis_miss)
                if (self._redis_hits + self._redis_miss)
                else 0.0
            )
            return {
                "ram_hit_ratio": round(ram_ratio, 6),
                "redis_hit_ratio": round(redis_ratio, 6),
                "bytes_in_ram": self.ram.stats.get("bytes_in_ram", 0),
                "flush_backlog": len(self._flush_q),
                "timestamp": int(time.time()),
            }
        except Exception as e:  # pragma: no cover
            logger.warning("metrics_snapshot failed: %s", e)
            return {"error": str(e)}
