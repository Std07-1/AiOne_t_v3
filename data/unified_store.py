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
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

import pandas as pd
from redis.asyncio import Redis
from rich.console import Console
from rich.logging import RichHandler

from config.config import DATASTORE_BASE_DIR, NAMESPACE

# ── Логування ──
logger = logging.getLogger("app.data.unified_store")
if not logger.handlers:  # guard проти повторної ініціалізації
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False

# ── Стандарти й константи ──

DEFAULT_NAMESPACE = NAMESPACE

_HAS_PARQUET = (
    False  # підтримка pyarrow прибрана (раніше була опціональним плейсхолдером)
)

REQUIRED_OHLCV_COLS = (
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
)
MIN_COLUMNS: set[str] = set(REQUIRED_OHLCV_COLS)


# ── Допоміжні структури ──
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
    intervals_ttl: dict[str, int] = field(
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
    base_dir: str = DATASTORE_BASE_DIR
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


# ── Ключі / імена ───────────────────────────────────────────────────────────
def k(namespace: str, *parts: str) -> str:
    """Будує стабільний Redis-ключ: ai_one:part1:part2..."""
    sane = [p.strip(":") for p in parts if p]
    return ":".join([namespace, *sane])


def file_name(symbol: str, context: str, event: str, ext: str = "parquet") -> str:
    """Ім'я файла у форматі: SYMBOL_context_event.ext"""
    return f"{symbol}_{context}_{event}.{ext}"


# ── Метрики ─────────────────────────────────────────────────────────────────
class _Noop:
    def inc(self, amount: float = 1.0) -> None:
        return None

    def set(self, value: float) -> None:
        return None

    def observe(self, amount: float, exemplar: dict[str, str] | None = None) -> None:
        return None

    # імітує chaining інтерфейс prometheus-клієнта
    def labels(self, *labelvalues: str, **labelkw: str) -> _Noop:
        return self


@runtime_checkable
class CounterLike(Protocol):
    def inc(self, amount: float = ...) -> None: ...

    def labels(self, *labelvalues: str, **labelkw: str) -> CounterLike: ...


@runtime_checkable
class GaugeLike(Protocol):
    def set(self, value: float) -> None: ...

    def labels(self, *labelvalues: str, **labelkw: str) -> GaugeLike: ...


@runtime_checkable
class HistogramLike(Protocol):
    def observe(
        self, amount: float, exemplar: dict[str, str] | None = None
    ) -> None: ...

    def labels(self, *labelvalues: str, **labelkw: str) -> HistogramLike: ...


class Metrics:
    """Проста обгортка метрик з опціональною Prometheus-інтеграцією.

    Примітка: текстові описи метрик (help) лишаємо англійською для сумісності з
    наявними дашбордами / алертами.
    """

    def __init__(self) -> None:
        try:
            from prometheus_client import Counter, Gauge, Histogram

            self._counter = Counter
            self._gauge = Gauge
            self._hist = Histogram
            self.enabled = True
        except Exception:  # pragma: no cover
            self.enabled = False

        # Атрибути метрик типізовані через Protocol-інтерфейси, щоби підтримувати _Noop
        self.get_latency: HistogramLike
        self.put_latency: HistogramLike
        self.ram_hit_ratio: GaugeLike
        self.redis_hit_ratio: GaugeLike
        self.bytes_in_ram: GaugeLike
        self.flush_backlog: GaugeLike
        self.evictions: CounterLike
        self.errors: CounterLike
        self.last_put_ts: GaugeLike

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


# ── RAM Layer ────────────────────────────────────────────────────────────────
class RamLayer:
    """RAM-кеш з TTL, LRU, квотами, пріоритетами й приблизною оцінкою пам'яті."""

    def __init__(self, profile: StoreProfile) -> None:
        self._store: dict[tuple[str, str], tuple[pd.DataFrame, float, int]] = {}
        self._lru: OrderedDict[tuple[str, str], None] = OrderedDict()
        self._prio: dict[str, int] = {}  # symbol -> Priority
        self._profile = profile
        self._bytes_in_ram: int = 0

    # ── Утиліти ─────────────────────────────────────────────────────────────

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

    # ── API ─────────────────────────────────────────────────────────────────

    def set_priority(self, symbol: str, level: int) -> None:
        self._prio[symbol] = level

    def get_priority(self, symbol: str) -> int:
        return self._prio.get(symbol, Priority.NORMAL)

    def get(self, symbol: str, interval: str) -> pd.DataFrame | None:
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

    def delete(self, key: tuple[str, str], *, reason: str = "evict") -> None:
        item = self._store.pop(key, None)
        if item:
            df, _, _ = item
            self._bytes_in_ram -= self._estimate_bytes(df)
        if key in self._lru:
            del self._lru[key]

    def sweep(self, metrics: Metrics) -> None:
        """Прибрати протухлі ключі/зайві записи."""
        now = time.time()
        expired: list[tuple[str, str]] = []
        for key, (_df, ts, ttl) in list(self._store.items()):
            if now - ts > ttl:
                expired.append(key)
        for key in expired:
            self.delete(key, reason="ttl_expired")
            metrics.evictions.labels(reason="ttl_expired").inc()

        self._enforce_quotas()

        metrics.bytes_in_ram.set(self._bytes_in_ram)

    # ── Внутрішнє ───────────────────────────────────────────────────────────

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
        ranked: list[tuple[int, int, tuple[str, str]]] = []
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

    # ── Інспектори ──────────────────────────────────────────────────────────

    @property
    def stats(self) -> dict[str, Any]:
        return {
            "entries": len(self._store),
            "bytes_in_ram": self._bytes_in_ram,
            "lru_len": len(self._lru),
        }


# ── Redis Adapter ──
class RedisAdapter:
    """Обгортка над redis.asyncio.Redis з JSON-нормалізацією та retry."""

    def __init__(self, redis: Redis[Any], cfg: StoreConfig) -> None:
        self.r = redis
        self.cfg = cfg

    async def jget(self, *parts: str, default: object | None = None) -> object | None:
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
        return default

    async def jset(self, *parts: str, value: object, ttl: int | None = None) -> None:
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


# ── Disk Adapter ──
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
            # Унікальне ім'я tmp, щоб уникнути гонок між паралельними флушами
            import os as _os
            import threading as _thr
            import time as _time

            def _uniq_tmp(base: Path) -> Path:
                return base.with_suffix(
                    base.suffix
                    + f".tmp.{_os.getpid()}.{_thr.get_ident()}.{int(_time.time()*1000)}"
                )

            tmp = _uniq_tmp(p)
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
            # Гарантуємо існування tmp (на випадок, якщо writer не створив файл)
            try:
                if not tmp.exists():
                    tmp.touch()
            except Exception:
                pass

            # На Windows os.replace може падати (WinError 32), якщо ціль тимчасово відкрита читачем.
            # 1) Ретраїмо PermissionError з backoff;
            # 2) Якщо FileNotFoundError (tmp зник) — створюємо новий tmp і пробуємо знову;
            # 3) Інші помилки — віддаємо нагору.
            last_exc: Exception | None = None
            for attempt in range(10):  # ~0.05..0.5s → сумарно ~2.75s
                try:
                    tmp.replace(p)  # atomic move
                    last_exc = None
                    break
                except PermissionError as e:
                    last_exc = e
                    _time.sleep(0.05 * (attempt + 1))
                    continue
                except FileNotFoundError as e:
                    # Можливі причини: паралельний флуш вже замінив файл і видалив наш tmp,
                    # або антивірус/cleanup. Якщо ціль існує і не порожня — вважаємо успіхом.
                    last_exc = e
                    try:
                        if p.exists() and p.stat().st_size >= 0:
                            last_exc = None
                            break
                    except Exception:
                        pass
                    # Інакше — відтворимо новий tmp і повторимо спробу
                    tmp = _uniq_tmp(p)
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
                    try:
                        if not tmp.exists():
                            tmp.touch()
                    except Exception:
                        pass
                    _time.sleep(0.05 * (attempt + 1))
                    continue
                except Exception as e:
                    last_exc = e
                    break
            if last_exc is not None:
                raise last_exc

        try:
            if _HAS_PARQUET:
                await loop.run_in_executor(None, _write_parquet, path, df)
            else:
                await loop.run_in_executor(None, _write_jsonl, path, df)
            return str(path)
        except Exception:
            # pragma: no cover
            # broad-except: повний traceback для діагностики нестабільних I/O
            logger.exception("Disk flush failed for %s %s", symbol, interval)
            raise

    async def load_bars(self, symbol: str, interval: str) -> pd.DataFrame | None:
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


# ── Unified DataStore ──
class UnifiedDataStore:
    """Єдине шарувате сховище даних для всієї системи.

    Основні методи:
    get_df(symbol, interval, limit) — отримати DataFrame
    (read-through RAM→Redis→Disk).
    put_bars(symbol, interval, bars) — запис нових барів
    (write-through RAM→Redis, write-behind Disk).
        get_last(symbol, interval) — останній бар (RAM або Redis).
        warmup(symbols, interval, bars_needed) — прогрів RAM зі snapshot-ів.
        set_priority(symbol, level) — задати пріоритет активу.
        start_maintenance/stop_maintenance — керування фоновою обслугою.

    Примітки:
        • Дані в Redis під ключами: ai_one:candles:{symbol}:{interval}
        • JSON-серіалізація в адаптері RedisAdapter.
        • На диск пишемо snapshot історії; агрегація/обчислення поза цим шаром.
    """

    # Публічні поля-атрибути з анотаціями типів
    _flush_q: deque[tuple[str, str, pd.DataFrame]]
    _maint_task: asyncio.Task[Any] | None

    def __init__(self, *, redis: Redis[Any], cfg: StoreConfig | None = None) -> None:
        self.cfg = cfg or StoreConfig()
        self.ram = RamLayer(self.cfg.profile)
        self.redis = RedisAdapter(redis, self.cfg)
        self.disk = StorageAdapter(self.cfg.base_dir, self.cfg)
        self.metrics = Metrics()

        # write-behind черга для диска
        self._flush_q = deque()
        self._ram_hits = 0
        self._ram_miss = 0
        self._redis_hits = 0
        self._redis_miss = 0

        self._mtx = asyncio.Lock()
        self._maint_task = None

    # ── Публічний API ───────────────────────────────────────────────────────

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

    # ── Symbol selection helpers (prefilter integration) ────────────────────

    async def set_fast_symbols(self, symbols: list[str], ttl: int = 600) -> None:
        """Зберігає список активних (prefiltered) символів у Redis.

        Args:
            symbols: перелік символів у нижньому регістрі.
            ttl: час життя запису (секунди).
        """
        await self.redis.jset("selectors", "fast_symbols", value=symbols, ttl=ttl)

    async def get_fast_symbols(self) -> list[str]:
        """Повертає перелік символів із префільтра, або порожній список."""
        res = await self.redis.jget("selectors", "fast_symbols", default=[])
        return list(res) if isinstance(res, list) else []

    async def get_last(self, symbol: str, interval: str) -> dict[str, Any] | None:
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
            # pandas returns dict[str, Any]
            return dict(df.iloc[-1].to_dict())

        self._ram_miss += 1

        # 2) Redis
        last = await self.redis.jget("candles", symbol, interval, default=None)
        if isinstance(last, dict):
            self._redis_hits += 1
            self.metrics.get_latency.labels(layer="redis").observe(
                time.perf_counter() - t0
            )
            return last

        self._redis_miss += 1
        self.metrics.get_latency.labels(layer="miss").observe(time.perf_counter() - t0)
        return None

    # ── Legacy cache compatibility (for raw_data & transitional code) ───────
    # DEPRECATED: перехідний blob CacheHandler API. Видалити після міграції
    # ws_worker.py та thresholds.py
    # на структуровані ключі (jget/jset) ai_one:candles:* та ai_one:selectors:*.
    # Blob ключі ізольовано під ai_one:blob:* щоби уникнути колізій.

    async def fetch_from_cache(
        self,
        symbol: str,
        interval: str,
        *,
        prefix: str = "candles",
        raw: bool | None = None,
    ) -> bytes | None:
        """Сумісний із застарілим cache_handler.fetch_from_cache (повертає сирі байти).

        Зберігаємо під ключем: <namespace>:blob:<prefix>:<symbol>:<interval>
        щоб уникнути колізій зі структурованими JSON-ключами.
        """
        key = k(self.cfg.namespace, "blob", prefix, symbol, interval)
        try:
            raw_bytes: bytes | None = await self.redis.r.get(key)
            return raw_bytes
        except Exception as e:
            # pragma: no cover
            # broad-except: legacy шлях не повинен зривати основний потік
            logger.warning("fetch_from_cache failed %s: %s", key, e)
            return None

    async def store_in_cache(
        self,
        symbol: str,
        interval: str,
        payload: bytes,
        *,
        ttl: int | None = None,
        prefix: str = "candles",
        raw: bool | None = None,
    ) -> None:
        """Сумісність зі старим cache_handler.store_in_cache.

        Очікує, що payload вже серіалізований у bytes якщо raw=True.
        """
        key = k(self.cfg.namespace, "blob", prefix, symbol, interval)
        try:
            if ttl:
                await self.redis.r.set(key, payload, ex=ttl)
            else:
                await self.redis.r.set(key, payload)
        except (
            Exception
        ) as e:  # pragma: no cover  # broad-except: збій запису blob не критичний
            logger.error("store_in_cache failed %s: %s", key, e)

    async def delete_from_cache(
        self,
        symbol: str,
        interval: str,
        *,
        prefix: str = "candles",
    ) -> None:
        """Legacy API: видалити blob-запис (сумісність зі старим CacheHandler).

        Старий код іноді викликає delete_from_cache(key, "global", "meta") з іншою
        сигнатурою. Тут ми зберігаємо спрощену форму: symbol+interval (+prefix).
        Якщо потрібно масове очищення або meta-ключі — слід переписати виклики на
        jset/jget рівень поза blob namespace.
        """
        key = k(self.cfg.namespace, "blob", prefix, symbol, interval)
        try:
            await self.redis.r.delete(key)
        except (
            Exception
        ) as e:  # pragma: no cover  # broad-except: видалення blob не критичне
            logger.warning("delete_from_cache failed %s: %s", key, e)

    async def get_df(
        self, symbol: str, interval: str, *, limit: int | None = None
    ) -> pd.DataFrame:
        """Повертає DataFrame барів (read-through RAM→Redis→Disk).

        Якщо доступний лише останній бар у Redis — історія не агрегується; історія
        підтримується батчами в RAM та snapshot-ами на диску.

        Аргументи:
            symbol: Напр. "BTCUSDT".
            interval: Напр. "1m".
            limit: (опційно) максимум рядків у відповіді.

        Повертає:
            DataFrame з OHLCV стовпцями.
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
        # Уникаємо FutureWarning: concat з порожніми або all‑NA DataFrame
        frames: list[pd.DataFrame] = []
        if disk_df is not None and not disk_df.empty:
            frames.append(disk_df)
        if not last_df.empty:
            frames.append(last_df)

        if frames:
            out = pd.concat(frames, ignore_index=True)
            out = self._dedup_sort(out)
        else:
            out = last_df  # обидва порожні → повертаємо порожній каркас

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

        # Нормалізуємо dtype open_time (ms int) щоб уникнути порівнянь Timestamp/int
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
            except Exception:  # broad-except: коерція open_time не критична
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
        except (
            Exception
        ):  # broad-except: fast-path оптимізація, fallback до загального merge
            pass

    async def warmup(self, symbols: list[str], interval: str, bars_needed: int) -> None:
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

    # ── Фонова обслуга ──────────────────────────────────────────────────────

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
                (
                    "[DataStore] Severe backpressure: backlog=%s, forcing "
                    "batch_limit=%s"
                ),
                size,
                limit,
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

    # ── Внутрішні перевірки / злиття ────────────────────────────────────────

    @staticmethod
    def _dedup_sort(df: pd.DataFrame) -> pd.DataFrame:
        if "open_time" in df.columns:
            df = df.drop_duplicates(subset=["open_time"]).sort_values("open_time")
        return df.reset_index(drop=True)

    def _merge_bars(
        self, current: pd.DataFrame | None, new: pd.DataFrame
    ) -> pd.DataFrame:
        # Приведення open_time в обох фреймах до однорідного int64 (ms)
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
                    # broad-except: best-effort коерція
                    # (пропускаємо пошкоджені значення)
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
                    # fast-path: просто конкатенація (вже монотонно)
                    parts = [df for df in (current, new) if df is not None and len(df)]
                    if len(parts) == 1:
                        return self._dedup_sort(parts[0].copy())
                    return self._dedup_sort(pd.concat(parts, ignore_index=True))
        except Exception:
            pass
        # fallback: злиття + dedup
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
            logger.error(f"[validate:{stage}] Відсутні стовпці: {missing}")
            try:
                self.metrics.errors.labels(stage=f"validate_{stage}").inc()
            except Exception:
                try:
                    self.metrics.errors.inc()
                except Exception:
                    pass
        # простий детектор гепів (по open_time)
        if "open_time" in cols:
            s_dt = pd.to_datetime(df["open_time"], unit="ms", errors="coerce")
            gaps = s_dt.isna().sum()
            if gaps:
                logger.warning(f"[validate:{stage}] NaT у open_time: {gaps}")
        # монотонність часу
        if "open_time" in cols and len(df) > 1:
            s_num = pd.to_numeric(df["open_time"], errors="coerce")
            if not pd.Series(s_num).is_monotonic_increasing:
                logger.warning(f"[validate:{stage}] Виявлено немонотонний open_time")

    def _publish_hit_ratios(self) -> None:
        total_ram = self._ram_hits + self._ram_miss
        total_redis = self._redis_hits + self._redis_miss
        if total_ram:
            self.metrics.ram_hit_ratio.set(self._ram_hits / total_ram)
        if total_redis:
            self.metrics.redis_hit_ratio.set(self._redis_hits / total_redis)

    # ── Інспектори ──────────────────────────────────────────────────────────

    def debug_stats(self) -> dict[str, Any]:
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

    # ── Зріз метрик для UI / публікації ───────────────────────────────────
    def metrics_snapshot(self) -> dict[str, Any]:
        """Легкий зріз ключових метрик для UI публікації.

        Prometheus вже зберігає часові ряди; це допоміжний формат для
        легкого Redis pub/sub без HTTP scraping.
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
        except (
            Exception
        ) as e:  # pragma: no cover  # broad-except: метрики не повинні кидати
            logger.warning("metrics_snapshot failed: %s", e)
            return {"error": str(e)}


# ── Публічні експортовані символи ─────────────────────────────────────────
__all__ = [
    "StoreConfig",
    "StoreProfile",
    "UnifiedDataStore",
    "Priority",
]
