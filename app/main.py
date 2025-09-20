"""AiOne_t — точка входу системи.

Завдання модуля:
    • Bootstrap UnifiedDataStore та пов'язані сервіси (metrics, admin, health)
    • Підготовка списку активів (ручний або автоматичний префільтр)
    • Preload історії / денні рівні / ініціалізація LevelManager
    • Запуск WebSocket стрімера (WSWorker) та Stage1 моніторингу
    • Запуск Screening Producer + публікація початкового snapshot у Redis
    • Запуск менеджера угод (TradeLifecycleManager) та оновлювача

Архітектурні акценти:
    • Єдине джерело даних: UnifiedDataStore (Redis + RAM)
    • Мінімум побічних ефектів у глобальному просторі — все через bootstrap()
    • Логування уніфіковане (RichHandler, українська локалізація повідомлень)
"""

import asyncio
import json
import logging
import os
import subprocess
import sys
from pathlib import Path

import aiohttp
from dotenv import load_dotenv
from redis.asyncio import Redis
from rich.console import Console
from rich.logging import RichHandler

from app.screening_producer import AssetStateManager, screening_producer
from app.settings import DataStoreCfg, load_datastore_cfg, settings
from app.utils.helper import (
    estimate_atr_pct,
    resample_5m,
    store_to_dataframe,
)
from config.config import (
    FAST_SYMBOLS_TTL_AUTO,
    FAST_SYMBOLS_TTL_MANUAL,
    MANUAL_FAST_SYMBOLS_SEED,
    PREFILTER_BASE_PARAMS,
    PREFILTER_INTERVAL_SEC,
    PRELOAD_1M_LOOKBACK_INIT,
    PRELOAD_DAILY_DAYS,
    SCREENING_LOOKBACK,
    STAGE1_MONITOR_PARAMS,
    STAGE1_PREFILTER_THRESHOLDS,
    STAGE2_CONFIG,  # (залишаємо якщо ще потрібні switch'і Stage2)
    USER_SETTINGS_DEFAULT,
)

# UnifiedDataStore now the single source of truth
from data.unified_store import StoreConfig, StoreProfile, UnifiedDataStore

# ─────────────────────────── Імпорти бізнес-логіки ───────────────────────────
from data.ws_worker import WSWorker
from monitoring.transcript_recorder import TranscriptConfig, TranscriptRecorder
from stage1.asset_monitoring import AssetMonitorStage1
from stage1.indicators import calculate_global_levels
from stage1.optimized_asset_filter import get_filtered_assets
from stage2.level_manager import LevelManager
from stage3.trade_manager import TradeLifecycleManager
from stage3.trade_manager_updater import trade_manager_updater
from UI.publish_full_state import publish_full_state
from UI.ui_consumer import UIConsumer
from utils.utils import get_tick_size

from .admin import DataStoreAdmin, admin_command_loop
from .preload_and_update import (
    periodic_prefilter_and_update,
    preload_1m_history,
    preload_daily_levels,
)

# Завантажуємо налаштування з .env
load_dotenv()

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("app.main")
if not logger.handlers:  # захист від повторної ініціалізації
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False


# (FastAPI вилучено) — якщо потрібен REST інтерфейс у майбутньому,
# повернемо створення app/router

# ───────────────────────────── Глобальні змінні модуля ─────────────────────────────
# Єдиний інстанс UnifiedDataStore (створюється в bootstrap)
store: UnifiedDataStore | None = None

# Повністю видалено калібрацію та RAMBuffer — єдиний шар даних UnifiedDataStore

# ───────────────────────────── Шлях / каталоги ─────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
# Каталог зі статичними файлами (фронтенд WebApp)
STATIC_DIR = BASE_DIR / "static"


async def bootstrap() -> UnifiedDataStore:
    """Ініціалізація інфраструктурних компонентів.

    Кроки:
      1. Завантаження datastore конфігурації
      2. Підключення до Redis
      3. Ініціалізація UnifiedDataStore + maintenance loop
      4. (Опційно) запуск Prometheus metrics server
      5. Запуск командного адміністративного циклу та health-pinger
    """
    global store
    cfg = load_datastore_cfg()
    logger.info(
        "[Launch] datastore.yaml loaded: namespace=%s base_dir=%s",
        cfg.namespace,
        cfg.base_dir,
    )
    redis = Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
    )
    logger.info(
        "[Launch] Redis client created host=%s port=%s",
        os.getenv("REDIS_HOST", "localhost"),
        os.getenv("REDIS_PORT", "6379"),
    )
    # Pydantic v2: use model_dump(); fallback to dict() for backward compat
    try:
        profile_data = cfg.profile.model_dump()  # type: ignore[attr-defined]
    except Exception:
        profile_data = cfg.profile.dict()
    store_cfg = StoreConfig(
        namespace=cfg.namespace,
        base_dir=cfg.base_dir,
        profile=StoreProfile(**profile_data),
        intervals_ttl=cfg.intervals_ttl,
        write_behind=cfg.write_behind,
        validate_on_read=cfg.validate_on_read,
        validate_on_write=cfg.validate_on_write,
        io_retry_attempts=cfg.io_retry_attempts,
        io_retry_backoff=cfg.io_retry_backoff,
    )
    store = UnifiedDataStore(redis=redis, cfg=store_cfg)
    await store.start_maintenance()
    logger.info("[Launch] UnifiedDataStore maintenance loop started")
    # Опційний запуск TranscriptRecorder (стенограми) через змінну середовища
    if os.getenv("MONITOR_TRANSCRIPT", "0") in ("1", "true", "True"):
        try:
            raw = os.getenv("MONITOR_SYMBOLS", "").lower()
            allowed = [s.strip() for s in raw.split(",") if s.strip()]
            allowed_set = set(allowed)
            tr_cfg = TranscriptConfig(
                base_dir=cfg.base_dir, allowed_symbols=allowed_set or None
            )
            store.transcript = TranscriptRecorder(tr_cfg)  # type: ignore[attr-defined]
            await store.transcript.start()  # type: ignore[attr-defined]
            try:
                # Діагностика: зафіксувати старт і whitelist символів у стенограмі
                store.transcript.log_meta(  # type: ignore[attr-defined]
                    started=True,
                    allowed_symbols=list(allowed_set) if allowed_set else None,
                )
            except Exception:
                pass
            logger.info("[Launch] Transcript recorder enabled")
        except Exception:
            logger.warning("Transcript recorder initialization failed", exc_info=True)
    # adapters removed – use store directly

    prom_started = start_prometheus_if_enabled(cfg)
    if prom_started:
        logger.info("[Launch] Prometheus metrics server on :%s", cfg.prometheus.port)
    if getattr(cfg.admin, "enabled", True):
        admin = DataStoreAdmin(store, store.redis, cfg)
        asyncio.create_task(admin_command_loop(admin))
        logger.info(
            "[Launch] Admin command loop started (channel=%s)",
            cfg.admin.commands_channel,
        )
    else:
        logger.info("[Launch] Admin command loop disabled via settings")
    asyncio.create_task(health_pinger(store.metrics, cfg))
    asyncio.create_task(event_loop_lag_sampler())
    logger.info("[Launch] Admin command loop + health pinger started")
    return store


def start_prometheus_if_enabled(cfg: DataStoreCfg) -> bool:
    """Запускає HTTP endpoint метрик, якщо активовано у конфізі.

    Returns:
        bool: True якщо сервер стартував, False якщо вимкнено або залежність відсутня.
    """
    if not cfg.prometheus.enabled:
        return False
    try:
        from prometheus_client import start_http_server  # type: ignore

        start_http_server(cfg.prometheus.port)
        return True
    except Exception:  # broad except: зовнішня залежність може бути не встановлена
        logger.warning("Prometheus client не встановлено – метрики HTTP не активні")
        return False


async def health_pinger(metrics: object, cfg: DataStoreCfg) -> None:
    """Проста періодична інкрементація лічильника для моніторингу життєздатності."""
    while True:
        try:
            # ds_errors_total має label 'stage' -> треба викликати через labels
            metrics.errors.labels(stage="health_ping").inc()  # type: ignore
        except Exception:
            # fallback на raw inc без labels якщо Noop або інша реалізація
            try:
                metrics.errors.inc()  # type: ignore
            except Exception:
                pass
        await asyncio.sleep(cfg.admin.health_ping_sec)


async def event_loop_lag_sampler(interval: float = 0.5) -> None:
    """Періодично вимірює лаг планування event loop та експортує
    histogram event_loop_lag_seconds.

    Лаг = (фактичний інтервал між циклами) - interval якщо позитивний.
    Негативні/нульові ігноруємо. Використовується кастомний набір бакетів
    для коротких затримок.
    """
    try:
        from prometheus_client import Histogram  # type: ignore
    except Exception:  # pragma: no cover
        return

    try:
        loop_lag = Histogram(
            "event_loop_lag_seconds",
            "Asyncio event loop scheduling lag (actual sleep - expected)",
            buckets=(
                0.0005,
                0.001,
                0.002,
                0.005,
                0.01,
                0.02,
                0.05,
                0.1,
                0.2,
                0.5,
                1.0,
                2.0,
            ),
        )
    except Exception:  # already registered
        try:  # pragma: no cover
            from prometheus_client import REGISTRY  # type: ignore

            loop_lag = None
            for m in REGISTRY.collect():
                if m.name == "event_loop_lag_seconds":
                    loop_lag = m
                    break
            if loop_lag is None:
                return
        except Exception:
            return

    loop = asyncio.get_event_loop()
    prev = loop.time()
    while True:
        await asyncio.sleep(interval)
        now = loop.time()
        actual = now - prev
        diff = actual - interval
        if diff > 0:
            try:
                loop_lag.observe(diff)  # type: ignore
            except Exception:
                pass
        prev = now


def launch_ui_consumer() -> None:
    """Запускає `UI.ui_consumer_entry` у новому терміналі (Windows / *nix)."""
    proj_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if sys.platform.startswith("win"):
        subprocess.Popen(
            ["start", "cmd", "/k", "python", "-m", "UI.ui_consumer_entry"],
            shell=True,
            cwd=proj_root,  # запуск з кореня проекту, щоб UI бачився як модуль
        )
    else:
        subprocess.Popen(
            ["gnome-terminal", "--", "python3", "-m", "UI.ui_consumer_entry"],
            cwd=proj_root,
        )


def validate_settings() -> None:
    """Перевіряє необхідні змінні середовища (Redis + Binance ключі)."""
    missing: list[str] = []
    if not os.getenv("REDIS_URL"):
        if not settings.redis_host:
            missing.append("REDIS_HOST")
        if not settings.redis_port:
            missing.append("REDIS_PORT")

    if not settings.binance_api_key:
        missing.append("BINANCE_API_KEY")
    if not settings.binance_secret_key:
        missing.append("BINANCE_SECRET_KEY")

    if missing:
        raise ValueError(f"Відсутні налаштування: {', '.join(missing)}")

    logger.info("Налаштування перевірено — OK.")


# Legacy init_system removed (UnifiedDataStore handles Redis connection)


async def noop_healthcheck() -> None:
    """Легкий healthcheck-плейсхолдер (RAMBuffer видалено)."""
    while True:
        await asyncio.sleep(120)


async def run_pipeline() -> None:
    """Основний асинхронний цикл застосунку (оркестрація компонентів)."""

    # 1. Ініціалізація
    # Initialize unified store
    ds = await bootstrap()
    # Калібрація та окремий RAMBuffer видалені — залишаємо лише Stage2 switches
    stage2_config = STAGE2_CONFIG
    level_manager = LevelManager()  # Менеджер рівнів підтримки/опору
    # Отримуємо налаштування користувача (з конфігураційного файлу)
    user_settings = USER_SETTINGS_DEFAULT.copy()

    # ATRManager не використовується без калібрації

    # Підключення до Redis
    redis_conn = Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        decode_responses=True,
        encoding="utf-8",
    )

    launch_ui_consumer()  # Запускаємо UI-споживача у новому терміналі
    trade_manager = TradeLifecycleManager(log_file="trade_log.jsonl")  # Менеджер угод
    thresholds = STAGE1_PREFILTER_THRESHOLDS.copy()

    # 2. Створюємо довгоживу ClientSession
    session = aiohttp.ClientSession()
    try:
        # Preload функції тепер працюють без окремого fetcher —
        # прямі HTTP виклики через session

        # ===== НОВА ЛОГІКА ВИБОРУ РЕЖИМУ =====
        use_manual_list = (
            True  # Змінити на False для автоматичного режиму, True - для ручного
        )

        if use_manual_list:
            # Ручний режим: використовуємо фіксований список
            fast_symbols = MANUAL_FAST_SYMBOLS_SEED.copy()
            await ds.set_fast_symbols(fast_symbols, ttl=FAST_SYMBOLS_TTL_MANUAL)
            logger.info(f"[Main] Використовуємо ручний список символів: {fast_symbols}")
        else:
            # Автоматичний режим: виконуємо первинний префільтр
            logger.info("[Main] Запускаємо первинний префільтр...")

            # Використовуємо новий механізм відбору активів
            fast_symbols = await get_filtered_assets(
                session=session,
                cache_handler=ds,
                min_quote_vol=thresholds["MIN_QUOTE_VOLUME"],
                min_price_change=thresholds["MIN_PRICE_CHANGE"],
                min_oi=thresholds["MIN_OPEN_INTEREST"],
                min_depth=float(PREFILTER_BASE_PARAMS["min_depth"]),
                min_atr=float(PREFILTER_BASE_PARAMS["min_atr"]),
                max_symbols=int(thresholds["MAX_SYMBOLS"]),
                dynamic=bool(PREFILTER_BASE_PARAMS["dynamic"]),
            )

            fast_symbols = [s.lower() for s in fast_symbols]
            await ds.set_fast_symbols(fast_symbols, ttl=FAST_SYMBOLS_TTL_AUTO)
            # Логуємо кількість символів
            logger.info(f"[Main] Первинний префільтр: {len(fast_symbols)} символів")

        # Отримуємо актуальний список символів
        fast_symbols = await ds.get_fast_symbols()
        if not fast_symbols:
            logger.error("[Main] Не вдалося отримати список символів. Завершення.")
            return

        logger.info(
            "[Main] Початковий список символів: %s (кількість: %s)",
            fast_symbols,
            len(fast_symbols),
        )

        # Preload історії
        # TODO: refactor preload to use store directly (task 4)
        await preload_1m_history(
            fast_symbols, ds, lookback=PRELOAD_1M_LOOKBACK_INIT, session=session
        )

        # Preload денних рівнів
        daily_data = await preload_daily_levels(
            fast_symbols, days=PRELOAD_DAILY_DAYS, session=session
        )
        for sym, df in daily_data.items():
            levels = calculate_global_levels(df, window=20)
            level_manager.set_daily_levels(sym, levels)

        # === Первинне наповнення рівнів із UnifiedDataStore (RAMBuffer видалено) ===
        for sym in fast_symbols:
            # buffer_to_dataframe will be replaced with store-based helper later
            df_1m = await store_to_dataframe(ds, sym, limit=500)  # unified store
            df_5m = resample_5m(df_1m)
            df_1d = daily_data.get(sym)  # у тебе вже є daily_data (30 днів)
            atr_pct = estimate_atr_pct(df_1m)
            price_hint = (
                float(df_1m["close"].iloc[-1])
                if df_1m is not None and not df_1m.empty
                else None
            )
            tick_size = get_tick_size(sym, price_hint=price_hint)

            level_manager.update_meta(sym, atr_pct=atr_pct, tick_size=tick_size)
            level_manager.update_from_bars(sym, df_1m=df_1m, df_5m=df_5m, df_1d=df_1d)

        # Калібрація вимкнена: створюємо тільки AssetStateManager
        assets_current = [s.lower() for s in fast_symbols]
        state_manager = AssetStateManager(assets_current)

        # Ініціалізація AssetMonitorStage1
        logger.info("[Main] Ініціалізуємо AssetMonitorStage1...")

        async def on_alert_stage2(signal: dict) -> None:
            try:
                from stage2.level_manager import LevelManager
                from stage2.processor import Stage2Processor

                proc = Stage2Processor(
                    timeframe="1m",
                    state_manager=state_manager,
                    level_manager=level_manager or LevelManager(),
                    user_lang=user_settings["lang"],
                    user_style=user_settings["style"],
                )
                result = await proc.process(signal)
                # Merge back key fields to state
                update = {
                    "stage2": True,
                    "stage2_status": "COMPLETED",
                    "last_updated": result.get("processing_time"),
                    "recommendation": result.get("recommendation"),
                    "market_context": result.get("market_context"),
                    "risk_parameters": result.get("risk_parameters"),
                    "confidence_metrics": result.get("confidence_metrics"),
                    "narrative": result.get("narrative"),
                }
                state_manager.update_asset(signal["symbol"], update)
            except Exception as _e:
                logger.debug("Stage2 on_alert callback failed: %s", _e)

        monitor = AssetMonitorStage1(
            cache_handler=ds,
            state_manager=state_manager,
            feature_switches=stage2_config.get("switches"),
            vol_z_threshold=float(STAGE1_MONITOR_PARAMS.get("vol_z_threshold", 2.0)),
            rsi_overbought=STAGE1_MONITOR_PARAMS.get("rsi_overbought"),
            rsi_oversold=STAGE1_MONITOR_PARAMS.get("rsi_oversold"),
            min_reasons_for_alert=int(
                STAGE1_MONITOR_PARAMS.get("min_reasons_for_alert", 2)
            ),
            dynamic_rsi_multiplier=float(
                STAGE1_MONITOR_PARAMS.get("dynamic_rsi_multiplier", 1.1)
            ),
            on_alert=on_alert_stage2,
        )
        # Надаємо доступ до монітора через store для WSWorker reactive hook
        try:
            # безпечніше пряме присвоєння, аніж setattr (ruff B010)
            ds.stage1_monitor = monitor  # type: ignore[attr-defined]
        except Exception:
            pass

        # ── Виконуємо фон-воркери ──────────────────────────────────────────
        # WSWorker still legacy; will be refactored to use store (task 5)
        # WSWorker v2 (selectors_key configurable,
        # intervals_ttl for legacy blob TTL overrides)
        ws_worker = WSWorker(
            fast_symbols,
            store=ds,
            selectors_key="selectors:fast_symbols",  # use store helper path
            intervals_ttl={"1m": 90, "1h": 65 * 60},
        )
        ws_task = asyncio.create_task(ws_worker.consume())
        health_task = asyncio.create_task(noop_healthcheck())

        # UI metrics publisher (Redis pub/sub) — lightweight snapshot every 5s
        async def ui_metrics_publisher() -> None:
            channel = "ui.metrics"
            while True:
                snap = ds.metrics_snapshot()
                # add hot symbols count (unique symbols in RAM layer)
                try:
                    hot_symbols = list({s for (s, _i) in ds.ram._lru.keys()})  # type: ignore[attr-defined]
                    snap["hot_symbols"] = len(hot_symbols)
                except Exception:
                    snap["hot_symbols"] = None
                try:
                    await ds.redis.r.publish(channel, json.dumps(snap))  # type: ignore[attr-defined]
                except Exception as e:
                    logger.debug("ui_metrics publish failed: %s", e)
                await asyncio.sleep(5)

        metrics_task = asyncio.create_task(ui_metrics_publisher())

        # Ініціалізуємо UI-споживача
        logger.info("[Main] Ініціалізуємо UI-споживача...")
        UIConsumer()

        # ── Reactive Stage1 (optional): rely on WS hook only ──
        reactive_enabled = os.getenv("REACTIVE_STAGE1", "0") in ("1", "true", "True")
        prod = None
        if not reactive_enabled:
            # Запускаємо Screening Producer (batch mode)
            logger.info("[Main] Запускаємо Screening Producer...")
            prod = asyncio.create_task(
                screening_producer(
                    monitor=monitor,
                    store=ds,
                    store_fast_symbols=ds,
                    assets=fast_symbols,
                    redis_conn=redis_conn,
                    trade_manager=trade_manager,
                    timeframe="1m",
                    lookback=SCREENING_LOOKBACK,
                    interval_sec=30,
                    state_manager=state_manager,
                    level_manager=level_manager,
                    user_lang=user_settings["lang"],
                    user_style=user_settings["style"],
                )
            )

        # Публікуємо початковий стан в Redis
        logger.info("[Main] Публікуємо початковий стан в Redis...")
        await publish_full_state(state_manager, ds, redis_conn)

        # Запускаємо TradeLifecycleManager для управління угодами
        logger.info("[Main] Запускаємо TradeLifecycleManager...")
        trade_update_task = asyncio.create_task(
            trade_manager_updater(
                trade_manager,
                ds,
                monitor,
            )
        )

        # Запускаємо періодичне оновлення тільки в автоматичному режимі
        prefilter_task = None
        if not use_manual_list:
            prefilter_task = asyncio.create_task(
                periodic_prefilter_and_update(
                    ds,
                    session,
                    thresholds,
                    interval=PREFILTER_INTERVAL_SEC,
                    buffer=ds,
                )
            )

        # Завдання для збору
        tasks_to_run = [ws_task, health_task, trade_update_task, metrics_task]
        if prod is not None:
            tasks_to_run.append(prod)

        if prefilter_task:
            tasks_to_run.append(prefilter_task)

        await asyncio.gather(*tasks_to_run)
    except Exception as e:
        logger.error("[Main] run_pipeline error: %s", e)
    finally:
        # Акуратне завершення стенографа (якщо увімкнено)
        try:
            tr = getattr(ds, "transcript", None)
            if tr is not None:
                await tr.stop()  # type: ignore[attr-defined]
        except Exception:
            pass
        await session.close()


# (metrics endpoint видалено разом із FastAPI роутингом)


if __name__ == "__main__":
    try:
        asyncio.run(run_pipeline())
    except Exception as e:
        logger.error("Помилка виконання: %s", e, exc_info=True)
        sys.exit(1)
