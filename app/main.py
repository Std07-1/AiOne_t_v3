"""
Основний модуль запуску системи AiOne_t
app/main.py
"""

import time
import time
import asyncio
import logging
import os
import sys
from pathlib import Path
from dataclasses import asdict

from redis.asyncio import Redis
import subprocess
import aiohttp
import pandas as pd
from dotenv import load_dotenv

# FastAPI було використано раніше для експорту metrics, зараз видаляємо залежність

# ─────────────────────────── Імпорти бізнес-логіки ───────────────────────────
from data.cache_handler import SimpleCacheHandler
from data.raw_data import OptimizedDataFetcher
from data.file_manager import FileManager
from data.ws_worker import WSWorker
from data.ram_buffer import RAMBuffer

from stage1.asset_monitoring import AssetMonitorStage1
from app.screening_producer import screening_producer, publish_full_state
from .preload_and_update import (
    preload_1m_history,
    preload_daily_levels,
    periodic_prefilter_and_update,
)
from UI.ui_consumer import UI_Consumer
from stage1.optimized_asset_filter import get_filtered_assets
from stage3.trade_manager import TradeLifecycleManager
from stage3.trade_manager_updater import trade_manager_updater
from app.settings import settings
from rich.console import Console
from rich.logging import RichHandler
from app.screening_producer import AssetStateManager
from stage2.config import STAGE2_CONFIG  # (залишаємо якщо ще потрібні switch'і Stage2)
from stage2.level_manager import LevelManager
from stage1.indicators import calculate_global_levels
from app.utils.helper import (
    buffer_to_dataframe,
    resample_5m,
    estimate_atr_pct,
    get_tick_size,
)

# Завантажуємо налаштування з .env
load_dotenv()

# --- Логування ---
main_logger = logging.getLogger("main")
main_logger.setLevel(logging.INFO)
main_logger.handlers.clear()
main_logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
main_logger.propagate = False  # ← Критично важливо!


# (FastAPI вилучено) — якщо потрібен REST інтерфейс у майбутньому, повернемо створення app/router

# Глобальний Redis-кеш, ініціалізується при старті
cache_handler: SimpleCacheHandler

# (calib_queue видалено — калібрація відключена)

# Шлях до кореня проекту
BASE_DIR = Path(__file__).resolve().parent.parent
# Каталог зі статичними файлами (фронтенд WebApp)
STATIC_DIR = BASE_DIR / "static"


def launch_ui_consumer():
    """
    Запуск UI/ui_consumer_entry.py у новому терміналі (Windows).
    """
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
    """
    Перевіряємо необхідні змінні оточення:
      - REDIS_URL або (REDIS_HOST + REDIS_PORT)
      - BINANCE_API_KEY, BINANCE_SECRET_KEY
    """
    missing = []
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

    main_logger.info("Налаштування перевірено — OK.")


async def init_system() -> SimpleCacheHandler:
    """
    Ініціалізація зовнішніх систем:
      - Валідація налаштувань
      - Підключення до Redis
    Повертає: інстанс SimpleCacheHandler
    """
    validate_settings()

    redis_url = os.getenv("REDIS_URL")
    if redis_url:
        handler = SimpleCacheHandler.from_url(redis_url)
        # logger.debug("Redis через URL: %s", redis_url)
    else:
        handler = SimpleCacheHandler(
            host=settings.redis_host,
            port=settings.redis_port,
        )
        # logger.debug(
        #    "Redis через host/port: %s:%s",
        #    settings.redis_host,
        #    settings.redis_port,
        # )
    return handler


# --- Дебаг-функція для RAMBuffer ---
def debug_ram_buffer(buffer, symbols, tf="1m"):
    """
    Дебажить свіжість барів у RAMBuffer для заданих символів.
    """
    now = int(time.time() * 1000)
    for sym in symbols:
        bars = buffer.get(sym, tf, 3)
        if bars:
            last_ts = bars[-1]["timestamp"]
            print(f"[{sym}] Last bar: {last_ts} | Age: {(now - last_ts) // 1000}s")
        else:
            print(f"[{sym}] No bars in RAMBuffer")


# --- HealthCheck для RAMBuffer ---
async def ram_buffer_healthcheck(
    buffer, symbols, max_age=90, interval=30, ws_worker=None, tf="1m"
):
    """
    Моніторинг живучості даних у RAMBuffer.
    Якщо дані по символу не оновлювались >max_age сек — лог WARN і опційно перезапуск WSWorker.
    """
    while True:
        now = int(time.time() * 1000)
        dead = []
        for sym in symbols:
            bars = buffer.get(sym, tf, 1)
            if not bars or (now - bars[-1]["timestamp"]) > max_age * 1000:
                dead.append(sym)
        if dead:
            main_logger.warning("[HealthCheck] Symbols stalled: %s", dead)
            if ws_worker is not None:
                main_logger.warning(
                    "[HealthCheck] Restarting WSWorker через застій символів."
                )
                await ws_worker.stop()
                asyncio.create_task(ws_worker.consume())
        else:
            main_logger.debug(
                "[HealthCheck] Всі символи активні (перевірено %d).", len(symbols)
            )
        await asyncio.sleep(interval)


async def run_pipeline() -> None:
    """
    Основний pipeline:
    1. Ініціалізація системи та кешу
    2. Pre-filter активів
    3. Запуск ws_worker + RAMBuffer для 1m даних + healthcheck
    4. Запуск скринінгу (screening_producer) для stage1
    5. (Опційно) запуск UI/live-stats/fastapi
    """

    # 1. Ініціалізація
    cache = await init_system()  # Ініціалізуємо кеш
    file_manager = FileManager()  # Файловий менеджер для зберігання даних
    buffer = RAMBuffer(max_bars=120)  # RAMBuffer для зберігання історії
    # Калібрація відключена: прибираємо CalibrationConfig / CalibrationEngine / Queue
    stage2_config = STAGE2_CONFIG  # Залишаємо лише конфіг Stage2 для switches тощо
    level_manager = LevelManager()  # Менеджер рівнів підтримки/опору
    # Отримуємо налаштування користувача (з конфігураційного файлу)
    user_settings = {
        "lang": "UA",  # Мова за замовчуванням (може бути "UA" або "EN")
        "style": "pro",  # Стиль за замовчуванням (може бути "pro", "explain", "short" )
    }

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
    file_manager = FileManager()  # Файловий менеджер
    buffer = RAMBuffer(max_bars=500)  # RAMBuffer для зберігання історії
    thresholds = {
        "MIN_QUOTE_VOLUME": 1_000_000.0,  # Мінімальний об'єм торгівлі
        "MIN_PRICE_CHANGE": 3.0,  # Мінімальна зміна ціни
        "MIN_OPEN_INTEREST": 500_000.0,  # Мінімальний відкритий інтерес
        "MAX_SYMBOLS": 350,  # Максимальна кількість символів
    }

    # 2. Створюємо довгоживу ClientSession
    session = aiohttp.ClientSession()
    try:
        fetcher = OptimizedDataFetcher(cache_handler=cache, session=session)

        # ===== НОВА ЛОГІКА ВИБОРУ РЕЖИМУ =====
        use_manual_list = (
            True  # Змінити на False для автоматичного режиму, True - для ручного
        )

        if use_manual_list:
            # Ручний режим: використовуємо фіксований список
            fast_symbols = [
                "btcusdt",
                "OMNIUSDT",
                "tonusdt",
                "PUMPUSDT",
            ]
            await cache.set_fast_symbols(fast_symbols, ttl=3600)  # TTL 1 година
            main_logger.info(
                f"[Main] Використовуємо ручний список символів: {fast_symbols}"
            )
        else:
            # Автоматичний режим: виконуємо первинний префільтр
            main_logger.info("[Main] Запускаємо первинний префільтр...")

            # Використовуємо новий механізм відбору активів
            fast_symbols = await get_filtered_assets(
                session=session,
                cache_handler=cache,
                min_quote_vol=1_000_000.0,
                min_price_change=3.0,
                min_oi=500_000.0,
                min_depth=50_000.0,
                min_atr=0.5,
                max_symbols=350,
                dynamic=False,  # Фіксований список для первинного префільтру
            )

            fast_symbols = [s.lower() for s in fast_symbols]
            await cache.set_fast_symbols(fast_symbols, ttl=600)  # TTL 10 хвилин
            # Логуємо кількість символів
            main_logger.info(
                f"[Main] Первинний префільтр: {len(fast_symbols)} символів"
            )

        # Отримуємо актуальний список символів
        fast_symbols = await cache.get_fast_symbols()
        if not fast_symbols:
            main_logger.error("[Main] Не вдалося отримати список символів. Завершення.")
            return

        main_logger.info(
            f"[Main] Початковий список символів: {fast_symbols} (кількість: {len(fast_symbols)})"
        )

        # Preload історії
        await preload_1m_history(
            fetcher, fast_symbols, buffer, lookback=500
        )  # 500 барів (~8.3 години)

        # Preload денних рівнів
        daily_data = await preload_daily_levels(fetcher, fast_symbols, days=30)
        for sym, df in daily_data.items():
            levels = calculate_global_levels(df, window=20)
            level_manager.set_daily_levels(sym, levels)

        # === LevelSystem v2: первинне наповнення з RAMBuffer (після preload) ===
        for sym in fast_symbols:
            df_1m = buffer_to_dataframe(buffer, sym, limit=500)
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
        main_logger.info("[Main] Ініціалізуємо AssetMonitorStage1...")
        monitor = AssetMonitorStage1(
            cache_handler=cache,
            state_manager=state_manager,  # Додаємо state_manager для інтеграції
            vol_z_threshold=2.5,
            rsi_overbought=70,
            rsi_oversold=30,
            min_reasons_for_alert=2,
            feature_switches=stage2_config.get("switches"),
        )

        # --- Виконуємо фон-воркери ---
        ws_task = asyncio.create_task(WSWorker(fast_symbols, buffer, cache).consume())
        health_task = asyncio.create_task(
            ram_buffer_healthcheck(buffer, fast_symbols, ws_worker=None)
        )

        # Ініціалізуємо UI-споживача
        main_logger.info("[Main] Ініціалізуємо UI-споживача...")
        ui = UI_Consumer()

        # Запускаємо Screening Producer
        main_logger.info("[Main] Запускаємо Screening Producer...")
        prod = asyncio.create_task(
            screening_producer(
                monitor,
                buffer,
                cache,
                fast_symbols,
                redis_conn,
                fetcher,
                trade_manager=trade_manager,
                timeframe="1m",
                lookback=50,
                interval_sec=30,
                # calib_engine / calib_queue видалені
                # Додаємо state_manager для повної інтеграції (опціонально)
                state_manager=state_manager,
                level_manager=level_manager,  # Менеджер рівнів підтримки/опору
                # Передаємо налаштування користувачаuser_lang=user_settings["lang"],
                user_lang=user_settings["lang"],
                user_style=user_settings["style"],
            )
        )

        # Публікуємо початковий стан в Redis
        main_logger.info("[Main] Публікуємо початковий стан в Redis...")
        await publish_full_state(state_manager, cache, redis_conn)

        # Запускаємо TradeLifecycleManager для управління угодами
        main_logger.info("[Main] Запускаємо TradeLifecycleManager...")
        trade_update_task = asyncio.create_task(
            trade_manager_updater(
                trade_manager,
                buffer,
                monitor,
            )
        )

        # Запускаємо періодичне оновлення тільки в автоматичному режимі
        prefilter_task = None
        if not use_manual_list:
            prefilter_task = asyncio.create_task(
                periodic_prefilter_and_update(
                    cache,
                    session,
                    thresholds,
                    interval=600,
                    buffer=buffer,
                    fetcher=fetcher,
                )
            )

        # Завдання для збору
        tasks_to_run = [
            ws_task,
            health_task,
            prod,
            trade_update_task,
        ]

        if prefilter_task:
            tasks_to_run.append(prefilter_task)

        await asyncio.gather(*tasks_to_run)
    finally:
        await session.close()


# (metrics endpoint видалено разом із FastAPI роутингом)


if __name__ == "__main__":
    try:
        asyncio.run(run_pipeline())
    except Exception as e:
        main_logger.error("Помилка виконання: %s", e, exc_info=True)
        sys.exit(1)
