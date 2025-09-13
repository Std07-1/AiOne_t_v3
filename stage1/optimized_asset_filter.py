# stage1/optimized_asset_filter.py
"""
Супершвидкий фільтр USDT‑M‑ф'ючерсів Binance з розширеними метриками

Головні можливості
------------------
* Паралельний збір даних з обмеженням семафорів
* Динамічні пороги на основі перцентилів
* Кешування exchangeInfo у Redis (3 год)
* Pydantic валідація параметрів
* Детальне логування та обробка помилок
* Ранжування за комбінованим liquidity_score
* Миттєва обробка до 500+ символів

Вихід: відсортований список тікерів, готовий для подальшої обробки
"""

from __future__ import annotations

from typing import List

import aiohttp

from config.config import FilterParams
from stage1.binance_future_asset_filter import BinanceFutureAssetFilter
import logging

from rich.console import Console
from rich.logging import RichHandler

# --- Налаштування логування ---
logger = logging.getLogger("optimized_asset_filter")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False


# PUBLIC INTERFACE
async def get_filtered_assets(
    session: aiohttp.ClientSession,
    cache_handler,
    min_quote_vol: float = 1_000_000,
    min_price_change: float = 3.0,
    min_oi: float = 500_000,
    min_depth: float = 50_000,
    min_atr: float = 0.5,
    max_symbols: int = 30,
    dynamic: bool = False,
) -> List[str]:
    """
    Публічний інтерфейс для отримання відфільтрованих активів Binance USDT-M Futures.
    Виконує всі етапи фільтрації через BinanceFutureAssetFilter.
    :param session: aiohttp.ClientSession для HTTP-запитів
    :param cache_handler: об'єкт кешу (наприклад, Redis)
    :param min_quote_vol: мінімальний об'єм торгів
    :param min_price_change: мінімальна зміна ціни
    :param min_oi: мінімальний open interest
    :param min_depth: мінімальна глибина orderbook
    :param min_atr: мінімальний ATR
    :param max_symbols: максимальна кількість символів у результаті
    :param dynamic: чи використовувати динамічні пороги
    :return: відсортований список символів

    Приклад використання:
    filtered = await get_filtered_assets(
        session, cache_handler,
        min_quote_vol=2_000_000,
        min_price_change=2.5,
        max_symbols=50
    )

    """
    logger.debug("[STEP] Ініціалізація параметрів фільтрації")
    params = FilterParams(
        min_quote_volume=min_quote_vol,  # мінімальний об'єм торгів
        min_price_change=min_price_change,  # мінімальна зміна ціни
        min_open_interest=min_oi,  # мінімальний open interest
        min_orderbook_depth=min_depth,  # мінімальна глибина orderbook
        min_atr_percent=min_atr,  # мінімальний ATR
        max_symbols=max_symbols,  # максимальна кількість символів у результаті
        # Динамічні пороги на основі перцентилів
        dynamic=dynamic,  # чи використовувати динамічні пороги
    )
    logger.debug(f"[EVENT] Параметри: {params.dict()}")

    logger.debug("[STEP] Створення екземпляра BinanceFutureAssetFilter")
    filter = BinanceFutureAssetFilter(session, cache_handler)
    result = await filter.filter_assets(params)
    logger.debug(f"[EVENT] Отримано {len(result)} символів після фільтрації")
    return result


async def get_filter_metrics() -> dict:
    """
    Отримання метрик останнього запуску фільтрації для налагодження.
    :return: dict з метриками, якщо доступні
    """
    logger.debug("[STEP] Запит метрик фільтрації")
    if hasattr(BinanceFutureAssetFilter, "last_metrics"):
        logger.debug("[EVENT] Метрики знайдено у класі BinanceFutureAssetFilter")
        return BinanceFutureAssetFilter.last_metrics
    logger.debug("[EVENT] Метрики не знайдено")
    return {}
