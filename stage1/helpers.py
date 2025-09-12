# stage1\helpers.py

"""
Утиліти для фільтрації активів на Binance Futures
"""

from aiohttp import ClientResponseError, ClientConnectionError
import asyncio
import aiohttp
import pandas as pd
import json
import logging
from typing import Dict, List, Tuple, Optional

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception,
    before_sleep_log,
)
from utils.utils_1_2 import ensure_timestamp_column

from stage1.config import (
    OI_SEMAPHORE,
    KLINES_SEMAPHORE,
    DEPTH_SEMAPHORE,
    REDIS_CACHE_TTL,
)
from rich.console import Console
from rich.logging import RichHandler

# --- Налаштування логування ---
logger = logging.getLogger("helpers")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False


def _is_retryable(exc: BaseException) -> bool:
    """
    Визначає, чи варто повторювати запит (для tenacity).
    :param exc: Exception
    :return: True якщо exception підлягає повтору
    """
    logger.debug(f"[RETRY] Перевірка exception: {exc}")
    if isinstance(exc, (ClientConnectionError, asyncio.TimeoutError)):
        logger.debug("[RETRY] ClientConnectionError або TimeoutError, повторюємо")
        return True
    if isinstance(exc, ClientResponseError) and exc.status >= 500:
        logger.debug(f"[RETRY] ClientResponseError {exc.status} >= 500, повторюємо")
        return True
    logger.debug("[RETRY] Exception не підлягає повтору")
    return False


retry_decorator = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=0.5, max=10),
    retry=retry_if_exception(_is_retryable),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)


async def _fetch_json(session: aiohttp.ClientSession, url: str) -> list | dict:
    """
    Виконує HTTP GET запит до вказаного URL і повертає JSON.
    Якщо виникає помилка (наприклад, HTTP 451 або інша),
    логгує повідомлення та повертає порожній словник.

    :param session: об'єкт aiohttp.ClientSession для виконання запиту.
    :param url: URL-адреса запиту.
    :return: JSON дані у вигляді списку або словника, або {} при помилці.
    """
    logger.debug(f"[STEP] _fetch_json: GET {url}")
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            logger.debug(f"[EVENT] Відповідь отримано, статус: {resp.status}")
            resp.raise_for_status()
            result = await resp.json()
            logger.debug(f"[EVENT] JSON отримано: {type(result)}")
            return result
    except ClientResponseError as e:
        if e.status == 451:
            logger.error("HTTP 451: Доступ заблоковано для URL %s", url)
        else:
            logger.error("HTTP помилка %s для URL %s", e.status, url)
    except Exception as exc:
        logger.error(
            "Неочікувана помилка при запиті до URL %s: %s", url, exc, exc_info=True
        )
    logger.debug(f"[EVENT] Повертаємо порожній словник для {url}")
    return {}


@retry_decorator
async def fetch_cached_data(
    session: aiohttp.ClientSession, cache_handler, key: str, url: str, process_fn=None
) -> dict:
    """
    Універсальна функція для кешованих запитів.
    :param session: aiohttp.ClientSession
    :param cache_handler: об'єкт кешу
    :param key: ключ кешу
    :param url: URL для запиту
    :param process_fn: функція обробки даних
    :return: оброблені дані
    """
    logger.debug(f"[STEP] _fetch_cached_data: key={key}, url={url}")
    cached = await cache_handler.fetch_from_cache(
        symbol=key, interval="global", prefix="meta"
    )
    logger.debug(f"[EVENT] Кеш отримано: {type(cached)}")

    if cached is not None:
        if isinstance(cached, pd.DataFrame):
            # logger.warning("Кешовані дані у форматі DataFrame, але очікується JSON")
            cached = ensure_timestamp_column(cached)
            if cached.empty:
                logger.debug("Кеш порожній, запит до API")
                cached = None
            else:
                data = cached.to_dict(orient="records")
                logger.debug(
                    f"[EVENT] Повертаємо кешовані дані DataFrame: {len(data)} записів"
                )
                return data
        elif isinstance(cached, (bytes, str)) and cached:
            try:
                data = json.loads(cached)
                logger.debug(f"[EVENT] Повертаємо кешовані дані JSON: {type(data)}")
                return data
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning("Помилка десеріалізації кешу %s: %s", key, e)
                await cache_handler.delete_from_cache(key, "global", "meta")

    logger.debug("[STEP] Кеш не валідний, запит до API")
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
        logger.debug(f"[EVENT] Відповідь API отримано, статус: {resp.status}")
        resp.raise_for_status()
        data = await resp.json()
        logger.debug(f"[EVENT] Дані з API отримано: {type(data)}")

        processed = process_fn(data) if process_fn else data
        logger.debug(f"[EVENT] Дані оброблено: {type(processed)}")

        await cache_handler.store_in_cache(
            symbol=key,
            interval="global",
            data=json.dumps(processed),
            ttl=REDIS_CACHE_TTL,
            prefix="meta",
        )
        logger.debug(f"[EVENT] Дані збережено у кеш: {key}")
        return processed


async def fetch_concurrently(
    session: aiohttp.ClientSession,
    symbols: List[str],
    endpoint_fn,
    semaphore: asyncio.Semaphore,
    progress_callback: Optional[callable] = None,  # Додаємо callback
) -> Dict[str, float]:
    """
    Паралельний збір даних для списку символів.
    :param session: aiohttp.ClientSession
    :param symbols: список символів
    :param endpoint_fn: функція для отримання метрики
    :param semaphore: семафор для обмеження паралелізму
    :return: dict {symbol: value}
    """
    logger.debug(f"[STEP] _fetch_concurrently: symbols={symbols}")

    async def _fetch_single(sym: str) -> Tuple[str, float]:
        async with semaphore:
            try:
                logger.debug(f"[EVENT] Запит метрики для {sym}")
                value = await endpoint_fn(session, sym)

                # Викликаємо callback для оновлення прогресу
                if progress_callback:
                    progress_callback()

                logger.debug(f"[EVENT] Метрика для {sym}: {value}")
                return sym, value
            except Exception as e:
                logger.error("Помилка для %s: %s", sym, e)
                return sym, 0.0

    tasks = [_fetch_single(sym) for sym in symbols]
    results = await asyncio.gather(*tasks)
    logger.debug(f"[EVENT] Зібрано результати: {results}")
    return {sym: value for sym, value in results}


async def fetch_atr(session: aiohttp.ClientSession, symbol: str) -> float:
    """
    Розрахунок ATR за останні 14 днів для символу.
    :param session: aiohttp.ClientSession
    :param symbol: символ
    :return: ATR у %%
    """
    url = (
        f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1d&limit=15"
    )
    logger.debug(f"[STEP] _fetch_atr: symbol={symbol}, url={url}")
    async with KLINES_SEMAPHORE:
        try:
            data = await _fetch_json(session, url)
            logger.debug(f"[EVENT] Дані для ATR: {len(data)} записів")
            if len(data) < 15:
                logger.debug(f"[EVENT] Недостатньо даних для ATR: {len(data)}")
                return 0.0

            closes = [float(c[4]) for c in data]
            highs = [float(h[2]) for h in data]
            lows = [float(l[3]) for l in data]

            tr_values = []
            for i in range(1, len(data)):
                tr = max(
                    highs[i] - lows[i],
                    abs(highs[i] - closes[i - 1]),
                    abs(lows[i] - closes[i - 1]),
                )
                tr_values.append(tr)

            atr = sum(tr_values) / len(tr_values)
            current_price = closes[-1]
            result = (atr / current_price) * 100 if current_price else 0.0
            logger.debug(f"[EVENT] ATR для {symbol}: {result}")
            return result
        except Exception as e:
            logger.error("ATR помилка для %s: %s", symbol, e)
            return 0.0


async def fetch_orderbook_depth(session: aiohttp.ClientSession, symbol: str) -> float:
    """
    Розрахунок глибини стакану для символу.
    :param session: aiohttp.ClientSession
    :param symbol: символ
    :return: глибина стакану
    """
    url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=10"
    logger.debug(f"[STEP] _fetch_orderbook_depth: symbol={symbol}, url={url}")
    async with DEPTH_SEMAPHORE:
        try:
            data = await _fetch_json(session, url)
            logger.debug(f"[EVENT] Дані для depth: {type(data)}")
            total_value = 0.0
            for side in ["bids", "asks"]:
                for price, qty in data.get(side, [])[:10]:
                    total_value += float(price) * float(qty)
            logger.debug(f"[EVENT] Глибина стакану для {symbol}: {total_value}")
            return total_value
        except Exception as e:
            logger.error("Depth помилка для %s: %s", symbol, e)
            return 0.0


async def fetch_open_interest(session: aiohttp.ClientSession, symbol: str) -> float:
    """
    Отримання Open Interest для символу.
    :param session: aiohttp.ClientSession
    :param symbol: символ
    :return: open interest
    """
    url = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol}"
    logger.debug(f"[STEP] _fetch_open_interest: symbol={symbol}, url={url}")
    async with OI_SEMAPHORE:
        try:
            data = await _fetch_json(session, url)
            value = float(data.get("openInterest", 0.0))
            logger.debug(f"[EVENT] Open Interest для {symbol}: {value}")
            return value
        except Exception as e:
            logger.error("OI помилка для %s: %s", symbol, e)
            return 0.0
