"""Планувальник періодичного prefilter та історичного preload.

Шлях: ``app/preload_and_update.py``

Фонові задачі:
    • periodic_prefilter_and_update — періодично виконує Stage1 prefilter і оновлює fast_symbols.
    • preload_1m_history — масове завантаження останніх 1m барів для холодного старту RAM шару.
    • preload_daily_levels — вибірка денних барів для глобальних рівнів / статистик.
"""

import asyncio
import logging
import pandas as pd
import aiohttp
from stage1.optimized_asset_filter import get_filtered_assets
from config.config import (
    PRELOAD_1M_LOOKBACK_INIT,
    SCREENING_LOOKBACK,
    PRELOAD_DAILY_DAYS,
    PREFILTER_INTERVAL_SEC,
)

from rich.console import Console
from rich.logging import RichHandler

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("app.preload")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False


# ── Фоновий таск: періодичне оновлення fast_symbols через prefilter ─────────
async def periodic_prefilter_and_update(
    cache,
    session: aiohttp.ClientSession,
    thresholds,
    interval: int = PREFILTER_INTERVAL_SEC,
    buffer=None,
    lookback: int = PRELOAD_1M_LOOKBACK_INIT,
):
    """
    Періодично виконує prefilter та оновлює fast_symbols у Redis.
    Додає preload історії для нових активів.
    """
    # Початковий набір символів
    initial_symbols = set(await cache.get_fast_symbols())
    prev_symbols = initial_symbols.copy()

    # Затримка перед першим оновленням (щоб уникнути конфлікту з первинним префільтром)
    await asyncio.sleep(interval)  # Чекаємо звичайний інтервал (600 сек)
    while True:
        try:
            logger.info("🔄 Оновлення списку fast_symbols через prefilter...")
            fast_symbols = await get_filtered_assets(
                session=session,
                cache_handler=cache,
                thresholds=thresholds,
                dynamic=True,
            )

            if fast_symbols:
                fast_symbols = [s.lower() for s in fast_symbols]
                current_symbols = set(fast_symbols)
                await cache.set_fast_symbols(
                    fast_symbols, ttl=interval * 2
                )  # TTL 1200 сек

                logger.info(
                    "Prefilter: %d символів записано у Redis: %s",
                    len(fast_symbols),
                    fast_symbols[:5],
                )

                # ── preload для нових активів ──────────────────────────────
                if buffer is not None:
                    # Знаходимо ТІЛЬКИ нові символи
                    new_symbols = current_symbols - prev_symbols

                    # Додаємо debug-лог для відстеження станів символів
                    logger.debug(
                        f"Стан символів: "
                        f"Поточні={len(current_symbols)}, "
                        f"Попередні={len(prev_symbols)}, "
                        f"Нові={len(new_symbols)}"
                    )
                    if new_symbols:
                        new_symbols_list = list(new_symbols)
                        logger.info(
                            f"Preload історії для {len(new_symbols_list)} нових активів"
                        )
                        await preload_1m_history(
                            new_symbols_list, buffer, lookback=lookback, session=session
                        )
                        await preload_daily_levels(
                            new_symbols_list, days=30, session=session
                        )

                # Оновлюємо попередні символи
                prev_symbols = current_symbols
            else:
                logger.warning(
                    "Prefilter повернув порожній список, fast_symbols не оновлено."
                )
        except Exception as e:
            logger.warning("Помилка оновлення prefilter: %s", e)

        await asyncio.sleep(interval)  # 600 сек


# ── Preload історії для Stage1 ─────────────────────────────────────────────
async def _fetch_klines(
    symbol: str, interval: str, limit: int, session: aiohttp.ClientSession
):
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
    async with session.get(url, params=params, timeout=15) as resp:
        resp.raise_for_status()
        data = await resp.json()
    cols = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base",
        "taker_buy_quote",
        "ignore",
    ]
    import pandas as _pd

    df = _pd.DataFrame(data, columns=cols)
    for c in [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_asset_volume",
        "taker_buy_base",
        "taker_buy_quote",
    ]:
        df[c] = df[c].astype(float)
    df["open_time"] = df["open_time"].astype(int)
    df["close_time"] = df["close_time"].astype(int)
    return df


async def _fetch_batch(
    symbols: list[str], interval: str, limit: int, session: aiohttp.ClientSession
):
    out: dict[str, pd.DataFrame] = {}
    # Simple sequential to avoid rate limits; can be optimized later
    for sym in symbols:
        try:
            out[sym] = await _fetch_klines(sym, interval, limit, session)
        except Exception as e:
            logger.warning("fetch klines failed %s %s", sym, e)
            out[sym] = pd.DataFrame()
            await asyncio.sleep(0.2)
    return out


async def preload_1m_history(
    fast_symbols,
    store,
    lookback: int = SCREENING_LOOKBACK,
    session: aiohttp.ClientSession | None = None,
):
    """Preload 1m history directly into UnifiedDataStore (без зовнішнього fetcher).

    Якщо передано `session`, дані тягнуться з Binance API; інакше виконується no-op.
    Returns stats dict: {symbol: {loaded:int, missing:int}} plus aggregates.
    """
    if lookback < 12:
        logger.warning(
            "lookback (%d) для 1m-барів занадто малий. Встановлено мінімум 12.",
            lookback,
        )
        lookback = 12

    logger.info(
        "Preload 1m: завантажуємо %d 1m-барів для %d символів…",
        lookback,
        len(fast_symbols),
    )
    if session is None:
        logger.warning(
            "preload_1m_history: session not provided — пропуск завантаження"
        )
        raw = {sym: pd.DataFrame() for sym in fast_symbols}
    else:
        raw = await _fetch_batch(fast_symbols, "1m", lookback, session)
    stats: dict[str, dict[str, int]] = {}
    for sym, df in raw.items():
        if df is None or df.empty:
            stats[sym] = {"loaded": 0, "missing": lookback}
            continue
        # Normalize to store schema (open_time/close_time)
        if "timestamp" in df.columns:
            df = df.rename(columns={"timestamp": "open_time"})
        if "close_time" not in df.columns:
            # assume 1m bars
            df["close_time"] = df["open_time"].astype("int64") + 60_000
        # Ensure ordering and tail limit
        df = df.sort_values("open_time").tail(lookback)
        try:
            await store.put_bars(sym.lower(), "1m", df)
        except Exception as e:
            logger.warning("put_bars preload failed for %s: %s", sym, e)
            stats[sym] = {"loaded": 0, "missing": lookback}
            continue
        loaded = len(df)
        stats[sym] = {"loaded": loaded, "missing": max(0, lookback - loaded)}

    total_loaded = sum(v["loaded"] for v in stats.values())
    logger.info(
        "Preload 1m завершено: %d барів по %d символах (avg=%.1f)",
        total_loaded,
        len(stats),
        total_loaded / max(1, len(stats)),
    )
    stats["_aggregate"] = {
        "symbols": len(stats),
        "total_loaded": total_loaded,
        "avg_loaded": total_loaded / max(1, len(stats)),
    }
    return stats


# ── Preload денних барів для глобальних рівнів ─────────────────────────────
async def preload_daily_levels(
    fast_symbols,
    days: int = PRELOAD_DAILY_DAYS,
    session: aiohttp.ClientSession | None = None,
):
    """
    Preload денного таймфрейму для розрахунку глобальних рівнів підтримки/опору.
    Перевіряє, що days >= 30.
    """
    if days < 30:
        logger.warning(
            "Кількість днів (%d) для денних барів занадто мала. Встановлено мінімум 30.",
            days,
        )
        days = 30

    logger.info(
        "Preload Daily: завантажуємо %d денних свічок для %d символів…",
        days,
        len(fast_symbols),
    )
    if session is None:
        logger.warning(
            "preload_daily_levels: session not provided — повертаємо порожні дані"
        )
        daily_data = {sym: pd.DataFrame() for sym in fast_symbols}
    else:
        daily_data = await _fetch_batch(fast_symbols, "1d", days, session)
    logger.info("Preload Daily завершено для %d символів.", len(daily_data))
    return daily_data
