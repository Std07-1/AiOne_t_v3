# periodic_prefilter_and_update.py
import logging
import asyncio
import pandas as pd
from stage1.optimized_asset_filter import get_filtered_assets

from rich.console import Console
from rich.logging import RichHandler

# --- Налаштування логування ---
logger = logging.getLogger("app.periodic_prefilter_and_update")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False


# --- Фоновий таск: періодичне оновлення fast_symbols через prefilter ---
async def periodic_prefilter_and_update(
    cache, session, thresholds, interval=600, buffer=None, fetcher=None, lookback=500
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

                # --- preload для нових активів ---
                if buffer is not None and fetcher is not None:
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
                            fetcher, new_symbols_list, buffer, lookback=lookback
                        )
                        await preload_daily_levels(fetcher, new_symbols_list, days=30)

                # Оновлюємо попередні символи
                prev_symbols = current_symbols
            else:
                logger.warning(
                    "Prefilter повернув порожній список, fast_symbols не оновлено."
                )
        except Exception as e:
            logger.warning("Помилка оновлення prefilter: %s", e)

        await asyncio.sleep(interval)  # 600 сек


# --- Preload історії для Stage1 ---
async def preload_1m_history(fetcher, fast_symbols, buffer, lookback=50):
    """
    Preload історичних 1m-барів для швидкого старту Stage1.
    Перевіряє, що lookback >= 12 (мінімум для індикаторів типу RSI/ATR).
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
    hist_data = await fetcher.get_data_batch(
        fast_symbols,
        interval="1m",
        limit=lookback,
        min_candles=lookback,
        show_progress=True,
        read_cache=False,
        write_cache=True,
    )

    # Додаємо бари в RAMBuffer
    for sym, df in hist_data.items():
        for bar in df.to_dict("records"):
            ts = bar["timestamp"]
            if isinstance(ts, pd.Timestamp):
                bar["timestamp"] = int(ts.value // 1_000_000)
            else:
                bar["timestamp"] = int(ts)
            buffer.add(sym.lower(), "1m", bar)
    logger.info(
        "Preload 1m завершено: історія додана в RAMBuffer для %d символів.",
        len(hist_data),
    )
    return hist_data


# --- Preload денних барів для глобальних рівнів ---
async def preload_daily_levels(fetcher, fast_symbols, days=30):
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
    daily_data = await fetcher.get_data_batch(
        fast_symbols,
        interval="1d",
        limit=days,
        min_candles=days,
        show_progress=False,
        read_cache=False,
        write_cache=False,
    )
    logger.info("Preload Daily завершено для %d символів.", len(daily_data))
    return daily_data
