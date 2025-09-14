"""Unified OHLCV loading facade for episodic analysis.

Призначення:
    * Єдина точка доступу до історичних барів для модуля `episodes` (та тестів).
    * Обгортає асинхронний `OptimizedDataFetcher` у синхронні функції (`fetch_bars`, `load_bars_auto`).
    * Забезпечує правильний DatetimeIndex, обрізання до `limit` та стабільний порядок колонок.

Особливості:
    * Лінива ініціалізація singleton fetcher + aiohttp.ClientSession з atexit teardown.
    * Використання локального snapshot/кешу (через raw_data fetcher) з параметрами read_cache/write_cache.
    * Без логіки повторних спроб тут – retry/бекоф реалізуються всередині fetcher.

Функції:
    fetch_bars(symbol, timeframe, limit): Повертає DataFrame OHLCV.
    load_bars_auto(config): Викликає fetch_bars на базі полів EpisodeConfig.

Contracts / Postconditions:
    * Індекс: tz-aware UTC DatetimeIndex з name='time'.
    * Мінімальний набір колонок: open, high, low, close, volume (інші зберігаються).
    * Якщо дані відсутні → повертає порожній DataFrame з базовими колонками.
"""

from __future__ import annotations
import asyncio
from typing import Optional
import pandas as pd
import aiohttp  # type: ignore

from data.raw_data import OptimizedDataFetcher  # type: ignore
import logging

try:
    from rich.console import Console  # type: ignore
    from rich.logging import RichHandler  # type: ignore

    _HAS_RICH = True
except ImportError:  # pragma: no cover
    _HAS_RICH = False

logger = logging.getLogger("ep2.data_loader")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    if _HAS_RICH:
        h = RichHandler(console=Console(stderr=True), show_path=False)  # type: ignore[arg-type]
    else:
        h = logging.StreamHandler()
        h.setFormatter(
            logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s")
        )
    logger.addHandler(h)
    logger.propagate = False

_FETCHER_SINGLETON: Optional[OptimizedDataFetcher] = None
_FETCH_LOOP: Optional[asyncio.AbstractEventLoop] = None


def _get_fetch_loop() -> asyncio.AbstractEventLoop:
    global _FETCH_LOOP
    if _FETCH_LOOP is None:
        _FETCH_LOOP = asyncio.new_event_loop()
    return _FETCH_LOOP


def _get_fetcher() -> OptimizedDataFetcher:
    global _FETCHER_SINGLETON
    if _FETCHER_SINGLETON is None:
        loop = _get_fetch_loop()
        try:
            asyncio.set_event_loop(loop)
        except Exception:  # pragma: no cover
            pass
        session = aiohttp.ClientSession()
        _FETCHER_SINGLETON = OptimizedDataFetcher(session=session, compress_cache=True)
        try:  # graceful shutdown
            import atexit

            def _close_session():  # pragma: no cover
                try:
                    if not session.closed:
                        loop = _get_fetch_loop()
                        if loop.is_running():
                            loop.create_task(session.close())
                        else:
                            loop.run_until_complete(session.close())
                except Exception:
                    pass

            atexit.register(_close_session)
        except Exception:  # pragma: no cover
            pass
    return _FETCHER_SINGLETON


def fetch_bars(symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
    """Синхронно отримати історичні бари.

    Args:
        symbol: Торговий символ (e.g. BTCUSDT).
        timeframe: Таймфрейм ("1m","5m","1h"...).
        limit: Максимальна кількість барів (обрізання з кінця, якщо fetcher повернув більше).

    Returns:
        DataFrame: OHLCV з DatetimeIndex або порожній DataFrame якщо невдача.
    """

    async def _runner():
        fetcher = _get_fetcher()
        return await fetcher.get_data(
            symbol,
            timeframe,
            limit=limit,
            read_cache=True,
            write_cache=True,
        )

    try:
        loop = _get_fetch_loop()
        try:
            asyncio.set_event_loop(loop)
        except Exception:  # pragma: no cover
            pass
        df = loop.run_until_complete(_runner())
    except Exception as e:  # pragma: no cover
        logger.error("[DATA] Помилка fetch_bars: %s", e)
        df = None
    if df is None or df.empty:
        logger.error("[DATA] Порожні дані для %s %s", symbol, timeframe)
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    if not isinstance(df.index, pd.DatetimeIndex):
        if "timestamp" in df.columns:
            df.index = pd.to_datetime(df["timestamp"], utc=True)
        else:
            df.index = pd.to_datetime(df.index, utc=True, errors="coerce")
    df.index.name = "time"
    base_cols = ["open", "high", "low", "close", "volume"]
    extra_cols = [c for c in df.columns if c not in base_cols]
    df = df[base_cols + extra_cols]
    if limit and len(df) > limit:
        df = df.iloc[-limit:]
    return df


def load_bars_auto(config) -> pd.DataFrame:
    """Завантажити бари з EpisodeConfig-подібного об'єкта.

    Args:
        config: Об'єкт з атрибутами symbol, timeframe, limit.

    Returns:
        DataFrame: OHLCV.

    Raises:
        RuntimeError: Якщо після fetch дані порожні.
    """
    symbol = config.symbol
    timeframe = config.timeframe
    limit = getattr(config, "limit", 0) or 0
    logger.info(
        "[DATA] Завантаження через OptimizedDataFetcher (single-source) %s %s limit=%s",
        symbol,
        timeframe,
        limit,
    )
    df = fetch_bars(symbol, timeframe, limit)
    if df.empty:
        raise RuntimeError(
            f"Не вдалося отримати дані для {symbol} {timeframe} (single-source raw_data)"
        )
    return df


__all__ = ["load_bars_auto", "fetch_bars"]
