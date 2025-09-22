"""Асинхронний пагінований фетчер свічок Binance для тестів.

Контракт:
- get_data(symbol, interval, limit) → DataFrame з колонками
  [timestamp, open, high, low, close, volume]
- timestamp — epoch мс (UTC), порядок строго зростаючий, без дублікатів
- Джерело: Binance Spot REST API v3 (api.binance.com), пагінація параметром endTime

Примітка: час лишаємо у мілісекундах без перетворень у datetime.
"""

from __future__ import annotations

from typing import Any

import aiohttp
import pandas as pd

BINANCE_V3_URL = "https://api.binance.com/api/v3/klines"


class OptimizedDataFetcher:
    """Мінімальний фетчер OHLCV з пагінацією назад.

    Args:
        session: відкритий aiohttp.ClientSession
        base_url: базовий URL для запитів klines
    """

    def __init__(
        self, session: aiohttp.ClientSession, *, base_url: str = BINANCE_V3_URL
    ) -> None:
        self.session = session
        self.base_url = base_url

    async def get_data(self, symbol: str, interval: str, *, limit: int) -> pd.DataFrame:
        """Повертає останні ``limit`` свічок як DataFrame.

        У разі помилки мережі повертає порожній DataFrame з потрібними колонками.
        """
        empty = pd.DataFrame(
            columns=["timestamp", "open", "high", "low", "close", "volume"]
        )
        if limit <= 0:
            return empty
        try:
            rows: list[list[Any]] = await self._fetch_paginated(symbol, interval, limit)
        except Exception:
            return empty
        if not rows:
            return empty
        df = self._build_df(rows)
        df = (
            df.sort_values("timestamp")
            .drop_duplicates(subset=["timestamp"], keep="last")
            .reset_index(drop=True)
        )
        if len(df) > limit:
            df = df.tail(limit).reset_index(drop=True)
        return df

    async def _fetch_paginated(
        self, symbol: str, interval: str, limit: int
    ) -> list[list[Any]]:
        remaining = int(limit)
        end_time: int | None = None
        collected: list[list[Any]] = []
        while remaining > 0:
            req_limit = min(1000, remaining)
            params: dict[str, str | int] = {
                "symbol": symbol.upper(),
                "interval": interval,
                "limit": req_limit,
            }
            if end_time is not None:
                params["endTime"] = int(end_time)
            timeout = aiohttp.ClientTimeout(total=10)
            async with self.session.get(
                self.base_url, params=params, timeout=timeout
            ) as resp:
                if resp.status != 200:
                    break
                data = await resp.json()
                if not isinstance(data, list) or not data:
                    break
                collected.extend(data)
                first_open_time = int(data[0][0])
                end_time = first_open_time - 1
                remaining -= len(data)
                if len(data) < req_limit:
                    break
        collected.sort(key=lambda r: int(r[0]))
        return collected

    @staticmethod
    def _build_df(rows: list[list[Any]]) -> pd.DataFrame:
        df = pd.DataFrame(
            rows,
            columns=[
                "open_time",
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
            ],
        )
        # timestamp — це open_time у мс
        df["timestamp"] = pd.to_numeric(df["open_time"], errors="coerce").astype(
            "Int64"
        )
        for c in ("open", "high", "low", "close", "volume"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        return df[["timestamp", "open", "high", "low", "close", "volume"]]


__all__ = ["OptimizedDataFetcher"]
