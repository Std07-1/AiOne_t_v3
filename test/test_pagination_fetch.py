import asyncio
import time
import sys
from pathlib import Path
from typing import List

import aiohttp
import pandas as pd

# Ensure project root on sys.path when running directly
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data.raw_data import OptimizedDataFetcher

# Simple helper to run async in sync context for tests


def run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def _validate_df(df: pd.DataFrame, symbol: str, interval: str, requested: int):
    assert not df.empty, f"{symbol}:{interval} returned empty"
    assert len(df) <= requested, "Fetcher returned more rows than requested slice"
    # timestamps monotonic strictly increasing
    ts = pd.to_datetime(df["timestamp"], utc=True)
    assert (
        ts.is_monotonic_increasing
    ), f"Timestamps not increasing for {symbol}:{interval}"
    # no duplicates
    assert ts.duplicated().sum() == 0, f"Duplicate timestamps for {symbol}:{interval}"


async def fetch_and_check(symbol: str, interval: str, limit: int):
    async with aiohttp.ClientSession() as sess:
        fetcher = OptimizedDataFetcher(session=sess)
        df = await fetcher.get_data(symbol, interval, limit=limit)
        _validate_df(df, symbol, interval, limit)
        return len(df)


async def main():
    symbols: List[str] = ["BTCUSDT", "ETHUSDT"]
    intervals = ["1m", "5m"]
    limits = [
        1200,
        2500,
        3000,
        4500,
    ]  # include larger values to test backward pagination

    summary = []
    for sym in symbols:
        for interval in intervals:
            for limit in limits:
                start = time.perf_counter()
                rows = await fetch_and_check(sym, interval, limit)
                elapsed = time.perf_counter() - start
                summary.append((sym, interval, limit, rows, elapsed))
                print(f"OK {sym} {interval} req={limit} got={rows} in {elapsed:.2f}s")
    print("--- SUMMARY ---")
    for row in summary:
        print(row)


if __name__ == "__main__":
    asyncio.run(main())
