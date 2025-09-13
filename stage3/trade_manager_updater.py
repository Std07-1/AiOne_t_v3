"""Stage3 utility task: periodic trade state refresh.

Оновлює активні угоди, підтягує агреговані статистики зі Stage1 замість
сирих барів (ATR/RSI/Volume) і логгує кількість active/closed.

Стиль уніфіковано: короткі секційні хедери, guard для логера,
коментарі до broad except.
"""

from __future__ import annotations

# ── Imports ──────────────────────────────────────────────────────────────────
import logging
import asyncio
from typing import Any

import pandas as pd
from rich.console import Console
from rich.logging import RichHandler

from stage3.trade_manager import TradeLifecycleManager
from stage1.asset_monitoring import AssetMonitorStage1

# ── Logger ───────────────────────────────────────────────────────────────────
logger = logging.getLogger("stage3.trade_manager_updater")
if not logger.handlers:  # guard щоб не дублювати хендлери
    logger.setLevel(logging.INFO)
    try:  # optional rich
        logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    except Exception:  # broad except: rich може бути недоступний у середовищі
        logger.addHandler(logging.StreamHandler())
    logger.propagate = False


async def trade_manager_updater(
    trade_manager: TradeLifecycleManager,
    store: Any,
    monitor: AssetMonitorStage1,
    timeframe: str = "1m",
    lookback: int = 20,
    interval_sec: int = 30,
    log_interval_sec: int | None = None,
    log_on_change: bool = True,
):
    """Фонове оновлення стану угод.

    Параметри:
        timeframe: таймфрейм барів для оновлення метрик.
        lookback: скільки барів брати для розрахунку поточних stats.
        interval_sec: базовий інтервал циклу (poll).
        log_interval_sec: мінімальний інтервал між логами (override log_on_change).
        log_on_change: логувати тільки якщо змінилась кількість активних/закритих.
    """
    last_log_ts = 0.0
    last_counts: tuple[int, int] | None = None
    if log_interval_sec is None:
        # за замовчуванням = interval_sec (раз на цикл) якщо немає режиму only-on-change
        log_interval_sec = interval_sec if not log_on_change else 0

    while True:
        cycle_start = asyncio.get_event_loop().time()

        # 1) Оновити активні угоди (best-effort)
        active_list = await trade_manager.get_active_trades()
        for tr in active_list:
            sym = tr["symbol"]
            try:
                df = await store.get_df(sym, timeframe, limit=lookback)
                if (
                    df is not None
                    and not df.empty
                    and "open_time" in df.columns
                    and "timestamp" not in df.columns
                ):
                    df = df.rename(columns={"open_time": "timestamp"})
            except Exception as e:  # broad-except: I/O / кеш / мережа не критичні
                logger.debug(f"Failed to fetch bars for {sym}: {e}")
                continue
            if df is None or df.empty or len(df) < lookback:
                continue
            stats = (
                await monitor.get_current_stats(sym, df)
                if hasattr(monitor, "get_current_stats")
                else {}
            )
            market_data = {
                "price": stats.get("current_price", 0),
                "atr": stats.get("atr", 0),
                "rsi": stats.get("rsi", 0),
                "volume": stats.get("volume_mean", 0),
                "context_break": stats.get("context_break", False),
            }
            await trade_manager.update_trade(tr["id"], market_data)

        # 2) Лічильники після оновлення
        active = await trade_manager.get_active_trades()
        closed = await trade_manager.get_closed_trades()
        counts = (len(active), len(closed))

        now = asyncio.get_event_loop().time()
        should_log = False
        if log_on_change and last_counts is not None and counts != last_counts:
            should_log = True
        elif log_on_change and last_counts is None:
            # перший лог обов'язково
            should_log = True
        if log_interval_sec and (now - last_log_ts) >= log_interval_sec:
            # якщо заданий інтервал — поважаємо його (може співіснувати з on_change)
            should_log = should_log or True

        if should_log:
            logger.info(
                f"🟢 Active trades: {counts[0]}    🔴 Closed trades: {counts[1]}"
            )
            last_log_ts = now
            last_counts = counts

        # 3) Засипаємо до наступного циклу, компенсуючи час виконання
        elapsed = asyncio.get_event_loop().time() - cycle_start
        sleep_for = max(0.0, interval_sec - elapsed)
        await asyncio.sleep(sleep_for)


__all__ = ["trade_manager_updater"]
