# trade_manager_updater.py
import logging
import asyncio
import pandas as pd
from stage3.trade_manager import TradeLifecycleManager
from stage1.asset_monitoring import AssetMonitorStage1
from data.ram_buffer import RAMBuffer

from rich.console import Console
from rich.logging import RichHandler

# --- Налаштування логування ---
logger = logging.getLogger("trade_manager_updater")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False


async def trade_manager_updater(
    trade_manager: TradeLifecycleManager,
    buffer: RAMBuffer,
    monitor: AssetMonitorStage1,
    timeframe: str = "1m",
    lookback: int = 20,
    interval_sec: int = 30,
):
    """
    Фоновий таск: оновлює активні угоди,
    бере ATR/RSI/VOLUME із Stage1.stats, а не з сирих барів,
    і виводить лічильники active/closed.
    """
    while True:
        # 1) Оновлюємо всі активні угоди
        active = await trade_manager.get_active_trades()
        for tr in active:
            sym = tr["symbol"]
            bars = buffer.get(sym, timeframe, lookback)
            if not bars or len(bars) < lookback:
                continue

            df = pd.DataFrame(bars)
            # Використовуємо AssetMonitorStage1 для отримання stats
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

        # 2) Після оновлення виводимо статистику
        active = await trade_manager.get_active_trades()
        closed = await trade_manager.get_closed_trades()
        logger.info(
            f"🟢 Active trades: {len(active)}    🔴 Closed trades: {len(closed)}"
        )

        await asyncio.sleep(interval_sec)
