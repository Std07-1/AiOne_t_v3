import pandas as pd
import logging

from rich.console import Console
from rich.logging import RichHandler

# --- Налаштування логування ---
logger = logging.getLogger("asset_triggers.vwap_deviation")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False


def vwap_deviation_trigger(
    df: pd.DataFrame, window: int = None, threshold: float = 0.01, symbol: str = ""
) -> tuple:
    """Перевіряє, чи відхилилася ціна від VWAP останніх барів більше, ніж на threshold (частка від 1).
    Повертає (triggered: bool, deviation: float)."""
    data = df if window is None or len(df) < window else df.tail(window)
    vwap = (data["close"] * data["volume"]).sum() / data["volume"].sum()
    current_price = df["close"].iloc[-1]
    deviation = (current_price / vwap) - 1.0
    triggered = abs(deviation) > threshold
    logger.debug(
        f"[{symbol}] [VWAPDeviation] deviation={deviation:.5f} (>{threshold})? {triggered}, VWAP={vwap:.4f}, close={current_price:.4f}"
    )
    return bool(triggered), float(deviation)
