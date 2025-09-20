"""VWAP Deviation Trigger.

Перевіряє наскільки поточна ціна відхилилась від VWAP останніх N барів.
"""

import logging

import pandas as pd
from rich.console import Console
from rich.logging import RichHandler

# ── Логування ───────────────────────────────────────────────────────────────
logger = logging.getLogger("asset_triggers.vwap_deviation")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False


def vwap_deviation_trigger(
    df: pd.DataFrame,
    window: int | None = None,
    threshold: float = 0.01,
    symbol: str = "",
) -> tuple:
    """Повертає (triggered, deviation) якщо |відхилення| > threshold."""
    data = df if window is None or len(df) < window else df.tail(window)
    vwap = (data["close"] * data["volume"]).sum() / data["volume"].sum()
    current_price = df["close"].iloc[-1]
    deviation = (current_price / vwap) - 1.0
    triggered = abs(deviation) > threshold
    logger.debug(
        f"[{symbol}] [VWAPDeviation] deviation={deviation:.5f} (>{threshold})? {triggered}, VWAP={vwap:.4f}, close={current_price:.4f}"
    )
    return bool(triggered), float(deviation)
