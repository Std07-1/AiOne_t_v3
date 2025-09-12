import pandas as pd
import logging

logger = logging

from rich.console import Console
from rich.logging import RichHandler

# --- Налаштування логування ---
logger = logging.getLogger("asset_triggers.volatility_spike")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False


def volatility_spike_trigger(
    df: pd.DataFrame, window: int = 14, threshold: float = 2.0, symbol: str = ""
) -> bool:
    """Виявляє різкий стрибок волатильності.
    Повертає True, якщо діапазон останнього бару більше, ніж threshold * середній діапазон попередніх window барів.
    """
    if len(df) < window + 1:
        logger.debug(
            f"[{symbol}] [VolatilitySpike] Недостатньо даних ({len(df)}) для window={window}"
        )
        return False
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr = pd.concat(
        [high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1
    ).max(axis=1)
    prev_atr = tr.iloc[-window - 1 : -1].mean()
    current_tr = tr.iloc[-1]
    triggered = bool(prev_atr > 0 and current_tr > threshold * prev_atr)
    logger.debug(
        f"[{symbol}] [VolatilitySpike] CurrentTR={current_tr:.4f}, PrevATR={prev_atr:.4f}, "
        f"Threshold={threshold}, Triggered={triggered}"
    )
    return triggered
