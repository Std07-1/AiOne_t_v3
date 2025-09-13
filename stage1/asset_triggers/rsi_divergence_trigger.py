"""RSI Divergence Trigger.

Призначення:
    • Розрахунок RSI (EMA варіант) та виявлення бичачої / ведмежої дивергенції
    • Повертає словник із прапорцями дивергенцій та поточним RSI

Стиль:
    • Українська мова, секційні розділювачі, захищена ініціалізація логера
    • Без побічних ефектів поза логуванням
"""

from scipy.signal import find_peaks
import pandas as pd
import logging

from rich.console import Console
from rich.logging import RichHandler

# ── Логування ───────────────────────────────────────────────────────────────
logger = logging.getLogger("asset_triggers.rsi_divergence")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False


def rsi_divergence_trigger(
    df: pd.DataFrame, rsi_period: int = 14, symbol: str = ""
) -> dict:
    """Обчислює RSI та виявляє бичачу/ведмежу дивергенцію.

    Returns:
        dict: { 'rsi': float|None, 'bearish_divergence': bool, 'bullish_divergence': bool }
    """
    close = df["close"]
    if len(close) < rsi_period + 3:
        logger.debug(
            f"[{symbol}] [RSIDivergence] Недостатньо даних ({len(close)}) для rsi_period={rsi_period}"
        )
        return {"rsi": None, "bearish_divergence": False, "bullish_divergence": False}
    delta = close.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    avg_gain = up.ewm(alpha=1 / rsi_period, adjust=False).mean()
    avg_loss = down.ewm(alpha=1 / rsi_period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi_series = 100 - 100 / (1 + rs)
    rsi_series = rsi_series.fillna(0)
    rsi_series[avg_loss == 0] = 100
    rsi_series[avg_gain == 0] = 0
    current_rsi = rsi_series.iloc[-1]
    prices = close.values
    peak_indices, _ = find_peaks(prices)
    trough_indices, _ = find_peaks(-prices)
    bearish_div = False
    bullish_div = False
    if len(peak_indices) >= 2:
        last_peak = peak_indices[-1]
        prev_peak = peak_indices[-2]
        if (
            prices[last_peak] > prices[prev_peak]
            and rsi_series.iloc[last_peak] < rsi_series.iloc[prev_peak]
        ):
            bearish_div = True
    if len(trough_indices) >= 2:
        last_trough = trough_indices[-1]
        prev_trough = trough_indices[-2]
        if (
            prices[last_trough] < prices[prev_trough]
            and rsi_series.iloc[last_trough] > rsi_series.iloc[prev_trough]
        ):
            bullish_div = True
    logger.debug(
        f"[{symbol}] [RSIDivergence] rsi={current_rsi:.2f}, bearish_div={bearish_div}, bullish_div={bullish_div}, "
        f"peaks={peak_indices[-2:] if len(peak_indices) >= 2 else peak_indices}, troughs={trough_indices[-2:] if len(trough_indices) >= 2 else trough_indices}"
    )
    return {
        "rsi": float(current_rsi),
        "bearish_divergence": bearish_div,
        "bullish_divergence": bullish_div,
    }
