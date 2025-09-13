import pandas as pd
import logging
from typing import Dict, Any

from rich.console import Console
from rich.logging import RichHandler

# --- Налаштування логування ---
logger = logging.getLogger("asset_triggers.breakout_level")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False


def breakout_level_trigger(
    df: pd.DataFrame,
    stats: Dict[str, Any],
    window: int = 20,
    near_threshold: float = 0.005,
    near_daily_threshold: float = 0.5,
    symbol: str = "",
) -> Dict[str, bool]:
    """
    Виявляє пробій локальних екстремумів і близькість до них,
    а також до глобальних денних рівнів.

    Параметри:
    - df: останні бари, мінімум window+1
    - stats: словник з update_statistics, повинен містити 'daily_levels': List[float]
    - window: число барів для локальних екстремумів
    - near_threshold: поріг близькості до локальних рівнів (в частках, напр. 0.005=0.5%)
    - near_daily_threshold: поріг близькості до денних рівнів (в %)
    - symbol: символ для логування

    Повертає словник із ключами:
    - breakout_up, breakout_down, near_high, near_low,
    - near_daily_support, near_daily_resistance
    """
    triggers: Dict[str, bool] = {
        "breakout_up": False,
        "breakout_down": False,
        "near_high": False,
        "near_low": False,
        "near_daily_support": False,
        "near_daily_resistance": False,
    }

    if len(df) < window + 1:
        logger.debug(
            "[%s] Недостатньо даних: %d < window+1=%d", symbol, len(df), window + 1
        )
        return triggers

    # Локальні рівні
    # Використовуємо min_periods щоб уникнути RuntimeWarning при недостатній кількості точок
    recent_high = (
        df["high"]
        .rolling(window=window, min_periods=max(3, window // 3))
        .max()
        .iloc[-2]
    )
    recent_low = (
        df["low"].rolling(window=window, min_periods=max(3, window // 3)).min().iloc[-2]
    )
    current_close = df["close"].iloc[-1]
    prev_close = df["close"].iloc[-2]

    # Пробій
    triggers["breakout_up"] = current_close > recent_high and prev_close <= recent_high
    triggers["breakout_down"] = current_close < recent_low and prev_close >= recent_low

    # Близькість до локальних рівнів
    if near_threshold > 0:
        triggers["near_high"] = (
            recent_high - current_close
        ) / recent_high < near_threshold
        triggers["near_low"] = (
            current_close - recent_low
        ) / recent_low < near_threshold

    # Глобальні денні рівні з stats
    price = current_close
    daily_levels = stats.get("daily_levels", [])
    if daily_levels:
        nearest = min(daily_levels, key=lambda lv: abs(price - lv))
        dist_pct = abs(price - nearest) / nearest * 100
        if dist_pct < near_daily_threshold:
            if price > nearest:
                triggers["near_daily_support"] = True
            else:
                triggers["near_daily_resistance"] = True

    logger.debug(
        "[%s] breakout_up=%s, breakout_down=%s, near_high=%s, near_low=%s, "
        "near_daily_support=%s, near_daily_resistance=%s, "
        "recent_high=%.4f, recent_low=%.4f, close=%.4f",
        symbol,
        triggers["breakout_up"],
        triggers["breakout_down"],
        triggers["near_high"],
        triggers["near_low"],
        triggers["near_daily_support"],
        triggers["near_daily_resistance"],
        recent_high,
        recent_low,
        current_close,
    )
    return triggers
