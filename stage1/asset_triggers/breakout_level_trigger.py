"""Breakout / Near-Level Trigger.

Виявляє:
    • Пробій локальних максимумів/мінімумів
    • Близькість до локальних та денних рівнів (support/resistance)
"""

import logging
from typing import Any

import pandas as pd
from rich.console import Console
from rich.logging import RichHandler

# ── Логування ───────────────────────────────────────────────────────────────
logger = logging.getLogger("asset_triggers.breakout_level")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False


def breakout_level_trigger(
    df: pd.DataFrame,
    stats: dict[str, Any],
    window: int = 20,
    near_threshold: float = 0.005,
    near_daily_threshold: float = 0.5,
    symbol: str = "",
    *,
    confirm_bars: int = 1,
    min_retests: int = 0,
) -> dict[str, bool]:
    """Виявляє пробій локальних екстремумів і близькість до них та денних рівнів.

    Returns:
        Dict[str, bool]: breakouts / near-level flags
    """
    triggers: dict[str, bool] = {
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

    # Пробій з підтвердженням confirm_bars
    confirm_bars = max(1, int(confirm_bars))
    try:
        prev_window = df["close"].iloc[-(confirm_bars + 1) : -1]
    except Exception:
        prev_window = df["close"].iloc[-2:-1]
    prev_ok_up = bool((prev_window <= recent_high).all())
    prev_ok_dn = bool((prev_window >= recent_low).all())
    triggers["breakout_up"] = (
        current_close > recent_high and prev_close <= recent_high and prev_ok_up
    )
    triggers["breakout_down"] = (
        current_close < recent_low and prev_close >= recent_low and prev_ok_dn
    )

    # Близькість до локальних рівнів
    if near_threshold > 0:
        triggers["near_high"] = (
            recent_high - current_close
        ) / recent_high < near_threshold
        triggers["near_low"] = (
            current_close - recent_low
        ) / recent_low < near_threshold

    # Retests: кількість торкань рівня перед пробоєм
    retests_high = 0
    retests_low = 0
    if min_retests and near_threshold > 0 and len(df) > window:
        try:
            before = df.iloc[-(window + 1) : -1]
            ch = before["close"]
            retests_high = int(
                (((recent_high - ch) / recent_high).abs() < near_threshold).sum()
            )
            retests_low = int(
                (((ch - recent_low) / recent_low).abs() < near_threshold).sum()
            )
        except Exception:
            retests_high = 0
            retests_low = 0
        # Вимога мінімальних ретестів лише для фінального виставлення прапора breakout
        if triggers["breakout_up"] and retests_high < int(min_retests):
            triggers["breakout_up"] = False
        if triggers["breakout_down"] and retests_low < int(min_retests):
            triggers["breakout_down"] = False

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
        "[%s] breakout params | window=%d near_thr=%.5f daily_thr=%.2f%% confirm_bars=%d min_retests=%d",
        symbol,
        window,
        float(near_threshold),
        float(near_daily_threshold),
        int(confirm_bars),
        int(min_retests),
    )
    logger.debug(
        "[%s] breakout flags | up=%s down=%s near_high=%s near_low=%s daily_supp=%s daily_res=%s | recent_high=%.4f recent_low=%.4f close=%.4f retests_high=%d retests_low=%d",
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
        retests_high,
        retests_low,
    )
    return triggers
