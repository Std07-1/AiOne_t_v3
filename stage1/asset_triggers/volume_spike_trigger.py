"""
Volume Spike Trigger (робастний, up-only)
Виявляє бичий сплеск обсягу за rolling Z-score та Volume/ATR, повертає прапор і метадані.

Особливості:
  • up-only: враховуємо лише, якщо close > open
  • rolling Z без лукапу: статистики по вікну без останнього бара
  • захист від нульової/мізерної дисперсії та NaN/inf
  • опційний шлях Volume/ATR (use_vol_atr)
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
from rich.console import Console
from rich.logging import RichHandler

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("asset_triggers.volume_spike")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False


# ───────────────────── Основна функція тригера ──────────────────────
def volume_spike_trigger(
    df: pd.DataFrame,
    *,
    z_thresh: float = 2.0,
    window: int = 50,
    atr_window: int = 14,
    use_vol_atr: bool = False,
    require_upbar: bool = True,
    upbar_tolerance: float = 5e-4,
    min_effect_ratio: float = 1.15,  # latest_vol / mean_vol ≥ цей коеф. як дод. умова
    symbol: str = "",
) -> tuple[bool, dict[str, float]]:
    """Визначає бичий сплеск обсягу на останньому барі.

    Args:
        df: Очікує колонки ['open','high','low','close','volume'].
        z_thresh: Поріг для Z-score.
        window: Вікно для rolling статистик volume.
        atr_window: Вікно ATR.
        use_vol_atr: Додатковий критерій Volume/ATR > 2.0.
        require_upbar: Вимога, щоб close > open.
        upbar_tolerance: Допустиме відхилення (у частках, наприклад 5e-4 = 0.05%),
            в межах якого бар вважається нейтральним/flat і не фільтрується.
        min_effect_ratio: Мінімальне відхилення latest_vol від середнього.
        symbol: Для логування.

    Returns:
        (flag, meta): Прапор сплеску та метадані для UI/Stage2.
    """
    n = len(df)
    min_required = max(atr_window + 1, 3)
    if n < min_required:
        logger.debug(
            "[%s] [VolSpike] Недостатньо даних (n=%d, need≥%d)",
            symbol,
            n,
            min_required,
        )
        return False, {
            "z": 0.0,
            "mean": 0.0,
            "std": 0.0,
            "vol_atr": 0.0,
            "upbar": False,
        }

    # --- Останній бар ---
    latest_series = pd.to_numeric(df["volume"].iloc[[-1]], errors="coerce")
    latest_vol = float(latest_series.iloc[0]) if not latest_series.isna().all() else 0.0
    open_ = float(df["open"].iloc[-1])
    close_ = float(df["close"].iloc[-1])
    tol = max(0.0, float(upbar_tolerance))
    if open_ != 0:
        rel_delta = (close_ - open_) / open_
    else:
        rel_delta = close_ - open_
    upbar = rel_delta >= -tol

    if require_upbar and not upbar:
        logger.debug(
            "[%s] [VolSpike] Відсічено: не upbar (Δ=%.5f, tol=%.5f)",
            symbol,
            rel_delta,
            tol,
        )
        return False, {
            "z": 0.0,
            "mean": 0.0,
            "std": 0.0,
            "vol_atr": 0.0,
            "upbar": False,
        }

    # --- Rolling статистики без останнього бара (анти-лукап) ---
    effective_window = min(window, max(1, n - 1))
    vol_history = pd.to_numeric(df["volume"].iloc[:-1], errors="coerce").dropna()
    vol_win = vol_history.tail(effective_window)
    if len(vol_win) < max(2, min(10, effective_window)):
        logger.debug(
            "[%s] [VolSpike] Недостатньо валідних volume значень для статистики (len=%d, eff_win=%d)",
            symbol,
            len(vol_win),
            effective_window,
        )
        return False, {
            "z": 0.0,
            "mean": 0.0,
            "std": 0.0,
            "vol_atr": 0.0,
            "upbar": bool(upbar),
            "latest_vol": float(latest_vol),
        }
    mean_vol = float(vol_win.mean())
    std_vol = float(vol_win.std(ddof=0))
    if std_vol == 0:
        std_vol = 1e-9

    # Захист від нульової дисперсії забезпечено вище

    z = (latest_vol - mean_vol) / std_vol
    effect_ok = (mean_vol > 0) and (latest_vol / mean_vol >= min_effect_ratio)

    # --- ATR шлях (опційно) ---
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    close = pd.to_numeric(df["close"], errors="coerce")
    prev_close = close.shift(1)
    tr = pd.concat(
        [high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1
    ).max(axis=1)
    tr_tail = tr.dropna().tail(atr_window)
    atr = float(tr_tail.mean()) if len(tr_tail) else 0.0
    vol_atr = (latest_vol / atr) if (atr > 0) else 0.0

    z_pass = (z >= z_thresh) and (z > 0.0) and effect_ok
    atr_pass = (vol_atr > 2.0) if use_vol_atr else False

    flag = bool(z_pass or atr_pass)

    logger.debug(
        "[%s] [VolSpike] upbar=%s | Z=%.2f thr=%.2f pass=%s | eff=%.2f× (min %.2f) | Vol/ATR=%.2f pass=%s",
        symbol,
        upbar,
        z,
        z_thresh,
        z_pass,
        (latest_vol / mean_vol) if mean_vol > 0 else np.nan,
        min_effect_ratio,
        vol_atr,
        atr_pass,
    )

    meta = {
        "z": float(z),
        "mean": float(mean_vol),
        "std": float(std_vol),
        "vol_atr": float(vol_atr),
        "upbar": bool(upbar),
        "latest_vol": float(latest_vol),
    }
    return flag, meta
