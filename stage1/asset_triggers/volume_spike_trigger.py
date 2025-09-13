"""Volume Spike Trigger.

Виявляє аномальний стрибок обсягу на основі Z-score та співвідношення Volume/ATR.
"""

import numpy as np
import pandas as pd
import logging

from rich.console import Console
from rich.logging import RichHandler

# ── Логування ───────────────────────────────────────────────────────────────
logger = logging.getLogger("asset_triggers.volume_spike")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False


def volume_spike_trigger(
    df: pd.DataFrame, z_thresh: float = 2.0, atr_window: int = 14, symbol: str = ""
) -> bool:
    """Повертає True якщо останній бар має аномально великий обсяг."""
    if len(df) < atr_window:
        logger.debug(
            f"[{symbol}] [VolumeSpike] Недостатньо даних ({len(df)}) для ATR={atr_window}"
        )
        return False
    latest_vol = df["volume"].iloc[-1]
    vol_mean = df["volume"].mean()
    vol_std = df["volume"].std(ddof=0)
    z_score = 0 if vol_std == 0 else (latest_vol - vol_mean) / vol_std
    vol_spike_z = z_score > z_thresh
    # ATR
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr = pd.concat(
        [high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1
    ).max(axis=1)
    atr = tr.tail(atr_window).mean()
    vol_atr_ratio = np.inf if atr is None or atr == 0 else latest_vol / atr
    vol_spike_atr = vol_atr_ratio > 2.0
    logger.debug(
        f"[{symbol}] [VolumeSpike] Z-score={z_score:.2f} (>{z_thresh})? {vol_spike_z}, "
        f"Volume/ATR={vol_atr_ratio:.2f} (>2.0)? {vol_spike_atr}"
    )
    return bool(vol_spike_z or vol_spike_atr)
