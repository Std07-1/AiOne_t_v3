"""Обчислення глобальних рівнів (піка/дна) для Stage1.

Евристика: застосовуємо ``find_peaks`` до highs та інверсії lows на останньому
вікні ``window`` барів. Повертаємо унікальні відсортовані рівні (support/resistance).
"""

from __future__ import annotations

from typing import Any  # noqa: F401

import pandas as pd
from scipy.signal import find_peaks


def calculate_global_levels(df: pd.DataFrame, window: int = 20) -> list[float]:
    """Обчислює унікальні рівні (high-peaks + low-troughs) за вікном ``window``."""
    highs = df["high"].to_numpy()
    peaks, _ = find_peaks(highs, distance=window)
    lows = df["low"].to_numpy()
    troughs, _ = find_peaks(-lows, distance=window)
    levels: list[float] = []
    levels += list(df["high"].iloc[peaks].astype(float))
    levels += list(df["low"].iloc[troughs].astype(float))
    return sorted(set(levels))


# ───────────────────────────── Публічний API ─────────────────────────────
__all__ = ["calculate_global_levels"]
