"""Інкрементальний менеджер Z‑score обсягу (Stage1).

Призначення: підтримувати короткий FIFO‑буфер обсягів для миттєвого
розрахунку Z‑score (виявлення сплесків / аномалій) у Stage1.
"""

from __future__ import annotations

import logging

import pandas as pd
from rich.console import Console
from rich.logging import RichHandler

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("stage1.indicators.volume_z")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False


class VolumeZManager:
    """Інкрементальний менеджер Z‑score обсягу (короткий FIFO)."""

    def __init__(self, window: int = 20):
        """Ініціалізація менеджера.

        Args:
            window: Вікно для розрахунку статистик.
        """
        self.window = window
        self.buffer_map: dict[str, pd.Series] = {}

    def ensure_buffer(self, symbol: str, df: pd.DataFrame, *, force: bool = False):
        """Ініціалізує FIFO буфер символу, не перезатираючи існуючий стан.

        Використовуйте ``force=True`` лише при необхідності повної ресинхронізації.
        """
        if not force and symbol in self.buffer_map and len(self.buffer_map[symbol]) > 0:
            return
        if len(df) > self.window:
            series = df["volume"].tail(self.window)
        else:
            series = df["volume"]
        self.buffer_map[symbol] = series.copy()
        logger.debug(
            f"[{symbol}] [VOLZ-BUFFER] Буфер ініціалізовано, rows={len(series)}."
        )

    def update(self, symbol: str, volume: float) -> float:
        """Додає новий обсяг та повертає Z‑score поточного бару."""
        buf = self.buffer_map.get(symbol)
        if buf is None:
            self.buffer_map[symbol] = pd.Series([volume])
            logger.debug(
                f"[{symbol}] [VOLZ-UPDATE] Буфер ініціалізовано з першим обсягом."
            )
            return 0.0
        # На цьому етапі buf гарантовано є Series
        new_buf = pd.concat([buf, pd.Series([volume])], ignore_index=True)
        if len(new_buf) > self.window:
            new_buf = new_buf.iloc[-self.window :]
        self.buffer_map[symbol] = new_buf
        mean = new_buf.mean()
        std = new_buf.std(ddof=0)
        if std == 0:
            logger.debug(f"[{symbol}] [VOLZ-UPDATE] std=0, Z=0.0")
            return 0.0
        z = (volume - mean) / std
        logger.debug(
            f"[{symbol}] [VOLZ-UPDATE] Z-score={z:.2f} "
            f"(volume={volume:.2f}, mean={mean:.2f}, std={std:.2f})"
        )
        return float(z)

    def get_last(self, symbol: str) -> float | None:
        """Повертає останній Z‑score або ``None`` якщо буфера нема."""
        buf = self.buffer_map.get(symbol)
        if buf is not None and len(buf):
            mean = buf.mean()
            std = buf.std(ddof=0)
            last_vol = buf.iloc[-1]
            if std == 0:
                return 0.0
            return float((last_vol - mean) / std)
        return None


# ───────────────────────────── Векторна версія ─────────────────────────────
def compute_volume_z(df: pd.DataFrame, window: int = 20, symbol: str = "") -> float:
    """Векторний розрахунок Z‑score для останнього бару DataFrame."""
    if len(df) < window:
        logger.debug(f"[{symbol}] Недостатньо даних для volume_z: {len(df)} < {window}")
        return float("nan")

    mean = df["volume"].tail(window).mean()
    std = df["volume"].tail(window).std(ddof=0)
    if std == 0:
        logger.debug(f"[{symbol}] Volume std=0, Z=0.0")
        return 0.0
    z = (df["volume"].iloc[-1] - mean) / std
    logger.debug(f"[{symbol}] Volume Z-score={z:.2f}")
    return float(z)


# ───────────────────────────── Публічний API ─────────────────────────────
__all__ = ["VolumeZManager", "compute_volume_z"]
