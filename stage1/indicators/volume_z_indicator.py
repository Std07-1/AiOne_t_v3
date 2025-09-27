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


def _sanitize_series(series: pd.Series, window: int) -> pd.Series:
    """Очищуємо та обрізаємо серію до робочого вікна без NaN."""
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if len(clean) > window:
        clean = clean.iloc[-window:]
    return clean.reset_index(drop=True)


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
        sanitized = _sanitize_series(df["volume"], self.window)
        if sanitized.empty:
            self.buffer_map[symbol] = sanitized
            logger.debug(f"[{symbol}] [VOLZ-BUFFER] Буфер порожній після sanitize.")
            return
        self.buffer_map[symbol] = sanitized
        logger.debug(
            f"[{symbol}] [VOLZ-BUFFER] Буфер ініціалізовано, rows={len(sanitized)}."
        )

    def update(self, symbol: str, volume: float) -> float:
        """Додає новий обсяг та повертає Z‑score поточного бару."""
        if pd.isna(volume):
            logger.debug(f"[{symbol}] [VOLZ-UPDATE] Отримано NaN volume, Z=0.0")
            return 0.0

        buf = self.buffer_map.get(symbol)
        if buf is None:
            sanitized = _sanitize_series(pd.Series([volume]), self.window)
            self.buffer_map[symbol] = sanitized
            logger.debug(
                f"[{symbol}] [VOLZ-UPDATE] Буфер ініціалізовано з першим обсягом."
            )
            return 0.0
        # На цьому етапі buf гарантовано є Series
        new_buf = pd.concat([buf, pd.Series([volume])], ignore_index=True)
        sanitized = _sanitize_series(new_buf, self.window)
        self.buffer_map[symbol] = sanitized
        if len(sanitized) < 2:
            logger.debug(f"[{symbol}] [VOLZ-UPDATE] буфер <2 значень, Z=0.0")
            return 0.0
        mean = sanitized.mean()
        std = sanitized.std(ddof=0)
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
            sanitized = _sanitize_series(buf, self.window)
            if sanitized.empty:
                return None
            if len(sanitized) < 2:
                return 0.0
            mean = sanitized.mean()
            std = sanitized.std(ddof=0)
            last_vol = sanitized.iloc[-1]
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

    series_tail = _sanitize_series(df["volume"], window)
    if len(series_tail) < 2:
        return 0.0
    mean = series_tail.mean()
    std = series_tail.std(ddof=0)
    if std == 0:
        logger.debug(f"[{symbol}] Volume std=0, Z=0.0")
        return 0.0
    latest = pd.to_numeric(pd.Series([df["volume"].iloc[-1]]), errors="coerce").iloc[0]
    if pd.isna(latest):
        return 0.0
    z = (latest - mean) / std
    logger.debug(f"[{symbol}] Volume Z-score={z:.2f}")
    return float(z)


# ───────────────────────────── Публічний API ─────────────────────────────
__all__ = ["VolumeZManager", "compute_volume_z"]
