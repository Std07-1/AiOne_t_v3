"""Інкрементальний менеджер VWAP + тригер відхилення (Stage1).

Призначення:
    • Тримати легкий FIFO‑буфер у RAM для кожного символу
    • Швидко перераховувати VWAP по обрізаному вікну (опційний ``window``)
    • Надати уніфікований тригер відхилення від VWAP (> threshold)

Примітки:
    Для малого вікна повне перерахування VWAP прийнятне та прозоре. Якщо
    продуктивність стане критичною — можна кешувати кумулятивні суми.
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
from rich.console import Console
from rich.logging import RichHandler

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("stage1.indicators.vwap")
if not logger.handlers:  # захист від подвійної ініціалізації
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False


class VWAPManager:
    """Інкрементальний VWAP менеджер (RAM FIFO per symbol)."""

    # ── Init ──
    def __init__(self, window: int | None = None):
        """Ініціалізація.

        Args:
            window: Розмір вікна (кількість барів) або ``None`` (усі дані).
        """
        self.window = window
        self.buffer_map: dict[str, pd.DataFrame] = {}

    # ── Buffer management ──
    def ensure_buffer(self, symbol: str, df: pd.DataFrame, *, force: bool = False):
        """Ініціалізує буфер символу (обрізаючи до ``window``).

        За замовчуванням не перезатирає існуючий буфер, щоб зберігати інкрементальний стан
        між тиками. Використовуйте ``force=True`` лише при ресинхронізації після гепів.
        """
        if not force and symbol in self.buffer_map and len(self.buffer_map[symbol]) > 0:
            return  # зберігаємо існуючий FIFO
        if self.window is not None and len(df) > self.window:
            df = df.tail(self.window)
        self.buffer_map[symbol] = df[["close", "volume"]].copy()
        logger.debug(f"[{symbol}] [VWAP-BUFFER] Буфер ініціалізовано, rows={len(df)}.")

    def update(self, symbol: str, close: float, volume: float):
        """Додає 1 бар у буфер (FIFO)."""
        df = self.buffer_map.get(symbol)
        new_row = pd.DataFrame([{"close": close, "volume": volume}])
        if df is None:
            self.buffer_map[symbol] = new_row
            logger.debug(
                f"[{symbol}] [VWAP-UPDATE] Буфер ініціалізовано з першим баром."
            )
            return
        df = pd.concat([df, new_row], ignore_index=True)
        if self.window is not None and len(df) > self.window:
            df = df.iloc[-self.window :]
        self.buffer_map[symbol] = df
        logger.debug(
            f"[{symbol}] [VWAP-UPDATE] Додано новий бар: close={close:.6f}, volume={volume:.2f}"
        )

    # ── Computation ──
    def compute_vwap(self, symbol: str) -> float:
        """Розраховує VWAP (або ``np.nan`` якщо недостатньо даних)."""
        df = self.buffer_map.get(symbol)
        if df is None or df["volume"].sum() == 0:
            logger.debug(f"[{symbol}] [VWAP] Недостатньо даних або volume==0.")
            # Використовуємо float('nan') щоб уникнути повернення Any (mypy no-any-return)
            return float("nan")
        vwap = (df["close"] * df["volume"]).sum() / df["volume"].sum()
        logger.debug(f"[{symbol}] [VWAP] VWAP={vwap:.6f} (rows={len(df)})")
        return float(vwap)

    def get_last(self, symbol: str) -> dict[str, Any] | None:
        """Повертає останній бар із буфера або ``None``."""
        df = self.buffer_map.get(symbol)
        if df is not None and len(df):
            row = df.iloc[-1]
            return {"close": row["close"], "volume": row["volume"]}
        return None


# ───────────────────────────── Тригер ─────────────────────────────
def vwap_deviation_trigger(
    vwap_manager: VWAPManager,
    symbol: str,
    current_price: float,
    threshold: float = 0.01,
) -> dict[str, Any]:
    # Тип повернення узагальнено до dict для сумісності з існуючими споживачами
    """Перевіряє відхилення ціни від VWAP > ``threshold``.

    Returns уніфікований словник: ``trigger`` / ``value`` / ``deviation`` / ``details``.
    """
    vwap = vwap_manager.compute_vwap(symbol)
    if np.isnan(vwap):
        logger.debug(f"[{symbol}] [VWAPDeviation] VWAP NaN — тригер неактивний.")
        return {
            "trigger": False,
            "value": None,
            "deviation": None,
            "details": {"vwap": None},
        }
    deviation = (current_price / vwap) - 1.0
    triggered = abs(deviation) > threshold
    logger.debug(
        f"[{symbol}] [VWAPDeviation] deviation={deviation:.5f} (>"
        f"{threshold})? {triggered}, VWAP={vwap:.4f}, close={current_price:.4f}"
    )
    return {
        "trigger": triggered,
        "value": vwap,
        "deviation": deviation,
        "details": {
            "vwap": vwap,
            "price": current_price,
            "threshold": threshold,
        },
    }


# ───────────────────────────── Публічний API ─────────────────────────────
__all__ = ["VWAPManager", "vwap_deviation_trigger"]
