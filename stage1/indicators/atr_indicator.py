"""Інкрементальний менеджер ATR + векторна функція (Stage1).

Тримує RAM‑стан для кожного символу і оновлює ATR без повного
перерахунку вікна. Для бек‑тестів / валідації доступна векторна
функція ``compute_atr``.
"""

from __future__ import annotations

import logging

import pandas as pd
from rich.console import Console
from rich.logging import RichHandler

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("stage1.indicators.atr")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False


class ATRManager:
    """Інкрементальний ATR менеджер (RAM state per symbol)."""

    def __init__(self, period: int = 14):
        """Init.

        Args:
            period: Період ATR (звично 14).
        """
        self.period = period
        self.state_map: dict[str, dict[str, float]] = {}

    def ensure_state(self, symbol: str, df: pd.DataFrame):
        """Первинне seed-налаштування стану (якщо даних достатньо)."""
        if symbol in self.state_map:
            return

        if len(df) < self.period + 1:
            logger.debug(
                "[%s] ATR seed пропущений: недостатньо барів (%d < %d)",
                symbol,
                len(df),
                self.period + 1,
            )
            return

        # Беремо останні period+1 барів
        seed_df = df.iloc[-(self.period + 1) :].reset_index(drop=True)
        tr = self._compute_tr(seed_df)
        # Перший ATR як середнє перших period TR
        atr_seed = tr.iloc[: self.period].mean()
        last_close = seed_df["close"].iloc[-1]

        self.state_map[symbol] = {
            "atr": float(atr_seed),
            "last_close": float(last_close),
        }
        logger.debug(
            "[%s] [ATR-SEED] ATR=%.6f last_close=%.6f", symbol, atr_seed, last_close
        )

    def _compute_tr(self, df: pd.DataFrame) -> pd.Series:
        """Обчислює True Range для DataFrame."""
        hl = df["high"] - df["low"]
        hc = (df["high"] - df["close"].shift()).abs()
        lc = (df["low"] - df["close"].shift()).abs()
        tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
        return tr

    def update(self, symbol: str, high: float, low: float, close: float) -> float:
        """Інкрементальний апдейт ATR (EMA формула)."""
        state = self.state_map.get(symbol)
        if state is None:
            raise KeyError(f"ATRManager: state for {symbol} not initialized")

        prev_atr = state["atr"]
        prev_close = state["last_close"]

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )
        atr_new = (prev_atr * (self.period - 1) + tr) / self.period

        state["atr"] = float(atr_new)
        state["last_close"] = float(close)

        logger.debug("[%s] [ATR-UPDATE] TR=%.6f ATR_new=%.6f", symbol, tr, atr_new)
        return atr_new

    def get_state(self, symbol: str) -> float:
        """Повертає останній ATR або ``np.nan`` якщо стану нема."""
        state = self.state_map.get(symbol)
        return state["atr"] if state else float("nan")


def compute_atr(df: pd.DataFrame, window: int = 14, symbol: str = "") -> float:
    """Векторизований ATR (бек‑тести / стат. перевірка)."""
    if len(df) < window + 1:
        logger.debug(f"[{symbol}] [ATR] Недостатньо даних: {len(df)} < {window+1}")
        return float("nan")
    hl = df["high"] - df["low"]
    hc = (df["high"] - df["close"].shift()).abs()
    lc = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    if len(tr) < window:
        return float("nan")
    atr_series = tr.rolling(window=window, min_periods=window).mean()
    atr = atr_series.iloc[-1]
    logger.debug(f"[{symbol}] [ATR] {atr:.6f}")
    return float(atr)


# ───────────────────────────── Публічний API ─────────────────────────────
__all__ = ["ATRManager", "compute_atr"]
