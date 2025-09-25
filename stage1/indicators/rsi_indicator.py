"""RSI індикатор (векторний + інкрементальний менеджер стану).

Можливості:
    • Векторизований Wilder RSI (``compute_rsi`` / ``compute_last_rsi``)
    • Інкрементальне оновлення стану для стрімінгових барів (``RSIManager``)
    • Динамічні адаптивні пороги (percentile) для overbought / oversold
"""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any  # noqa: F401

import numpy as np
import pandas as pd
from rich.console import Console
from rich.logging import RichHandler

logger = logging.getLogger("stage1.indicators.rsi")
if not logger.handlers:  # захист від подвійної ініціалізації
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False


@dataclass(slots=True)
class RSIState:
    """Зберігає усереднені прирости/спади та попередню ціну."""

    avg_up: float
    avg_dn: float
    prev: float

    # --------------------------------------------------------------------- #
    # Factory-методи                                                         #
    # --------------------------------------------------------------------- #
    @classmethod
    def from_seed(cls, closes: list[float]) -> RSIState:
        """
        Формує початковий стан на основі ``closes`` довжиною ≥ *period*+1.

        Args:
            closes (List[float]): Послідовність цін закриття.

        Returns:
            RSIState: Ініціалізований стан.
        """
        diff = np.diff(closes)
        return cls(
            avg_up=np.clip(diff, 0, None).mean(),
            avg_dn=np.clip(-diff, 0, None).mean(),
            prev=closes[-1],
        )


def _update_rsi(
    state: RSIState,
    price: float,
    period: int = 14,
    symbol: str = "",
) -> tuple[float, RSIState]:
    """
    Оновлює RSI інкрементально на 1 бар.

    Args:
        state (RSIState): Поточний стан.
        price (float): Ціна закриття нового бару.
        period (int): Період RSI.
        symbol (str): Символ (для логів).

    Returns:
    tuple[float, RSIState]: (нове значення RSI, оновлений стан)
    """
    delta = price - state.prev
    up, dn = max(delta, 0.0), max(-delta, 0.0)
    avg_up = (state.avg_up * (period - 1) + up) / period
    avg_dn = (state.avg_dn * (period - 1) + dn) / period
    rs = avg_up / (avg_dn + 1e-9)
    rsi = 100 - 100 / (1 + rs)

    logger.debug(
        "[%s] [RSI-INCR] price=%.6f avg_up=%.6f avg_dn=%.6f rsi=%.2f",
        symbol,
        price,
        avg_up,
        avg_dn,
        rsi,
    )
    return rsi, RSIState(avg_up, avg_dn, price)


def compute_rsi(series: pd.Series, period: int = 14, symbol: str = "") -> pd.Series:
    """
    Векторизований RSI по всій серії (EMA-версія, Wilder).

    Args:
        series (pd.Series): Ціни закриття.
        period (int): Період RSI.
        symbol (str): Символ (для логів).

    Returns:
        pd.Series: Серія RSI (лише останні *len(series) − period* значень валідні).
    """
    if len(series) < period + 1:
        logger.debug(
            "[%s] [RSI] Недостатньо даних: %d < %d", symbol, len(series), period + 1
        )
        return pd.Series([np.nan] * len(series), index=series.index)

    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / (avg_loss + 1e-9)
    rsi = 100 - (100 / (1 + rs))
    logger.debug("[%s] [RSI] Обчислено %d значень", symbol, rsi.count())
    return rsi


def compute_last_rsi(series: pd.Series, period: int = 14, symbol: str = "") -> float:
    """
    Повертає останнє значення RSI.

    Args:
        series (pd.Series): Ціни закриття.
        period (int): Період RSI.
        symbol (str): Символ (для логів).

    Returns:
        float: Останній RSI або `np.nan`, якщо даних мало.
    """
    rsi_series = compute_rsi(series, period, symbol)
    return float(rsi_series.iloc[-1]) if not rsi_series.empty else float("nan")


class RSIManager:
    """
    Оперативний менеджер RSI для Stage 1.

    Зберігає *state* та *history* повністю у RAM:
    * `state_map`   — інкрементальний стан для швидкого `update()`;
    * `history_map` — останні N значень RSI  (для динамічних порогів).
    """

    def __init__(self, period: int = 14, history_window: int = 120):
        """
        Args:
            period (int): Базовий період RSI.
            history_window (int): Скільки останніх значень RSI зберігати
                                  для динамічних порогів.
        """
        self.period = period
        self.history_window = history_window

        self.state_map: dict[str, RSIState] = {}
        self.history_map: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=self.history_window)
        )

    def ensure_state(self, symbol: str, closes: pd.Series) -> RSIState:
        """
        Ініціалізує стан, якщо його ще нема.

        Args:
            symbol (str): Тікер.
            closes (pd.Series): Серія ``close`` (≥ period+1 значень).

        Returns:
            RSIState: Готовий стан.
        """
        state = self.state_map.get(symbol)
        if state is None:
            seed = closes.tail(self.period + 1).tolist()
            if len(seed) < self.period + 1:
                logger.debug(
                    "[%s] [RSI-STATE] Seed короткий: %d < %d",
                    symbol,
                    len(seed),
                    self.period + 1,
                )
            state = RSIState.from_seed(seed)
            self.state_map[symbol] = state

        # Обовʼязково оновлюємо history_map (потрібно для динамічних порогів)
        rsi_series = compute_rsi(
            closes.tail(self.history_window + 1), self.period, symbol
        ).dropna()
        self.history_map[symbol].extend(rsi_series.tolist())
        return state

    def update(self, symbol: str, new_price: float) -> float:
        """
        Інкрементальний апдейт RSI.

        Args:
            symbol (str): Тікер.
            new_price (float): Ціна закриття нового бару.

        Returns:
            float: Нове значення RSI.
        """
        state = self.state_map.get(symbol)
        if state is None:
            raise ValueError(f"[{symbol}] [RSI-STATE] State не ініціалізовано")

        rsi, new_state = _update_rsi(state, new_price, self.period, symbol)
        self.state_map[symbol] = new_state

        self.history_map[symbol].append(float(rsi))
        return rsi

    def batch_update(self, symbol: str, closes: pd.Series) -> float:
        """
        Перераховує стан на довгій історії (корисно після гепів).

        Args:
            symbol (str): Тікер.
            closes (pd.Series): Серія ``close``.

        Returns:
            float: Останній RSI.
        """
        if len(closes) < self.period + 1:
            return compute_last_rsi(closes, self.period, symbol)

        state = RSIState.from_seed(closes.iloc[: self.period + 1].tolist())
        for price in closes.iloc[self.period + 1 :]:
            _, state = _update_rsi(state, price, self.period)
        self.state_map[symbol] = state

        rsi, _ = _update_rsi(state, closes.iloc[-1], self.period)
        return rsi

    def get_state(self, symbol: str) -> RSIState | None:
        """Повертає поточний `RSIState` або `None`."""
        return self.state_map.get(symbol)

    def reset_state(self, symbol: str) -> None:
        """Видаляє стан та історію для символу."""
        self.state_map.pop(symbol, None)
        self.history_map.pop(symbol, None)

    def get_dynamic_level(
        self,
        symbol: str,
        level_type: str = "overbought",
        window: int = 50,
        q_hi: float = 0.95,
        q_lo: float = 0.05,
    ) -> float | None:
        """
        Повертає адаптивний поріг RSI (percentile від історії).

        Args:
            symbol (str): Тікер.
            level_type (str): ``"overbought"`` / ``"oversold"``.
            window (int): Останні *window* значень для розрахунку.
            q_hi (float): Квантиль для overbought, default 0.95.
            q_lo (float): Квантиль для oversold,  default 0.05.

        Returns:
            Optional[float]: Поріг або `None`, якщо історії мало.
        """
        hist = self.history_map.get(symbol)
        if not hist or len(hist) < 10:
            return None

        arr = np.array(list(hist)[-window:])
        if level_type == "overbought":
            return float(np.nanpercentile(arr, q_hi * 100))
        if level_type == "oversold":
            return float(np.nanpercentile(arr, q_lo * 100))
        raise ValueError(f"Невідомий level_type: {level_type!r}")


def format_rsi(rsi: float, bar_length: int = 10, symbol: str = "") -> str:
    """
    Форматує RSI як текстовий прогрес-бар.

    Args:
        rsi (float): Значення RSI ∈ [0, 100].
        bar_length (int): Довжина прогрес-бару (кількість символів).
        symbol (str): Тікер (для логів).

    Returns:
        str: Форматований рядок, напр. ``"[███░░░░░░░] 28.5"``.
    """
    try:
        filled = int(round((rsi / 100) * bar_length))
    except Exception as exc:  # noqa: BLE001
        logger.debug("[%s] [RSI-FORMAT] %s | rsi=%s", symbol, exc, rsi)
        filled = 0

    bar = "█" * filled + "░" * (bar_length - filled)
    return f"[{bar}] {rsi:.1f}"


# ───────────────────────────── Публічний API ─────────────────────────────
__all__ = [
    "compute_rsi",
    "compute_last_rsi",
    "RSIManager",
    "format_rsi",
    "RSIState",
]
