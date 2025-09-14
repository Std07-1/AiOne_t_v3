# ep_2\indicators.py

# indicators.py
# -*- coding: utf-8 -*-
"""
Єдиний модуль індикаторів для всієї системи.
Забезпечує консистентність розрахунків у всіх модулях (episodes, QDE, stage1/stage2).
"""
import logging
import numpy as np
import pandas as pd
from typing import Dict, Tuple, List

# Налаштування логування
try:
    from rich.console import Console  # type: ignore
    from rich.logging import RichHandler  # type: ignore

    _HAS_RICH = True
except ImportError:
    _HAS_RICH = False

logger = logging.getLogger("indicators")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    if _HAS_RICH:
        _h = RichHandler(console=Console(stderr=True), show_path=False)  # type: ignore[arg-type]
    else:
        _h = logging.StreamHandler()
        _h.setFormatter(
            logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s")
        )
    logger.addHandler(_h)
    logger.propagate = False


# базові індикатори
def calculate_vwap(df: pd.DataFrame) -> pd.Series:
    """
    Розрахунок VWAP (Volume Weighted Average Price).
    Повертає серію VWAP для кожного бару.
    """
    logger.debug("Розрахунок VWAP...")
    typical_price = (df["high"] + df["low"] + df["close"]) / 3.0
    if "quote_volume" in df.columns:
        vol_quote = df["quote_volume"].astype(float).clip(lower=0.0)
        num = (typical_price * vol_quote).cumsum()
        den = vol_quote.cumsum().replace(0.0, np.nan)
    else:
        vol = df["volume"].astype(float).clip(lower=0.0)
        num = (typical_price * vol).cumsum()
        den = vol.cumsum().replace(0.0, np.nan)
    vwap = (num / den).bfill().ffill()
    logger.debug(f"VWAP розраховано, перші значення: {vwap.head(3).tolist()}")
    return vwap


def calculate_ema(series: pd.Series, span: int = 50) -> pd.Series:
    """
    Розрахунок EMA (Exponential Moving Average).
    span - період згладжування.
    """
    logger.debug(f"Розрахунок EMA з періодом {span}...")
    ema = series.ewm(span=span, adjust=False).mean()
    logger.debug(f"EMA розраховано, перші значення: {ema.head(3).tolist()}")
    return ema


def calculate_rsi(series: pd.Series, length: int = 14) -> pd.Series:
    """
    Розрахунок RSI (Relative Strength Index).
    length - період для розрахунку.
    """
    logger.debug(f"Розрахунок RSI з періодом {length}...")
    delta = series.diff()
    up = np.where(delta > 0, delta, 0.0)
    down = np.where(delta < 0, -delta, 0.0)
    roll_up = (
        pd.Series(up, index=series.index).ewm(alpha=1 / length, adjust=False).mean()
    )
    roll_down = (
        pd.Series(down, index=series.index).ewm(alpha=1 / length, adjust=False).mean()
    )
    rs = roll_up / roll_down.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    logger.debug(f"RSI розраховано, перші значення: {rsi.head(3).tolist()}")
    return rsi.fillna(50.0)


def calculate_atr(df: pd.DataFrame, length: int = 14) -> pd.Series:
    """
    Розрахунок ATR (Average True Range).
    length - період для розрахунку.
    """
    logger.debug(f"Розрахунок ATR з періодом {length}...")
    high, low, close = df["high"], df["low"], df["close"]
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1
    ).max(axis=1)
    atr = tr.ewm(alpha=1 / length, adjust=False).mean()
    logger.debug(f"ATR розраховано, перші значення: {atr.head(3).tolist()}")
    return atr


def calculate_atr_percent(df: pd.DataFrame, atr_col: str = "atr") -> pd.Series:
    """
    ATR у відсотках від ціни.
    """
    logger.debug("Розрахунок ATR у відсотках...")
    if atr_col in df.columns:
        close = df["close"]
        atrp = (df[atr_col] / close).replace([np.inf, -np.inf], np.nan)
        return atrp.fillna(0.0)
    atr = calculate_atr(df)
    close = df["close"]
    atrp = (atr / close).replace([np.inf, -np.inf], np.nan)
    logger.debug(f"ATR% розраховано, перші значення: {atrp.head(3).tolist()}")
    return atrp.fillna(0.0)


def calculate_zscore(series: pd.Series, window: int = 200) -> pd.Series:
    """
    Z-score для об'ємів (або іншої серії).
    win - розмір вікна.
    """
    logger.debug(f"Розрахунок Z-score з вікном {window}...")
    min_periods = max(5, window // 5)
    mean = series.rolling(window, min_periods=min_periods).mean()
    std = series.rolling(window, min_periods=min_periods).std()
    z = (series - mean) / std.replace(0, np.nan)
    logger.debug(f"Z-score розраховано, перші значення: {z.head(3).tolist()}")
    return z


def calculate_linreg_slope(series: pd.Series) -> float:
    """
    Нахил лінійної регресії для серії з обробкою коротких серій.
    """
    logger.debug("Розрахунок нахилу лінійної регресії...")
    series = series.astype(float).values
    n = len(series)

    if n < 2:
        logger.debug("Занадто коротка серія для регресії.")
        return 0.0

    # Видаляємо NaN значення
    valid_indices = ~np.isnan(series)
    x = np.arange(n, dtype=float)[valid_indices]
    y = series[valid_indices]

    if len(x) < 2:
        return 0.0

    x_mean = x.mean()
    y_mean = y.mean()

    numerator = np.dot(x - x_mean, y - y_mean)
    denominator = np.dot(x - x_mean, x - x_mean)

    if denominator == 0:
        return 0.0

    slope = float(numerator / denominator)
    logger.debug(f"Нахил регресії: {slope}")
    return slope


# похідні індикатори
def calculate_vwap_slope(df: pd.DataFrame, window: int = 5) -> pd.Series:
    logger.debug(f"Розрахунок нахилу VWAP з вікном {window}...")
    vwap = calculate_vwap(df)
    # Додаємо min_periods=2 та коректне заповнення NaN
    slope = (
        vwap.rolling(window, min_periods=2)
        .apply(calculate_linreg_slope, raw=False)
        .fillna(0.0)
    )
    logger.debug(f"Нахил VWAP розраховано, перші значення: {slope.head(3).tolist()}")
    return slope


def calculate_ema_slope(df: pd.DataFrame, span: int = 50, window: int = 5) -> pd.Series:
    logger.debug(f"Розрахунок нахилу EMA з span={span}, window={window}...")
    ema = calculate_ema(df["close"], span)
    # Додаємо min_periods=2 та коректне заповнення NaN
    slope = (
        ema.rolling(window, min_periods=2)
        .apply(calculate_linreg_slope, raw=False)
        .fillna(0.0)
    )
    logger.debug(f"Нахил EMA розраховано, перші значення: {slope.head(3).tolist()}")
    return slope


# Допоміжні функції
def segment_data(df: pd.DataFrame, start_idx: int, end_idx: int) -> pd.DataFrame:
    """
    Витягує сегмент DataFrame за індексами.
    """
    logger.debug(f"Сегментація даних: start_idx={start_idx}, end_idx={end_idx}")
    if start_idx < 0:
        start_idx = 0
    if end_idx >= len(df):
        end_idx = len(df) - 1
    seg = df.iloc[start_idx : end_idx + 1].copy()
    logger.debug(f"Сегмент отримано, розмір: {seg.shape}")
    return seg


def calculate_max_runup_drawdown(
    close_prices: pd.Series, direction: str
) -> Tuple[float, float]:
    """
    Розрахунок максимального руху (runup) та просадки (drawdown) для серії цін.
    direction - 'up' або 'down'.
    """
    logger.debug(f"Розрахунок runup/drawdown для напрямку: {direction}")
    prices = close_prices.astype(float).values
    n = len(prices)
    if n == 0:
        logger.debug("Порожня серія цін.")
        return 0.0, 0.0
    if direction == "up":
        min_so_far = prices[0]
        max_runup = 0.0
        max_drawdown = 0.0
        max_so_far = prices[0]
        for t in range(n):
            min_so_far = min(min_so_far, prices[t])
            max_so_far = max(max_so_far, prices[t])
            runup = (prices[t] - min_so_far) / max(min_so_far, 1e-12)
            drawdown = (max_so_far - prices[t]) / max(max_so_far, 1e-12)
            max_runup = max(max_runup, runup)
            max_drawdown = max(max_drawdown, drawdown)
        logger.debug(f"Runup: {max_runup}, Drawdown: {max_drawdown}")
        return float(max_runup), float(max_drawdown)
    else:
        max_so_far = prices[0]
        max_runup = 0.0
        max_drawdown = 0.0
        min_so_far = prices[0]
        for t in range(n):
            max_so_far = max(max_so_far, prices[t])
            min_so_far = min(min_so_far, prices[t])
            runup = (max_so_far - prices[t]) / max(prices[t], 1e-12)
            drawdown = (prices[t] - min_so_far) / max(prices[t], 1e-12)
            max_runup = max(max_runup, runup)
            max_drawdown = max(max_drawdown, drawdown)
        logger.debug(f"Runup: {max_runup}, Drawdown: {max_drawdown}")
        return float(max_runup), float(max_drawdown)


# Головна функція для забезпечення всіх індикаторів
def ensure_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Додає всі необхідні індикатори до DataFrame.

    Args:
        df: Вхідний DataFrame з даними цін (має містити 'close', 'high', 'low', 'volume' або 'quote_volume')

    Returns:
        DataFrame з доданими колонками індикаторів:
        - rsi
        - atr, atr_pct
        - volume_z
        - vwap, vwap_slope
        - ema, ema_slope
    """
    logger.debug("Запуск ensure_indicators: перевірка та розрахунок індикаторів...")
    result = df.copy()

    # Додаємо RSI, якщо його немає
    if "rsi" not in result.columns and "close" in result.columns:
        logger.debug("Додаємо RSI...")
        result["rsi"] = calculate_rsi(result["close"], length=14)

    # Додаємо ATR та ATR%, якщо їх немає
    if ("atr" not in result.columns or "atr_pct" not in result.columns) and {
        "high",
        "low",
        "close",
    }.issubset(result.columns):
        logger.debug("Додаємо ATR та ATR%...")
        atr = calculate_atr(result, length=14)
        result["atr"] = atr
        result["atr_pct"] = (
            (atr / result["close"]).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        )

    # Додаємо Volume Z-score, якщо його немає
    if "volume_z" not in result.columns:
        vol_col = "quote_volume" if "quote_volume" in result.columns else "volume"
        if vol_col in result.columns:
            logger.debug("Додаємо volume_z...")
            safe_vol = result[vol_col].replace(0, np.nan).ffill().bfill()
            vol_log = np.log(safe_vol.clip(lower=1e-12))
            result["volume_z"] = calculate_zscore(vol_log, window=200).fillna(0.0)
        else:
            logger.debug("Об'єм не знайдено, volume_z=0.0")
            result["volume_z"] = 0.0

    # Додаємо VWAP та його нахил, якщо їх немає
    if "vwap" not in result.columns:
        logger.debug("Додаємо VWAP...")
        result["vwap"] = calculate_vwap(result)

    if "vwap_slope" not in result.columns:
        logger.debug("Додаємо vwap_slope...")
        result["vwap_slope"] = calculate_vwap_slope(result)
        nan_count = result["vwap_slope"].isna().sum()
        if nan_count > 0:
            logger.debug(
                f"NaN у vwap_slope: {nan_count} значень. Перші NaN: {result['vwap_slope'][result['vwap_slope'].isna()].index.tolist()[:5]}"
            )
            logger.debug(
                f"vwap_slope NaN debug: {result[['vwap','vwap_slope']].head(10)}"
            )

    # Додаємо EMA та його нахил, якщо їх немає
    if "ema" not in result.columns and "close" in result.columns:
        logger.debug("Додаємо EMA...")
        result["ema"] = calculate_ema(result["close"], span=50)

    if "ema_slope" not in result.columns and "close" in result.columns:
        logger.debug("Додаємо ema_slope...")
        result["ema_slope"] = calculate_ema_slope(result, span=50)
        nan_count = result["ema_slope"].isna().sum()
        if nan_count > 0:
            logger.debug(
                f"NaN у ema_slope: {nan_count} значень. Перші NaN: {result['ema_slope'][result['ema_slope'].isna()].index.tolist()[:5]}"
            )
            logger.debug(f"ema_slope NaN debug: {result[['ema','ema_slope']].head(10)}")

    logger.debug(
        f"ensure_indicators завершено. Додані колонки: {set(result.columns) - set(df.columns)}"
    )
    return result


# Функція для генерації сигналів (може бути винесена в окремий модуль)
def create_early_burst_signals(df: pd.DataFrame, config: dict) -> pd.Series:
    """
    Генерує сигнали на основі індикаторів (ранній спалах).

    Args:
        df: DataFrame з даними та індикаторами
        config: Словник з параметрами конфігурації

    Returns:
        Серія з булевими сигналами
    """
    # Перевіряємо наявність індикаторів
    df = ensure_indicators(df)
    signals = pd.Series(False, index=df.index, name="entry_signal")

    # Anti-lag: корекція порогів на основі ATR%
    atr_pct = df["atr_pct"].fillna(0)
    base_volz_threshold = getattr(config, "volume_z_threshold", 2.0)

    # Корекція порогу volume_z
    volz_threshold = np.where(
        atr_pct < getattr(config, "low_gate", 0.001),
        base_volz_threshold + 0.2,  # Підвищуємо поріг у періоди низької волатильності
        base_volz_threshold,
    )

    # Корекція RSI порогів
    rsi_overbought = getattr(config, "rsi_overbought", 80.0)
    rsi_oversold = getattr(config, "rsi_oversold", 20.0)
    rsi_overbought_adj = np.where(
        atr_pct < getattr(config, "low_gate", 0.001),
        rsi_overbought + 5.0,  # Розширюємо діапазон RSI
        rsi_overbought,
    )
    rsi_oversold_adj = np.where(
        atr_pct < getattr(config, "low_gate", 0.001),
        rsi_oversold - 5.0,  # Розширюємо діапазон RSI
        rsi_oversold,
    )

    # Комбінований slope
    weights = getattr(config, "weights", {"slope_vwap": 0.5, "slope_ema": 0.5})
    combined_slope = weights.get("slope_vwap", 0.5) * df["vwap_slope"].fillna(
        0
    ) + weights.get("slope_ema", 0.5) * df["ema_slope"].fillna(0)

    # Детектор перетину RSI
    rsi = df["rsi"].fillna(50)
    rsi_prev = rsi.shift(1)

    # Умови для long
    long_condition = (
        (df["volume_z"] >= volz_threshold)
        & (rsi >= rsi_overbought_adj)
        & (rsi_prev < rsi_overbought_adj)  # Детектор перетину вгору
        & (combined_slope > getattr(config, "vwap_gate", 0.0))
        & (df["close"] > df["open"])
    )

    # Умови для short
    short_condition = (
        (df["volume_z"] >= volz_threshold)
        & (rsi <= rsi_oversold_adj)
        & (rsi_prev > rsi_oversold_adj)  # Детектор перетину вниз
        & (combined_slope < -getattr(config, "vwap_gate", 0.0))
        & (df["close"] < df["open"])
    )

    signals[long_condition | short_condition] = True
    return signals


# Функція для знаходження піку імпульсу в епізоді
def find_impulse_peak(segment: pd.DataFrame, direction: str) -> pd.Timestamp:
    """
    Знаходить момент піку імпульсу в епізоді
    """
    if direction == "up":
        # Шукаємо максимум volume_z або максимальний приріст ціни
        volz_peak = segment["volume_z"].idxmax()
        price_change = segment["close"].diff().abs()
        price_peak = price_change.idxmax()
        # Комбінуємо обидва піки
        return max(volz_peak, price_peak)
    else:
        # Аналогічно для медвежого руху
        volz_peak = segment["volume_z"].idxmax()
        price_change = segment["close"].diff().abs()
        price_peak = price_change.idxmax()
        return max(volz_peak, price_peak)


# Допоміжні аналітичні функції
def calculate_returns(series: pd.Series, periods: int = 1) -> pd.Series:
    """
    Розраховує прибутковість за вказану кількість періодів.

    Args:
        series: Серія цін
        periods: Кількість періодів для розрахунку

    Returns:
        Серія з прибутковістю
    """
    return series.pct_change(periods)


def calculate_volatility(series: pd.Series, window: int = 20) -> pd.Series:
    """
    Розраховує волатильність як стандартне відхилення прибутковості.

    Args:
        series: Серія цін
        window: Вікно для розрахунку волатильності

    Returns:
        Серія з волатильністю
    """
    returns = calculate_returns(series)
    return returns.rolling(window).std()


def analyze_data_volatility(
    df: pd.DataFrame, window_sizes: List[int] = [5, 30, 60]
) -> Dict[str, float]:
    """
    Аналізує волатильність даних для різних вікон.
    """
    close = df["close"].astype(float)
    results = {}

    for window in window_sizes:
        returns = close.pct_change(window)
        max_return = returns.abs().max()
        results[f"max_{window}bar_return"] = max_return

    # Аналізуємо максимальний рух у вікнах
    for window in window_sizes:
        rolling_max = df["high"].rolling(window).max()
        rolling_min = df["low"].rolling(window).min()
        max_move = ((rolling_max - rolling_min) / rolling_min).max()
        results[f"max_{window}bar_move"] = max_move

    return results


# Експортовані функції (для зручності імпорту)
__all__ = [
    "calculate_vwap",
    "calculate_ema",
    "calculate_rsi",
    "calculate_atr",
    "calculate_atr_percent",
    "calculate_zscore",
    "calculate_linreg_slope",
    "calculate_vwap_slope",
    "calculate_ema_slope",
    "segment_data",
    "calculate_max_runup_drawdown",
    "ensure_indicators",
    "create_early_burst_signals",
    "calculate_returns",
    "calculate_volatility",
    "analyze_data_volatility",
]
