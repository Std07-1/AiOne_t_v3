"""Допоміжні перетворення даних для UI та сервісів.

Шлях: ``app/utils/helper.py``

Функції:
    • buffer_to_dataframe / store_to_dataframe — уніфікація форматів барів
    • resample_5m — агрегація 1m → 5m
    • estimate_atr_pct — груба оцінка ATR% для адаптивних порогів
    • get_tick_size — вибір tick_size (overrides → config map → brackets → fallback)
    • make_serializable_safe — рекурсивне приведення структур до JSON-сумісних

Примітка:
    normalize_result_types тепер єдиний у ``utils.utils`` (single source of truth).
"""

import logging
from rich.console import Console
from rich.logging import RichHandler
import pandas as pd
from typing import Any

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("app.utils.helper")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False


def buffer_to_dataframe(RAMBuffer, symbol: str, limit: int = 500) -> pd.DataFrame:
    rows = RAMBuffer.get(symbol, "1m", limit)  # ← ПРАВИЛЬНО: RAMBuffer.get(...)
    if not rows:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(rows)[["timestamp", "open", "high", "low", "close", "volume"]]
    df = df.dropna().reset_index(drop=True)
    df = df.rename(columns={"timestamp": "time"})
    return df


async def store_to_dataframe(
    store, symbol: str, interval: str = "1m", limit: int = 500
) -> pd.DataFrame:
    """Отримує бари з UnifiedDataStore і повертає DataFrame у форматі time,open,high,low,close,volume.

    store.get_df повертає DataFrame з колонками open_time,...; конвертуємо у старий формат для сумісності.
    """
    try:
        df = await store.get_df(symbol, interval, limit=limit)
    except Exception as e:
        logger.warning(f"store_to_dataframe error for {symbol}: {e}")
        return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])
    if df is None or df.empty:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])
    # Normalize column names
    if "open_time" in df.columns:
        df = df.rename(columns={"open_time": "time"})
    # Ensure required columns exist
    needed = ["time", "open", "high", "low", "close", "volume"]
    for c in needed:
        if c not in df.columns:
            df[c] = 0.0
    out = df[needed].tail(limit).copy()
    out = out.dropna().reset_index(drop=True)
    return out


def resample_5m(df_1m: pd.DataFrame) -> pd.DataFrame:
    """
    Ресемпл 1m → 5m. Використовує '5min' (замість застарілого '5T').
    Повертає DF зі стовпцями: time(ms), open, high, low, close, volume.
    """
    if df_1m is None or df_1m.empty:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])

    df = df_1m.copy()
    # time очікується у мілісекундах
    df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
    df = df.set_index("time").sort_index()

    o = df["open"].resample("5min").first()
    h = df["high"].resample("5min").max()
    l = df["low"].resample("5min").min()
    c = df["close"].resample("5min").last()
    v = df["volume"].resample("5min").sum()

    out = pd.concat([o, h, l, c, v], axis=1).dropna()
    out = out.reset_index()
    # повертаємо time знову у мілісекундах
    out["time"] = out["time"].astype("int64") // 10**6
    out.columns = ["time", "open", "high", "low", "close", "volume"]
    return out


def estimate_atr_pct(df_1m: pd.DataFrame) -> float:
    if df_1m is None or df_1m.empty:
        return 0.5
    try:
        rng = df_1m["high"] - df_1m["low"]
        if len(rng) >= 14:
            # min_periods захищає від degrees-of-freedom warning на коротких серіях
            tr_val = rng.rolling(window=14, min_periods=5).mean().iloc[-1]
        else:
            tr_val = rng.mean()
        if pd.isna(tr_val) or not (tr_val >= 0):  # noqa: E701 simplify
            return 0.5
        price = float(df_1m["close"].iloc[-1]) if len(df_1m["close"]) else 0.0
        if price <= 0:
            return 0.5
        pct = (tr_val / price) * 100.0
        return float(max(0.05, min(5.0, pct)))
    except Exception:
        return 0.5


def make_serializable_safe(data) -> Any:
    """
    Рекурсивно перетворює об'єкти у JSON-сумісні формати:
    - DataFrame → список словників
    - Series → словник
    - Обробляє вкладені структури
    """
    if isinstance(data, pd.DataFrame):
        return data.to_dict(orient="records")

    if hasattr(data, "to_dict") and not isinstance(data, dict):
        return data.to_dict()

    if isinstance(data, dict):
        return {key: make_serializable_safe(value) for key, value in data.items()}

    if isinstance(data, list):
        return [make_serializable_safe(item) for item in data]

    return data
