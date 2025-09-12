# utils/helpers.py
import logging
from rich.console import Console
from rich.logging import RichHandler
import pandas as pd
from typing import Optional, Dict, Any
from utils.utils_1_2 import safe_float

try:
    # якщо в тебе є мапа в конфігу — використовуємо її
    from app.config import TICK_SIZE_MAP  # optional
except Exception:
    TICK_SIZE_MAP = {}

# --- Налаштування логування ---
logger = logging.getLogger("app.helpers")
logger.setLevel(logging.INFO)  # Змінено на INFO для зменшення шуму
logger.handlers.clear()
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
    tr = (
        (df_1m["high"] - df_1m["low"]).rolling(14).mean().iloc[-1]
        if len(df_1m) >= 14
        else (df_1m["high"] - df_1m["low"]).mean()
    )
    price = float(df_1m["close"].values[-1])
    return float(max(0.05, min(5.0, (tr / max(price, 1e-9)) * 100.0)))


def get_tick_size(
    symbol: str,
    price_hint: Optional[float] = None,
    overrides: Optional[Dict[str, float]] = None,
) -> float:
    """
    Повертає tick_size для символу.
    Пріоритет: overrides → TICK_SIZE_MAP → евристика за ціною.
    """
    sym = (symbol or "").lower()

    if overrides and sym in overrides:
        return float(overrides[sym])
    if isinstance(TICK_SIZE_MAP, dict) and sym in TICK_SIZE_MAP:
        return float(TICK_SIZE_MAP[sym])

    # Евристика, якщо немає довідника:
    # (Binance часто має дрібні кроки на дешевих активах і крупніші — на дорогих)
    if price_hint is not None:
        p = float(price_hint)
        if p < 0.01:
            return 1e-6
        if p < 0.1:
            return 1e-5
        if p < 1:
            return 1e-4
        if p < 10:
            return 1e-3
        if p < 100:
            return 1e-2
        if p < 1000:
            return 1e-1
        return 1.0

    # дефолт, якщо зовсім нічого не знаємо
    return 1e-3


def normalize_result_types(result: dict) -> dict:
    """Нормалізує типи даних та додає стан для UI"""
    numeric_fields = [
        "confidence",
        "tp",
        "sl",
        "current_price",
        "atr",
        "rsi",
        "volume",
        "volume_mean",
        "volume_usd",
        "volume_z",
        "open_interest",
        "btc_dependency_score",
    ]

    if "calibrated_params" in result:
        result["calibrated_params"] = {
            k: float(v) for k, v in result["calibrated_params"].items()
        }

    for field in numeric_fields:
        if field in result:
            result[field] = safe_float(result[field])
        elif "stats" in result and field in result["stats"]:
            result["stats"][field] = safe_float(result["stats"][field])

    # Визначення стану сигналу
    signal_type = result.get("signal", "NONE").upper()
    if signal_type == "ALERT" or signal_type.startswith("ALERT_"):
        result["state"] = "alert"
    elif signal_type == "NORMAL":
        result["state"] = "normal"
    else:
        result["state"] = "no_trade"

    # Додаємо поле для відображення в UI
    result["visible"] = True

    return result


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
