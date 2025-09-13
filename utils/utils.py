"""Універсальні утиліти (форматування, нормалізація, допоміжні хелпери).

Призначення:
    • Форматування числових метрик (ціна, обсяг, OI)
    • Нормалізація TP/SL відповідно до напрямку угоди
    • Маппінг рекомендацій Stage2 → сигнали UI
    • Уніфікація та очищення часових колонок DataFrame
    • Нормалізація назв тригерів до канонічних коротких ідентифікаторів

Принципи:
    • Відсутність побічних ефектів (чиста логіка)
    • Українська мова для коментарів / докстрінгів
    • Мінімальна залежність від решти системи (центральні константи з config)
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, time
from typing import Any, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

# ── Спроба використати RichHandler (не є обов'язковим) ─────────────────────────
try:
    from rich.console import Console
    from rich.logging import RichHandler

    _HAS_RICH = True
except Exception:  # pragma: no cover - опціональна залежність
    _HAS_RICH = False

# ── Конфіг/константи проєкту ──────────────────────────────────────────────────
# ВАЖЛИВО: ми централізуємо константи у app.config
# TRIGGER_NAME_MAP також живе у конфігу (див. нижче).
from config.config import (
    BUY_SET,
    SELL_SET,
    TICK_SIZE_MAP,
    TICK_SIZE_BRACKETS,
    TICK_SIZE_DEFAULT,
    TRIGGER_TP_SL_SWAP_LONG,
    TRIGGER_TP_SL_SWAP_SHORT,
    INTERVAL_TTL_MAP,
    STAGE2_STATUS,
    ASSET_STATE,
)

# TRIGGER_NAME_MAP переносимо у конфіг (централізація канонічних імен тригерів).
# Якщо в конфігу ще не додано — використовуємо локальний fallback.
try:
    from app.config import TRIGGER_NAME_MAP  # type: ignore
except Exception:  # pragma: no cover
    TRIGGER_NAME_MAP: dict[str, str] = {
        # volume / volatility
        "volume_spike_trigger": "volume_spike",
        "volume_spike": "volume_spike",
        "volatility_spike_trigger": "volatility_burst",
        "volatility_spike": "volatility_burst",
        "volatility_burst": "volatility_burst",
        # breakout / near levels
        "breakout_level_trigger_up": "breakout_up",
        "breakout_level_trigger_down": "breakout_down",
        "breakout_up": "breakout_up",
        "breakout_down": "breakout_down",
        "near_high": "near_high",
        "near_low": "near_low",
        "near_daily_support": "near_daily_support",
        "near_daily_resistance": "near_daily_resistance",
        # rsi / дивергенції
        "rsi_divergence_bearish": "bearish_div",
        "rsi_divergence_bullish": "bullish_div",
        "bearish_div": "bearish_div",
        "bullish_div": "bullish_div",
        "rsi_overbought": "rsi_overbought",
        "rsi_oversold": "rsi_oversold",
        # vwap / мікро-ліквідність
        "vwap_deviation_trigger": "vwap_deviation",
        "vwap_deviation": "vwap_deviation",
        "liquidity_gap": "liquidity_gap",
        "order_imbalance": "order_imbalance",
    }

# ── Локальний логер модуля ────────────────────────────────────────────────────
_logger = logging.getLogger("app.utils")
if not _logger.handlers:  # захист від повторної ініціалізації
    _logger.setLevel(logging.INFO)
    if _HAS_RICH:
        _logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    else:  # pragma: no cover - fallback без rich
        _logger.addHandler(logging.StreamHandler())
    _logger.propagate = False


# ──────────────────────────────────────────────────────────────────────────────
# БАЗОВІ ХЕЛПЕРИ
# ──────────────────────────────────────────────────────────────────────────────
def safe_float(value: Any) -> Optional[float]:
    """Безпечно перетворює значення у float.

    Повертає None, якщо значення не можна конвертувати або воно не є скінченним числом.

    Args:
        value: Будь-який об'єкт.

    Returns:
        Optional[float]: Коректний float або None.
    """
    try:
        # Допомагаємо рядкам з комою як десятковим роздільником
        if isinstance(value, str):
            value = value.strip().replace(",", ".")
        f = float(value)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        _logger.debug("safe_float: не вдалося привести %r до float", value)
        return None


def first_not_none(seq: Optional[Sequence[Optional[Any]]]) -> Optional[Any]:
    """Повертає перший елемент, що не є None.

    Args:
        seq: Послідовність значень.

    Returns:
        Optional[Any]: Перший не-None, або None (якщо таких немає).
    """
    if not seq:
        return None
    for x in seq:
        if x is not None:
            return x
    return None


# ──────────────────────────────────────────────────────────────────────────────
# TIMESTAMP / DATAFRAME
# ──────────────────────────────────────────────────────────────────────────────
def ensure_timestamp_column(
    df: pd.DataFrame,
    *,
    as_index: bool = False,
    drop_duplicates: bool = True,
    sort: bool = True,
    logger_obj: Optional[logging.Logger] = None,
    min_rows: int = 1,
    log_prefix: str = "",
) -> pd.DataFrame:
    """Уніфікує колонку/індекс `timestamp` у DataFrame.

    Можливості:
      - гарантує наявність `timestamp: datetime64[ns, UTC]`;
      - за потреби перетворює у колонку/індекс;
      - видаляє дублі та `NaT`;
      - стабільно сортує за часом;
      - повертає порожній DataFrame, якщо після обробки рядків < `min_rows`.

    Args:
        df: Вхідний DataFrame.
        as_index: Якщо True — встановити `timestamp` індексом.
        drop_duplicates: Видаляти дублі `timestamp`.
        sort: Сортувати за `timestamp`.
        logger_obj: Логер для детальніших повідомлень.
        min_rows: Мінімальна кількість рядків після обробки.
        log_prefix: Префікс до діагностичних повідомлень.

    Returns:
        pd.DataFrame: Очищена/уніфікована таблиця або порожній DataFrame.
    """

    def _log(msg: str) -> None:
        if logger_obj:
            logger_obj.debug("%s%s", log_prefix, msg)

    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        _log("[ensure_timestamp_column] DataFrame порожній або невалідний.")
        return pd.DataFrame()

    # Якщо timestamp є індексом — переносимо у колонку
    if "timestamp" not in df.columns and df.index.name == "timestamp":
        df = df.reset_index()
        _log("[ensure_timestamp_column] Перенесено timestamp з індексу у колонку.")

    # Нормалізація колонки
    if "timestamp" in df.columns:
        if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
            _log("[ensure_timestamp_column] Конвертовано 'timestamp' у datetime (UTC).")

        before = len(df)
        df = df.dropna(subset=["timestamp"]).copy()
        removed = before - len(df)
        if removed > 0:
            _log(
                f"[ensure_timestamp_column] Видалено {removed} рядків із NaT у 'timestamp'."
            )

        if drop_duplicates:
            before = len(df)
            df = df.drop_duplicates(subset=["timestamp"])
            dups = before - len(df)
            if dups > 0:
                _log(
                    f"[ensure_timestamp_column] Видалено {dups} дублікатів по 'timestamp'."
                )

        if sort:
            df = df.sort_values("timestamp", kind="stable")
            _log("[ensure_timestamp_column] Відсортовано за 'timestamp' (stable).")

        if as_index and df.index.name != "timestamp":
            df = df.set_index("timestamp")
            _log("[ensure_timestamp_column] Встановлено 'timestamp' як індекс.")
        elif not as_index and df.index.name == "timestamp":
            df = df.reset_index()
            _log(
                "[ensure_timestamp_column] Переведено 'timestamp' з індексу у колонку."
            )
    else:
        _log("[ensure_timestamp_column] Відсутня колонка 'timestamp' у DataFrame.")

    # Діагностика прикладу
    if len(df) > 0:
        if "timestamp" in df.columns:
            _log(f"[ensure_timestamp_column] Приклад: {df['timestamp'].iloc[0]!r}")
        elif df.index.name == "timestamp":
            _log(f"[ensure_timestamp_column] Приклад (індекс): {df.index[0]!r}")

    if len(df) < min_rows:
        _log(
            f"[ensure_timestamp_column] Після обробки залишилось {len(df)} рядків (<{min_rows}). "
            "Повертаю порожній DataFrame."
        )
        return pd.DataFrame()

    return df


# ──────────────────────────────────────────────────────────────────────────────
# MAP / NORMALIZE: РЕКОМЕНДАЦІЇ, TP/SL, ТРИГЕРИ
# ──────────────────────────────────────────────────────────────────────────────
def map_reco_to_signal(recommendation: Optional[str]) -> str:
    """Перетворює Stage2-рекомендацію у тип сигналу для UI.

    Args:
        recommendation: Рекомендація (наприклад, 'BUY_IN_DIPS').

    Returns:
        str: 'ALERT_BUY' | 'ALERT_SELL' | 'NORMAL'
    """
    reco = (recommendation or "").upper()
    if reco in BUY_SET:
        return "ALERT_BUY"
    if reco in SELL_SET:
        return "ALERT_SELL"
    return "NORMAL"


def normalize_tp_sl(
    tp: Optional[float],
    sl: Optional[float],
    recommendation: Optional[str],
    current_price: Optional[float] = None,
) -> Tuple[Optional[float], Optional[float], bool, Optional[str]]:
    """Гарантує логічний порядок TP/SL відповідно до напрямку угоди.

    Args:
        tp: Ціль (Take Profit).
        sl: Стоп (Stop Loss).
        recommendation: Рекомендація Stage2 (визначає напрям).
        current_price: Поточна ціна (опційно, для додаткових перевірок).

    Returns:
        Tuple[tp, sl, swapped, note_tag]:
            tp (Optional[float]): Можливо змінене TP.
            sl (Optional[float]): Можливо змінений SL.
            swapped (bool): Чи були місця TP/SL переставлені.
            note_tag (Optional[str]): Тег примітки (наприклад, 'tp_sl_swapped_long').
    """
    swapped = False
    note: Optional[str] = None

    if not isinstance(tp, (int, float)) or not isinstance(sl, (int, float)):
        return tp, sl, swapped, note

    reco = (recommendation or "").upper()
    if reco in BUY_SET:
        if sl >= tp:
            tp, sl = sl, tp
            swapped = True
            note = TRIGGER_TP_SL_SWAP_LONG
    elif reco in SELL_SET:
        if tp >= sl:
            tp, sl = sl, tp
            swapped = True
            note = TRIGGER_TP_SL_SWAP_SHORT

    return tp, sl, swapped, note


def normalize_trigger_reasons(reasons: Iterable[str] | None) -> list[str]:
    """Нормалізує список причин-тригерів до канонічних коротких імен.

    Операції:
      - trim + lower для кожного елементу,
      - мапінг через TRIGGER_NAME_MAP,
      - унікалізація зі збереженням порядку.

    Args:
        reasons: Список/кортеж сирих назв тригерів.

    Returns:
        list[str]: Впорядкований список уніфікованих імен.
    """
    if not reasons:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for raw in reasons:
        key = str(raw).strip().lower()
        std = TRIGGER_NAME_MAP.get(key, key)
        if std and std not in seen:
            out.append(std)
            seen.add(std)
    _logger.debug("normalize_trigger_reasons: in=%r → out=%r", list(reasons), out)
    return out


# ──────────────────────────────────────────────────────────────────────────────
# ФОРМАТУВАННЯ: ОБСЯГ, OI, ЦІНА
# ──────────────────────────────────────────────────────────────────────────────
def format_volume_usd(volume: float | str) -> str:
    """Форматує оборот у USD (K/M/G/T).

    Args:
        volume: float або вже відформатований рядок.

    Returns:
        str: Відформатований рядок.
    """
    if isinstance(volume, str):
        return volume
    try:
        v = float(volume)
    except (TypeError, ValueError):
        return "-"
    if v >= 1e12:
        return f"{v / 1e12:.2f}T USD"
    if v >= 1e9:
        return f"{v / 1e9:.2f}G USD"
    if v >= 1e6:
        return f"{v / 1e6:.2f}M USD"
    if v >= 1e3:
        return f"{v / 1e3:.2f}K USD"
    return f"{v:.2f} USD"


def format_open_interest(oi: float | str) -> str:
    """Форматує Open Interest у коротку форму (K/M/B).

    Args:
        oi: Значення OI.

    Returns:
        str: Відформатоване значення або "-".
    """
    try:
        val = float(oi)
    except (ValueError, TypeError):
        return "-"
    if val >= 1e9:
        return f"{val / 1e9:.2f}B"
    if val >= 1e6:
        return f"{val / 1e6:.2f}M"
    if val >= 1e3:
        return f"{val / 1e3:.2f}K"
    return f"{val:.2f} USD"


def get_tick_size(
    symbol: str,
    price_hint: Optional[float] = None,
    overrides: Optional[dict[str, float]] = None,
) -> float:
    """Єдина функція визначення tick_size.

    Пріоритет:
        1) overrides
        2) TICK_SIZE_MAP
        3) TICK_SIZE_BRACKETS (перший поріг де ціна < limit)
        4) TICK_SIZE_DEFAULT
    """
    sym = (symbol or "").lower()
    if overrides and sym in overrides:
        try:
            v = float(overrides[sym])
            if v > 0:
                return v
        except Exception:
            pass
    tick_conf = TICK_SIZE_MAP.get(sym) or TICK_SIZE_MAP.get(sym.upper())
    if isinstance(tick_conf, (int, float)) and tick_conf > 0:
        return float(tick_conf)
    if price_hint is not None and TICK_SIZE_BRACKETS:
        try:
            p = float(price_hint)
            for limit_price, tick in TICK_SIZE_BRACKETS:
                if p < limit_price:
                    return float(tick)
        except Exception:
            pass
    return float(TICK_SIZE_DEFAULT)


def _infer_decimals_for_symbol(symbol: str, price: float) -> int:
    """Визначає кількість знаків після коми, відштовхуючись від tick_size.

    Логіка:
        - Отримати tick_size через get_tick_size(price_hint=price)
        - Якщо >0 → кількість знаків у дробовій частині тіку
        - Інакше (неможливо) — fallback стара евристика
    """
    tick = get_tick_size(symbol, price_hint=price)
    if tick > 0:
        s = f"{tick:.16f}".rstrip("0").rstrip(".")
        return len(s.split(".")[1]) if "." in s else 0
    sym = symbol.lower()
    if "btc" in sym or "eth" in sym or price >= 100:
        return 2
    if price < 1:
        return 4 if price >= 0.01 else 6
    return 4


def format_price(price: float, symbol: str) -> str:
    """Форматує ціну відповідно до специфіки активу.

    Args:
        price: Поточна ціна.
        symbol: Тікер (використовується для евристик та TICK_SIZE_MAP).

    Returns:
        str: Відформатована ціна (з розділювачем тисяч для великих чисел).
    """
    try:
        p = float(price)
    except (TypeError, ValueError):
        return "-"
    decimals = _infer_decimals_for_symbol(symbol, p)
    if p < 1000:
        return f"{p:.{decimals}f}"
    return f"{p:,.{decimals}f}"


# ──────────────────────────────────────────────────────────────────────────────
# CACHE / TTL HELPERS (мапа винесено в config.INTERVAL_TTL_MAP)
# ──────────────────────────────────────────────────────────────────────────────
def get_ttl_for_interval(interval: str) -> int:
    """Повертає рекомендований TTL (сек) для кешу свічок таймфрейму.

    Логіка:
      1. Використовує попередньо визначене значення з `_INTERVAL_TTL_MAP`.
      2. Якщо інтервал не відомий (наприклад, '7m'), намагається:
         - якщо закінчується на 'm' → множимо хвилини * 90% (у сек) * 1.5 запасу
         - якщо закінчується на 'h' → години * 3600 * 1.1
         - якщо закінчується на 'd' → дні * 86400 * 1.05
         - інакше повертаємо дефолт 3600

    Args:
        interval: Рядок таймфрейму ("1m", "1h", "1d", ...).

    Returns:
        int: TTL у секундах (завжди > 0).
    """
    iv = interval.strip().lower()
    ttl = INTERVAL_TTL_MAP.get(iv)
    if ttl is not None:
        return ttl
    try:
        if iv.endswith("m"):
            mins = float(iv[:-1])
            return int(mins * 60 * 1.5)  # 150% довжини інтервалу
        if iv.endswith("h"):
            hrs = float(iv[:-1])
            return int(hrs * 3600 * 1.1)
        if iv.endswith("d"):
            days = float(iv[:-1])
            return int(days * 86400 * 1.05)
        if iv.endswith("w"):
            weeks = float(iv[:-1])
            return int(weeks * 7 * 86400 * 1.02)
    except ValueError:
        pass
    # Фолбек: година
    return 3600


# ──────────────────────────────────────────────────────────────────────────────
# СЕСІЇ / КАЛЕНДАР
# ──────────────────────────────────────────────────────────────────────────────
def is_us_session(current_time: datetime) -> bool:
    """Перевіряє, чи поточний час входить у робочі години US-сесії (NYSE, 09:30–16:00 ET).

    Args:
        current_time: Час у будь-якому часовому поясі (aware).

    Returns:
        bool: True, якщо зараз робочі години біржі (пн–пт, 09:30–16:00 ET).
    """
    try:
        from zoneinfo import ZoneInfo  # stdlib з Python 3.9+

        eastern = current_time.astimezone(ZoneInfo("America/New_York"))
    except Exception:
        # Якщо tz недоступний — поводимось консервативно
        return False
    start = time(9, 30)
    end = time(16, 0)
    return eastern.weekday() < 5 and start <= eastern.time() <= end


# ──────────────────────────────────────────────────────────────────────────────
# НОРМАЛІЗАЦІЯ РЕЗУЛЬТАТІВ
# ──────────────────────────────────────────────────────────────────────────────
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

    result["visible"] = True
    return result


def make_serializable_safe(data) -> Any:
    """Робить вкладені структури JSON-сумісними."""
    if isinstance(data, pd.DataFrame):
        return data.to_dict(orient="records")
    if hasattr(data, "to_dict") and not isinstance(data, dict):
        return data.to_dict()
    if isinstance(data, dict):
        return {k: make_serializable_safe(v) for k, v in data.items()}
    if isinstance(data, list):
        return [make_serializable_safe(x) for x in data]
    return data


# ──────────────────────────────────────────────────────────────────────────────
# SIGNAL HELPERS (раніше всередині screening_producer)
# ──────────────────────────────────────────────────────────────────────────────
def create_no_data_signal(symbol: str) -> dict:
    """Формує стандартний сигнал відсутності даних для активу.

    Args:
        symbol: Ідентифікатор активу.

    Returns:
        dict: Нормалізований сигнал без даних.
    """
    base = {
        "symbol": symbol,
        "signal": "NONE",
        "trigger_reasons": ["no_data"],
        "confidence": 0.0,
        "hints": ["Недостатньо даних для аналізу"],
        "state": ASSET_STATE["NO_DATA"],
        "stage2_status": STAGE2_STATUS["SKIPPED"],
    }
    return normalize_result_types(base)


def create_error_signal(symbol: str, error: str) -> dict:
    """Формує стандартний сигнал помилки для активу.

    Args:
        symbol: Актив.
        error: Текст помилки.

    Returns:
        dict: Нормалізований сигнал помилки.
    """
    base = {
        "symbol": symbol,
        "signal": "NONE",
        "trigger_reasons": ["processing_error"],
        "confidence": 0.0,
        "hints": [f"Помилка: {error}"],
        "state": ASSET_STATE["ERROR"],
        "stage2_status": STAGE2_STATUS["ERROR"],
    }
    return normalize_result_types(base)


# ──────────────────────────────────────────────────────────────────────────────
# ПУБЛІЧНИЙ API МОДУЛЯ
# ──────────────────────────────────────────────────────────────────────────────
__all__ = [
    # базові
    "safe_float",
    "first_not_none",
    # dataframe/timestamp
    "ensure_timestamp_column",
    # mapping/normalize
    "map_reco_to_signal",
    "normalize_tp_sl",
    "normalize_trigger_reasons",
    # форматування
    "format_volume_usd",
    "format_open_interest",
    "format_price",
    # tick size
    "get_tick_size",
    # cache ttl
    "get_ttl_for_interval",
    # сесії
    "is_us_session",
    # screening helpers (extracted)
    # (примітка: create_no_data_signal / create_error_signal наразі залишаються в producer)
    "create_no_data_signal",
    "create_error_signal",
]
