# stage1\utils_1_2.py
# -*- coding: utf-8 -*-

"""
Модуль utils_1_2
-----------------

Універсальні утиліти для роботи з фінансовими DataFrame, зокрема:
- Безпечне приведення до float (_safe_float)
- Уніфікація, захист і стандартизація колонки 'timestamp' у DataFrame (ensure_timestamp_column)

Всі логи — україномовні, з підтримкою RichHandler для зручної діагностики.
"""

import logging
from typing import Optional
import pandas as pd

from rich.console import Console
from rich.logging import RichHandler

# --- Налаштування логування ---
logger = logging.getLogger("utils_1_2")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False


def safe_float(value) -> Optional[float]:
    """
    Безпечне перетворення значення у float з обробкою помилок.
    Повертає None, якщо значення не можна привести до коректного float або воно є NaN/Inf.

    Args:
        value: Будь-яке значення для приведення до float.
    Returns:
        float або None, якщо значення невалідне.
    """
    try:
        f = float(value)
        if f is None or f != f or f == float("inf") or f == float("-inf"):
            logger.debug(f"safe_float: некоректне значення {value}")
            return None
        return f
    except (TypeError, ValueError):
        logger.debug(f"safe_float: не вдалося привести {value} до float")
        return None


def ensure_timestamp_column(
    df: pd.DataFrame,
    as_index: bool = False,
    drop_duplicates: bool = True,
    sort: bool = True,
    logger_obj=None,
    min_rows: int = 1,
    log_prefix: str = "",
) -> pd.DataFrame:
    """
    Універсальний хелпер для уніфікації, захисту та стандартизації колонки 'timestamp' у DataFrame.

    Основні можливості:
      - Гарантує наявність колонки 'timestamp' типу datetime (UTC)
      - За потреби переводить timestamp у індекс або колонку
      - Видаляє дублікатні часові мітки (опціонально)
      - Сортує за часом (опціонально)
      - Логує всі ключові дії українською (через RichHandler або переданий logger)
      - Якщо після обробки рядків менше min_rows — повертає порожній DataFrame

    Args:
        df (pd.DataFrame): Вхідний DataFrame з колонкою 'timestamp' або індексом 'timestamp'.
        as_index (bool): Якщо True — переводить 'timestamp' у індекс, інакше залишає як колонку.
        drop_duplicates (bool): Видаляти дублікатні часові мітки (default: True).
        sort (bool): Сортувати за часом (default: True).
        logger_obj: Об'єкт логера (default: None, тоді логування вимкнено).
        min_rows (int): Мінімальна кількість рядків після обробки (default: 1).
        log_prefix (str): Префікс для логів (default: "").
    Returns:
        pd.DataFrame: Оброблений DataFrame з уніфікованим timestamp або порожній DataFrame, якщо не вдалося уніфікувати.
    """

    def log(msg):
        if logger_obj:
            logger_obj.debug(f"{log_prefix}{msg}")
        else:
            pass

    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        log("[ensure_timestamp_column] DataFrame порожній або невалідний.")
        return pd.DataFrame()
    # Якщо timestamp вже індекс, але не колонка — повертаємо колонку
    if "timestamp" not in df.columns and df.index.name == "timestamp":
        df = df.reset_index()
        log("[ensure_timestamp_column] Переведено timestamp з індексу у колонку.")
    # Якщо timestamp є, але не datetime — конвертуємо
    if "timestamp" in df.columns:
        if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
            log("[ensure_timestamp_column] Конвертовано 'timestamp' у datetime (UTC).")
        # Видаляємо рядки з некоректним timestamp
        before = len(df)
        df = df.dropna(subset=["timestamp"]).copy()
        after = len(df)
        if after < before:
            log(
                f"[ensure_timestamp_column] Видалено {before-after} рядків з некоректним timestamp."
            )
        # Видаляємо дублікатні часові мітки
        if drop_duplicates:
            before = len(df)
            df = df.drop_duplicates(subset=["timestamp"])
            after = len(df)
            if after < before:
                log(
                    f"[ensure_timestamp_column] Видалено {before-after} дублікатів по timestamp."
                )
        # Сортуємо за часом
        if sort:
            df = df.sort_values("timestamp")
            log("[ensure_timestamp_column] Відсортовано за timestamp.")
        # За потреби переводимо у індекс
        if as_index:
            if df.index.name != "timestamp":
                df = df.set_index("timestamp")
                log("[ensure_timestamp_column] Встановлено timestamp як індекс.")
        else:
            if df.index.name == "timestamp":
                df = df.reset_index()
                log(
                    "[ensure_timestamp_column] Переведено timestamp з індексу у колонку (as_index=False)."
                )
    else:
        log(
            "[ensure_timestamp_column] Увага: відсутня колонка 'timestamp' у DataFrame."
        )
    # Діагностика
    if "timestamp" in df.columns and not df.empty:
        log(
            f"[ensure_timestamp_column] Тип та приклад timestamp: {type(df['timestamp'].iloc[0])}, {df['timestamp'].iloc[0]}"
        )
    elif df.index.name == "timestamp" and not df.empty:
        log(
            f"[ensure_timestamp_column] Тип та приклад timestamp (індекс): {type(df.index[0])}, {df.index[0]}"
        )
    # Мінімальна кількість рядків
    if len(df) < min_rows:
        log(
            f"[ensure_timestamp_column] Після обробки залишилось лише {len(df)} рядків (<{min_rows}), повертаю порожній DataFrame."
        )
        return pd.DataFrame()
    return df
