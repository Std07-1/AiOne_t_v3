"""Порогові значення для сигналів (без динамічного калібрування).

Шлях: ``app/thresholds.py``

Завдання:
    • модель `Thresholds` з калібруванням на основі ATR та історії;
    • серіалізація/десеріалізація для кешу (Redis / файл);
    • допоміжні утиліти завантаження/збереження порогів.
"""

from __future__ import annotations
import json
import logging
import pandas as pd
from datetime import timedelta
from typing import Any, Dict, Optional, Union

from rich.console import Console
from rich.logging import RichHandler

from stage1.indicators.atr_indicator import ATRManager  # залишено для сумісності типів
from config.config import CACHE_TTL_DAYS

# ───────────────────────────── Логування ─────────────────────────────
log = logging.getLogger("app.thresholds")
if not log.handlers:
    log.setLevel(logging.INFO)
    log.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    log.propagate = False


class Thresholds:
    """Статичні порогові значення для символу (спрощено, без ATR-калібрування)."""

    def __init__(
        self,
        symbol: str,
        config: dict,  # Конфігурація передається ззовні
        data: Optional[pd.DataFrame] = None,  # ігнорується (калібрування вимкнено)
        atr_manager: Optional[ATRManager] = None,  # ігнорується
        calibrated_params: Optional[Dict] = None,  # legacy (ігнорується)
    ):
        """
        symbol: Біржовий символ (наприклад "BTCUSDT")
        data: DataFrame з історичними даними (high, low, close)
        config: Словник конфігурації з параметрами
        atr_manager: Екземпляр ATRManager для інкрементальних розрахунків
        """
        self.symbol = symbol
        self.low_gate = config.get("low_gate", 0.006)
        self.high_gate = config.get("high_gate", 0.015)
        self.atr_target = config.get("atr_target", 0.5)
        # Canonical key: volume_z_threshold (backward compat: vol_z_threshold)
        self.vol_z_threshold = config.get(
            "volume_z_threshold",
            config.get("vol_z_threshold", 1.2),
        )
        self.rsi_oversold = config.get("rsi_oversold", 30.0)
        self.rsi_overbought = config.get("rsi_overbought", 70.0)

        # Калібрування відключено – просто пост-обробка
        self._post_init()

    # _calibrate видалено – динамічне калібрування не підтримується

    def _post_init(self) -> None:
        """Автокорекція точності значень"""
        self.low_gate = round(self.low_gate, 4)
        self.high_gate = round(self.high_gate, 4)
        self.atr_target = round(self.atr_target, 2)
        self.vol_z_threshold = round(self.vol_z_threshold, 1)
        self.rsi_oversold = round(self.rsi_oversold, 1)
        self.rsi_overbought = round(self.rsi_overbought, 1)

        # Гарантоване співвідношення high_gate > low_gate
        if self.high_gate <= self.low_gate:
            self.high_gate = self.low_gate * 1.5

    @classmethod
    def from_mapping(cls, data: Dict) -> "Thresholds":
        """Створює Thresholds зі словника (symbol не передається у calibrated_params)"""
        symbol = data.get("symbol")
        # Перевірка наявності символу
        if not symbol or not isinstance(symbol, str):
            raise ValueError(
                f"[Thresholds] Недійсний symbol у from_mapping(): {symbol}"
            )

        # Видаляємо symbol із calibrated_params, щоб уникнути float('btcusdt')
        params = {k: v for k, v in data.items() if k != "symbol"}

        return cls(
            symbol=symbol,
            config={
                "low_gate": data.get(
                    "low_gate", 0.0015
                ),  # Нижня межа ATR/price (0.15%)
                "high_gate": data.get(
                    "high_gate", 0.0134
                ),  # Верхня межа ATR/price (1.34%)
                "atr_target": float(
                    data.get("atr_target", 0.3)
                ),  # Цільовий ATR (float; раніше помилково список)
                "volume_z_threshold": data.get(
                    "volume_z_threshold", data.get("vol_z_threshold", 1.2)
                ),  # сплеск обсягу ≥1.2σ (уніфіковано з fallback)
                "rsi_oversold": data.get(
                    "rsi_oversold", 23.0
                ),  # Рівень перепроданності RSI (23%)
                "rsi_overbought": data.get(
                    "rsi_overbought", 74.0
                ),  # Рівень перекупленості RSI (74%)
                "atr_period": 14,  # Період ATR (14)
                "min_atr_percent": 0.002,  # Мінімальний ATR у відсотках (0.002)
            },
            calibrated_params=None,  # калібрування вимкнено
        )

    def to_dict(self) -> Dict[str, Union[float, str]]:
        """Повертає словник з каліброваними значеннями (symbol не повертається)"""
        return {
            "low_gate": self.low_gate,
            "high_gate": self.high_gate,
            "atr_target": self.atr_target,
            "vol_z_threshold": self.vol_z_threshold,
            "rsi_oversold": self.rsi_oversold,
            "rsi_overbought": self.rsi_overbought,
        }


# Redis-ключ
def _redis_key(symbol: str) -> str:
    return f"thresholds:{symbol}"


# Збереження та завантаження
async def save_thresholds(
    symbol: str,
    thr: Thresholds,
    cache_or_store: Any,
) -> None:
    """Зберігає калібровані параметри в Redis"""
    key = _redis_key(symbol)
    payload = json.dumps(
        {
            "low_gate": thr.low_gate,
            "high_gate": thr.high_gate,
            "atr_target": thr.atr_target,
            "vol_z_threshold": thr.vol_z_threshold,
            "rsi_oversold": thr.rsi_oversold,
            "rsi_overbought": thr.rsi_overbought,
            "symbol": symbol,
        },
        ensure_ascii=False,
    )

    # UnifiedDataStore path (has redis.jset)
    if hasattr(cache_or_store, "redis") and hasattr(cache_or_store.redis, "jset"):
        ttl = int(timedelta(days=CACHE_TTL_DAYS).total_seconds())
        await cache_or_store.redis.jset(
            "thresholds", symbol, value=json.loads(payload), ttl=ttl
        )
    # Legacy SimpleCacheHandler
    elif hasattr(cache_or_store, "store_in_cache"):
        await cache_or_store.store_in_cache(
            key,
            "global",
            payload,
            ttl=timedelta(days=CACHE_TTL_DAYS),
            raw=True,
        )
    else:  # fallback log
        log.warning("save_thresholds: Unsupported cache interface for %s", symbol)


async def load_thresholds(
    symbol: str,
    cache_or_store: Any,
    data: pd.DataFrame = pd.DataFrame(),
    config: dict = {},
    atr_manager: ATRManager = None,
) -> Thresholds:
    if not symbol or not isinstance(symbol, str):
        raise ValueError("load_thresholds: Некоректний символ")

    # 1. Спроба завантажити з thresholds-кешу
    key = f"thresholds:{symbol}"
    raw = None
    # UnifiedDataStore path
    if hasattr(cache_or_store, "redis") and hasattr(cache_or_store.redis, "jget"):
        rec = await cache_or_store.redis.jget("thresholds", symbol, default=None)
        if rec:
            raw = json.dumps(rec, ensure_ascii=False)
    elif hasattr(cache_or_store, "fetch_from_cache"):
        raw = await cache_or_store.fetch_from_cache(key, "global", raw=True)

    if raw:
        try:
            return Thresholds.from_mapping(json.loads(raw))
        except Exception as e:
            log.warning(f"[{symbol}] Помилка декодування thresholds: {e}")

    # 2. Калібрування та calib-кеш видалені – перехід одразу до дефолтів
    log.info(f"[{symbol}] Використання дефолтних порогів")
    return Thresholds.from_mapping(
        {
            "symbol": symbol,
            "low_gate": config.get("low_gate", 0.006),
            "high_gate": config.get("high_gate", 0.015),
            "atr_target": config.get("atr_target", 0.5),
            "vol_z_threshold": config.get("vol_z_threshold", 1.2),
            "rsi_oversold": config.get("rsi_oversold", 30.0),
            "rsi_overbought": config.get("rsi_overbought", 70.0),
        }
    )
