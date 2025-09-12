# app/thresholds.py

from __future__ import annotations
import json
import logging
import numpy as np
import pandas as pd
from datetime import timedelta
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Optional, Union

from data.cache_handler import SimpleCacheHandler
from stage1.indicators.atr_indicator import compute_atr, ATRManager

log = logging.getLogger("thresholds")
log.setLevel(logging.WARNING)

# Константи
CACHE_TTL_DAYS: int = 14
OPTUNA_SQLITE_URI = "sqlite:///storage/optuna.db"


@dataclass
class Thresholds:
    """Адаптивні порогові значення для символу з калібруванням на основі ATR."""

    def __init__(
        self,
        symbol: str,
        config: dict,  # Конфігурація передається ззовні
        data: Optional[pd.DataFrame] = None,
        atr_manager: Optional[ATRManager] = None,
        calibrated_params: Optional[
            Dict
        ] = None,  # Додано параметр для каліброваних значень
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
        self.vol_z_threshold = config.get("vol_z_threshold", 1.2)
        self.rsi_oversold = config.get("rsi_oversold", 30.0)
        self.rsi_overbought = config.get("rsi_overbought", 70.0)

        # Застосування каліброваних параметрів
        if calibrated_params:
            for key, value in calibrated_params.items():
                if hasattr(self, key):
                    setattr(self, key, value)

        # Калібрування тільки якщо є дані
        if data is not None and not data.empty:
            self._calibrate(data, config, atr_manager)

        self._post_init()

    def _calibrate(
        self, data: pd.DataFrame, config: dict, atr_manager: ATRManager = None
    ):
        """Виконує адаптивне калібрування параметрів на основі ATR"""
        # Дефолтні значення з конфігурації
        self.low_gate = config.get("low_gate", 0.0015)  # Нижня межа ATR/price (0.15%)
        self.high_gate = config.get(
            "high_gate", 0.0134
        )  # Верхня межа ATR/price (1.34%)
        self.atr_target = config.get(
            "atr_target", 0.3
        )  # Цільовий ATR у відсотках від ціни (30%)
        self.vol_z_threshold = config.get(
            "volume_z_threshold", 1.2
        )  # Сплеск обсягу ≥1.2σ (уніфіковано)
        self.rsi_oversold = config.get(
            "rsi_oversold", 23.0
        )  # Рівень перепроданності RSI (23%)
        self.rsi_overbought = config.get(
            "rsi_overbought", 74.0
        )  # Рівень перекупленості RSI (74%)

        # Перевірка наявності даних
        min_bars = config.get("atr_period") + 1
        if data.empty or len(data) < min_bars:
            log.warning(f"[{self.symbol}] Недостатньо даних для калібрування")
            return

        current_price = data["close"].iloc[-1]

        # Розрахунок ATR
        if atr_manager:
            # Інкрементальний розрахунок через ATRManager
            atr_manager.ensure_state(self.symbol, data)
            atr_value = atr_manager.get_state(self.symbol)
        else:
            # Векторний розрахунок
            atr_value = compute_atr(
                data, window=config.get("atr_period", 14), symbol=self.symbol
            )

        if not np.isnan(atr_value):
            # Адаптивне калібрування low_gate
            min_atr_percent = config.get("min_atr_percent", 0.002)
            self.low_gate = max((atr_value * 1.5) / current_price, min_atr_percent)

            # Адаптивне калібрування high_gate
            self.high_gate = min(self.low_gate * 1.8, config.get("high_gate", 0.015))

            # Валідація співвідношення
            if self.high_gate <= self.low_gate:
                self.high_gate = self.low_gate * 1.5

            # Адаптивне калібрування atr_target
            if self.atr_target is None:
                self.atr_target = max(atr_value * 0.5, 0.5)  # Мінімум 0.5 ATR
            else:
                self.atr_target = max(self.atr_target, atr_value * 0.5)
        else:
            log.warning(
                f"[{self.symbol}] ATR не розраховано, використано дефолтні значення"
            )
            self.low_gate = config.get("low_gate", 0.0015)
            self.high_gate = config.get("high_gate", 0.0134)
            self.atr_target = config.get("atr_target", 0.5)

        # Автокорекція точності
        self._post_init()

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
                "atr_target": data.get(
                    "atr_target", [0.3, 1.5]
                ),  # Цільовий ATR у відсотках
                "volume_z_threshold": data.get(
                    "vol_z_threshold", 1.2
                ),  # сплеск обсягу ≥1.2σ (уніфіковано)
                "rsi_oversold": data.get(
                    "rsi_oversold", 23.0
                ),  # Рівень перепроданності RSI (23%)
                "rsi_overbought": data.get(
                    "rsi_overbought", 74.0
                ),  # Рівень перекупленості RSI (74%)
                "atr_period": 14,  # Період ATR (14)
                "min_atr_percent": 0.002,  # Мінімальний ATR у відсотках (0.002)
            },
            calibrated_params=params,  # symbol не передаємо
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
    cache: SimpleCacheHandler,
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

    await cache.store_in_cache(
        key,
        "global",
        payload,
        ttl=timedelta(days=CACHE_TTL_DAYS),
        raw=True,
    )


async def load_thresholds(
    symbol: str,
    cache: SimpleCacheHandler,
    data: pd.DataFrame = pd.DataFrame(),
    config: dict = {},
    atr_manager: ATRManager = None,
) -> Thresholds:
    if not symbol or not isinstance(symbol, str):
        raise ValueError("load_thresholds: Некоректний символ")

    # 1. Спроба завантажити з thresholds-кешу
    key = f"thresholds:{symbol}"
    raw = await cache.fetch_from_cache(key, "global", raw=True)

    if raw:
        try:
            return Thresholds.from_mapping(json.loads(raw))
        except Exception as e:
            log.warning(f"[{symbol}] Помилка декодування thresholds: {e}")

    # 2. Спроба завантажити з calib-кешу (де зберігає CalibrationQueue)
    calib_key = f"calib:{symbol}"
    calib_data = await cache.fetch_from_cache(calib_key, "global", raw=True)

    if calib_data:
        try:
            params = json.loads(calib_data)
            thr = Thresholds.from_mapping(params)
            await save_thresholds(symbol, thr, cache)
            return thr
        except Exception as e:
            log.warning(f"[{symbol}] Помилка декодування calib: {e}")

    # 3. Калібрування лише при наявності даних
    if not data.empty and config:
        thr = Thresholds(symbol, data, config, atr_manager)
        await save_thresholds(
            symbol, thr, cache
        )  # Виправлено: зберігаємо thr, не config
        return thr

    # 4. Дефолтний fallback
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
