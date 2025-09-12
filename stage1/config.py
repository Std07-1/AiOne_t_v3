# stage1\config
# -*- coding: utf-8 -*-

"""
Файл конфігурації для фільтрації активів на Binance Futures
"""

import asyncio
import numpy as np
import logging
from pydantic import BaseModel, confloat, conint, validator

from rich.console import Console
from rich.logging import RichHandler

# --- Налаштування логування ---
logger = logging.getLogger("stage1.config")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False

# --------------------------------------------------------------------------- #
#                               ── CONSTANTS ──                               #
# --------------------------------------------------------------------------- #
OI_SEMAPHORE = asyncio.Semaphore(15)  # макс. 15 паралельних запитів OI
KLINES_SEMAPHORE = asyncio.Semaphore(20)  # для запитів колинків
DEPTH_SEMAPHORE = asyncio.Semaphore(20)  # для запитів глибини стакану
REDIS_CACHE_TTL = 3 * 3600  # 3 години кешування


class AssetFilterError(Exception):
    """
    Базовий виняток для помилок фільтрації.
    """

    def __init__(self, message: str):
        """
        Ініціалізує виняток AssetFilterError.
        :param message: текст помилки
        """
        logger.debug(f"[AssetFilterError] Ініціалізація з повідомленням: {message}")
        super().__init__(message)
        self.message = message

    def __str__(self):
        """
        Повертає рядкове представлення винятку.
        """
        logger.debug(f"[AssetFilterError] __str__ викликано")
        return f"AssetFilterError: {self.message}"


# MODELS


class SymbolInfo(BaseModel):
    """
    Схема одного елемента exchangeInfo["symbols"].
    """

    symbol: str
    status: str
    baseAsset: str
    quoteAsset: str
    contractType: str


class FilterParams(BaseModel):
    """
    Вхідні параметри фільтрації.
    """

    min_quote_volume: confloat(ge=0) = 1_000_000.0
    min_price_change: confloat(ge=0) = 3.0
    min_open_interest: confloat(ge=0) = 500_000.0
    min_orderbook_depth: confloat(ge=0) = 50_000.0
    min_atr_percent: confloat(ge=0) = 0.5
    max_symbols: conint(ge=1) = 30
    dynamic: bool = False
    strict_validation: bool = True

    @validator("*", pre=True)
    def replace_nan(cls, v):
        """
        Валідатор: замінює NaN на 0 для float-параметрів.
        :param v: значення параметра
        :return: 0 якщо NaN, інакше v
        """
        logger.debug(f"[FilterParams] replace_nan: {v}")
        return 0 if isinstance(v, float) and np.isnan(v) else v


class MetricResults(BaseModel):
    """
    Метрики фільтрації для візуалізації результатів.
    """

    initial_count: int
    prefiltered_count: int
    filtered_count: int
    result_count: int
    elapsed_time: float
    params: dict

    def dict(self) -> dict:
        return {
            "initial_count": self.initial_count,
            "prefiltered_count": self.prefiltered_count,
            "filtered_count": self.filtered_count,
            "result_count": self.result_count,
            "elapsed_time": self.elapsed_time,
            "params": self.params,
        }
