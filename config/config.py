"""AiOne_t — централізована конфігурація (single source of truth).

Містить:
    • Глобальні константи та таймінги
    • Канали / ключі Redis
    • Локалізовані підписи для UI
    • Таксономія активів та Stage2 (QDE) конфіг
    • Моделі Pydantic (валідація параметрів / метрик)
    • Семафори для контролю паралельності зовнішніх запитів
    • Карти TTL для історичних інтервалів

Принципи:
    • Жодної бізнес-логіки: лише дані, моделі та легкі структури
    • Українська мова для всіх коментарів / докстрінгів
    • Сталі імена експорту через __all__ для явного API
"""

from __future__ import annotations

# ──────────────────────────────────────────────────────────────────────────────
# ІМПОРТИ
# ──────────────────────────────────────────────────────────────────────────────
import asyncio
import logging
from typing import Any, Dict, List, Optional

import numpy as np
from pydantic import BaseModel, confloat, conint, validator


# ──────────────────────────────────────────────────────────────────────────────
# ЛОГУВАННЯ МОДУЛЯ
# ──────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger("app.config")
if not logger.handlers:  # захист від повторної ініціалізації
    logger.setLevel(logging.INFO)


# ──────────────────────────────────────────────────────────────────────────────
# БАЗОВІ КОНСТАНТИ ПАЙПЛАЙНА
# ──────────────────────────────────────────────────────────────────────────────
#: Дефолтна глибина історії барів для швидких перевірок (шт.)
DEFAULT_LOOKBACK: int = 20

#: Робочий таймфрейм пайплайна
DEFAULT_TIMEFRAME: str = "1m"

#: Мінімальна частка активів, для яких має бути готова історія, щоби стартувати цикл
MIN_READY_PCT: float = 0.10

#: Ліміт одночасних задач Stage2 (захист від «віяла»)
MAX_PARALLEL_STAGE2: int = 10

#: Інтервал оновлення торгового стану / життєвого циклу угод (сек)
TRADE_REFRESH_INTERVAL: int = 60


# ──────────────────────────────────────────────────────────────────────────────
# МАПІНГ РЕКОМЕНДАЦІЙ НА СИГНАЛИ ДЛЯ UI
# ──────────────────────────────────────────────────────────────────────────────
BUY_SET = {"STRONG_BUY", "BUY_IN_DIPS"}
SELL_SET = {"STRONG_SELL", "SELL_ON_RALLIES"}


# ──────────────────────────────────────────────────────────────────────────────
# REDIS: КАНАЛИ ТА КЛЮЧІ
# ──────────────────────────────────────────────────────────────────────────────
#: Канал публікації агрегованого стану активів для UI-консюмера
REDIS_CHANNEL_ASSET_STATE: str = "asset_state_update"

#: Ключ для зберігання останнього знімка стану (для «холодного» старту UI)
REDIS_SNAPSHOT_KEY: str = "asset_state_snapshot"


# ──────────────────────────────────────────────────────────────────────────────
# UI / ЛОКАЛІЗАЦІЯ
# ──────────────────────────────────────────────────────────────────────────────
#: Основна локаль UI (використовуй у форматуванні дат/чисел за потреби)
UI_LOCALE: str = "uk-UA"

#: Підпис колонки для обігу в доларах США
UI_COLUMN_VOLUME: str = "Оборот USD"

#: Кастомні розміри кроку ціни (override), якщо потрібні точні значення по символах
#: Використовується утилітою get_tick_size() через `from app.config import TICK_SIZE_MAP`
TICK_SIZE_MAP: Dict[str, float] = {
    # приклад: "btcusdt": 0.1,
}

#: Формалізовані пороги (brackets) для евристики вибору tick_size за ціною.
#  Список впорядкований за зростанням граничної ціни. Перше співпадіння → відповідний tick.
#  Використовується у get_tick_size(): overrides → TICK_SIZE_MAP → TICK_SIZE_BRACKETS → fallback.
TICK_SIZE_BRACKETS: list[tuple[float, float]] = [
    # (гранична_ціна_exclusive, tick_size)
    (0.01, 1e-6),
    (0.1, 1e-5),
    (1, 1e-4),
    (10, 1e-3),
    (100, 1e-2),
    (1000, 1e-1),
]

#: Дефолтний tick_size якщо ціна вища за всі пороги або відсутній price_hint.
TICK_SIZE_DEFAULT: float = 1.0

#: Канонічні статуси Stage2 (уніфікація по всьому коду)
STAGE2_STATUS = {
    "PENDING": "pending",
    "PROCESSING": "processing",
    "COMPLETED": "completed",
    "ERROR": "error",
    "SKIPPED": "skipped",
}

#: Канонічні стани активу (UI/state machine)
ASSET_STATE = {
    "INIT": "init",
    "ALERT": "alert",
    "NORMAL": "normal",
    "NO_TRADE": "no_trade",
    "NO_DATA": "no_data",
    "ERROR": "error",
}


# ──────────────────────────────────────────────────────────────────────────────
# TEГИ ТРИГЕРІВ (для уніфікації протягом пайплайна та аналітики)
# ──────────────────────────────────────────────────────────────────────────────
TRIGGER_TP_SL_SWAP_LONG = "tp_sl_swapped_long"
TRIGGER_TP_SL_SWAP_SHORT = "tp_sl_swapped_short"
TRIGGER_SIGNAL_GENERATED = "signal_generated"

# Канонічні назви тригерів (уніфікація для всього пайплайна)
TRIGGER_NAME_MAP: Dict[str, str] = {
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


# ──────────────────────────────────────────────────────────────────────────────
# ПАРАЛЕЛІЗМ/СЕМAФОРИ ДЛЯ ЗОВНІШНІХ ЗАПИТІВ (Stage1 фільтри, Binance тощо)
# ──────────────────────────────────────────────────────────────────────────────
#: Максимум паралельних запитів до ендпойнтів відкритого інтересу (OI)
OI_SEMAPHORE = asyncio.Semaphore(15)

#: Обмеження для запитів свічок (klines)
KLINES_SEMAPHORE = asyncio.Semaphore(20)

#: Обмеження для глибини стакану (order book depth)
DEPTH_SEMAPHORE = asyncio.Semaphore(20)

#: TTL для кешу у Redis (сек) — актуально для важких довідкових відповідей
REDIS_CACHE_TTL: int = 3 * 3600  # 3 години


# ──────────────────────────────────────────────────────────────────────────────
# ПОМИЛКИ / ВИНЯТКИ
# ──────────────────────────────────────────────────────────────────────────────
class AssetFilterError(Exception):
    """Базовий виняток для помилок фільтрації активів."""

    def __init__(self, message: str) -> None:
        self.message = str(message)
        super().__init__(self.message)
        logger.debug("[AssetFilterError] %s", self.message)

    def __str__(self) -> str:  # pragma: no cover — простий форматер
        return f"AssetFilterError: {self.message}"


# ──────────────────────────────────────────────────────────────────────────────
# МОДЕЛІ ДАНИХ (Pydantic)
# ──────────────────────────────────────────────────────────────────────────────
class SymbolInfo(BaseModel):
    """Схема одного запису з Binance `exchangeInfo["symbols"]` (USDT-PERPETUAL)."""

    symbol: str
    status: str
    baseAsset: str
    quoteAsset: str
    contractType: str


class FilterParams(BaseModel):
    """Вхідні параметри фільтрації активів (Stage1)."""

    min_quote_volume: float = 1_000_000.0
    min_price_change: float = 3.0
    min_open_interest: float = 500_000.0
    min_orderbook_depth: float = 50_000.0
    min_atr_percent: float = 0.5
    max_symbols: int = 30
    dynamic: bool = False
    strict_validation: bool = True

    @validator("*", pre=True)
    def _replace_nan(cls, v: Any) -> Any:
        """Замінює NaN → 0 для float-параметрів (захист під час агрегацій)."""
        return 0 if isinstance(v, float) and np.isnan(v) else v


class MetricResults(BaseModel):
    """Метрики виконання фільтрації — корисно для логування та UI-діагностики."""

    initial_count: int
    prefiltered_count: int
    filtered_count: int
    result_count: int
    elapsed_time: float
    params: Dict[str, Any]

    def dict(self, *args, **kwargs) -> Dict[str, Any]:  # noqa: D401
        """Серіалізує модель у звичайний словник (стисле подання)."""
        return {
            "initial_count": self.initial_count,
            "prefiltered_count": self.prefiltered_count,
            "filtered_count": self.filtered_count,
            "result_count": self.result_count,
            "elapsed_time": self.elapsed_time,
            "params": self.params,
        }


# ──────────────────────────────────────────────────────────────────────────────
# ТАКСОНОМІЯ АКТИВІВ ТА КОНФІГ STAGE2 (QDE)
# ──────────────────────────────────────────────────────────────────────────────
ASSET_CLASS_MAPPING: Dict[str, List[str]] = {
    "spot": [
        ".*BTC.*",
        ".*ETH.*",
        ".*XRP.*",
        ".*LTC.*",
        ".*ADA.*",
        ".*SOL.*",
        ".*DOT.*",
        ".*LINK.*",
    ],
    "futures": [
        ".*BTCUSD.*",
        ".*ETHUSD.*",
        ".*XRPUSD.*",
        ".*LTCUSD.*",
        ".*ADAUSD.*",
        ".*SOLUSD.*",
        ".*DOTUSD.*",
        ".*LINKUSD.*",
    ],
    "meme": [
        ".*DOGE.*",
        ".*SHIB.*",
        ".*PEPE.*",
        ".*FLOKI.*",
        ".*KISHU.*",
        ".*HOGE.*",
        ".*SAITAMA.*",
    ],
    "defi": [
        ".*UNI.*",
        ".*AAVE.*",
        ".*COMP.*",
        ".*MKR.*",
        ".*CRV.*",
        ".*SUSHI.*",
        ".*YFI.*",
        ".*LDO.*",
        ".*RUNE.*",
    ],
    "nft": [".*APE.*", ".*SAND.*", ".*MANA.*", ".*BLUR.*", ".*RARI.*"],
    "metaverse": [".*ENJ.*", ".*AXS.*", ".*GALA.*", ".*ILV.*", ".*HIGH.*"],
    "ai": [".*AGIX.*", ".*FET.*", ".*OCEAN.*", ".*RNDR.*", ".*AKT.*"],
    "stable": [".*USDT$", ".*BUSD$", ".*DAI$", ".*USD$", ".*FDUSD$"],
}

STAGE2_CONFIG: Dict[str, Any] = {
    # Класи активів + їх ваги пріоритезації
    "asset_class_mapping": ASSET_CLASS_MAPPING,
    "priority_levels": {
        "futures": 1.0,
        "spot": 0.7,
        "meme": 0.6,
        "defi": 0.6,
        "nft": 0.5,
        "metaverse": 0.4,
        "ai": 0.3,
        "stable": 0.2,
        "default": 0.1,
    },
    # Волатильність/ATR (відн. до ціни)
    "low_gate": 0.0015,  # 0.15%
    "high_gate": 0.0134,  # 1.34%
    "atr_target": [0.3, 1.5],  # у %
    # Базові індикатори
    "rsi_period": 10,
    "volume_window": 30,
    "atr_period": 10,
    "volume_z_threshold": 1.2,
    "rsi_oversold": 30.0,
    "rsi_overbought": 70.0,
    "stoch_oversold": 20.0,
    "stoch_overbought": 80.0,
    "macd_threshold": 0.02,
    "ema_cross_threshold": 0.005,
    "vwap_threshold": 0.001,
    # Мінімальні вимоги якості сигналів
    "min_volume_usd": 10_000,
    "min_atr_percent": 0.002,
    "exclude_hours": [0, 1, 2, 3],  # умовно «тонкі» години
    "cooldown_period": 300,  # сек
    "max_correlation_threshold": 0.85,
    # Ризик-менеджмент (TP/SL)
    "tp_mult": 3.0,
    "sl_mult": 1.0,
    "min_risk_reward": 2.0,
    "entry_spread_percent": 0.05,
    # Ваги факторів композитного скорингу
    "factor_weights": {
        "volume": 0.25,
        "rsi": 0.18,
        "macd": 0.20,
        "ema_cross": 0.22,
        "stochastic": 0.15,
        "atr": 0.20,
        "orderbook": 0.20,
        "vwap": 0.25,
        "velocity": 0.18,
        "volume_profile": 0.20,
    },
    # Пороги для маршрутизації/доступності
    "min_cluster": 4,
    "min_confidence": 0.75,
    "context_min_correlation": 0.7,
    "max_concurrent": 5,
    # SR / TP сумісність
    "sr_window": 50,
    "tp_buffer_eps": 0.0005,
    # Ваги метрик для калібрування (якщо активуєш Optuna)
    "metric_weights": {
        "sharpe": 0.4,
        "sortino": 0.3,
        "win_rate": 0.3,
        "profit_factor": 0.3,
    },
    "min_trades_for_calibration": 5,
    "n_trials": 20,
    "run_calibration": True,
    # Вимикачі підсистем
    "switches": {
        "use_qde": True,
        "legacy_fallback": False,
        "fallback_when": {"qde_scenario_uncertain": True, "qde_conf_lt": 0.55},
        "analysis": {
            "context": True,
            "anomaly": True,
            "confidence": True,
            "risk": True,
            "recommendation": True,
            "narrative": True,
        },
        "qde_levels": {"micro": True, "meso": True, "macro": True},
        "triggers": {
            "volume_spike": True,
            "breakout": True,
            "volatility_spike": True,
            "rsi": True,
            "vwap_deviation": True,
        },
    },
}

OPTUNA_PARAM_RANGES: Dict[str, tuple] = {
    "volume_z_threshold": (0.5, 3.0),
    "rsi_oversold": (15.0, 40.0),
    "rsi_overbought": (60.0, 85.0),
    "tp_mult": (0.5, 5.0),
    "sl_mult": (0.5, 5.0),
    "min_confidence": (0.3, 0.9),
    "min_score": (1.0, 2.5),
    "min_atr_ratio": (0.0005, 0.01),
    "volatility_ratio": (0.5, 1.2),
    "fallback_confidence": (0.4, 0.7),
    "atr_target": (0.3, 1.5),
    "low_gate": (0.002, 0.015),
    "high_gate": (0.005, 0.03),
}

# ──────────────────────────────────────────────────────────────────────────────
# CACHE / TTL CONFIG (свічки / історія)
# ──────────────────────────────────────────────────────────────────────────────
INTERVAL_TTL_MAP: Dict[str, int] = {
    # ultra-short
    "15s": 30,
    "30s": 45,
    # minutes
    "1m": 90,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    # hours
    "1h": 3900,  # 65 хв — запас поверх базової години (інкременти)
    "2h": 7500,
    "4h": 15000,
    "6h": 22000,
    # days
    "12h": 43000,
    "1d": 26 * 3600,
    "3d": 80 * 3600,
    "1w": 8 * 24 * 3600,
}


# ──────────────────────────────────────────────────────────────────────────────
# APP DEFAULTS / PREFILTER / PRELOAD (перенесено з app/*.py)
# ──────────────────────────────────────────────────────────────────────────────
#: Базові пороги первинного Stage1 prefilter (раніше локальний dict у main.py)
STAGE1_PREFILTER_THRESHOLDS: Dict[str, float | int] = {
    "MIN_QUOTE_VOLUME": 1_000_000.0,  # Мінімальний об'єм торгів USDT
    "MIN_PRICE_CHANGE": 3.0,  # Мінімальна зміна ціни (%)
    "MIN_OPEN_INTEREST": 500_000.0,  # Мінімальний відкритий інтерес USD
    "MAX_SYMBOLS": 350,  # Верхня межа кількості активів у списку
}

#: Початковий (ручний) список символів для швидкого старту (manual mode)
MANUAL_FAST_SYMBOLS_SEED: list[str] = [
    "btcusdt",
    "OMNIUSDT",
    "tonusdt",
    "PUMPUSDT",
]

#: Дефолтні уподобання користувача (UI / генерація описів)
USER_SETTINGS_DEFAULT: dict[str, str] = {
    "lang": "UA",  # Мова інтерфейсу / наративів
    "style": "pro",  # Стиль наративів (pro | explain | short)
}

#: Lookback для первинного preload 1m історії (глибокий холодний старт)
PRELOAD_1M_LOOKBACK_INIT: int = 500
#: Lookback для робочого циклу screening_producer (легкий швидкий контекст)
SCREENING_LOOKBACK: int = 50
#: Кількість денних свічок для глобальних рівнів
PRELOAD_DAILY_DAYS: int = 30

#: TTL fast_symbols у manual режимі
FAST_SYMBOLS_TTL_MANUAL: int = 3600
#: TTL fast_symbols у auto режимі (prefilter)
FAST_SYMBOLS_TTL_AUTO: int = 600
#: Інтервал виконання періодичного prefilter (сек)
PREFILTER_INTERVAL_SEC: int = 600

#: TTL (у днях) для кешованих порогів Thresholds (було CACHE_TTL_DAYS у thresholds.py)
CACHE_TTL_DAYS: int = 14
#: Шлях до Optuna SQLite (було OPTUNA_SQLITE_URI у thresholds.py)
OPTUNA_SQLITE_URI: str = "sqlite:///storage/optuna.db"

#: Тимчасовий плейсхолдер (раніше MIN_CONFIDENCE_TRADE у screening_producer.py)
MIN_CONFIDENCE_TRADE_PLACEHOLDER: float = (
    0.5  # може бути видалено якщо не використовується
)

# ──────────────────────────────────────────────────────────────────────────────
# STAGE1 MONITOR (AssetMonitorStage1) ПАРАМЕТРИ
# ──────────────────────────────────────────────────────────────────────────────
STAGE1_MONITOR_PARAMS: dict[str, float | int] = {
    "vol_z_threshold": 2.5,  # Поріг сплеску обсягу (Z-score)
    "rsi_overbought": 70.0,  # RSI перекупленості
    "rsi_oversold": 30.0,  # RSI перепроданості
    "min_reasons_for_alert": 2,  # Мін. кількість тригерів для ALERT
    "dynamic_rsi_multiplier": 1.1,  # Множник динамічного RSI
}

# ──────────────────────────────────────────────────────────────────────────────
# PREFILTER (OptimizedAssetFilter) ДЕФОЛТНІ ДОД. ПАРАМЕТРИ
# ──────────────────────────────────────────────────────────────────────────────
PREFILTER_BASE_PARAMS: dict[str, float | int | bool] = {
    "min_depth": 50_000.0,
    "min_atr": 0.5,
    "dynamic": False,
}


# ──────────────────────────────────────────────────────────────────────────────
# ЕКСПОРТ СИМВОЛІВ МОДУЛЯ (публічний API)
# ──────────────────────────────────────────────────────────────────────────────
__all__ = [
    # Базові константи
    "DEFAULT_LOOKBACK",
    "DEFAULT_TIMEFRAME",
    "MIN_READY_PCT",
    "MAX_PARALLEL_STAGE2",
    "TRADE_REFRESH_INTERVAL",
    # Сигнали/рекомендації
    "BUY_SET",
    "SELL_SET",
    # Redis
    "REDIS_CHANNEL_ASSET_STATE",
    "REDIS_SNAPSHOT_KEY",
    # UI
    "UI_LOCALE",
    "UI_COLUMN_VOLUME",
    "TICK_SIZE_MAP",
    "TICK_SIZE_BRACKETS",
    "TICK_SIZE_DEFAULT",
    "STAGE2_STATUS",
    "ASSET_STATE",
    # Тригери
    "TRIGGER_TP_SL_SWAP_LONG",
    "TRIGGER_TP_SL_SWAP_SHORT",
    "TRIGGER_SIGNAL_GENERATED",
    "TRIGGER_NAME_MAP",
    # Семафори/кеш
    "OI_SEMAPHORE",
    "KLINES_SEMAPHORE",
    "DEPTH_SEMAPHORE",
    "REDIS_CACHE_TTL",
    # Cache maps
    "INTERVAL_TTL_MAP",
    # Моделі
    "AssetFilterError",
    "SymbolInfo",
    "FilterParams",
    "MetricResults",
    # Stage2/QDE
    "ASSET_CLASS_MAPPING",
    "STAGE2_CONFIG",
    "OPTUNA_PARAM_RANGES",
    # App defaults / seeds
    "STAGE1_PREFILTER_THRESHOLDS",
    "MANUAL_FAST_SYMBOLS_SEED",
    "USER_SETTINGS_DEFAULT",
    "PRELOAD_1M_LOOKBACK_INIT",
    "SCREENING_LOOKBACK",
    "PRELOAD_DAILY_DAYS",
    "FAST_SYMBOLS_TTL_MANUAL",
    "FAST_SYMBOLS_TTL_AUTO",
    "PREFILTER_INTERVAL_SEC",
    "CACHE_TTL_DAYS",
    "OPTUNA_SQLITE_URI",
    "MIN_CONFIDENCE_TRADE_PLACEHOLDER",
    "STAGE1_MONITOR_PARAMS",
    "PREFILTER_BASE_PARAMS",
]
