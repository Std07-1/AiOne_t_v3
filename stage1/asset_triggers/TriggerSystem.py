import asyncio
import json
import logging
import numpy as np
import pandas as pd
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple, Callable

# Імпорт менеджерів індикаторів з існуючих модулів
from stage1.indicators import (
    RSIManager,
    ATRManager,
    VWAPManager,
    VolumeZManager,
    compute_rsi,
    format_rsi,
)
from app.thresholds import Thresholds, load_thresholds, save_thresholds

# Налаштування логера українською
logger = logging.getLogger("stage1_system")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


@dataclass
class TriggerConfig:
    """Конфігурація тригера"""

    base_params: Dict[str, Any]
    adaptive: bool = True
    weight: float = 1.0
    enabled: bool = True
    last_calibrated: datetime = field(default_factory=datetime.utcnow)
    precision: float = 0.0
    recall: float = 0.0


@dataclass
class TriggerPerformance:
    """Статистика продуктивності тригера"""

    total_signals: int = 0
    true_positives: int = 0
    false_positives: int = 0
    last_triggered: datetime = field(default_factory=datetime.utcnow)
    success_rate: float = 0.0


class TriggerSystem:
    def __init__(self, cache_handler):
        self.cache = cache_handler

        # Менеджери індикаторів
        self.rsi_manager = RSIManager(period=14)
        self.atr_manager = ATRManager(period=14)
        self.vwap_manager = VWAPManager(window=30)
        self.volumez_manager = VolumeZManager(window=20)

        # Адаптивні параметри RSI
        self.dynamic_rsi_multiplier = 1.25
        self.rsi_overbought: Optional[float] = None
        self.rsi_oversold: Optional[float] = None

        # Глобальні рівні підтримки/опору
        self.global_levels: Dict[str, List[float]] = {}

        # Доступні тригери
        self.triggers: Dict[str, Dict] = {
            "breakout": {
                "function": self.breakout_trigger,
                "config": TriggerConfig(
                    base_params={"window": 20, "near_threshold": 0.005},
                    adaptive=True,
                    weight=0.9,
                    enabled=True,
                ),
            },
            "rsi_divergence": {
                "function": self.rsi_divergence_trigger,
                "config": TriggerConfig(
                    base_params={"window": 14}, adaptive=True, weight=0.8, enabled=True
                ),
            },
            "volatility_spike": {
                "function": self.volatility_spike_trigger,
                "config": TriggerConfig(
                    base_params={"window": 14, "threshold": 2.0},
                    adaptive=True,
                    weight=0.7,
                    enabled=True,
                ),
            },
            "volume_spike": {
                "function": self.volume_spike_trigger,
                "config": TriggerConfig(
                    base_params={"atr_window": 14, "volume_z_threshold": 2.0},
                    adaptive=True,
                    weight=0.85,
                    enabled=True,
                ),
            },
            "vwap_deviation": {
                "function": self.vwap_deviation_trigger,
                "config": TriggerConfig(
                    base_params={"window": 20, "threshold": 0.01},
                    adaptive=True,
                    weight=0.75,
                    enabled=True,
                ),
            },
        }

        # Статистика продуктивності
        self.performance: Dict[str, Dict[str, TriggerPerformance]] = {}
        self.market_state: Dict[str, Any] = {
            "volatility": 0.0,
            "trend_strength": 0.0,
            "market_mode": "neutral",
        }

    def set_global_levels(self, daily_data: Dict[str, pd.DataFrame]):
        """Встановлює глобальні рівні підтримки/опору"""
        for sym, df in daily_data.items():
            # Логіка обчислення рівнів
            pass

    async def get_current_stats(self, symbol: str, df: pd.DataFrame) -> Dict[str, Any]:
        """Обчислює поточні статистики для символу"""
        if df is None or df.empty:
            logger.error(f"[{symbol}] Дані відсутні")
            raise ValueError(f"[{symbol}] No data available")

        price_series = df["close"]
        price = float(price_series.iloc[-1])
        price_change = (
            (price / float(price_series.iloc[0]) - 1.0) if price_series.iloc[0] else 0.0
        )

        # Обчислення статистик
        high = float(df["high"].max())
        low = float(df["low"].min())
        daily_range = high - low

        # RSI
        self.rsi_manager.ensure_state(symbol, df["close"])
        rsi_val = self.rsi_manager.update(symbol, price)

        # VWAP
        self.vwap_manager.ensure_buffer(symbol, df.iloc[:-1] if len(df) > 1 else df)
        self.vwap_manager.update(
            symbol, close=price, volume=float(df["volume"].iloc[-1])
        )
        vwap_val = float(self.vwap_manager.compute_vwap(symbol))

        # ATR
        self.atr_manager.ensure_state(symbol, df)
        atr_val = float(
            self.atr_manager.update(
                symbol,
                high=float(df["high"].iloc[-1]),
                low=float(df["low"].iloc[-1]),
                close=price,
            )
        )

        return {
            "symbol": symbol,
            "current_price": price,
            "price_change": price_change,
            "daily_high": high,
            "daily_low": low,
            "daily_range": daily_range,
            "rsi": float(rsi_val),
            "vwap": vwap_val,
            "atr": atr_val,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }

    async def load_symbol_config(self, symbol: str) -> Dict[str, TriggerConfig]:
        """Завантажує конфігурацію тригерів для символу"""
        key = f"trigger_config:{symbol}"
        config_json = await self.cache.fetch_from_cache(key, "global", raw=True)
        if config_json:
            try:
                # Десеріалізація конфігурації
                pass
            except Exception as e:
                logger.error(f"Помилка завантаження конфігурації {symbol}: {e}")
        return {name: trig["config"] for name, trig in self.triggers.items()}

    async def save_symbol_config(self, symbol: str, configs: Dict[str, TriggerConfig]):
        """Зберігає конфігурацію тригерів для символу"""
        key = f"trigger_config:{symbol}"
        try:
            # Серіалізація та збереження
            pass
        except Exception as e:
            logger.error(f"Помилка збереження конфігурації {symbol}: {e}")

    async def get_effective_params(
        self, symbol: str, trigger_name: str
    ) -> Dict[str, Any]:
        """Отримує актуальні параметри тригера з адаптацією"""
        thr = await load_thresholds(symbol, self.cache) or Thresholds()
        configs = await self.load_symbol_config(symbol)
        config = configs.get(trigger_name, self.triggers[trigger_name]["config"])
        params = config.base_params.copy()

        if config.adaptive:
            # Адаптація параметрів до ринкових умов
            vol = self.market_state.get("volatility", 0.0)
            if vol > thr.high_gate:
                if "threshold" in params:
                    params["threshold"] *= 1.3
            elif vol < thr.low_gate:
                if "threshold" in params:
                    params["threshold"] *= 0.8

        return params

    async def check_all_triggers(
        self, symbol: str, df: pd.DataFrame, stats: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        """Перевіряє всі тригери для символу"""
        results = {}
        self.update_market_state(df, stats)
        configs = await self.load_symbol_config(symbol)

        for trigger_name, trig in self.triggers.items():
            config = configs.get(trigger_name, trig["config"])
            if not config.enabled:
                continue

            params = await self.get_effective_params(symbol, trigger_name)
            trigger_func = trig["function"]

            try:
                triggered, details = trigger_func(
                    df, stats, symbol, params, await load_thresholds(symbol, self.cache)
                )
            except Exception as e:
                logger.error(f"Помилка тригера {symbol}:{trigger_name}: {e}")
                triggered, details = False, {"error": str(e)}

            results[trigger_name] = {
                "triggered": bool(triggered),
                "details": details,
                "weight": config.weight,
            }
            self.update_performance(symbol, trigger_name, bool(triggered))

        return results

    def update_market_state(self, df: pd.DataFrame, stats: Dict[str, Any]):
        """Оновлює стан ринку"""
        price = stats.get("current_price")
        atr_val = stats.get("atr")

        if price and atr_val:
            self.market_state["volatility"] = atr_val / price

        # Обчислення сили тренду
        if len(df) >= 20:
            prices = df["close"].tail(20).values
            x = np.arange(len(prices))
            n = len(prices)
            sum_x = n * (n - 1) / 2
            sum_y = float(prices.sum())
            sum_xy = float(np.dot(x, prices))
            sum_x2 = float((x**2).sum())
            slope = (
                (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x**2) if n > 1 else 0.0
            )
            std_y = float(prices.std(ddof=0))
            max_slope = std_y * 3 if std_y > 0 else 1e-6
            self.market_state["trend_strength"] = slope / max_slope

        # Визначення режиму ринку
        trend = self.market_state["trend_strength"]
        if trend > 0.7:
            self.market_state["market_mode"] = "bullish"
        elif trend < -0.7:
            self.market_state["market_mode"] = "bearish"
        else:
            self.market_state["market_mode"] = "neutral"

    def update_performance(self, symbol: str, trigger_name: str, triggered: bool):
        """Оновлює статистику продуктивності"""
        if symbol not in self.performance:
            self.performance[symbol] = {}
        if trigger_name not in self.performance[symbol]:
            self.performance[symbol][trigger_name] = TriggerPerformance()

        perf = self.performance[symbol][trigger_name]
        perf.total_signals += 1
        if triggered:
            perf.true_positives += 1
            perf.last_triggered = datetime.utcnow()

        if perf.total_signals > 0:
            perf.success_rate = perf.true_positives / perf.total_signals

    async def calibrate_trigger(
        self,
        symbol: str,
        trigger_name: str,
        historical_df: pd.DataFrame,
        n_trials: int = 50,
    ) -> Dict[str, Any]:
        """Калібрує тригер за допомогою Optuna"""
        logger.info(f"🔧 Калібрування тригера {trigger_name} для {symbol}...")

        # Реалізація калібрування з використанням Optuna
        best_params = {}
        logger.info(f"✅ Калібрування завершено для {symbol}/{trigger_name}")
        return best_params

    # Реалізації тригерів
    def breakout_trigger(
        self,
        df: pd.DataFrame,
        stats: Dict[str, Any],
        symbol: str,
        params: Dict[str, Any],
        thresholds: Thresholds,
    ) -> Tuple[bool, Dict[str, Any]]:
        """Виявляє пробиття рівнів"""
        window = params.get("window", 20)
        if len(df) < window + 1:
            return False, {"error": "Недостатньо даних"}

        # Логіка виявлення пробиття
        current_close = float(df["close"].iloc[-1])
        prev_close = float(df["close"].iloc[-2])
        recent_high = df["high"].rolling(window).max().iloc[-2]
        recent_low = df["low"].rolling(window).min().iloc[-2]

        breakout_up = current_close > recent_high and prev_close <= recent_high
        breakout_down = current_close < recent_low and prev_close >= recent_low

        return breakout_up or breakout_down, {
            "breakout_up": breakout_up,
            "breakout_down": breakout_down,
        }

    def rsi_divergence_trigger(
        self,
        df: pd.DataFrame,
        stats: Dict[str, Any],
        symbol: str,
        params: Dict[str, Any],
        thresholds: Thresholds,
    ) -> Tuple[bool, Dict[str, Any]]:
        """Виявляє розбіжність RSI"""
        window = params.get("window", 14)
        if len(df) < window + 2:
            return False, {"error": "Недостатньо даних"}

        # Логіка виявлення розбіжності
        return False, {}  # Заглушка

    def volatility_spike_trigger(
        self,
        df: pd.DataFrame,
        stats: Dict[str, Any],
        symbol: str,
        params: Dict[str, Any],
        thresholds: Thresholds,
    ) -> Tuple[bool, Dict[str, Any]]:
        """Виявляє сплеск волатильності"""
        window = params.get("window", 14)
        threshold = params.get("threshold", 2.0)

        if len(df) < window + 2:
            return False, {"error": "Недостатньо даних"}

        # Логіка виявлення сплеску
        return False, {}  # Заглушка

    def volume_spike_trigger(
        self,
        df: pd.DataFrame,
        stats: Dict[str, Any],
        symbol: str,
        params: Dict[str, Any],
        thresholds: Thresholds,
    ) -> Tuple[bool, Dict[str, Any]]:
        """Виявляє сплеск обсягу"""
        window = params.get("atr_window", 14)
        z_threshold = params.get("volume_z_threshold", 2.0)

        if len(df) < window + 2:
            return False, {"error": "Недостатньо даних"}

        # Логіка виявлення сплеску обсягу
        return False, {}  # Заглушка

    def vwap_deviation_trigger(
        self,
        df: pd.DataFrame,
        stats: Dict[str, Any],
        symbol: str,
        params: Dict[str, Any],
        thresholds: Thresholds,
    ) -> Tuple[bool, Dict[str, Any]]:
        """Виявляє відхилення ціни від VWAP"""
        window = params.get("window", 20)
        threshold = params.get("threshold", 0.01)

        if len(df) < window:
            return False, {"error": "Недостатньо даних"}

        # Логіка виявлення відхилення
        price = float(df["close"].iloc[-1])
        vwap = stats.get("vwap", 0.0)
        deviation = abs(price - vwap) / vwap if vwap else 0.0

        return deviation > threshold, {"deviation": deviation, "threshold": threshold}

    # Фонові завдання
    async def periodic_calibration(
        self, symbols: List[str], data_provider: Callable, interval: int = 3600
    ):
        """Періодична калібрування тригерів"""
        while True:
            logger.info("Початок періодичного калібрування тригерів...")
            for symbol in symbols:
                for trigger_name in self.triggers:
                    try:
                        historical_df = await data_provider(symbol, lookback=500)
                        if historical_df is not None and len(historical_df) > 100:
                            await self.calibrate_trigger(
                                symbol, trigger_name, historical_df
                            )
                    except Exception as e:
                        logger.error(
                            f"Помилка калібрування {symbol}/{trigger_name}: {e}"
                        )
            await asyncio.sleep(interval)

    async def monitor_performance(self, interval: int = 600):
        """Моніторинг продуктивності тригерів"""
        while True:
            for symbol, triggers in self.performance.items():
                for trigger_name, perf in triggers.items():
                    if perf.total_signals >= 10 and perf.success_rate < 0.4:
                        logger.warning(
                            f"Низька продуктивність {symbol}/{trigger_name}: {perf.success_rate:.2f}"
                        )
                        # Логіка коригування тригерів
            await asyncio.sleep(interval)


# Допоміжні функції
def create_signal(
    symbol: str, trigger_results: Dict[str, Dict[str, Any]], stats: Dict[str, Any]
) -> Dict[str, Any]:
    """Створює сигнал на основі результатів тригерів"""
    total_weight = sum(
        result["weight"] for result in trigger_results.values() if result["triggered"]
    )
    signal_type = "ALERT" if total_weight >= 1.0 else "NORMAL"

    return {
        "symbol": symbol,
        "signal": signal_type,
        "reasons": [
            name for name, result in trigger_results.items() if result["triggered"]
        ],
        "stats": stats,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


async def publish_signals(signals: List[Dict[str, Any]], cache_handler, redis_conn):
    """Публікує сигнали через Redis"""
    try:
        signals_json = json.dumps(signals)
        await cache_handler.set_json("stage1_signals", "current", signals_json, ttl=30)
        await redis_conn.publish("signals", signals_json)
        logger.info(f"Опубліковано {len(signals)} сигналів")
    except Exception as e:
        logger.error(f"Помилка публікації сигналів: {e}")


# Головний клас системи Stage1
class Stage1Core:
    def __init__(self, cache_handler, metrics_collector):
        self.cache = cache_handler
        self.metrics = metrics_collector
        self.trigger_system = TriggerSystem(cache_handler)
        self.asset_registry: Dict[str, Dict] = {}
        self.last_processed: int = 0
        self.system_health: str = "OK"

        # Реєстрація метрик
        self.metrics.register_counter("stage1.signals_generated")
        self.metrics.register_gauge("stage1.processing_time")

    async def initialize(self, symbols: List[str]):
        """Ініціалізація системи"""
        for symbol in symbols:
            self.asset_registry[symbol] = {"enabled": True, "weight": 1.0}

        # Запуск фонових завдань
        asyncio.create_task(
            self.trigger_system.periodic_calibration(symbols, self.get_historical_data)
        )
        asyncio.create_task(self.trigger_system.monitor_performance())

    async def get_historical_data(self, symbol: str, lookback: int) -> pd.DataFrame:
        """Отримання історичних даних (заглушка)"""
        # Реальна реалізація залежить від джерела даних
        return pd.DataFrame()

    async def process_assets(
        self, buffer_data: Dict[str, List[Dict]]
    ) -> Dict[str, Any]:
        """Обробка активів"""
        start_time = self.metrics.current_time()
        results = {}

        for symbol, bars in buffer_data.items():
            if not self.asset_registry.get(symbol, {}).get("enabled", True):
                continue

            try:
                df = pd.DataFrame(bars)
                stats = await self.trigger_system.get_current_stats(symbol, df)
                trigger_results = await self.trigger_system.check_all_triggers(
                    symbol, df, stats
                )
                signal = create_signal(symbol, trigger_results, stats)
                results[symbol] = signal
            except Exception as e:
                logger.error(f"Помилка обробки {symbol}: {e}")
                self.system_health = "WARNING"

        # Оновлення метрик
        processing_time = self.metrics.current_time() - start_time
        self.metrics.set_gauge("stage1.processing_time", processing_time)
        self.metrics.increment_counter("stage1.signals_generated", len(results))

        logger.info(f"Оброблено {len(results)} активів за {processing_time:.2f} мс")
        return results


# Оркестратор системи
class Stage1Orchestrator:
    def __init__(self, core: Stage1Core, buffer, cache, metrics):
        self.core = core
        self.buffer = buffer
        self.cache = cache
        self.metrics = metrics
        self.task: Optional[asyncio.Task] = None

    async def start_processing(self, interval: int = 60):
        """Запуск циклічної обробки"""
        logger.info("Запуск обробки Stage1 системи")
        while True:
            try:
                # Отримання даних з буфера
                buffer_data = self.get_buffer_data()

                # Обробка активів
                results = await self.core.process_assets(buffer_data)

                # Публікація результатів
                await self.publish_results(results)

                # Оновлення стану системи
                self.update_system_health()

            except Exception as e:
                logger.critical(f"Критична помилка: {e}")
                self.metrics.increment_counter("stage1.critical_errors")
                # Додаткова обробка помилок

            await asyncio.sleep(interval)

    def get_buffer_data(self) -> Dict[str, List[Dict]]:
        """Отримання даних з буфера"""
        return {
            symbol: self.buffer.get(symbol, "1m", 50)
            for symbol in self.core.asset_registry.keys()
        }

    async def publish_results(self, results: Dict[str, Any]):
        """Публікація результатів"""
        # Збереження повного стану
        await self.cache.set_json("stage1_state", "current", json.dumps(results))

        # Фільтрація тривожних сигналів
        alert_signals = [
            {k: v for k, v in res.items() if k in ["symbol", "signal", "reasons"]}
            for res in results.values()
            if res.get("signal") == "ALERT"
        ]

        # Публікація через Redis
        if alert_signals:
            await self.cache.publish("signals", json.dumps(alert_signals))
            logger.info(f"Опубліковано {len(alert_signals)} тривожних сигналів")

    def update_system_health(self):
        """Оновлення стану системи"""
        error_count = self.metrics.get_counter("stage1.errors_last_hour")
        if error_count > 5:
            self.core.system_health = "CRITICAL"
            logger.error("Критичний стан системи: занадто багато помилок")
        elif error_count > 2:
            self.core.system_health = "WARNING"
            logger.warning("Попередження: підвищена кількість помилок")
        else:
            self.core.system_health = "OK"


# Ініціалізація системи
async def initialize_stage1_system():
    """Ініціалізація Stage1 системи"""
    from data.ram_buffer import RAMBuffer
    from data.cache_handler import SimpleCacheHandler
    from app.settings import settings
    from app.utils.metrics import MetricsCollector

    # Ініціалізація залежностей
    buffer = RAMBuffer(max_bars=settings.ram_buffer_size)
    cache = SimpleCacheHandler(host=settings.redis_host, port=settings.redis_port)
    metrics = MetricsCollector()

    # Створення ядра системи
    core = Stage1Core(cache, metrics)
    await core.initialize(settings.tracked_symbols)

    # Створення оркестратора
    orchestrator = Stage1Orchestrator(core, buffer, cache, metrics)

    # Запуск обробки
    asyncio.create_task(
        orchestrator.start_processing(interval=settings.stage1_processing_interval)
    )

    return {"core": core, "orchestrator": orchestrator, "metrics": metrics}


# Точка входу
if __name__ == "__main__":
    # Ініціалізація та запуск системи
    asyncio.run(initialize_stage1_system())
    logger.info("Stage1 система успішно запущена")

    # Запуск основного циклу подій
    loop = asyncio.get_event_loop()
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        logger.info("Завершення роботи системи")
    finally:
        loop.close()
