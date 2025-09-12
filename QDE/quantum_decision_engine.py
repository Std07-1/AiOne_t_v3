# QDE\quantum_decision_engine.py

# -*- coding: utf-8 -*-
"""
QuantumDecisionEngine — модуль багаторівневого аналізу для Stage2 AiOne_t.

- Безпечний до вхідних даних (sanitization, _safe/_nz).
- 7 сценаріїв + UNCERTAIN, динамічні ваги Micro/Meso/Macro.
- Інтеграція класів активів (meme/ai/defi/futures/spot/stable/default).
- Підтримка аномалій (triggers) для MANIPULATED/HIGH_VOLATILITY.
- Сумісний із recommendation_engine (контракт виходу стабільний).

PEP8, type hints, Google-style docstrings.

Сценарії, які визначає Engine:

BULLISH_BREAKOUT — пробій із силою тренду, позитивним макро/сектором, підтвердженим обсягом та тиском над VWAP.

BEARISH_REVERSAL — шортова перевага: шок ціни + розріджена ліквідність + слабкий тренд + слабкий сектор.

RANGE_BOUND — вузька вола, низька імпульсність, невиражений тренд; пріоритет торгівлі від меж діапазону.

BULLISH_CONTROL — покупець «контролює» (низький спред-gap, тренд, якісні рівні, сектор).

BEARISH_CONTROL — продавець контролює (ліквідність рвана, слабкий тренд проти/сектор, негативний сентимент).

HIGH_VOLATILITY — режим підвищеної волатильності: шок волатильності + зсуви кореляцій/сектора.

MANIPULATED — підозра на маніпулятивність (volume_spike, тонка книга, різкі кореляційні зсуви, whip над/під VWAP).

Примітка: UNCERTAIN повертається як fallback при недостатній перевазі одного сценарію.
"""

import logging
import math
import re
from typing import Dict, Any, List, Tuple, Deque, Optional
from collections import deque
import numpy as np

from stage2.config import STAGE2_CONFIG, ASSET_CLASS_MAPPING

try:
    from stage2.calibration_queue import AssetClassConfig

    _HAS_ASSET_CLASSES = True
except Exception:  # noqa: BLE001
    _HAS_ASSET_CLASSES = False
    ASSET_CLASS_MAPPING = {
        "spot": [".*USDT$", ".*USD$"],
        "futures": [".*PERP$", ".*USD[TP]$"],
        "meme": [
            ".*DOGE.*",
            ".*SHIB.*",
            ".*PEPE.*",
            ".*FLOKI.*",
            ".*BONK.*",
            ".*WIF.*",
        ],
        "ai": [".*FET.*", ".*RNDR.*", ".*AGIX.*", ".*AKT.*"],
        "defi": [".*UNI.*", ".*AAVE.*", ".*COMP.*", ".*MKR.*", ".*CRV.*", ".*SUSHI.*"],
        "stable": [".*USDT$", ".*USDC$", ".*BUSD$", ".*DAI$"],
        "default": [".*"],
    }

logger = logging.getLogger("stage2.quantum_decision_engine")
logger.setLevel(logging.INFO)
# Додаємо RichHandler для красивого логування (якщо потрібно)
if not logger.handlers:
    from rich.console import Console
    from rich.logging import RichHandler

    handler = RichHandler(console=Console(stderr=True), show_path=False)
    logger.addHandler(handler)
logger.propagate = False


def _safe(val, default=0.0):
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except Exception:  # noqa: BLE001
        return default


def _nz(val, default=0.0):
    v = _safe(val, default)
    return default if v == 0 or v is None else v


class QuantumDecisionEngine:
    """
    Квантовий рушій прийняття рішень для аналізу ринку.
    Інкапсулює багаторівневий аналіз (мікро/мезо/макро), динамічне зважування, класифікацію активу та формування сценарію.

    Args:
        symbol: Торговий інструмент.
        normal_state: Базові норми волатильності/спреду/кореляції.
        historical_window: Розмір вікна історії для slope/медіани спреду.
        asset_class_config: Зовнішній конфіг маппінгу класів активів (опційно).

    Example:
        >>> qde = QuantumDecisionEngine("BTCUSDT", {"volatility": 0.01, "spread": 0.0008, "correlation": 0.5})
        >>> out = qde.quantum_decision({"current_price": 65000, "rsi": 58, "volume_z": 1.9})
        >>> out["scenario"], out["confidence"]
        ('BULLISH_BREAKOUT', 0.62)
    """

    def __init__(
        self,
        symbol: str,
        normal_state: Dict[str, Any] | None = None,
        historical_window: int = 60,
        asset_class_config: Optional[AssetClassConfig] = None,
        **kwargs: Any,
    ):
        self.symbol = symbol
        logger.info(f"[QDE] Ініціалізація рушія для {symbol}")
        # Зберігаємо нормальні стани ринку для порівняння
        self.normal_state = {
            "volatility": _safe((normal_state or {}).get("volatility"), 0.01),
            "spread": _safe((normal_state or {}).get("spread"), 0.0008),
            "correlation": _safe((normal_state or {}).get("correlation"), 0.5),
        }
        self.historical_window = max(
            20, int(historical_window)
        )  # Вікно історії для аналізу
        self.price_history: Deque[float] = deque(
            maxlen=self.historical_window
        )  # Історія цін
        self.volume_history: Deque[float] = deque(
            maxlen=self.historical_window
        )  # Історія обсягів
        self.spread_history: Deque[float] = deque(
            maxlen=self.historical_window
        )  # Історія спредів
        # Конфігурація Stage2
        self.config = STAGE2_CONFIG
        # self.asset_class_config = kwargs.get("asset_class_config")
        # self.asset_class = kwargs.get("asset_class")

        # Класи активів
        if asset_class_config:
            self.asset_class_config = (
                asset_class_config  # Використовуємо зовнішній конфіг
            )
        else:
            if _HAS_ASSET_CLASSES:
                self.asset_class_config = AssetClassConfig(
                    ASSET_CLASS_MAPPING
                )  # Використовуємо маппінг класів активів
            else:
                self.asset_class_config = None  # Використовуємо стандартну конфігурацію
        self.asset_class = self._identify_asset_class()  # Визначаємо клас активу
        logger.info(f"[QDE] Клас активу для {symbol}: {self.asset_class}")

    # -------- ПУБЛІЧНИЙ ІНТЕРФЕЙС --------

    def update_history(self, current_stats: Dict[str, Any]):
        """
        Оновлює історію цін, обсягів та спреду для подальшого аналізу.
        """
        logger.debug(
            f"[QDE] Оновлення історії для {self.symbol}: ціна={current_stats.get('current_price')}, обсяг={current_stats.get('volume')}"
        )
        self.price_history.append(_safe(current_stats.get("current_price"), 0.0))
        self.volume_history.append(_safe(current_stats.get("volume"), 0.0))
        self.spread_history.append(
            _safe(current_stats.get("bid_ask_spread"), self.normal_state["spread"])
        )

    def quantum_decision(
        self, current_stats: Dict[str, Any], trigger_reasons: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Основний метод: формує рішення на основі багаторівневого аналізу та динамічного зважування.

        Args:
            current_stats: Вхідний словник метрик (див. _sanitize_stats).
            trigger_reasons: Список аномалій/подій (наприклад, "volume_spike").

        Returns:
            Словник результату для recommendation_engine.
        """
        logger.info(f"[QDE] Запуск прийняття рішення для {self.symbol}")
        stats = self._sanitize_stats(current_stats)
        logger.debug(f"[QDE] Санітизовані дані: {stats}")
        self.update_history(stats)
        micro = self.analyze_micro_level(stats, trigger_reasons or [])
        logger.debug(f"[QDE] Мікро-рівень: {micro}")
        meso = self.analyze_meso_level(stats)
        logger.debug(f"[QDE] Мезо-рівень: {meso}")
        macro = self.analyze_macro_level(stats)
        logger.debug(f"[QDE] Макро-рівень: {macro}")
        weights = self._calculate_dynamic_weights(micro, meso, macro)
        logger.debug(f"[QDE] Динамічні ваги: {weights}")

        # Матриця рішень
        decision_matrix = {
            "BULLISH_BREAKOUT": self._calc_breakout_score(micro, meso, macro, weights),
            "BEARISH_REVERSAL": self._calc_reversal_score(micro, meso, macro, weights),
            "RANGE_BOUND": self._calc_range_score(micro, meso, macro, weights),
            "BULLISH_CONTROL": self._calc_bullish_control_score(
                micro, meso, macro, weights
            ),
            "BEARISH_CONTROL": self._calc_bearish_control_score(
                micro, meso, macro, weights
            ),
            "HIGH_VOLATILITY": self._calc_high_volatility_score(
                micro, meso, macro, weights
            ),
            "MANIPULATED": self._calc_manipulated_score(
                micro, meso, macro, weights, trigger_reasons or []
            ),
        }
        scenario, confidence = self._select_scenario(decision_matrix, weights)
        logger.info(f"[QDE] Обраний сценарій: {scenario}, впевненість: {confidence}")
        result = {
            "symbol": self.symbol,
            "scenario": scenario,
            "confidence": confidence,
            "decision_matrix": decision_matrix,
            "weights": weights,
            "micro": micro,
            "meso": meso,
            "macro": macro,
            "asset_class": self.asset_class,
        }

        logger.debug(f"[QDE] Підсумковий результат: {result}")
        return result

    # -------- РІВНІ АНАЛІЗУ --------

    def analyze_micro_level(
        self, s: Dict[str, Any], triggers: List[str]
    ) -> Dict[str, float]:
        """
        Аналізує короткострокові (мікро) фактори: шоки ціни, аномалії обсягу, ліквідність, тиск VWAP.
        """
        logger.debug(f"[QDE] Аналіз мікро-рівня для {self.symbol}")
        last_price = (
            self.price_history[-2]
            if len(self.price_history) > 1
            else s["current_price"]
        )
        price_change = abs(s["current_price"] - last_price) / max(1e-9, last_price)
        price_shock = min(
            price_change / max(1e-6, self.normal_state["volatility"]), 3.0
        )
        volume_z = _safe(s.get("volume_z"), 0.0)
        vol_thr = _safe(
            s.get("calibrated_vol_z"), self.config.get("volume_z_threshold", 1.2)
        )
        volume_anomaly = max(0.0, min(volume_z / max(1e-6, vol_thr * 2.5), 1.0))
        spread = _safe(s.get("bid_ask_spread"), self.normal_state["spread"])
        med_spread = (
            float(np.median(self.spread_history))
            if len(self.spread_history) >= 5
            else self.normal_state["spread"]
        )
        liquidity_gap = 1.0 if spread > max(1e-9, med_spread * 2.0) else 0.0
        vwap = _safe(s.get("vwap"), s["current_price"])
        vwap_dev = abs(s["current_price"] - vwap) / max(1e-9, vwap)
        vwap_pressure = min(
            vwap_dev / max(1e-6, self.config.get("vwap_threshold", 0.001)), 2.0
        )
        return {
            "price_shock": price_shock,
            "volume_anomaly": volume_anomaly,
            "liquidity_gap": liquidity_gap,
            "vwap_pressure": vwap_pressure,
        }

    def analyze_meso_level(self, s: Dict[str, Any]) -> Dict[str, float]:
        """
        Аналізує середньострокові (мезо) фактори: тренд, якість рівнів, breadth ринку.
        """
        logger.debug(f"[QDE] Аналіз мезо-рівня для {self.symbol}")
        rsi = _safe(s.get("rsi"), 50.0)
        rsi_strength = self._normalize(rsi, 30, 70)
        adx = _safe(s.get("adx"), 20.0)
        adx_strength = self._normalize(adx, 18, 35)
        volume_strength = min(max(_safe(s.get("volume_z"), 0.0) / 2.5, 0.0), 1.0)
        trend_slope = self._price_slope()
        trend_strength = (
            0.35 * rsi_strength
            + 0.25 * adx_strength
            + 0.25 * volume_strength
            + 0.15 * trend_slope
        )
        levels_quality = self._assess_levels_quality(s)
        market_breath = _safe(s.get("market_breath"), 0.5)
        return {
            "trend_strength": max(0.0, min(1.0, trend_strength)),
            "levels_quality": levels_quality,
            "market_breath": market_breath,
        }

    def analyze_macro_level(self, s: Dict[str, Any]) -> Dict[str, float]:
        """
        Аналізує макро-фактори: здоров'я сектору, кореляції, макро-сентимент.
        """
        logger.debug(f"[QDE] Аналіз макро-рівня для {self.symbol}")
        sector = self.asset_class or "default"
        sector_health = _safe((s.get("sector_health") or {}).get(sector), 0.5)
        correlation_shifts = self._detect_correlation_changes(s)
        macro_sentiment = _safe(s.get("sentiment_index"), 0.0)
        return {
            "sector_health": sector_health,
            "correlation_shifts": correlation_shifts,
            "macro_sentiment": macro_sentiment,
        }

    # -------- ВАГИ, РЕЙТИНГ, SCORING --------

    def _calculate_dynamic_weights(
        self, micro: Dict[str, float], meso: Dict[str, float], macro: Dict[str, float]
    ) -> Dict[str, float]:
        """
        Динамічно визначає ваги для кожного рівня аналізу залежно від волатильності та класу активу.
        """
        base = {"micro": 0.3, "meso": 0.5, "macro": 0.2}
        vol_factor = micro["price_shock"] / max(1e-6, self.normal_state["volatility"])
        if vol_factor > 2.0:
            base = {"micro": 0.65, "meso": 0.25, "macro": 0.10}
        elif vol_factor > 1.5:
            base = {"micro": 0.5, "meso": 0.35, "macro": 0.15}
        if self.asset_class in ("meme", "ai"):
            base["micro"] += 0.1
            base["macro"] -= 0.1
        if self.asset_class in ("futures",):
            base["meso"] += 0.05
            base["macro"] -= 0.05
        total = sum(base.values())
        return {k: v / total for k, v in base.items()}

    def _select_scenario(
        self, dm: Dict[str, float], weights: Dict[str, float]
    ) -> Tuple[str, float]:
        """
        Вибирає фінальний сценарій та рівень впевненості на основі decision matrix.
        """
        ranked = sorted(dm.items(), key=lambda x: x[1], reverse=True)
        if len(ranked) < 2:
            return "UNCERTAIN", 0.0
        top_scn, top = ranked[0]
        snd = ranked[1][1]
        rel = (top / max(1e-9, snd)) if snd > 0 else 2.0
        abs_min = 0.35
        pref25 = 1.25
        pref10 = 1.10
        if self.asset_class in ("meme",):
            abs_min = 0.4
            pref25 = 1.3
        if top < abs_min:
            return "UNCERTAIN", round(max(0.0, min(1.0, top)), 3)
        if rel >= pref25:
            return top_scn, round(max(0.0, min(1.0, top)), 3)
        if rel >= pref10:
            return top_scn, round(max(0.0, min(1.0, top * 0.8)), 3)
        return "UNCERTAIN", round(max(0.0, min(1.0, top * 0.6)), 3)

    # --- Scoring формули ---

    def _calc_breakout_score(
        self, micro: Dict, meso: Dict, macro: Dict, w: Dict
    ) -> float:
        """
        Розрахунок скору для сценарію пробою (breakout).
        """
        return float(
            w["micro"]
            * (
                micro["volume_anomaly"] * 0.6
                + (1 - micro["liquidity_gap"]) * 0.2
                + micro["vwap_pressure"] * 0.2
            )
            + w["meso"] * (meso["trend_strength"] * 0.6 + meso["levels_quality"] * 0.4)
            + w["macro"]
            * (macro["sector_health"] * 0.7 + max(0.0, macro["macro_sentiment"]) * 0.3)
        )

    def _calc_reversal_score(
        self, micro: Dict, meso: Dict, macro: Dict, w: Dict
    ) -> float:
        """
        Розрахунок скору для сценарію розвороту (reversal).
        """
        return float(
            w["micro"]
            * (
                micro["price_shock"] * 0.5
                + micro["liquidity_gap"] * 0.3
                + (1 - micro["vwap_pressure"]) * 0.2
            )
            + w["meso"]
            * ((1 - meso["trend_strength"]) * 0.7 + meso["levels_quality"] * 0.3)
            + w["macro"] * (1 - macro["sector_health"])
        )

    def _calc_range_score(self, micro: Dict, meso: Dict, macro: Dict, w: Dict) -> float:
        """
        Розрахунок скору для сценарію флету (range-bound).
        """
        vol_norm = min(
            (micro["price_shock"] / max(1e-6, self.normal_state["volatility"])), 3.0
        )
        range_factor = max(0.0, 1.0 - min(1.0, vol_norm))
        return float(
            w["micro"] * range_factor
            + w["meso"] * (1 - meso["trend_strength"]) * 0.8
            + w["meso"] * (1 - meso["levels_quality"]) * 0.2
            + w["macro"] * 0.5
        )

    def _calc_bullish_control_score(
        self, micro: Dict, meso: Dict, macro: Dict, w: Dict
    ) -> float:
        """
        Розрахунок скору для сценарію bullish control.
        """
        return float(
            w["micro"] * (1 - micro["liquidity_gap"]) * 0.5
            + w["micro"] * (micro["vwap_pressure"] > 1.0) * 0.2
            + w["meso"] * meso["trend_strength"] * 0.7
            + w["meso"] * meso["levels_quality"] * 0.3
            + w["macro"]
            * (macro["sector_health"] * 0.6 + max(0.0, macro["macro_sentiment"]) * 0.4)
        )

    def _calc_bearish_control_score(
        self, micro: Dict, meso: Dict, macro: Dict, w: Dict
    ) -> float:
        """
        Розрахунок скору для сценарію bearish control.
        """
        return float(
            w["micro"]
            * (
                micro["liquidity_gap"] * 0.4
                + (1 - micro["vwap_pressure"]) * 0.3
                + micro["price_shock"] * 0.3
            )
            + w["meso"] * (1 - meso["trend_strength"]) * 0.7
            + w["meso"] * (1 - meso["levels_quality"]) * 0.3
            + w["macro"] * (1 - macro["sector_health"]) * 0.8
            + w["macro"] * max(0.0, -macro["macro_sentiment"]) * 0.2
        )

    def _calc_high_volatility_score(
        self, micro: Dict, meso: Dict, macro: Dict, w: Dict
    ) -> float:
        """
        Розрахунок скору для сценарію high volatility.
        """
        vol_norm = min(
            (micro["price_shock"] / max(1e-6, self.normal_state["volatility"])), 3.0
        )
        return float(
            w["micro"] * min(1.0, vol_norm / 1.5) * 0.7
            + w["micro"] * micro["liquidity_gap"] * 0.3
            + w["macro"]
            * (
                abs(macro["correlation_shifts"]) * 0.5
                + (1 - macro["sector_health"]) * 0.5
            )
        )

    def _calc_manipulated_score(
        self, micro: Dict, meso: Dict, macro: Dict, w: Dict, triggers: List[str]
    ) -> float:
        """
        Розрахунок скору для сценарію маніпуляцій (manipulated).
        """
        vol_spike = "volume_spike" in triggers or micro["volume_anomaly"] > 0.7
        thin_book = micro["liquidity_gap"] > 0.0
        corr_jump = abs(macro["correlation_shifts"]) > 0.2
        vwap_whip = micro["vwap_pressure"] > 1.5
        base = 0.0
        base += 0.35 if vol_spike else 0.0
        base += 0.25 if thin_book else 0.0
        base += 0.25 if corr_jump else 0.0
        base += 0.15 if vwap_whip else 0.0
        return float(max(0.0, min(1.0, base)))

    # -------- УТИЛІТИ --------

    def _sanitize_stats(self, s: Dict[str, Any]) -> Dict[str, Any]:
        """
        Санітизує (очищає та нормалізує) вхідні ринкові дані для подальшого аналізу.
        """
        out = dict(s or {})
        out["current_price"] = _safe(out.get("current_price"), 0.0)
        out["volume"] = _safe(out.get("volume"), _safe(out.get("volume_mean"), 0.0))
        out["volume_z"] = _safe(out.get("volume_z"), 0.0)
        out["bid_ask_spread"] = _safe(
            out.get("bid_ask_spread"), self.normal_state["spread"]
        )
        out["rsi"] = _safe(out.get("rsi"), 50.0)
        out["vwap"] = _safe(out.get("vwap"), out["current_price"])
        out["daily_high"] = _safe(out.get("daily_high"), out["current_price"])
        out["daily_low"] = _safe(out.get("daily_low"), out["current_price"])
        out["key_levels"] = out.get("key_levels") or [
            out["daily_low"],
            out["daily_high"],
        ]
        out["sentiment_index"] = _safe(out.get("sentiment_index"), 0.0)
        out["correlation"] = _safe(
            out.get("correlation"), self.normal_state["correlation"]
        )
        return out

    def _normalize(self, value: float, min_val: float, max_val: float) -> float:
        """
        Нормалізує значення у діапазон [0, 1].
        """
        if max_val == min_val:
            return 0.0
        x = (value - min_val) / (max_val - min_val)
        return max(0.0, min(1.0, x))

    def _assess_levels_quality(self, s: Dict[str, Any]) -> float:
        """
        Оцінює якість ключових рівнів (щільність, релевантність, підтримка обсягом).
        """
        levels = s.get("key_levels") or []
        if not levels or len(levels) < 2:
            return 0.2
        in_range = [lv for lv in levels if s["daily_low"] <= lv <= s["daily_high"]]
        density = len(in_range) / max(2, len(levels))
        vr = _safe(s.get("volume_z"), 0.0)
        return float(max(0.0, min(1.0, 0.6 * density + 0.4 * min(1.0, abs(vr) / 3.0))))

    def _detect_correlation_changes(self, s: Dict[str, Any]) -> float:
        """
        Виявляє зміни кореляції відносно нормального стану.
        """
        curr = _safe(s.get("correlation"), 0.5)
        return float(abs(curr - self.normal_state["correlation"]))

    def _identify_asset_class(self) -> str:
        """
        Визначає клас активу на основі символу та патернів.
        """
        sym = self.symbol.upper()
        # Зовнішній об’єкт?
        if self.asset_class_config:
            m = self.asset_class_config.match_symbol(sym)  # type: ignore[attr-defined]
            if m:
                return m
        for asset_class, patterns in ASSET_CLASS_MAPPING.items():
            for pattern in patterns:
                if re.search(pattern, sym, re.IGNORECASE):
                    return asset_class
        return "default"

    def _price_slope(self) -> float:
        """
        Оцінює нахил ціни (тренд) на основі історії цін. 0.5 — нейтрально, <0.5 — ведмежий, >0.5 — бичачий.
        """
        if len(self.price_history) < 6:
            return 0.5
        y = np.array(self.price_history, dtype=float)
        x = np.arange(len(y), dtype=float)
        x = (x - x.mean()) / (x.std() + 1e-9)
        y = (y - y.mean()) / (y.std() + 1e-9)
        beta = float(np.dot(x, y) / (np.dot(x, x) + 1e-9))
        return max(0.0, min(1.0, 0.5 + beta / 2.0))
        # Normalize to [0, 1] range
        # 0.5 is neutral, < 0.5 is bearish, > 0.5 is bullish
        # This ensures that the slope is always between 0 and 1, where 0.5 is neutral
        # and values below 0.5 indicate bearish sentiment while values above 0.5 indicate bullish sentiment
