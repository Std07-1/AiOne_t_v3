r"""
Модуль тестування Stage2Processor для трейдингової системи AiOne_t

Містить сценарні тести, де явно задаються:
- Вхідний сигнал (контекст, стани індикаторів, аномалії)
- Очікуваний ринковий сценарій
- Очікувана торгова рекомендація
- Очікувані ключові елементи нарративу

Підготовка до запуску:

Активуємо віртуальне середовище та встановлюємо залежності:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate  # Windows

Перейдемо в директорію з проектом:
cd test

Встановимо залежності:
pip install pytest pytest-asyncio faker numpy pandas


Для запуску:
pytest test_stage2_processor.py

Запуск всіх тестів:
pytest test_stage2_processor.py -v

Принципи тестування:
1. Явне задання контексту та очікувань
2. Тестування логіки, а не просто формату
3. Контрольовані сценарії з чіткими очікуваннями
4. Додаткове рандомізоване тестування для виявлення несподіваних помилок
"""

import logging
import random
from unittest.mock import MagicMock

import pytest

from app.asset_state_manager import AssetStateManager
from stage2.level_manager import LevelManager
from stage2.process_single_stage2 import process_single_stage2
from stage2.processor import Stage2Processor

# Налаштування логування для тестів
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger("test")
logger.setLevel(logging.INFO)


# DummyCalibration для спрощення тестування
class DummyCalibration:
    """Проста імітація системи калібрування для тестування"""

    async def get_cached(self, symbol, timeframe):
        return {
            "volume_z": 1.2,
            "rsi_oversold": 30,
            "rsi_overbought": 70,
            "weights": {
                "volume": 0.4,
                "rsi": 0.3,
                "price_position": 0.3,
            },
            "sensitivity_adjustments": {"volume_spike": 1.0},
        }


# Фікстури для спільного використання об'єктів
@pytest.fixture
def level_manager():
    """Фікстура для LevelManager з мокнутими рівнями"""
    lm = LevelManager()
    lm.get_nearest_levels = MagicMock(return_value=(85000, 92000))
    lm.get_all_levels = MagicMock(return_value=[85000, 87000, 90000, 92000, 95000])
    return lm


@pytest.fixture
def processor(level_manager):
    """Фікстура для Stage2Processor з налаштуваннями"""
    return Stage2Processor(
        calib_queue=DummyCalibration(),
        timeframe="1m",
        state_manager=MagicMock(),
        level_manager=level_manager,
        user_lang="UA",
        user_style="pro",
    )


# Контрольовані сценарії для тестування
TEST_CASES = [
    {
        "name": "Бичий пробій з високим обсягом",
        "input_signal": {
            "symbol": "btcusdt",
            "stats": {
                "symbol": "btcusdt",
                "current_price": 90000,
                "daily_high": 92000,
                "daily_low": 85000,
                "vwap": 89000,
                "atr": 1000,
                "volume_z": 2.5,
                "rsi": 45,
                "cluster_factors": [
                    {"type": "support", "impact": "positive"},
                    {"type": "resistance", "impact": "negative"},
                ],
            },
            "trigger_reasons": ["volume_spike", "breakout_up"],
            "anomalies": {},
        },
        "expected_scenario": "BULLISH_BREAKOUT",
        "expected_recommendation": "BUY_IN_DIPS",
        "expected_keywords": ["пробій", "бичий", "обсяг", "зростання"],
    },
    {
        "name": "Ведмежий розворот з RSI дивергенцією",
        "input_signal": {
            "symbol": "ethusdt",
            "stats": {
                "symbol": "ethusdt",
                "current_price": 3500,
                "daily_high": 3600,
                "daily_low": 3400,
                "vwap": 3500,
                "atr": 50,
                "volume_z": 1.8,
                "rsi": 75,
                "cluster_factors": [{"type": "resistance", "impact": "strong"}],
            },
            "trigger_reasons": ["rsi_overbought", "bearish_div"],
            "anomalies": {},
        },
        "expected_scenario": "BEARISH_REVERSAL",
        "expected_recommendation": "SELL_ON_RALLIES",
        "expected_keywords": ["розворот", "ведмежий", "перекупленість", "дивергенція"],
    },
    {
        "name": "Флет з низькою волатильністю",
        "input_signal": {
            "symbol": "xrpusdt",
            "stats": {
                "symbol": "xrpusdt",
                "current_price": 0.55,
                "daily_high": 0.56,
                "daily_low": 0.54,
                "vwap": 0.55,
                "atr": 0.001,
                "volume_z": 0.5,
                "rsi": 50,
                "cluster_factors": [],
            },
            "trigger_reasons": [],
            "anomalies": {},
        },
        "expected_scenario": "RANGE_BOUND",
        "expected_recommendation": "WAIT_FOR_CONFIRMATION",
        "expected_keywords": ["флет", "діапазон", "боковик", "торгівля на межах"],
    },
    {
        "name": "Висока волатильність без чіткого напрямку",
        "input_signal": {
            "symbol": "adausdt",
            "stats": {
                "symbol": "adausdt",
                "current_price": 0.45,
                "daily_high": 0.50,
                "daily_low": 0.40,
                "vwap": 0.45,
                "atr": 0.025,  # 5.5% від ціни - висока волатильність
                "volume_z": 1.2,
                "rsi": 55,
                "cluster_factors": [],
            },
            "trigger_reasons": ["volatility_burst"],
            "anomalies": {"volatility_spike": True},
        },
        "expected_scenario": "HIGH_VOLATILITY",
        "expected_recommendation": "WAIT_FOR_CONFIRMATION",
        "expected_keywords": ["волатильність", "різк", "обереж"],
    },
    {
        "name": "Перекупленість з високим обсягом",
        "input_signal": {
            "symbol": "solusdt",
            "stats": {
                "symbol": "solusdt",
                "current_price": 150,
                "daily_high": 155,
                "daily_low": 140,
                "vwap": 148,
                "atr": 3,
                "volume_z": 2.2,
                "rsi": 78,
                "cluster_factors": [],
            },
            "trigger_reasons": ["rsi_overbought", "volume_spike"],
            "anomalies": {},
        },
        "expected_scenario": "BEARISH_REVERSAL",
        "expected_recommendation": "SELL_ON_RALLIES",
        "expected_keywords": ["перекупленість", "продажі", "корекція", "ризик"],
    },
    {
        "name": "Нульова ціна",
        "input_signal": {
            "symbol": "btcusdt",
            "stats": {
                "symbol": "btcusdt",
                "current_price": 0,
                "daily_high": 92000,
                "daily_low": 85000,
                "vwap": 89000,
                "atr": 1000,
                "volume_z": 0,
                "rsi": 0,
                "cluster_factors": [],
            },
            "trigger_reasons": [],
            "anomalies": {},
        },
        "expected_recommendation": "AVOID",
        "expected_keywords": ["помилка", "невизначений", "дані"],
        "expect_narrative": True,
    },
    {
        "name": "Відсутність тригерів",
        "input_signal": {
            "symbol": "ethusdt",
            "stats": {
                "symbol": "ethusdt",
                "current_price": 3500,
                "daily_high": 3550,
                "daily_low": 3480,
                "vwap": 3500,
                "atr": 20,
                "volume_z": 0.2,
                "rsi": 52,
                "cluster_factors": [],
            },
            "trigger_reasons": [],
            "anomalies": {},
        },
        "expected_scenario": "RANGE_BOUND",
        "expected_recommendation": "WAIT_FOR_CONFIRMATION",
        "expected_keywords": ["очіку", "невизначеність", "флет"],
    },
]


# Параметризований тест для контрольованих сценаріїв
@pytest.mark.asyncio
@pytest.mark.parametrize("test_case", TEST_CASES, ids=[tc["name"] for tc in TEST_CASES])
async def test_controlled_scenarios(processor, test_case):
    """
    Тестування контрольованих сценаріїв з чіткими вхідними даними та очікуваннями
    """
    # Обробляємо сигнал
    result = await processor.process(test_case["input_signal"])

    # Перевіряємо сценарій
    if test_case.get("expected_scenario", "ERROR") != "ERROR":
        assert "market_context" in result
        assert result["market_context"]["scenario"] == test_case["expected_scenario"]

    # Спеціальна обробка для тестів, де ми очікуємо narrative
    if test_case.get("expect_narrative", False):
        assert "narrative" in result

    # Перевіряємо рекомендацію
    assert "recommendation" in result
    assert result["recommendation"] == test_case["expected_recommendation"]

    # Перевіряємо нарратив
    assert "narrative" in result
    narrative = result["narrative"].lower()

    # Перевіряємо очікувані ключові слова
    for keyword in test_case["expected_keywords"]:
        assert (
            keyword.lower() in narrative
        ), f"Ключове слово '{keyword}' не знайдено в нарративі: {narrative}"


# Додаткове рандомізоване тестування для виявлення несподіваних помилок
@pytest.mark.asyncio
async def test_randomized_stress(processor):
    """
    Тестування з рандомізованими даними для виявлення прихованих помилок
    """

    # Простіший генератор випадкових сигналів
    def generate_simple_signal():
        return {
            "symbol": "test",
            "stats": {
                "symbol": "test",
                "current_price": random.uniform(0.1, 100000),
                "daily_high": random.uniform(1.01, 1.05) * 90000,
                "daily_low": random.uniform(0.95, 0.99) * 90000,
                "vwap": random.uniform(0.99, 1.01) * 90000,
                "atr": random.uniform(0.001, 0.1) * 90000,
                "volume_z": random.uniform(-1, 5),
                "rsi": random.uniform(10, 90),
                "cluster_factors": [],
            },
            "trigger_reasons": random.sample(
                [
                    "volume_spike",
                    "rsi_oversold",
                    "breakout_up",
                    "ma_crossover",
                    "volatility_burst",
                ],
                k=random.randint(0, 3),
            ),
            "anomalies": {
                "liquidity_issues": random.choice([True, False]),
                "suspected_manipulation": random.choice([True, False]),
            },
        }

    # Тестуємо 20 випадкових сценаріїв
    for i in range(20):
        signal = generate_simple_signal()
        try:
            result = await processor.process(signal)
            # Базові перевірки структури
            assert "narrative" in result
            assert "recommendation" in result
            assert "market_context" in result
        except Exception as e:
            pytest.fail(f"Помилка при обробці сигналу #{i+1}: {str(e)}")


# Тестування локалізації
@pytest.mark.asyncio
async def test_localization(level_manager):
    """Тестування коректності локалізації"""
    # UA версія
    processor_ua = Stage2Processor(
        calib_queue=DummyCalibration(),
        timeframe="1m",
        state_manager=MagicMock(),
        level_manager=level_manager,
        user_lang="UA",
        user_style="pro",
    )

    # EN версія
    processor_en = Stage2Processor(
        calib_queue=DummyCalibration(),
        timeframe="1m",
        state_manager=MagicMock(),
        level_manager=level_manager,
        user_lang="EN",
        user_style="pro",
    )

    # Тестовий сигнал
    test_signal = {
        "symbol": "btcusdt",
        "stats": {
            "symbol": "btcusdt",
            "current_price": 90000,
            "daily_high": 92000,
            "daily_low": 85000,
            "vwap": 89000,
            "atr": 1000,
            "volume_z": 2.5,
            "rsi": 45,
            "cluster_factors": [],
        },
        "trigger_reasons": ["volume_spike", "breakout_up"],
        "anomalies": {},
    }

    # Обробка в різних локалізаціях
    result_ua = await processor_ua.process(test_signal)
    result_en = await processor_en.process(test_signal)

    # Перевіряємо, що мова змінилася
    assert (
        "цін" in result_ua["narrative"].lower()
        or "рівн" in result_ua["narrative"].lower()
    )
    assert (
        "price" in result_en["narrative"].lower()
        or "level" in result_en["narrative"].lower()
    )

    # Перевіряємо, що рекомендації відповідають
    assert result_ua["recommendation"] == result_en["recommendation"]


@pytest.mark.asyncio
async def test_stage2_low_vol_high_confidence_keeps_alert() -> None:
    """Перевіряємо, що високий конфіденс обходить low-vol даунгрейд."""

    state_manager = AssetStateManager(["testusdt"])
    stage1_signal = {
        "symbol": "testusdt",
        "signal": "ALERT_BUY",
        "stats": {"symbol": "testusdt", "current_price": 10.0, "atr": 0.004},
        "thresholds": {"low_gate": 0.006},
        "trigger_reasons": ["breakout_up"],
    }
    state_manager.update_asset("testusdt", stage1_signal)

    level_manager = MagicMock()
    level_manager.get_corridor.return_value = {
        "support": 9.8,
        "resistance": 10.4,
        "mid": 10.1,
        "band_pct": 0.012,
        "confidence": 0.75,
        "dist_to_support_pct": 0.002,
        "dist_to_resistance_pct": 0.006,
    }
    level_manager.evidence_around.return_value = {}

    processor = Stage2Processor(
        calib_queue=DummyCalibration(),
        timeframe="1m",
        state_manager=state_manager,
        level_manager=level_manager,
        user_lang="UA",
        user_style="pro",
    )
    processor.engine.process = MagicMock(
        return_value={
            "recommendation": "BUY_IN_DIPS",
            "confidence_metrics": {"composite_confidence": 0.88},
            "market_context": {
                "scenario": "BULLISH_BREAKOUT",
                "meta": {"htf_ok": True},
                "key_levels": {
                    "immediate_support": 9.8,
                    "immediate_resistance": 10.4,
                },
                "key_levels_meta": {
                    "band_pct": 0.012,
                    "dist_to_support_pct": 0.002,
                    "dist_to_resistance_pct": 0.006,
                },
            },
            "risk_parameters": {"tp_targets": [10.5], "sl_level": 9.7},
            "narrative": "Сигнал на прорив з високою впевненістю.",
            "trigger_reasons": ["volume_spike"],
        }
    )

    await process_single_stage2(stage1_signal, processor, state_manager)

    asset_state = state_manager.state["testusdt"]
    assert asset_state["signal"] == "ALERT_BUY"
    assert asset_state.get("reco_original") is None
    assert state_manager.passed_alerts == 1
    assert state_manager.blocked_alerts_lowvol == 0
    assert state_manager.blocked_alerts_lowconf == 0


@pytest.mark.asyncio
async def test_stage2_low_vol_low_confidence_downgrades_alert() -> None:
    """Даунгрейд при низькому ATR та недостатньому conf повинен відбутися."""

    state_manager = AssetStateManager(["test2usdt"])
    stage1_signal = {
        "symbol": "test2usdt",
        "signal": "ALERT_BUY",
        "stats": {"symbol": "test2usdt", "current_price": 5.0, "atr": 0.002},
        "thresholds": {"low_gate": 0.006},
        "trigger_reasons": ["breakout_up"],
    }
    state_manager.update_asset("test2usdt", stage1_signal)

    level_manager = MagicMock()
    level_manager.get_corridor.return_value = {
        "support": 4.9,
        "resistance": 5.3,
        "mid": 5.1,
        "band_pct": 0.015,
        "confidence": 0.6,
        "dist_to_support_pct": 0.003,
        "dist_to_resistance_pct": 0.007,
    }
    level_manager.evidence_around.return_value = {}

    processor = Stage2Processor(
        calib_queue=DummyCalibration(),
        timeframe="1m",
        state_manager=state_manager,
        level_manager=level_manager,
        user_lang="UA",
        user_style="pro",
    )
    processor.engine.process = MagicMock(
        return_value={
            "recommendation": "BUY_IN_DIPS",
            "confidence_metrics": {"composite_confidence": 0.7},
            "market_context": {
                "scenario": "BULLISH_BREAKOUT",
                "meta": {"htf_ok": True},
                "key_levels": {
                    "immediate_support": 4.9,
                    "immediate_resistance": 5.3,
                },
                "key_levels_meta": {
                    "band_pct": 0.015,
                    "dist_to_support_pct": 0.003,
                    "dist_to_resistance_pct": 0.007,
                },
            },
            "risk_parameters": {"tp_targets": [5.4], "sl_level": 4.8},
            "narrative": "Сигнал недостатньо впевнений у низькій волатильності.",
            "trigger_reasons": ["volume_spike"],
        }
    )

    await process_single_stage2(stage1_signal, processor, state_manager)

    asset_state = state_manager.state["test2usdt"]
    assert asset_state["signal"] == "NORMAL"
    assert asset_state.get("recommendation") == "WAIT_FOR_CONFIRMATION"
    assert asset_state.get("reco_original") == "BUY_IN_DIPS"
    assert asset_state.get("reco_gate_reason") == "low_volatility+low_confidence"
    assert state_manager.downgraded_alerts == 1
    assert state_manager.blocked_alerts_lowvol == 1
    assert state_manager.blocked_alerts_lowconf == 1
    assert state_manager.blocked_alerts_lowvol_lowconf == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
