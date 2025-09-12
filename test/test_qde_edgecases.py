# Тести для QDE (QuantumDecisionEngine): edge-cases, стабільність, захист від аномалій
#
# Навіщо саме ці тести:
# - negative volume_z: ловить джерело «хибного спайка» зі старої логіки.
# - missing vwap/NaN: гарантує, що sanitize тримає систему.
# - абсурдні рівні: QDE не валиться на поганих даних Stage1/LevelManager.
# - zero price/low vola: захист від ділення/inf/NaN.
# - liquidity gap: перевіряє логіку медіанного спреда й гепів.
# - мікро‑бенчмарк: фіксує продуктивність QDE у межах, придатних для 350+ активів.
#
# Як запустити якщо тести лежать у папці test\:
#   pytest -q test/test_qde_edgecases.py або -s для повного логу
#   pytest -q test/test_qde_benchmark.py або -s для бенчмарку
#   pytest -q test/test_qde_benchmark.py -s

# Edge‑cases з видимим логом:
# pytest -s -vv test_qde_edgecases.py

# Лише бенчмарк з друком метрик:
# pytest -s test_qde_benchmark.py

# або (еквівалентно):
# python -m pytest -s test_qde_benchmark.py


# Додавання нового тест-кейсу:
# 1. Створіть context зі всіма потрібними полями (див. інші приклади)
# 2. Додайте специфічні ключі до key_levels/stats для edge-case.
# 3. Використовуйте актуальні фрази з NARRATIVE_BLOCKS[lang][style]
# 4. Перевіряйте не суворо по всій фразі, а по ключових словах!
#
# Рекомендації щодо стилю тестів:
# - Використовуйте fail-friendly assert: пояснюйте у assert, що саме не так (assert ..., f"Очікували ... в: {narrative}")
# - Додавайте короткий опис (докстрінг) до кожного тесту: що саме тестує, який edge-case ловить
# - Оновлюйте цей docstring і коментарі при зміні шаблонів/логіки!

import math
import pytest
from QDE.quantum_decision_engine import QuantumDecisionEngine


def _engine(symbol="test", vola=0.01, spread=0.0008, corr=0.5, win=60):
    ns = {"volatility": vola, "spread": spread, "correlation": corr}
    return QuantumDecisionEngine(symbol=symbol, normal_state=ns, historical_window=win)


def test_negative_volume_z_not_spike():
    """
    Тестує edge-case: негативний volume_z не повинен трактуватись як аномалія обсягу (volume_anomaly ~ 0).
    """
    qde = _engine()
    s = {"current_price": 100, "volume": 10, "volume_z": -2.0, "bid_ask_spread": 0.0008}
    ctx = qde.quantum_decision(s, [])
    assert (
        0 <= ctx["micro"]["volume_anomaly"] <= 0.1
    ), f"Очікували низьку аномалію обсягу, отримали: {ctx['micro']['volume_anomaly']}"
    assert (
        "scenario" in ctx and 0.0 <= ctx["confidence"] <= 1.0
    ), f"Некоректний сценарій або confidence: {ctx}"


def test_missing_vwap_and_fields_sanitized():
    """
    Тестує edge-case: відсутність vwap та частини полів не повинна призводити до помилки, всі поля санітизуються.
    """
    qde = _engine()
    s = {"current_price": 250, "volume": 0, "rsi": 55, "adx": 22}
    ctx = qde.quantum_decision(s, [])
    assert (
        ctx["micro"]["vwap_pressure"] >= 0.0
    ), f"Очікували vwap_pressure >= 0, отримали: {ctx['micro']['vwap_pressure']}"
    assert (
        isinstance(ctx["decision_matrix"], dict) and len(ctx["decision_matrix"]) == 7
    ), f"Очікували 7 сценаріїв у decision_matrix, отримали: {ctx['decision_matrix']}"


def test_absurd_high_low_order_does_not_crash():
    """
    Тестує edge-case: daily_high < daily_low — система не повинна падати, сценарій все одно визначається.
    """
    qde = _engine()
    s = {"current_price": 10, "daily_high": 5, "daily_low": 15, "volume_z": 0.0}
    ctx = qde.quantum_decision(s, [])
    assert (
        ctx["scenario"] in ctx["decision_matrix"].keys()
    ), f"Сценарій {ctx['scenario']} не знайдено у decision_matrix: {ctx['decision_matrix']}"


def test_zero_price_and_zero_volatility_safe():
    """
    Тестує edge-case: ціна = 0, волатильність = 0 — не повинно бути падіння, confidence у [0, 1].
    """
    # навіть 0 ціна не повинна падати; використовується нормалізація
    qde = _engine(vola=0.01)
    s = {"current_price": 0.0, "volume": 0.0, "volume_z": 0.0}
    ctx = qde.quantum_decision(s, [])
    assert (
        0.0 <= ctx["confidence"] <= 1.0
    ), f"Очікували confidence у [0, 1], отримали: {ctx['confidence']}"


def test_liquidity_gap_triggered_on_spread_blowout():
    """
    Тестує edge-case: різке збільшення спреду має призводити до liquidity_gap == 1.0.
    """
    qde = _engine(spread=0.001)
    # наповнюємо історію нормальним спредом
    for _ in range(10):
        qde.update_history({"current_price": 100, "volume": 1, "bid_ask_spread": 0.001})
    s = {"current_price": 100, "volume": 1, "bid_ask_spread": 0.0031, "volume_z": 0.0}
    ctx = qde.quantum_decision(s, [])
    assert (
        ctx["micro"]["liquidity_gap"] == 1.0
    ), f"Очікували liquidity_gap == 1.0, отримали: {ctx['micro']['liquidity_gap']}"


def test_confidence_and_ranking_shape():
    """
    Тестує коректність формування decision_matrix та межі confidence для різних сценаріїв.
    """
    qde = _engine()
    s = {
        "current_price": 100,
        "volume": 5,
        "volume_z": 3.0,
        "rsi": 65,
        "adx": 30,
        "bid_ask_spread": 0.0006,
        "sentiment_index": 0.2,
    }
    ctx = qde.quantum_decision(s, ["volume_spike"])
    dm = ctx["decision_matrix"]
    assert all(
        0.0 <= float(v) <= 1.0 for v in dm.values()
    ), f"Очікували всі значення decision_matrix у [0, 1], отримали: {dm}"
    assert (
        ctx["scenario"] in dm
    ), f"Сценарій {ctx['scenario']} не знайдено у decision_matrix: {dm}"
