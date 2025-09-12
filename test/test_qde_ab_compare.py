# test_qde_ab_compare.py
# -*- coding: utf-8 -*-
#
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

"""
A/B порівняння QuantumDecisionEngine з legacy-логікою на конфліктних кейсах.
Цей тест перевіряє, чи нова логіка QDE не конфліктує з legacy-логікою
на відомих кейсах, де вони можуть давати різні рішення.
Якщо конфліктів більше 35% — це сигнал до перевірки логіки тригерів/порогів.

Як запустити:
    pytest -q test/test_qde_ab_compare.py

Як інтерпретувати:
- Якщо конфліктів менше 35% — все ок.
- Якщо більше 35% — перевірте логіку тригерів/порогів.

Примітки інтеграції

Для A/B‑тесту увімкни змінну середовища ENABLE_LEGACY_AB=1 і реалізуй legacy_decision()
(виклик твоєї старої логіки або адаптера).

Для жорсткого порога у бенчмарку додай STRICT_QDE_BENCH=1.
Без нього тест лише фіксує показник і ловить грубі регресії.

Edge‑кейс тести зосереджені на стабільності: нульова ціна, інверсні рівні, негативний volume_z,
розрив ліквідності, вплив історії та шуму — саме ті місця, де системи зазвичай «пливуть».
"""
import os
import time
import math
import random
import statistics as stats
import pytest

# Імпорт QDE з основного шляху проекту
from QDE.quantum_decision_engine import QuantumDecisionEngine  # type: ignore


# ==== Адаптер для legacy-логіки ===============================================
# TODO: підключи свою стару логіку тут; зараз — заглушка.
def legacy_decision(input_stats: dict) -> tuple[str, float]:
    """
    Очікуваний протокол: ("SCENARIO", confidence[0..1])
    """
    raise NotImplementedError("Legacy engine adapter is not wired yet.")


# ==== Допоміжні конструктори даних ============================================


def _ns(vol=0.012, sp=0.0008, corr=0.5):
    return {"volatility": float(vol), "spread": float(sp), "correlation": float(corr)}


def _stats(
    price: float,
    volume_z: float | None = None,
    rsi: float | None = None,
    adx: float | None = None,
    vwap: float | None = None,
    spread: float | None = None,
    daily_high: float | None = None,
    daily_low: float | None = None,
    sentiment: float | None = None,
    correlation: float | None = None,
):
    d = {
        "current_price": price,
    }
    if volume_z is not None:
        d["volume_z"] = volume_z
    if rsi is not None:
        d["rsi"] = rsi
    if adx is not None:
        d["adx"] = adx
    if vwap is not None:
        d["vwap"] = vwap
    if spread is not None:
        d["bid_ask_spread"] = spread
    if daily_high is not None:
        d["daily_high"] = daily_high
    if daily_low is not None:
        d["daily_low"] = daily_low
    if sentiment is not None:
        d["sentiment_index"] = sentiment
    if correlation is not None:
        d["correlation"] = correlation
    return d


def _make_qde(symbol="ab", ns=None):
    return QuantumDecisionEngine(symbol=symbol, normal_state=ns or _ns())


# ==== A/B порівняння на конфліктних кейсах ===================================


@pytest.mark.skipif(
    os.environ.get("ENABLE_LEGACY_AB") is None,
    reason="Увімкни A/B, встановивши ENV ENABLE_LEGACY_AB=1 і підключивши legacy_decision().",
)
def test_ab_compare_on_known_conflicts():
    """
    A/B тест: порівнює QDE з legacy-логікою на відомих конфліктних кейсах.
    Edge-case: негативний volume_z, екстремальні RSI, шум, інверсія рівнів, нестандартні спреди.
    Якщо конфліктів >35% — сигнал до ревізії логіки тригерів/порогів.
    """
    qde = _make_qde("ab", _ns())
    # Конфліктні кейси з реальних логів: low_atr + near_high/near_low + негативний volume_z
    samples = [
        _stats(
            price=116_928.8,
            volume_z=-0.54,
            rsi=53.6,
            adx=25,
            spread=0.0008,
            sentiment=0.1,
        ),
        _stats(
            price=4041.38,
            volume_z=-1.09,
            rsi=23.5,
            adx=20,
            spread=0.0009,
            sentiment=-1.0,
        ),
        # Додаткові варіації
        _stats(price=3.3535, volume_z=-0.67, rsi=30.4, adx=18, sentiment=-0.98),
        _stats(price=0.7964, volume_z=-1.58, rsi=48.8, adx=22, sentiment=-0.06),
    ]
    # Розширимо до ~20 кейсів невеликим шумом
    base = samples[:]
    for _ in range(12):
        s = random.choice(base).copy()
        s["current_price"] *= 1 + random.uniform(-0.002, 0.002)
        s["volume_z"] = (s.get("volume_z", 0.0) or 0.0) + random.uniform(-0.2, 0.2)
        s["rsi"] = max(5, min(95, (s.get("rsi", 50.0) or 50.0) + random.uniform(-2, 2)))
        samples.append(s)

    disagreements = []
    for s in samples:
        q = qde.quantum_decision(s, trigger_reasons=[])
        sc_q, cf_q = q["scenario"], float(q["confidence"])
        sc_old, cf_old = legacy_decision(s)
        if sc_q != sc_old:
            disagreements.append((s, sc_q, sc_old, cf_q, cf_old))

    print(f"\nA/B disagreements: {len(disagreements)}/{len(samples)}")
    for row in disagreements:
        print(row)

    # У «м’якому» режимі лише друкуємо репорт; за потреби можна ввести поріг:
    max_allowed_disagreements = int(0.35 * len(samples))
    assert (
        len(disagreements) <= max_allowed_disagreements
    ), "Занадто багато розбіжностей між QDE і legacy — перевір логіку тригерів/порогів."


# ==== Edge cases: підвальні кейси даних =======================================


def test_edge_zero_price_and_missing_vwap():
    """
    Edge-case: ціна = 0, відсутній vwap — QDE не повинен падати, сценарій та confidence у межах.
    """
    qde = _make_qde("edge0", _ns(vol=0.01))
    s = _stats(price=0.0, volume_z=0.0, rsi=0.0)  # відсутній vwap, спред, adx
    out = qde.quantum_decision(s, [])
    assert isinstance(out, dict)
    assert out["scenario"] in {"UNCERTAIN", "RANGE_BOUND", "HIGH_VOLATILITY"}
    assert 0.0 <= float(out["confidence"]) <= 1.0


def test_edge_absurd_levels_inverted_high_low():
    """
    Edge-case: daily_high < daily_low — QDE не повинен падати, confidence у [0, 1].
    """
    qde = _make_qde("edge1", _ns(vol=0.02))
    s = _stats(
        price=100.0,
        daily_high=90.0,  # абсурд: high < low
        daily_low=110.0,
        volume_z=0.1,
        rsi=50.0,
        adx=20.0,
        spread=0.0012,
    )
    out = qde.quantum_decision(s, [])
    assert isinstance(out, dict)
    # не повинно падати; сценарій має обратись і з невисокою впевненістю
    assert 0.0 <= float(out["confidence"]) <= 1.0


def test_edge_negative_volume_is_not_spike():
    """
    Edge-case: негативний volume_z не має трактуватись як сплеск/manipulated/high_volatility.
    """
    qde = _make_qde("edge2", _ns(vol=0.015))
    # негативний volume_z — не має трактуватись як "сплеск"
    s = _stats(
        price=1000.0, volume_z=-2.0, rsi=45, adx=22, spread=0.0007, sentiment=0.0
    )
    out = qde.quantum_decision(s, trigger_reasons=[])
    assert out["decision_matrix"]["MANIPULATED"] < 0.6
    assert out["decision_matrix"]["HIGH_VOLATILITY"] < 0.8


def test_edge_liquidity_gap_effect():
    """
    Edge-case: широкий спред має підвищувати HIGH_VOLATILITY, але не валити QDE.
    """
    qde = _make_qde("edge3", _ns(vol=0.01, sp=0.0004))
    # дуже широкий спред → має підштовхувати до HV/BEARISH_CONTROL
    s_bad = _stats(price=10.0, volume_z=0.3, rsi=52, adx=18, spread=0.005)
    s_ok = _stats(price=10.0, volume_z=0.3, rsi=52, adx=18, spread=0.0003)
    out_bad = qde.quantum_decision(s_bad, [])
    out_ok = qde.quantum_decision(s_ok, [])
    assert (
        out_bad["decision_matrix"]["HIGH_VOLATILITY"]
        >= out_ok["decision_matrix"]["HIGH_VOLATILITY"]
    )


def test_edge_rsi_extremes_interaction_with_levels():
    """
    Edge-case: екстремальний RSI біля опору — BEARISH_REVERSAL має бути не менше BULLISH_BREAKOUT.
    """
    qde = _make_qde("edge4", _ns(vol=0.012))
    s = _stats(
        price=4041.38,
        volume_z=-1.09,
        rsi=23.5,
        adx=20,
        spread=0.0009,
        daily_high=4053.96,
        daily_low=3940.81,
        sentiment=-1.0,
    )
    out = qde.quantum_decision(s, [])
    # при RSI ~24 біля опору пробій має бути менш імовірний, ніж відмова
    assert (
        out["decision_matrix"]["BEARISH_REVERSAL"]
        >= out["decision_matrix"]["BULLISH_BREAKOUT"]
    )


def test_edge_history_influence_on_trend_slope():
    """
    Edge-case: зростаюча історія цін має підвищувати BULLISH_CONTROL.
    """
    qde = _make_qde("edge5", _ns(vol=0.01))
    # накрутимо зростаючу історію, щоби slope вийшов позитивний
    base = 100.0
    for i in range(40):
        s = _stats(price=base + i * 0.2, volume_z=0.0, rsi=55, adx=25)
        qde.quantum_decision(s, [])
    # останній крок з нейтральними вхідними — має показати підвищений bullish_control
    out = qde.quantum_decision(
        _stats(price=base + 40 * 0.2, volume_z=0.1, rsi=55, adx=25), []
    )
    assert out["decision_matrix"]["BULLISH_CONTROL"] > 0.35


def test_edge_noisy_volume_z_stability():
    """
    Edge-case: шумний volume_z не повинен призводити до нестабільних сценаріїв — очікуємо нейтральність.
    """
    qde = _make_qde("edge6", _ns(vol=0.012))
    decisions = []
    for _ in range(50):
        s = _stats(
            price=100.0 * (1 + random.uniform(-0.001, 0.001)),
            volume_z=random.uniform(-1.5, 1.5),
            rsi=50 + random.uniform(-5, 5),
            adx=20 + random.uniform(-3, 3),
            spread=0.0008 + random.uniform(-0.0002, 0.0002),
        )
        out = qde.quantum_decision(s, [])
        decisions.append(out["scenario"])
    # очікуємо більшість нейтральних сценаріїв при шумних обсягах без тренду
    neutral_share = sum(
        1 for d in decisions if d in {"RANGE_BOUND", "UNCERTAIN"}
    ) / len(decisions)
    assert neutral_share >= 0.5


# ==== Мікробенчмарк продуктивності ===========================================


def test_qde_micro_benchmark_report():
    """
    Мікробенчмарк: перевіряє продуктивність QDE на стабільному вході, друкує статистику.
    """
    qde = _make_qde("bench", _ns(vol=0.012, sp=0.0008))
    # "реалістичний" стабільний вхід
    s = _stats(
        price=1000.0,
        volume_z=0.3,
        rsi=52.0,
        adx=22.0,
        vwap=1000.0,
        spread=0.0009,
        daily_high=1010.0,
        daily_low=990.0,
        sentiment=0.0,
        correlation=0.5,
    )

    # прогрів
    for _ in range(100):
        qde.quantum_decision(s, [])

    runs = int(os.environ.get("QDE_BENCH_RUNS", "2000"))
    t0 = time.perf_counter()
    for _ in range(runs):
        qde.quantum_decision(s, [])
    t1 = time.perf_counter()
    total = t1 - t0
    avg = total / runs

    print(
        f"\nQDE micro-benchmark: runs={runs}, total={total:.6f}s, avg={avg*1e6:.2f} µs/call"
    )

    # За замовчуванням лише фіксуємо показник; у STRICT-режимі — жорсткий assert
    if os.environ.get("STRICT_QDE_BENCH") == "1":
        # 0.1 ms = 100 µs
        assert avg <= 0.0001, f"Повільно: {avg*1e6:.2f}µs > 100µs (0.1ms)"
    else:
        # sanity-поріг, щоб ловити регресії
        assert avg <= 0.003, f"Надто повільно: {avg*1e6:.2f}µs (очікували < 3000µs)"


# ==== Узгодженість виходу (контракт) =========================================


def test_output_contract_fields_presence():
    """
    Контрактний тест: перевіряє наявність усіх ключових полів у виході QDE та повноту decision_matrix.
    """
    qde = _make_qde("contract", _ns())
    out = qde.quantum_decision(_stats(price=500.0, volume_z=0.0, rsi=50, adx=20), [])
    for field in (
        "scenario",
        "confidence",
        "decision_matrix",
        "weights",
        "micro",
        "meso",
        "macro",
        "asset_class",
        "symbol",
    ):
        assert field in out, f"Вихід QDE не містить ключ '{field}'"

    dm = out["decision_matrix"]
    required_scenarios = {
        "BULLISH_BREAKOUT",
        "BEARISH_REVERSAL",
        "RANGE_BOUND",
        "BULLISH_CONTROL",
        "BEARISH_CONTROL",
        "HIGH_VOLATILITY",
        "MANIPULATED",
    }
    assert required_scenarios.issubset(
        set(dm.keys())
    ), "Неповна матриця сценаріїв у виході QDE"
