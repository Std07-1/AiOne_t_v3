# test/test_narrative_scenarios.py
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
test_narrative_scenarios.py

Тести для перевірки генерації текстових наративів Stage2 (narrative_generator).
Покривають різні ринкові сценарії: флет, пробій, довга консолідація, відсутність рівнів, кластерні ефекти тощо.

Інструкція для розробника:
- Якщо змінюються шаблони у config_NLP.py — переконайтесь, що тести актуальні!
- Додаючи нові сценарії — слідуйте прикладам, використовуйте context+stats.
- Для зручності валідуйте ключові фрагменти, а не жорсткі підрядки.
- Якщо тест фейлиться через розбіжність шаблону — оновіть або тест, або відповідний шаблон.

Додавання нового тест-кейсу:
1. Створіть context зі всіма потрібними полями (див. інші приклади)
2. Додайте специфічні ключі до key_levels/stats для edge-case.
3. Використовуйте актуальні фрази з NARRATIVE_BLOCKS[lang][style]
4. Перевіряйте не суворо по всій фразі, а по ключових словах!

Рекомендації щодо стилю тестів:
- Використовуйте fail-friendly assert: пояснюйте у assert, що саме не так (assert ..., f"Очікували ... в: {narrative}")
- Додавайте короткий опис (докстрінг) до кожного тесту: що саме тестує, який edge-case ловить
- Оновлюйте цей docstring і коментарі при зміні шаблонів/логіки!
"""


import pytest

from stage2.narrative_generator import generate_trader_narrative


def test_flat_with_cluster_on_support():
    """
    Тестує генерацію нарративу для флетового ринку з кластером на підтримці.
    Edge-case: має бути опис кластера, але без TP/SL, breakout та аномалій.
    """
    context = {
        "symbol": "adausdt",
        "current_price": 0.75,  # Поточна ціна
        "key_levels": {
            "immediate_support": {"value": 0.74, "type": "local"},  # Локальна підтримка
            "immediate_resistance": {"value": 0.79, "type": "local"},  # Локальний опір
        },
        "composite_confidence": 0.65,  # Композитна впевненість
        "scenario": "RANGE_BOUND",  # Сценарій флету
        "breakout_probability": 0.42,  # Ймовірність пробою
    }
    anomalies = {}  # Немає аномалій
    triggers = ["low_volatility", "volume_spike"]  # Тригери для флету
    stats = {
        "cluster_factors": [{"type": "support", "impact": "positive"}]
    }  # Кластер на підтримці

    narrative = generate_trader_narrative(
        context,
        anomalies,
        triggers,
        stats,
        lang="UA",
        style="pro",  # Використовуємо українську мову та професійний стиль
    )

    # Оновлені перевірки
    assert (
        "локальної підтримки" in narrative
    ), f"Очікували 'локальної підтримки' в: {narrative}"
    assert (
        "локального опору" in narrative
    ), f"Очікували 'локального опору' в: {narrative}"
    assert (
        "Кластерна структура" in narrative
    ), f"Очікували 'Кластерна структура' в: {narrative}"
    assert (
        "100.0% позитивних" in narrative
    ), f"Очікували '100.0% позитивних' в: {narrative}"
    assert (
        "▲" not in narrative
    ), f"Не повинно бути '▲' (високої ймовірності пробою) в: {narrative}"


def test_no_key_levels():
    """
    Тестує генерацію нарративу при відсутності ключових рівнів (edge-case: key_levels={}).
    Має бути згадка про віддаленість рівнів та підвищену волатильність.
    """
    context = {
        "symbol": "btcusdt",  # Символ для тесту
        "current_price": 65000,  # Поточна ціна
        "key_levels": {},  # Відсутні ключові рівні
        "composite_confidence": 0.6,  # Композитна впевненість
        "scenario": "RANGE_BOUND",  # Сценарій флету
        "breakout_probability": 0.5,  # Ймовірність пробою
    }
    anomalies = {}  # Немає аномалій
    triggers = []  # Немає тригерів
    stats = {}  # Немає статистики

    narrative = generate_trader_narrative(
        context,
        anomalies,
        triggers,
        stats,
        lang="UA",
        style="short",  # Використовуємо українську мову та короткий стиль
    )

    assert (
        "Рівні далеко, волатильність підвищена" in narrative
    ), f"Очікували згадку про віддаленість рівнів у: {narrative}"


def test_far_levels_with_anomaly():
    """
    Тестує генерацію нарративу для ринку з далекими рівнями та аномаліями (ліквідність, волатильність).
    Edge-case: має бути згадка про аномалії та відсотки дистанції.
    """
    context = {
        "symbol": "ethusdt",  # Символ для тесту
        "current_price": 3000,  # Поточна ціна
        "key_levels": {
            "immediate_support": {"value": 2500, "type": "local"},  # Локальна підтримка
            "immediate_resistance": {"value": 3800, "type": "local"},  # Локальний опір
        },
        "composite_confidence": 0.52,  # Композитна впевненість
        "scenario": "RANGE_BOUND",  # Сценарій флету
        "breakout_probability": 0.4,  # Ймовірність пробою
    }
    anomalies = {
        "liquidity_issues": True,
        "volatility_spike": True,
    }  # Аномалії: проблеми з ліквідністю та волатильність
    triggers = ["volume_spike"]  # Тригери для флету
    stats = {}  # Немає статистики

    narrative = generate_trader_narrative(
        context,
        anomalies,
        triggers,
        stats,
        lang="UA",
        style="explain",  # Використовуємо українську мову та пояснювальний стиль
    )

    assert "ліквідність" in narrative, f"Очікували 'ліквідність' в: {narrative}"
    assert "волатильність" in narrative, f"Очікували 'волатильність' в: {narrative}"
    assert (
        "16.67%" in narrative
    ), f"Очікували '16.67%' (дистанція до підтримки) в: {narrative}"
    assert (
        "26.67%" in narrative
    ), f"Очікували '26.67%' (дистанція до опору) в: {narrative}"


def test_breakout_scenario():
    """
    Тестує сценарій пробою з високою впевненістю та аномалією маніпуляції.
    Edge-case: має бути згадка про пробій, маніпуляцію, кластер та символ '▲'.
    """
    context = {
        "symbol": "tonusdt",  # Символ для тесту
        "current_price": 3.80,  # Поточна ціна
        "key_levels": {
            "immediate_support": {"value": 3.60, "type": "local"},  # Локальна підтримка
            "immediate_resistance": {"value": 3.83, "type": "local"},  # Локальний опір
        },
        "composite_confidence": 0.82,  # Композитна впевненість
        "scenario": "BULLISH_BREAKOUT",  # Сценарій пробою
        "breakout_probability": 0.81,  # Ймовірність пробою
    }
    anomalies = {"suspected_manipulation": True}  # Аномалія: підозра на маніпуляцію
    triggers = ["breakout_up", "volume_spike"]  # Тригери для пробою
    stats = {
        "cluster_factors": [  # Кластерний аналіз
            {"type": "support", "impact": "positive"},
            {"type": "resistance", "impact": "negative"},
        ]
    }

    narrative = generate_trader_narrative(
        context,
        anomalies,
        triggers,
        stats,
        lang="UA",
        style="pro",  # Використовуємо українську мову та професійний стиль
    )

    assert "пробою" in narrative, f"Очікували 'пробою' в: {narrative}"
    assert "маніпуляції" in narrative, f"Очікували 'маніпуляції' в: {narrative}"
    assert (
        "Кластерна структура" in narrative
    ), f"Очікували 'Кластерна структура' в: {narrative}"
    assert "▲" in narrative, f"Очікували '▲' (висока ймовірність пробою) в: {narrative}"


def test_float_key_levels():
    """
    Тестує обробку float-значень у key_levels (має бути автоконвертація у словник).
    """
    context = {
        "symbol": "adausdt",  # Символ для тесту
        "current_price": 0.75,  # Поточна ціна
        "key_levels": {
            "immediate_support": 0.74,  # Тест на обробку float
            "immediate_resistance": 0.79,  # Тест на обробку float
        },
        "composite_confidence": 0.5,  # Композитна впевненість
        "scenario": "RANGE_BOUND",  # Сценарій флету
        "breakout_probability": 0.3,  # Ймовірність пробою
    }
    anomalies = {}  # Немає аномалій
    triggers = []  # Немає тригерів
    stats = {}  # Немає статистики

    narrative = generate_trader_narrative(
        context,
        anomalies,
        triggers,
        stats,
        lang="UA",
        style="short",  # Використовуємо українську мову та короткий стиль
    )

    # Система має автоматично конвертувати float у словник
    assert (
        "0.74" in narrative and "0.79" in narrative
    ), f"Очікували '0.74' і '0.79' у: {narrative}"


def test_long_consolidation():
    """
    Тестує згадку про тривалу консолідацію (consolidation_days).
    Edge-case: має бути згадка про 'Консолідація' та кількість днів.
    """
    context = {
        "symbol": "btcusdt",  # Символ для тесту
        "current_price": 90000,  # Поточна ціна
        "key_levels": {
            "immediate_support": {
                "value": 86000,
                "type": "local",
            },  # Локальна підтримка
            "immediate_resistance": {"value": 95000, "type": "local"},  # Локальний опір
        },
        "composite_confidence": 0.45,  # Композитна впевненість
        "scenario": "RANGE_BOUND",  # Сценарій флету
        "breakout_probability": 0.35,  # Ймовірність пробою
        "stats": {"consolidation_days": 6},  # Кількість днів консолідації
    }
    anomalies = {}  # Немає аномалій
    triggers = []  # Немає тригерів

    narrative = generate_trader_narrative(
        context, anomalies, triggers, lang="UA", style="explain"
    )
    assert (
        "Консолідація" in narrative and "6" in narrative
    ), f"Очікували 'Консолідація' і '6' в: {narrative}"


def test_missing_breakout_probability():
    """
    Тестує обробку відсутності breakout_probability (має не падати, якщо поле є/немає).
    """
    """Тест на обробку відсутності обов'язкового поля"""
    context = {
        "symbol": "xrpusdt",
        "current_price": 0.52,
        "key_levels": {},
        "composite_confidence": 0.5,
        "scenario": "RANGE_BOUND",
        "breakout_probability": 0.4,  # <-- може бути відсутнім
    }
    anomalies = {}
    triggers = []
    stats = {}

    # Очікуємо успішне виконання без помилок
    narrative = generate_trader_narrative(
        context, anomalies, triggers, stats, lang="UA", style="short"
    )
    assert isinstance(narrative, str), f"Очікували рядок, отримали: {type(narrative)}"


def test_high_breakout_probability():
    """
    Тестує сценарій з високою breakout_probability (має бути символ '▲').
    """
    context = {
        "symbol": "solusdt",
        "current_price": 150,
        "key_levels": {
            "immediate_support": {"value": 145, "type": "local"},
            "immediate_resistance": {"value": 155, "type": "local"},
        },
        "scenario": "BULLISH_BREAKOUT",
        "breakout_probability": 0.75,
    }
    anomalies = {}
    triggers = ["volume_spike"]
    stats = {}

    narrative = generate_trader_narrative(
        context, anomalies, triggers, stats=stats, lang="UA", style="explain"
    )

    assert (
        "▲ Висока ймовірність пробою (>70%)" in narrative
    ), f"Очікували '▲ Висока ймовірність пробою' в: {narrative}"


def test_low_breakout_probability():
    """
    Тестує сценарій з низькою breakout_probability (має бути символ '▼').
    """
    context = {
        "symbol": "dotusdt",
        "current_price": 6.5,
        "key_levels": {
            "immediate_support": {"value": 6.4, "type": "local"},
            "immediate_resistance": {"value": 6.7, "type": "local"},
        },
        "scenario": "RANGE_BOUND",
        "breakout_probability": 0.25,
    }
    anomalies = {}
    triggers = []
    stats = {}

    narrative = generate_trader_narrative(
        context, anomalies, triggers, stats=stats, lang="UA", style="explain"
    )

    assert (
        "▼ Низька ймовірність пробою (<30%)" in narrative
    ), f"Очікували '▼ Низька ймовірність пробою' в: {narrative}"


# noqa: F811 (keep single definition of test_low_breakout_probability above)


def test_tight_range():
    """
    Тестує вузький діапазон (tight range) — має бути відповідна згадка.
    """
    context = {
        "symbol": "ltcusdt",
        "current_price": 80,
        "key_levels": {
            "immediate_support": {"value": 79.9, "type": "local"},
            "immediate_resistance": {"value": 80.1, "type": "local"},
        },
        "scenario": "RANGE_BOUND",
        "breakout_probability": 0.5,
    }
    anomalies = {}
    triggers = []
    stats = {}

    narrative = generate_trader_narrative(
        context, anomalies, triggers, stats=stats, lang="UA", style="explain"
    )

    assert (
        "Дуже вузький діапазон" in narrative
    ), f"Очікували 'Дуже вузький діапазон' в: {narrative}"


def test_range_narrowing():
    """
    Тестує згадку про звуження діапазону (range_narrowing_ratio).
    """
    context = {
        "symbol": "linkusdt",
        "current_price": 18,
        "key_levels": {
            "immediate_support": {"value": 17.5, "type": "local"},
            "immediate_resistance": {"value": 18.5, "type": "local"},
        },
        "scenario": "RANGE_BOUND",
        "breakout_probability": 0.5,
        "stats": {"range_narrowing_ratio": 0.8},
    }
    anomalies = {}
    triggers = []
    stats = {"range_narrowing_ratio": 0.8}

    narrative = generate_trader_narrative(
        context, anomalies, triggers, stats=stats, lang="UA", style="explain"
    )

    assert (
        "Діапазон звужується" in narrative
    ), f"Очікували 'Діапазон звужується' в: {narrative}"


def test_missing_support_resistance():
    """
    Тестує edge-case: відсутність підтримки/опору (має бути згадка про далекі рівні).
    """
    context = {
        "symbol": "shibusdt",
        "current_price": 0.000025,
        "key_levels": {},
        "scenario": "RANGE_BOUND",
        "breakout_probability": 0.5,
    }
    anomalies = {}
    triggers = []
    stats = {}

    narrative = generate_trader_narrative(
        context, anomalies, triggers, stats=stats, lang="UA", style="explain"
    )

    assert "рівні далекі" in narrative, f"Очікували 'рівні далекі' в: {narrative}"


def test_support_bounce():
    """
    Тестує згадку про відскок від підтримки (support bounce).
    """
    context = {
        "symbol": "avaxusdt",
        "current_price": 35,
        "key_levels": {
            "immediate_support": {"value": 34.99, "type": "local"},
            "immediate_resistance": {"value": 38, "type": "local"},
        },
        "scenario": "RANGE_BOUND",
        "breakout_probability": 0.5,
    }
    anomalies = {}
    triggers = []
    stats = {}

    narrative = generate_trader_narrative(
        context, anomalies, triggers, stats=stats, lang="UA", style="explain"
    )

    assert "шанс на відскок" in narrative, f"Очікували 'шанс на відскок' в: {narrative}"


def test_resistance_rejection():
    """
    Тестує згадку про розворот від опору (resistance rejection).
    """
    context = {
        "symbol": "maticusdt",
        "current_price": 0.75,
        "key_levels": {
            "immediate_support": {"value": 0.7, "type": "local"},
            "immediate_resistance": {"value": 0.755, "type": "local"},
        },
        "scenario": "RANGE_BOUND",
        "breakout_probability": 0.5,
    }
    anomalies = {}
    triggers = []
    stats = {}

    narrative = generate_trader_narrative(
        context, anomalies, triggers, stats=stats, lang="UA", style="explain"
    )

    assert "розворот вниз" in narrative, f"Очікували 'розворот вниз' в: {narrative}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
