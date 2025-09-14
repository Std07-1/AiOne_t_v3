# config_episodes.py
# Конфігурація параметрів для епізодного аналізу

# Оновлена конфігурація для 1хв
EPISODE_CONFIG_1M = {
    "symbol": "TONUSDT",
    "timeframe": "1m",
    # Binance Futures /fapi/v1/klines підтримує максимум ~1500 свічок за запит
    # (раніше стояло 2000, що спричиняло HTTP 400 code -1130)
    "limit": 3000,
    "min_gap_bars": 12,  # Зменшено з 4
    "merge_adjacent": True,
    "max_episodes": None,
    "move_pct_up": 0.014,  # Зменшено з 0.029
    "move_pct_down": 0.014,  # Зменшено з 0.029
    "min_bars": 4,  # Зменшено з 5
    "max_bars": 90,  # Зменшено з 120
    "close_col": "close",
    "ema_span": 34,
    "impulse_k": 5,
    "cluster_k": 3,
    "pad_bars": 2,  # Зменшено з 3
    "retrace_ratio": 0.28,
    "require_retrace": False,  # для оцінки @anchor
    "adaptive_threshold": True,
    "save_results": True,
    # create_sample_signals: (видалено — використовуються лише системні сигнали Stage1)
    # Нові параметри
    "volume_z_threshold": 1.5,  # +0.2, якщо ATR% < low_gate (дуже низька волатильність)
    # або <0.2, якщо ATR% > high_gate (дуже висока волатильність)
    # (уніфіковано)
    "rsi_overbought": 78,  # Зменшено з 81
    "rsi_oversold": 18,  # Збільшено з 15
    "tp_mult": 8,
    "sl_mult": 3.5,
    "low_gate": 0.0008,  # Зменшено з 0.001
}

EPISODE_CONFIG_5M = {
    "symbol": "BTCUSDT",
    "timeframe": "5m",
    "limit": 3000,
    "min_gap_bars": 12,
    "merge_adjacent": True,
    "max_episodes": None,
    "move_pct_up": 0.022,
    "move_pct_down": 0.022,
    "min_bars": 5,
    "max_bars": 120,
    "close_col": "close",
    "ema_span": 34,
    "impulse_k": 6,
    "cluster_k": 3,
    "pad_bars": 3,
    "retrace_ratio": 0.28,
    "require_retrace": False,
    "adaptive_threshold": True,
    "save_results": True,
    # create_sample_signals: (видалено — використовуються лише системні сигнали Stage1)
    # Нові параметри
    "volume_z_threshold": 1.5,
    "rsi_overbought": 84,
    "rsi_oversold": 22,
    "tp_mult": 8,
    "sl_mult": 3.5,
    "low_gate": 0.001,
}

EPISODE_CONFIG = {
    "symbol": "BTCUSDT",  # Торговий символ для аналізу
    "timeframe": "5m",  # Таймфрейм для барів
    "limit": 3000,  # Ліміт барів (макс. для Binance klines)
    # Параметри обробки епізодів
    "min_gap_bars": [
        3,
        4,
        5,
    ],  # Мінімальна кількість барів між епізодами для їх розділення
    "merge_adjacent": True,  # Чи зливати сусідні епізоди, якщо вони близькі
    "max_episodes": None,  # Максимальна кількість епізодів (None — без обмежень)
    # Параметри виявлення епізодів
    "move_pct_up": 0.03,  # 3% рух вгору для визначення епізоду
    "move_pct_down": 0.03,  # 3% рух вниз для визначення епізоду
    "min_bars": 5,  # Мінімальна кількість барів для епізоду
    "max_bars": 120,  # Максимальна кількість барів для епізоду
    "close_col": "close",  # Назва колонки з ціною закриття бару
    # Параметри індикаторів
    "ema_span": 50,  # Параметр для обчислення експоненціального середнього (EMA)
    "impulse_k": 6,  # Параметр для визначення імпульсу (чутливість)
    "cluster_k": 3,  # Параметр для кластеризації імпульсів (кількість кластерів)
    # Додаткові параметри
    "pad_bars": 3,  # Кількість барів для доповнення епізоду (перед і після)
    "retrace_ratio": 0.4,  # Співвідношення для корекції (40%), для визначення відскоку
    "require_retrace": True,  # Чи вимагати корекцію, щоб вважати епізод дійсним
    "adaptive_threshold": True,  # Чи використовувати адаптивний поріг
    # Параметри експорту
    "save_results": True,  # Чи зберігати результати аналізу
    # create_sample_signals: True,  # (застаріло) тестові сигнали більше не генеруються
}

# Параметри генерації системних сигналів (Stage1) для епізодного аналізу
SYSTEM_SIGNAL_CONFIG = {
    "enabled": True,
    "lookback": 100,  # розмір ковзного вікна для аналізу історії
    # майбутні параметри можна додати тут (наприклад, мультиплікатори RSI, vol_z overrides)
}

# Для імпорту:
# from config_episodes import SYSTEM_SIGNAL_CONFIG

# Для імпорту:
# from config_episodes import EPISODE_CONFIG

"""
Оптимальні параметри для інтрадей:
{
  "move_pct_up": 0.005,
  "move_pct_down": 0.005,
  "min_bars": 5,
  "max_bars": 60,
  "close_col": "close",
  "ema_span": 50,
  "impulse_k": 8,
  "cluster_k": 2,
  "pad_bars": 3,
  "retrace_ratio": 0.5,
  "require_retrace": false,
  "min_gap_bars": 5,
  "merge_adjacent": true,
  "max_episodes": null,
  "adaptive_threshold": true
}


Спостереження за експериментами:

Перший експеримент (move_pct=0.01):

    Знайдено 59 епізодів.

    Покриття сигналами: 93.2% (55/59).

    Середній рух: 1.0% - 2.1%.

    Тривалість: 16-121 бар.

Другий експеримент (move_pct=0.015):

    Знайдено 19 епізодів.

    Покриття сигналами: 94.7% (18/19).

    Середній рух: 1.5% - 3.4%.

    Тривалість: 17-121 бар.

Третій експеримент (move_pct=0.02):

    Знайдено 9 епізодів.

    Покриття сигналами: 100% (9/9).

    Середній рух: 2.0% - 3.3%.

    Тривалість: 63-121 бар.

Четвертий експеримент (move_pct=0.02, min_bars=30, max_bars=750):

    Знайдено 22 епізоди.

    Покриття сигналами: 100% (22/22).

    Середній рух: 2.0% - 3.0%.

    Тривалість: 89-728 бар (значно довші епізоди).

П'ятий експеримент (move_pct=0.04, min_bars=50, max_bars=1750):

    Знайдено 3 епізоди.

    Покриття сигналами: 100% (3/3).

    Середній рух: 4.0% - 4.2%.

    Тривалість: 149-1536 бар (дуже довгі епізоди).

"""
