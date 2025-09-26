# AiOne_t

**Мета**: Легковаговий асинхронний пайплайн для Stage1 → Stage2 → Stage3 обробки крипто-даних (REST + WS), генерації сигналів, агрегації стану та публікації у UI.

## 🔧 Архітектура (Огляд)

Пайплайн складається з трьох основних шарів:

1. Stage1 (фільтрація та моніторинг): швидке отримання сирих біржових даних, попередня фільтрація активів (ліквідність, волатильність, OI, depth), виявлення базових тригерів (обсяг, волатильність, breakout, RSI, VWAP девіація).
2. Stage2 (QDE / поглиблений аналіз): композитна оцінка активу, контекст рівнів, ATR/RSI/VWAP, класифікація ризиків, формування рекомендацій (BUY/SELL сценарії, TP/SL мультиплікатори, confidence score).
3. Stage3 (trade state / менеджмент): управління відкритими позиціями, оновлення стану угод, адаптація TP/SL (roadmap), підготовка агрегованого стану для UI.

Дані циркулюють через `UnifiedDataStore` (RAM + Redis) та публікуються через Redis канал `asset_state_update`. UI-споживач відновлює попередній стан через ключ `asset_state_snapshot`.

### Потоки Даних

REST preload → історичні бари (1m + daily) → Stage1 моніторинг → тригери → Stage2 (побудова контексту + скоринг) → Stage3 (оновлення торгових станів) → Публікація (UI / WS).

### Основні Компоненти

- `config/config.py` — єдине джерело констант, семафорів, статусів, порогів, моделей.
- `data/unified_store.py` — багатошаровий сховище (in‑memory dict + Redis; закладені хуки для диск persistence).
- `data/ws_worker.py` — серіалізація (orjson + lz4) та публікація списків/стану.
- `stage1/*` — фільтри (prefilter), моніторинг активів, тригери (об'єм, RSI дивергенції, breakout, волатильність, VWAP девіація).
- `stage2/*` — рівні, процесор, qde_core (контекст + композитний скоринг + рекомендації).
- `stage3/*` — агрегування торгових станів та менеджмент відкритих угод.
- `UI/*` — споживач стану та публікація агрегованого фрейму для інтерфейсу.
- `app/*.py` — glue‑логіка запуску циклів, preload історії, продюсер скринінгу.
- `utils/utils.py` — універсальні утиліти форматування, tick size, нормалізації результатів.

Примітки щодо UI та TP/SL:
- UI виконує лише рендер. Будь-які обчислення/нормалізація — поза UI.
- Значення TP/SL для рядків таблиці беруться виключно зі Stage3 core:trades.targets (єдине джерело правди). Якщо таргетів немає — UI публікує `tp_sl='-'`.
- Для A/B перевірок доступний фіче‑тогл `UI_TP_SL_FROM_STAGE3_ENABLED` у `config/config.py`. Якщо вимкнути — `tp_sl` буде вимкнено для всіх рядків.

### ASCII Діаграма Потоку
```
                               ┌───────────────────────────────────┐
                               │            Зовнішні Джерела       │
                               │  Binance REST (klines, tickers)   │
                               │  Binance WS (streaming, roadmap*) │
                               └───────────────────────────────────┘
                                              │
                                              ▼
                               ┌───────────────────────────────────┐
                               │          Preload (app/)          │
                               │  • 1m історія (LOOKBACK)         │
                               │  • Daily свічки (рівні)          │
                               └───────────────────────────────────┘
                                              │ (історичні дані)
                                              ▼
┌─────────────────────────┐      ┌───────────────────────────────────┐
│ UnifiedDataStore (RAM) │◄────►│          Redis (кеш/стан)        │
└─────────────────────────┘      └───────────────────────────────────┘
          │   ▲                               ▲   │
          │   │                               │   │
          │   │write/read                     │   │publish snapshot
          ▼   │                               │   ▼
┌───────────────────────────────────────────────────────────────────┐
│                           Stage1                                  │
│  • Prefilter (ліквідність, vol, OI, depth)                         │
│  • Моніторинг + тригери (volume, volatility, RSI div, breakout)   │
└───────────────────────────────────────────────────────────────────┘
          │   (список активів + тригери)
          ▼
┌───────────────────────────────────────────────────────────────────┐
│                           Stage2 (QDE)                            │
│  • Рівні (мікро/мезо/макро)                                       │
│  • Контекст (ATR/RSI/VWAP/Volume профіль)                         │
│  • Композитний скоринг + рекомендації (BUY/SELL, TP/SL, conf)     │
└───────────────────────────────────────────────────────────────────┘
          │   (рекомендації + метрики)
          ▼
┌───────────────────────────────────────────────────────────────────┐
│                           Stage3                                  │
│  • Trade state manager                                            │
│  • Агрегація стану для UI                                         │
└───────────────────────────────────────────────────────────────────┘
          │   (агрегований стан)
          ▼
┌─────────────────────────┐     publish (Redis pub/sub)    ┌──────────────────┐
│ data/ws_worker.py       │ ─────────────────────────────► │ UI Consumer / UI │
│  • orjson + lz4 payload │                                │  таблиця/панель  │
└─────────────────────────┘                                └──────────────────┘
          ▲                                                   │
          │ metrics snapshots (внутрішні)                     │pull snapshot
          │                                                   ▼
      ┌────────────────────────────────────────────────────────────┐
      │                MetricsCollector / Prometheus               │
      │  • stage1/2 timings • ws counters • prefilter stats       │
      └────────────────────────────────────────────────────────────┘
```

## ⚙️ Config Structure (`config/config.py`)

Конфіг структуровано блоками, кожен блок з чітким призначенням.

### Базові Константи
- `DEFAULT_LOOKBACK`, `DEFAULT_TIMEFRAME`, `MIN_READY_PCT`, `MAX_PARALLEL_STAGE2`, `TRADE_REFRESH_INTERVAL`.

### Рекомендації / Сигнали
- `BUY_SET`, `SELL_SET` — мапа наборів рекомендацій до BUY/SELL сигналів (UI шар).

### Redis Канали / Ключі
- `REDIS_CHANNEL_ASSET_STATE`, `REDIS_SNAPSHOT_KEY` — публікація та cold‑start відновлення.

### UI / Tick Size / Статуси
- `UI_LOCALE`, `UI_COLUMN_VOLUME`.
- `TICK_SIZE_MAP`, `TICK_SIZE_BRACKETS`, `TICK_SIZE_DEFAULT` — евристика та overrides для кроку ціни.
- `STAGE2_STATUS`, `ASSET_STATE` — канонічні рядкові стани.

### Trigger Tags / Normalize Map
- `TRIGGER_TP_SL_SWAP_LONG`, `TRIGGER_TP_SL_SWAP_SHORT`, `TRIGGER_SIGNAL_GENERATED`.
- `TRIGGER_NAME_MAP` — уніфікація сирих назв тригерів через усі шари.

### Паралельність / Семафори / Кеш
- `OI_SEMAPHORE`, `KLINES_SEMAPHORE`, `DEPTH_SEMAPHORE` — керування конкурентністю зовнішніх викликів.
- `REDIS_CACHE_TTL`, `INTERVAL_TTL_MAP` — TTL для історичних інтервалів та довідкових відповідей.

### Моделі (Pydantic)
- `SymbolInfo`, `FilterParams`, `MetricResults` — строгі схеми даних для валідації.

### Таксономія + Stage2 Config (QDE)
- `ASSET_CLASS_MAPPING`, `STAGE2_CONFIG`, `OPTUNA_PARAM_RANGES` — класи активів, ваги, пороги, фактори скорингу.

### Prefilter / Preload / Seeds
- `STAGE1_PREFILTER_THRESHOLDS`, `MANUAL_FAST_SYMBOLS_SEED`, `USER_SETTINGS_DEFAULT`.
- `PRELOAD_1M_LOOKBACK_INIT`, `SCREENING_LOOKBACK`, `PRELOAD_DAILY_DAYS`.
- `FAST_SYMBOLS_TTL_MANUAL`, `FAST_SYMBOLS_TTL_AUTO`, `PREFILTER_INTERVAL_SEC`.

### Моніторинг / Prefilter Параметри
- `STAGE1_MONITOR_PARAMS` — vol_z_threshold, rsi пороги, агрегована логіка ALERT.
- `PREFILTER_BASE_PARAMS` — min_depth, min_atr, dynamic flags.

### Calibration / Legacy Placeholders
- `OPTUNA_SQLITE_URI`, `MIN_CONFIDENCE_TRADE_PLACEHOLDER` — artefacts для можливого майбутнього повернення калібрування.

## 🧪 Тестування

Тести у каталозі `test/` покривають:
- Smoke для індикаторів (RSI, VWAP, VolumeZ, ATR, Levels).
- Tick size / decimals (перевірка `get_tick_size`, `format_price`).
- Stage2 processor / QDE сценарії (edge cases, A/B порівняння, benchmark).
- Наративні сценарії (генерація описів / рекомендацій).

Запуск (Windows PowerShell):
```
pytest -q
```

## 📊 Метрики

Внутрішній MetricsCollector наразі працює у no-op режимі (Prometheus видалено). Залишені тільки легкі агрегати для UI (див. `monitoring/metrics_reporter.py`).

## 🚀 Запуск

1. Встанови залежності:
```
pip install -r requirements.txt
```
2. (Опційно) Налаштуй `.env` (наприклад LOG_LEVEL, Redis URL).
3. Запусти основний сценарій:
```
python -m app.main
```
4. UI консюмер (якщо окремо):
```
python -m UI.ui_consumer_entry
```

## 🧩 Стиль та STYLE_GUIDE

Основні принципи:
- Українська мова у докстрінгах/коментарях.
- Секційні розділювачі: `# ── Назва ──` (однолінійні, без блоків).
- Guard для логера: `if not logger.handlers:` перед конфігурацією.
- Мінімум broad `except:`; якщо потрібен — коментар з причиною або `logger.exception`.
- Явний `__all__` у кінці модуля для публічного API.
- Відсутність бізнес-логіки в `config/` (лише дані/моделі).

## 🧱 Залежності

Переглянь `Recomm.md` (секція Dependencies) для класифікації:
- Core (aiohttp, websockets, numpy, pandas, redis, lz4, orjson, etc.)
- Metrics (prometheus_client)
- Experimental/Removed (optuna, SQLAlchemy стек, pyarrow тощо)

## ♻️ Потенційні Подальші Кроки

- Повернення калібрування через Optuna (реактивація `run_calibration` + persistence).
- Disk layer у `UnifiedDataStore` (write-behind flush, mmap parquet/shmem).
- Розширення метрик (P99 latency, розподіли величин сигналів).
- Інтеграція live-order виконання (REST trading client) — окремий модуль.

## ❓ FAQ (Стисле)

Q: Як змінити поріг Volume Spike?
A: Онови `volume_z_threshold` у `STAGE2_CONFIG` або у `STAGE1_MONITOR_PARAMS` залежно від шару використання.

Q: Як додати новий тригер?
A: Створи модуль у `stage1/asset_triggers/`, додай у `TRIGGER_NAME_MAP`, увімкни у `STAGE2_CONFIG['switches']['triggers']`.

Q: Як вимкнути Stage2 для тестів продуктивності?
A: Встанови `STAGE2_CONFIG['switches']['use_qde'] = False` перед запуском або через patch у тесті.

## 🧮 Audit Bus

Легковаговий in‑process журнал метрик та подій (Stage1 → Stage2 → QDE) з експортом у CSV / JSONL.

Основні можливості:
* Append уніфікованих записів: `stage`, `kind`, `symbol`, `timeframe`, `tags`, payload (префікс `p_` при flatten у DataFrame).
* Ліміт буфера: `max_size` (trim найстаріших).
* Streaming JSONL + size-based ротація.
* Sugar-хелпер `log_metrics`.

### API Швидкий Старт
```python
from ep_2.audit import audit_bus

# Налаштування (один раз на старті)
audit_bus.configure(
        max_size=100_000,
        jsonl_path="results/explain/audit_stream.jsonl",
        jsonl_rotate_mb=5,  # ротація при ~5MB
)

# Лог події метрик Stage1
audit_bus.log_metrics(
        "stage1", "BTCUSDT", "1m", kind="metrics", dens_per_1k=6.7, avg_rank=0.24
)

# Експорт snapshot у CSV (explain mode)
path = audit_bus.auto_export("results/explain")

# Повний dump JSONL (одноразово)
audit_bus.export_jsonl("results/explain/audit_full.jsonl", append=False)
```

### Внутрішній Формат JSONL (приклад рядка)
```json
{
    "ts": 1699999999.123,
    "stage": "stage1",
    "kind": "metrics",
    "symbol": "BTCUSDT",
    "timeframe": "1m",
    "tags": ["system_signals"],
    "p_dens_per_1k": 6.73,
    "p_avg_rank": 0.245
}
```

### Поточні Події
| Stage | kind            | Опис |
|-------|-----------------|------|
| stage1 | metrics        | Метрики генерації сигналів (щільність, ранги, burst) |
| stage1 | alignment      | Результати вирівнювання сигналів до барів |
| stage1 | dyn_thresholds_pre | Попередні динамічні пороги перед генерацією сигналів |
| stage2 | dyn_thresholds | Після аналізу волатильності адаптовані move_pct_* |
| stage2 | validation     | Підсумок валідації епізодів |
| qde    | decision       | Рішення QDE: сценарій, рекомендація, confidence |

## 🧠 Dynamic Thresholds

Система YAML-правил для адаптації порогів:
* Stage1: RSI overbought/oversold, мінімальний `volume_z` (через `stage1.*`).
* Stage2: множники до `move_pct_up/down` (через `stage2.move_pct_*_factor`).

### NEW (2025-09)
Додано:
1. Пріоритети правил (`priority`) – вищий застосовується раніше; при конфлікті пізніше правило з тим самим ключем перепише попереднє, але порядок базується на сортуванні за `priority desc, id asc`.
2. Композитні умови (`when.all`, `when.any`) – підтримка мульти-метричних AND/OR.
3. Layered overrides: каскад завантаження `threshold_rules_example.yaml` → `threshold_rules_<SYMBOL>.yaml` → `threshold_rules_<SYMBOL>_<TF>.yaml` (override по `id`).
4. Smoothing (експоненційне згладжування) для `move_pct_up` / `move_pct_down` щоб уникнути різких стрибків (`dyn_smooth_alpha`, default 0.4).
5. Audit розширено: `dyn_stats` тепер включає `priority` для кожного правила.

### Оновлений YAML Формат (Single Metric)
```yaml
rules:
    - id: rsi_extreme_expand
        priority: 70
        when: { metric: volatility.max_60bar_move, op: ">=", value: 0.05 }
        actions:
            stage1.rsi_overbought: 82
            stage1.rsi_oversold: 18
            stage1.volume_z_min: 3.5
            stage2.move_pct_up_factor: 1.2
            stage2.move_pct_down_factor: 1.2
        tags: [volatility, expansion]
```

### Композитні Умови (AND / OR)
Використовуємо `all` (усі мусять пройти) і/або `any` (хоча б один):
```yaml
rules:
    - id: quiet_market_relax
        priority: 60
        when:
            all:
                - { metric: volatility.max_60bar_move, op: "<", value: 0.020 }
                - { metric: volatility.max_30bar_move, op: "<", value: 0.014 }
            any:
                - { metric: volatility.max_5bar_move, op: "<", value: 0.006 }
                - { metric: volatility.max_5bar_return, op: "<", value: 0.004 }
        actions:
            stage1.volume_z_min: 2.4
            stage2.move_pct_up_factor: 0.88
            stage2.move_pct_down_factor: 0.88
```
Якщо присутні обидва блоки – логіка: `(all AND any)`.

### Layered Overrides (Приклад ETHUSDT)
Файли:
```
config/threshold_rules_example.yaml               # базовий шар
config/threshold_rules_ETHUSDT.yaml               # пер-символьний
config/threshold_rules_ETHUSDT_1m.yaml            # конкретний таймфрейм
```
Правило з однаковим `id` у нижчих шарах заміщує попереднє (але counters eval/applied починаються заново в поточному ранi). Це дозволяє точково налаштовувати вузькі таймфрейми без дублю всієї логіки.

### Адаптація та Smoothing
Після оцінки правил Stage2 множники (`stage2.move_pct_*_factor`) не застосовуються миттєво – вони формують цільові значення, які згладжуються:
```
target = prev * factor
new = prev + alpha * (target - prev)   # alpha = dyn_smooth_alpha (0..1)
```
Це запобігає хаотичним змінам порогів між сусідніми аналізами різних волатильнісних режимів.

### Audit Події
| kind | Опис |
|------|------|
| dyn_thresholds_pre | Попередні overrides перед Stage1 (швидкий proxy) |
| dyn_thresholds | Повна оцінка після аналізу волатильності |
| dyn_stats | Статистика по кожному правилу: evaluations, applied, priority |

Приклад `dyn_stats` payload (flatten):
```
stage=stage2 kind=dyn_stats rule_id=quiet_market_relax priority=60 evaluations=1 applied=1
```

### Стратегії Для Налаштування
1. Встанови базові режими (quiet / normal / expansion) через прості single-metric правила.
2. Додай composite для edge-кейсів (надзвичайно низька мікро-волатильність + стислий діапазон).
3. Використовуй layered overrides для специфічних символів з іншим профілем ліквідності (наприклад ETH проти BTC).
4. Регулюй `dyn_smooth_alpha`:
     * 0.2–0.4 — плавно, інерційно.
     * 0.6–0.8 — швидше реагує на зміни.

### Обмеження Поточної Реалізації
* Немає cooldown/hold періодів (правило може застосовуватись кожен цикл).
* Немає вагового мерджу – останній апдейт перезаписує ключ.
* Контекст метрик обмежено волатильністю (`volatility.*`) — легко розширити (наприклад `regime.trend_slope`).

### Майбутні Ідеї (оновлено)
* Cooldown per rule (min_interval_seconds / max_apply_per_window).
* Composite групи (іменовані режими з пріоритетними кластерами).
* Додавання smoothing до Stage1 параметрів (RSI пороги, volume_z_min).
* Rolling aggregation для `dyn_stats` (періодичний summary snapshoot).
* Вивід попередніх і нових значень у audit для кожного ключа (diff payload).

## 🔭 Подальші Ідеї (Audit + Thresholds)
* Винос audit_stream у окремий async sink (S3 / Kafka / Redis Stream).
* Інтеграція правил для Stage3 (динамічні TP/SL множники).




## 🧩 State‑aware Thresholds (Stage1, мінімалістично)

Для швидкої адаптації порогів у Stage1 без складних правил підтримано два додаткові поля в пороговій конфігурації символу (див. `app/thresholds.py`):

- `signal_thresholds` — прості вкладені налаштування для тригерів (наприклад, `volume_spike.z_score`, `vwap_deviation.threshold`).
- `state_overrides` — дельти значень за простими станами ринку: `range_bound`, `trend_strong`, `high_volatility`.

Stage1 викликає `Thresholds.effective_thresholds(market_state=...)` і застосовує результат мінімально: наразі використовуються `vol_z_threshold` і `vwap_deviation` (інші значення залишаються для сумісності і майбутніх розширень).

Приклад конфігурації (Python dict):

```python
{
    "symbol": "btcusdt",
    "low_gate": 0.006,
    "high_gate": 0.015,
    "vol_z_threshold": 2.0,
    "vwap_deviation": 0.01,
    "signal_thresholds": {
        "volume_spike": {"z_score": 3.0},
        "vwap_deviation": {"threshold": 0.02}
    },
    "state_overrides": {
        "range_bound": {
            "vol_z_threshold": +1.0,
            "vwap_deviation.threshold": +0.01  # dot‑path у вкладені ключі
        },
        "high_volatility": {"vol_z_threshold": -0.5}
    }
}
```

Примітки:
- Беккомпат збережено: якщо `signal_thresholds`/`state_overrides` відсутні, працюють плоскі поля (`vol_z_threshold`, `vwap_deviation`, тощо).
- `state_overrides` обробляються як дельти до базових значень. Після застосування дельт гарантується коректність `high_gate > low_gate`.
- Якщо в `signal_thresholds` задано `volume_spike.z_score` чи `vwap_deviation.threshold`, ці значення синхронізуються до верхнього рівня (`vol_z_threshold`/`vwap_deviation`) для зручності Stage1.

Логування:
- Рівень логів можна задати через `.env` або змінну середовища `LOG_LEVEL` (наприклад: `INFO`, `DEBUG`).
- При зміні стану ринку для символу Stage1 видає INFO‑подію із знімком ефективних порогів (volZ/vwap/gates).


## 📑 Контракт Stage1 → Stage2 → UI (Ключі і дефолти)

Канонічні ключі (див. `config/config.py`):

- Stage1Signal:
    - symbol (str) — інструмент.
    - signal (str) — "ALERT" або "NORMAL".
    - trigger_reasons (list[str]) — уніфіковані теги (див. TRIGGER_NAME_MAP).
    - stats (dict) — обовʼязково містить: current_price, atr, vwap, daily_low, daily_high; опційно rsi, volume_z, bid_ask_spread тощо.
    - raw_trigger_reasons (list[str], опц.) — сирі теги до нормалізації.
    - thresholds (dict, опц.) — застосовані пороги.

- Stage2Output:
    - market_context (dict) — scenario, triggers, key_levels{immediate_support, immediate_resistance, next_major_level}, key_levels_meta{band_pct, confidence, mid, dist_to_support_pct, dist_to_resistance_pct}.
    - recommendation (str) — наприклад: BUY_IN_DIPS, SELL_ON_RALLIES, AVOID, HOLD, RANGE_TRADE, AVOID_HIGH_RISK, WAIT_FOR_CONFIRMATION.
    - confidence_metrics (dict) — breakout_probability, pullback_probability, composite_confidence.
    - risk_parameters (dict) — sl_level, tp_targets[], risk_reward_ratio.
    - anomaly_detection (dict) — volume_spike, wide_spread, vwap_whipsaw …



## 📦 Приклад структури стану активу (K_*)

Нижче наведено компактний приклад одного елемента стану активу у форматі з канонічними ключами K_* (див. `config/config.py`). Це орієнтир для UI/логів/обробників:

```python
from config.config import (
    K_SYMBOL, K_SIGNAL, K_TRIGGER_REASONS, K_STATS,
    K_MARKET_CONTEXT, K_RECOMMENDATION,
    K_CONFIDENCE_METRICS, K_RISK_PARAMETERS,
)

asset_state = {
    K_SYMBOL: "btcusdt",
    K_SIGNAL: "ALERT_BUY",            # або "ALERT_SELL" / "NORMAL"
    K_TRIGGER_REASONS: ["volume_spike", "breakout_up"],
    K_STATS: {
        "current_price": 65234.5,
        "atr": 0.0123,
        "vwap": 65180.2,
        "daily_low": 64010.0,
        "daily_high": 65890.0,
        # опційно: "rsi": 62.1, "volume_z": 3.4, ...
    },
    # Результати Stage2 (за наявності):
    K_MARKET_CONTEXT: {"scenario": "BREAKOUT", "key_levels": {"immediate_resistance": 65500}},
    K_RECOMMENDATION: "BUY_IN_DIPS",
    K_CONFIDENCE_METRICS: {"composite_confidence": 0.81},
    K_RISK_PARAMETERS: {"sl_level": 64500.0, "tp_targets": [66000.0, 67200.0]},
    # Додаткові службові поля (не K_*):
    "state": "alert",
    "stage2": True,
    "stage2_status": "completed",
    "last_updated": "2025-09-21T12:34:56Z",
    "hints": ["Розширення діапазону на високому обсязі"],
}
```

Рекомендації:
- Завжди користуйтесь K_* константами замість хардкод-рядків.
- Поле `stats` має містити принаймні `current_price`, `atr`, `vwap`, `daily_low`, `daily_high`.
- Після обробки Stage2 очікуються заповнені `market_context`, `recommendation`, `confidence_metrics`, `risk_parameters`.

Примітки:
- Усі строкові ключі централізовані: K_SYMBOL, K_SIGNAL, K_STATS, K_TRIGGER_REASONS, K_MARKET_CONTEXT, K_RECOMMENDATION, K_CONFIDENCE_METRICS, K_RISK_PARAMETERS, K_ANOMALY_DETECTION.
- Використовуйте ці константи у новому коді замість хардкод‑рядків.



