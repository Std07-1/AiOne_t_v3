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

Внутрішній `MetricsCollector` агрегує лічильники та таймінги (Stage1/Stage2 цикли, WS повідомлення). Якщо встановлено `prometheus_client`, автоматично піднімається HTTP endpoint (`:8001/metrics`).

Метрики включають (приклади):
- `stage1_cycle_seconds`, `stage2_cycle_seconds`
- `ws_messages_total`, `symbols_filtered_total`
- `prefilter_elapsed_seconds`

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



