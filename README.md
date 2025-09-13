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


