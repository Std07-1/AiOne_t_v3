# stage2/config.py
# Єдиний конфіг для Stage2 аналізу та калібрування

ASSET_CLASS_MAPPING = {
    "spot": [
        ".*BTC.*",
        ".*ETH.*",
        ".*XRP.*",
        ".*LTC.*",
        ".*ADA.*",
        ".*SOL.*",
        ".*DOT.*",
        ".*LINK.*",
    ],
    "futures": [
        ".*BTCUSD.*",
        ".*ETHUSD.*",
        ".*XRPUSD.*",
        ".*LTCUSD.*",
        ".*ADAUSD.*",
        ".*SOLUSD.*",
        ".*DOTUSD.*",
        ".*LINKUSD.*",
    ],
    "meme": [
        ".*DOGE.*",
        ".*SHIB.*",
        ".*PEPE.*",
        ".*FLOKI.*",
        ".*KISHU.*",
        ".*HOGE.*",
        ".*SAITAMA.*",
    ],
    "defi": [
        ".*UNI.*",
        ".*AAVE.*",
        ".*COMP.*",
        ".*MKR.*",
        ".*CRV.*",
        ".*SUSHI.*",
        ".*YFI.*",
        ".*LDO.*",
        ".*RUNE.*",
    ],
    "nft": [".*APE.*", ".*SAND.*", ".*MANA.*", ".*BLUR.*", ".*RARI.*"],
    "metaverse": [".*ENJ.*", ".*AXS.*", ".*GALA.*", ".*ILV.*", ".*HIGH.*"],
    "ai": [".*AGIX.*", ".*FET.*", ".*OCEAN.*", ".*RNDR.*", ".*AKT.*"],
    "stable": [".*USDT$", ".*BUSD$", ".*DAI$", ".*USD$", ".*FDUSD$"],
}

STAGE2_CONFIG = {
    "asset_class_mapping": ASSET_CLASS_MAPPING,
    "priority_levels": {  # Додаємо пріоритети для класів
        "futures": 1.0,  # Найвищий пріоритет для ф'ючерсів
        "spot": 0.7,  # Високий пріоритет для споту
        "meme": 0.6,  # Середній пріоритет для мем-коінів
        "defi": 0.6,  # Середній пріоритет для DeFi
        "nft": 0.5,  # Низький пріоритет для NFT
        "metaverse": 0.4,  # Низький пріоритет для метавсесвіту
        "ai": 0.3,  # Низький пріоритет для AI
        "stable": 0.2,  # Низький пріоритет для стейблкоїнів
        "default": 0.1,  # Для невизначених класів
    },
    "low_gate": 0.0015,  # Нижня межа ATR/price для «тихого» ринку (0.15%)
    "high_gate": 0.0134,  # Верхня межа ATR/price для «високої» волатильності (1.34%)
    "atr_target": [0.3, 1.5],  # Цільовий ATR у відсотках від ціни
    "rsi_period": 10,  # Період RSI (10)
    "volume_window": 30,  # Вікно обсягу для аналізу (30)
    "atr_period": 10,  # Період ATR (10)
    "volume_z_threshold": 1.2,  # сплеск обсягу ≥1.2σ (уніфіковано) (1.2)
    "rsi_oversold": 30.0,  # Рівень перепроданності RSI (30%)
    "rsi_overbought": 70.0,  # Рівень перекупленості RSI (70%)
    "stoch_oversold": 20.0,  # Рівень перепроданності стохастика (20%)
    "stoch_overbought": 80.0,  # Рівень перекупленості стохастика (80%)
    "macd_threshold": 0.02,  # Поріг MACD для сигналів (0.02)
    "ema_cross_threshold": 0.005,  # Поріг EMA перетину (0.005)
    "vwap_threshold": 0.001,  # 0.1% відхилення (0.001)
    "min_volume_usd": 10000,  # Мінімальний об'єм в USD (10000)
    "min_atr_percent": 0.002,  # Мінімальний ATR у відсотках (0.002)
    "exclude_hours": [0, 1, 2, 3],  # Години, які виключаємо з аналізу
    "cooldown_period": 300,  # 5 хвилин охолодження між запитами
    "max_correlation_threshold": 0.85,  # Максимальний поріг кореляції
    "tp_mult": 3.0,  # Множник для Take Profit
    "sl_mult": 1.0,  # Множник для Stop Loss
    "min_risk_reward": 2.0,  # Мінімальне співвідношення ризику до винагороди
    "entry_spread_percent": 0.05,  # Відсоток спреду для входу
    "factor_weights": {
        "volume": 0.25,  # Вага обсягу
        "rsi": 0.18,  # Вага RSI
        "macd": 0.20,  # Вага MACD
        "ema_cross": 0.22,  # Вага EMA перетину
        "stochastic": 0.15,  # Вага стохастика
        "atr": 0.20,  # Вага ATR
        "orderbook": 0.20,  # Вага глибини orderbook
        "vwap": 0.25,  # Вага VWAP
        "velocity": 0.18,  # Вага швидкості
        "volume_profile": 0.20,  # Вага об'ємного профілю
    },
    "min_cluster": 4,  # Мінімальна кількість кластерів
    "min_confidence": 0.75,  # Мінімальна впевненість сигналу
    "context_min_correlation": 0.7,  # Мінімальна кореляція для контексту
    "max_concurrent": 5,  # Максимум 5 паралельних калібрувань
    # залишаємо sr_window, tp_buffer_eps для SR/TP сумісності
    "sr_window": 50,  # Вікно для SR розрахунків (50)
    "tp_buffer_eps": 0.0005,  # Буфер для TP розрахунків (0.0005)
    "metric_weights": {
        "sharpe": 0.4,  # Вага Sharpe Ratio
        "sortino": 0.3,  # Вага Sortino Ratio
        "win_rate": 0.3,  # Вага Win Rate
        "profit_factor": 0.3,  # Вага Profit Factor
    },  # Ваги метрик для калібрування
    "min_trades_for_calibration": 5,  # Мінімальна кількість угод для калібрування
    "n_trials": 20,  # Кількість trial для Optuna
    "run_calibration": True,  # Запускати калібрування
    # Вимикачі
    # за замовчуванням вмикаємо QDE та fallback-спадкоємність
    "switches": {
        # Головне:
        "use_qde": True,  # QDE як основний аналіз
        "legacy_fallback": False,  # використовувати стару логіку як fallback
        "fallback_when": {  # коли падати у legacy
            "qde_scenario_uncertain": True,  # якщо QDE => UNCERTAIN
            "qde_conf_lt": 0.55,  # або якщо QDE confidence нижче порогу
        },
        # Вмикання/вимикання підсистем Stage2:
        "analysis": {
            "context": True,
            "anomaly": True,
            "confidence": True,
            "risk": True,
            "recommendation": True,
            "narrative": True,
        },
        # Гранулярні вимикачі рівнів QDE:
        "qde_levels": {"micro": True, "meso": True, "macro": True},
        # Тригери Stage1 (перевіряються у AssetMonitorStage1):
        "triggers": {
            "volume_spike": True,
            "breakout": True,
            "volatility_spike": True,
            "rsi": True,
            "vwap_deviation": True,
        },
    },
}

OPTUNA_PARAM_RANGES = {
    "volume_z_threshold": (0.5, 3.0),  # Поріг Z-score обсягу
    "rsi_oversold": (15.0, 40.0),  # Рівень перепроданності RSI
    "rsi_overbought": (60.0, 85.0),  # Рівень перекупленості RSI
    "tp_mult": (0.5, 5.0),  # Множник для Take Profit
    "sl_mult": (0.5, 5.0),  # Множник для Stop Loss
    "min_confidence": (0.3, 0.9),  # Мінімальна впевненість сигналу
    "min_score": (1.0, 2.5),  # Мінімальний скор для сигналу
    "min_atr_ratio": (0.0005, 0.01),  # Мінімальний ATR у відсотках від ціни
    "volatility_ratio": (0.5, 1.2),  # Відношення волатильності до ATR
    "fallback_confidence": (0.4, 0.7),  # Впевненість для fallback сигналів
    "atr_target": (0.3, 1.5),  # Цільовий ATR у відсотках від ціни
    "low_gate": (0.002, 0.015),  # Нижня межа ATR/price для «тихого» ринку
    "high_gate": (0.005, 0.03),  # Верхня межа ATR/price для «високої» волатильності
}
