# ruff: noqa: N999
# Порогові значення підібрано з урахуванням типу активу та його волатильності.
# Мега-коригування (BTC, ETH) мають нижчі пороги (RSI ~70/30,
# невисокий vol_z) через стабільніші обсяги.
# Мем-коїни (DOGE, SHIB, PEPE, FLOKI тощо) отримали ширші межі (RSI ~80/20)
# і вищий vol_z_threshold, оскільки їх обсяги та ціни сильно коливаються.
# Активи середньої капіталізації налаштовано на середні значення
# (RSI ~75/25, vol_z ~2.5σ), а дрібні та схильні до маніпуляцій токени –
# на найбільші пороги для фільтрації шумів.


from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def get_top100_threshold(symbol: str) -> dict[str, Any] | None:
    """Повертає мапу дефолтних порогів для конкретного символу (Top100),
    сумісну з Thresholds.from_mapping.

    Примітки:
      - В цьому модулі конфіг має ключі типу vol_z_threshold, rsi_overbought/oversold,
        додатково atr_pct_min, vwap_deviation (які не обов'язкові для Thresholds).
      - Ми приводимо до очікуваних ключів: volume_z_threshold, rsi_*; додаємо базові
        low_gate/high_gate/atr_target, якщо не задані.
    """
    try:
        if not symbol or not isinstance(symbol, str):
            return None
        sym_l = symbol.lower()
        cfg = TOP100_THRESHOLDS.get(sym_l)
        if not cfg:
            return None
        # Базові загальні значення (можете скоригувати під актив при потребі)
        mapping = {
            "symbol": symbol.upper(),
            "low_gate": 0.006,
            "high_gate": 0.015,
            "atr_target": 0.5,
        }
        # Перенесення ключів у канонічний формат
        if "vol_z_threshold" in cfg:
            mapping["volume_z_threshold"] = cfg.get("vol_z_threshold")
        if "rsi_overbought" in cfg:
            mapping["rsi_overbought"] = cfg.get("rsi_overbought")
        if "rsi_oversold" in cfg:
            mapping["rsi_oversold"] = cfg.get("rsi_oversold")
        # Додаткові розширені ключі (пас-тру) для сучасної логіки порогів
        #  • atr_pct_min → min_atr_percent (бек-сов сумісна назва)
        #  • vwap_deviation, signal_thresholds, state_overrides, meta — без змін
        if "atr_pct_min" in cfg:
            mapping["min_atr_percent"] = cfg.get("atr_pct_min")
        if "vwap_deviation" in cfg:
            mapping["vwap_deviation"] = cfg.get("vwap_deviation")
        st = cfg.get("signal_thresholds")
        if isinstance(st, dict):
            mapping["signal_thresholds"] = st
            # Синхронізація: якщо верхній vwap_deviation не заданий, але є
            # signal_thresholds.vwap_deviation.threshold — підтягуємо його як базовий.
            try:
                if "vwap_deviation" not in mapping:
                    vwap_thr = st.get("vwap_deviation", {}).get("threshold")
                    if isinstance(vwap_thr, (int, float)):
                        mapping["vwap_deviation"] = float(vwap_thr)
            except Exception:
                # не критично: пропускаємо, логи нижче покажуть включені ключі
                pass
            # Аналогічно: якщо верхній volume_z_threshold відсутній —
            # візьмемо його зі signal_thresholds.volume_spike.z_score (як дефолт).
            try:
                if "volume_z_threshold" not in mapping:
                    z_thr = st.get("volume_spike", {}).get("z_score")
                    if isinstance(z_thr, (int, float)):
                        mapping["volume_z_threshold"] = float(z_thr)
            except Exception:
                pass
        so = cfg.get("state_overrides")
        if isinstance(so, dict):
            mapping["state_overrides"] = so
        meta = cfg.get("meta")
        if isinstance(meta, dict):
            mapping["meta"] = meta

        logger.debug(
            "get_top100_threshold: застосовано мапінг",
            extra={
                "symbol": symbol.upper(),
                "included_keys": sorted(list(mapping.keys())),
                "source_keys": sorted(list(cfg.keys())),
            },
        )

        return mapping
    except Exception:
        return None


# ───────────────────────────── TOP-10: розширені пороги ─────────────────────────────
# Логіка:
#  • mega-cap (BTC, ETH): нижчі vol_z / тісніші VWAP-пороги; суворіші ретести на breakout.
#  • high-beta (SOL, AVAX, LINK): середні vol_z, ширший ATR-band для breakout.
#  • noisy/meme (DOGE, частково XRP): підвищені vol_z / RSI; ширші VWAP-відхилення.
#  • нові/агресивні (TON): високі vol_z/ATR гейти, але допускаємо швидкі breakouts.
#
# Пояснення ключів:
#  • atr_pct_min — мінімальна волатильність (ATR% від ціни), нижче якої сигнали занижуються/ігноруються.
#  • vwap_deviation — базовий поріг відхилення від VWAP (частка від ціни).
#  • signal_thresholds.* — детальні пороги для Stage1 тригерів.
#  • state_overrides — мультиплікатори/дельти до порогів залежно від поточного стану (range, trend, high_vol).


TOP100_THRESHOLDS: dict[str, dict[str, Any]] = {
    "btcusdt": {
        # --- Загальні параметри ---
        "vol_z_threshold": 2.0,  # сплеск обсягу ≥2.0σ, помірний, бо BTC має великі стабільні обсяги
        "rsi_overbought": 70.0,
        "rsi_oversold": 30.0,
        "atr_pct_min": 0.005,  # мінімальна волатильність ~0.5% для врахування сигналів
        "vwap_deviation": 0.010,  # відхилення 1% від VWAP як сигнал
        # --- Параметри за типами сигналів ---
        "signal_thresholds": {
            "volume_spike": {
                "z_score": 2.2,  # сильний сплеск для BTC вважаємо від 2.2σ
                "min_notional_usd": 5_000_000.0,  # мінімальний обсяг 5 млн USD
                "cooldown_bars": 2,  # мінімум 2 бари між сплесками
            },
            "rsi_trigger": {
                "overbought": 72.0,  # трохи суворіше для BTC
                "oversold": 28.0,
                "divergence_strength": 1.2,  # помірна вимога до сили дивергенції
            },
            "breakout": {
                "band_pct_atr": 0.80,  # відстань до рівня в % ATR
                "min_retests": 2,  # мінімум 2 ретести підтверджень рівня
                "confirm_bars": 2,  # підтверджуючі бари для надійності
            },
            "vwap_deviation": {
                "threshold": 0.012,  # відхилення 1.2% від VWAP як сигнал
                "duration_bars": 3,
            },  # тривалість підтвердження в барах, зберігається хоча б 3 хвилини
            "atr_volatility": {
                "low_gate_pct": 0.40,  # 0.4% ATR/price - нижня межа волатильності для сигналів
                "high_gate_pct": 1.20,  # 1.2% ATR/price - верхня межа волатильності для сигналів
            },  # у % від ціни
        },
        # --- Специфічні налаштування для BTC ---
        "state_overrides": {
            "range_bound": {
                "vwap_deviation": +0.002,  # трохи суворіше для range-bound, для уникнення шумів
                "signal_thresholds.breakout.band_pct_atr": +0.10,  # трохи ширше для breakout, для уникнення фальшивих спрацьовувань
            },
            "trend_strong": {
                "rsi_trigger.overbought": +2.0,  # трохи вищий поріг для тренду, для уникнення фальшивих спрацьовувань
                "rsi_trigger.oversold": -2.0,  # трохи нижчий поріг для тренду, для уникнення фальшивих спрацьовувань
            },
            "high_volatility": {
                "volume_spike.z_score": +0.2,  # трохи вищий поріг для шумних періодів
                "vwap_deviation": +0.002,
            },  # трохи суворіше для шумних періодів
        },
        # --- Метадані для аналізу стану ---
        "meta": {
            "class": "mega_cap",  # клас активу, mega-cap
            "sensitivity": "low_noise",
        },  # низька чутливість до шумів
    },
    "ethusdt": {
        "vol_z_threshold": 2.0,
        "rsi_overbought": 70.0,
        "rsi_oversold": 30.0,
        "atr_pct_min": 0.005,
        "vwap_deviation": 0.010,
        "signal_thresholds": {
            "volume_spike": {
                "z_score": 2.1,
                "min_notional_usd": 3_000_000.0,
                "cooldown_bars": 2,
            },
            "rsi_trigger": {
                "overbought": 71.0,
                "oversold": 29.0,
                "divergence_strength": 1.2,
            },
            "breakout": {"band_pct_atr": 0.85, "min_retests": 2, "confirm_bars": 2},
            "vwap_deviation": {"threshold": 0.012, "duration_bars": 3},
            "atr_volatility": {"low_gate_pct": 0.45, "high_gate_pct": 1.30},
        },
        "state_overrides": {
            "range_bound": {"vwap_deviation": +0.002},
            "trend_strong": {"breakout.confirm_bars": -1},
            "high_volatility": {"volume_spike.z_score": +0.2},
        },
        "meta": {"class": "mega_cap", "sensitivity": "low_noise"},
    },
    "solusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 80.0,
        "rsi_oversold": 20.0,
        "atr_pct_min": 0.008,
        "vwap_deviation": 0.020,
        "signal_thresholds": {
            "volume_spike": {
                "z_score": 2.6,
                "min_notional_usd": 1_800_000.0,
                "cooldown_bars": 2,
            },
            "rsi_trigger": {
                "overbought": 82.0,
                "oversold": 18.0,
                "divergence_strength": 1.3,
            },
            "breakout": {"band_pct_atr": 0.95, "min_retests": 2, "confirm_bars": 1},
            "vwap_deviation": {"threshold": 0.022, "duration_bars": 2},
            "atr_volatility": {"low_gate_pct": 0.60, "high_gate_pct": 1.60},
        },
        "state_overrides": {
            "range_bound": {"breakout.band_pct_atr": +0.10},
            "trend_strong": {"vwap_deviation": -0.002},
            "high_volatility": {"volume_spike.z_score": +0.3, "vwap_deviation": +0.003},
        },
        "meta": {"class": "high_beta", "sensitivity": "fast_trend"},
    },
    "dogeusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 80.0,
        "rsi_oversold": 20.0,
        "atr_pct_min": 0.010,
        "vwap_deviation": 0.040,
        "signal_thresholds": {
            "volume_spike": {
                "z_score": 3.2,
                "min_notional_usd": 1_000_000.0,
                "cooldown_bars": 3,
            },
            "rsi_trigger": {
                "overbought": 83.0,
                "oversold": 17.0,
                "divergence_strength": 1.6,
            },
            "breakout": {"band_pct_atr": 1.10, "min_retests": 1, "confirm_bars": 2},
            "vwap_deviation": {"threshold": 0.045, "duration_bars": 2},
            "atr_volatility": {"low_gate_pct": 0.70, "high_gate_pct": 2.20},
        },
        "state_overrides": {
            "range_bound": {"volume_spike.z_score": +0.2},
            "trend_strong": {"breakout.confirm_bars": -1},
            "high_volatility": {"vwap_deviation": +0.005},
        },
        "meta": {"class": "meme", "sensitivity": "noise_resistant"},
    },
    "xrpusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 80.0,
        "rsi_oversold": 20.0,
        "atr_pct_min": 0.008,
        "vwap_deviation": 0.030,
        "signal_thresholds": {
            "volume_spike": {
                "z_score": 2.8,
                "min_notional_usd": 1_200_000.0,
                "cooldown_bars": 2,
            },
            "rsi_trigger": {
                "overbought": 82.0,
                "oversold": 18.0,
                "divergence_strength": 1.35,
            },
            "breakout": {"band_pct_atr": 1.00, "min_retests": 1, "confirm_bars": 2},
            "vwap_deviation": {"threshold": 0.032, "duration_bars": 2},
            "atr_volatility": {"low_gate_pct": 0.60, "high_gate_pct": 1.80},
        },
        "state_overrides": {
            "range_bound": {"breakout.min_retests": +1},
            "trend_strong": {
                "rsi_trigger.overbought": +2.0,
                "rsi_trigger.oversold": -2.0,
            },
            "high_volatility": {"vwap_deviation": +0.003},
        },
        "meta": {"class": "noisy_liquidity", "sensitivity": "whipsaw_filter"},
    },
    "bnbusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 75.0,
        "rsi_oversold": 25.0,
        "atr_pct_min": 0.007,
        "vwap_deviation": 0.018,
        "signal_thresholds": {
            "volume_spike": {
                "z_score": 2.4,
                "min_notional_usd": 1_500_000.0,
                "cooldown_bars": 2,
            },
            "rsi_trigger": {
                "overbought": 76.0,
                "oversold": 24.0,
                "divergence_strength": 1.25,
            },
            "breakout": {"band_pct_atr": 0.90, "min_retests": 2, "confirm_bars": 2},
            "vwap_deviation": {"threshold": 0.020, "duration_bars": 2},
            "atr_volatility": {"low_gate_pct": 0.55, "high_gate_pct": 1.40},
        },
        "state_overrides": {
            "range_bound": {"vwap_deviation": +0.003},
            "trend_strong": {"breakout.confirm_bars": -1},
            "high_volatility": {"volume_spike.z_score": +0.2},
        },
        "meta": {"class": "large_cap", "sensitivity": "balanced"},
    },
    "1000shibusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 80.0,
        "rsi_oversold": 20.0,
        "atr_pct_min": 0.015,
        "vwap_deviation": 0.05,
    },
    "adausdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 75.0,
        "rsi_oversold": 25.0,
        "atr_pct_min": 0.007,
        "vwap_deviation": 0.020,
        "signal_thresholds": {
            "volume_spike": {
                "z_score": 2.6,
                "min_notional_usd": 1_000_000.0,
                "cooldown_bars": 2,
            },
            "rsi_trigger": {
                "overbought": 77.0,
                "oversold": 23.0,
                "divergence_strength": 1.3,
            },
            "breakout": {"band_pct_atr": 0.95, "min_retests": 2, "confirm_bars": 2},
            "vwap_deviation": {"threshold": 0.022, "duration_bars": 2},
            "atr_volatility": {"low_gate_pct": 0.55, "high_gate_pct": 1.50},
        },
        "state_overrides": {
            "range_bound": {"breakout.min_retests": +1},
            "trend_strong": {"vwap_deviation": -0.002},
            "high_volatility": {"volume_spike.z_score": +0.2},
        },
        "meta": {"class": "large_cap", "sensitivity": "balanced"},
    },
    "trxusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 72.0,
        "rsi_oversold": 28.0,
        "atr_pct_min": 0.006,
        "vwap_deviation": 0.02,
    },
    "maticusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 75.0,
        "rsi_oversold": 25.0,
        "atr_pct_min": 0.007,
        "vwap_deviation": 0.02,
    },
    "avaxusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 75.0,
        "rsi_oversold": 25.0,
        "atr_pct_min": 0.008,
        "vwap_deviation": 0.020,
        "signal_thresholds": {
            "volume_spike": {
                "z_score": 2.7,
                "min_notional_usd": 1_200_000.0,
                "cooldown_bars": 2,
            },
            "rsi_trigger": {
                "overbought": 77.0,
                "oversold": 23.0,
                "divergence_strength": 1.35,
            },
            "breakout": {"band_pct_atr": 1.00, "min_retests": 2, "confirm_bars": 1},
            "vwap_deviation": {"threshold": 0.022, "duration_bars": 2},
            "atr_volatility": {"low_gate_pct": 0.60, "high_gate_pct": 1.70},
        },
        "state_overrides": {
            "range_bound": {"breakout.band_pct_atr": +0.10},
            "trend_strong": {
                "rsi_trigger.overbought": +2.0,
                "rsi_trigger.oversold": -2.0,
            },
            "high_volatility": {"volume_spike.z_score": +0.2},
        },
        "meta": {"class": "high_beta", "sensitivity": "fast_trend"},
    },
    "linkusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 78.0,
        "rsi_oversold": 22.0,
        "atr_pct_min": 0.008,
        "vwap_deviation": 0.020,
        "signal_thresholds": {
            "volume_spike": {
                "z_score": 2.7,
                "min_notional_usd": 1_200_000.0,
                "cooldown_bars": 2,
            },
            "rsi_trigger": {
                "overbought": 80.0,
                "oversold": 20.0,
                "divergence_strength": 1.35,
            },
            "breakout": {"band_pct_atr": 0.95, "min_retests": 2, "confirm_bars": 1},
            "vwap_deviation": {"threshold": 0.022, "duration_bars": 2},
            "atr_volatility": {"low_gate_pct": 0.60, "high_gate_pct": 1.60},
        },
        "state_overrides": {
            "range_bound": {"breakout.band_pct_atr": +0.10},
            "trend_strong": {"breakout.confirm_bars": -1},
            "high_volatility": {"vwap_deviation": +0.003},
        },
        "meta": {"class": "high_beta", "sensitivity": "trend_follow"},
    },
    "suiusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 75.0,
        "rsi_oversold": 25.0,
        "atr_pct_min": 0.008,
        "vwap_deviation": 0.02,
    },
    "tonusdt": {
        # --- Загальні параметри ---
        "vol_z_threshold": 2.8,  # вище, щоб уникати фальшивих сплесків
        "rsi_overbought": 75.0,
        "rsi_oversold": 25.0,
        "atr_pct_min": 0.009,  # мінімальна волатильність ~0.9%
        "vwap_deviation": 0.018,  # відхилення 1.8% від VWAP як сигнал
        # --- Параметри за типами сигналів ---
        "signal_thresholds": {
            "volume_spike": {
                "z_score": 3.0,  # TON часто має шумні обсяги, потрібен вищий поріг
                "min_notional_usd": 1_500_000.0,
                "cooldown_bars": 2,  # мінімум 2 бари між сплесками
            },
            "rsi_trigger": {
                "overbought": 78.0,
                "oversold": 22.0,
                "divergence_strength": 1.5,  # жорсткіше, бо багато фейкових дивергенцій
            },
            "breakout": {
                "band_pct": 1.0,  # ATR*1.0 → ширший допуск
                "min_retests": 1,  # можна пропускати з одним підтвердженням
                "confirm_bars": 1,  # підтверджуючий бар для швидких рухів
            },
            "vwap_deviation": {
                "threshold": 0.025,  # 2.5% відхилення
                "duration_bars": 2,  # тривалість підтвердження в барах, зберігається хоча б 2 хвилини
            },
            "atr_volatility": {
                "low_gate": 0.6,  # 0.6% ATR/price - нижня межа волатильності для сигналів
                "high_gate": 2.0,  # високі пороги через агресивні рухи
            },
        },
        # --- Метадані для аналізу стану ---
        "meta": {
            "class": "mid_cap_new",  # клас активу, mid-cap, новий
            "sensitivity": "high_volatility_high_risk",  # висока чутливість до волатильності і ризиків
        },
    },
    "nearusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 75.0,
        "rsi_oversold": 25.0,
        "atr_pct_min": 0.007,
        "vwap_deviation": 0.02,
    },
    "bchusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 78.0,
        "rsi_oversold": 22.0,
        "atr_pct_min": 0.010,
        "vwap_deviation": 0.03,
    },
    "opusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 75.0,
        "rsi_oversold": 25.0,
        "atr_pct_min": 0.008,
        "vwap_deviation": 0.02,
    },
    "arbusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 75.0,
        "rsi_oversold": 25.0,
        "atr_pct_min": 0.008,
        "vwap_deviation": 0.02,
    },
    "1000pepeusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 85.0,
        "rsi_oversold": 15.0,
        "atr_pct_min": 0.015,
        "vwap_deviation": 0.05,
    },
    "1000flokusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 85.0,
        "rsi_oversold": 15.0,
        "atr_pct_min": 0.015,
        "vwap_deviation": 0.05,
    },
    "atomusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 73.0,
        "rsi_oversold": 27.0,
        "atr_pct_min": 0.007,
        "vwap_deviation": 0.02,
    },
    "ltcusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 70.0,
        "rsi_oversold": 30.0,
        "atr_pct_min": 0.006,
        "vwap_deviation": 0.02,
    },
    "xlmusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 78.0,
        "rsi_oversold": 22.0,
        "atr_pct_min": 0.008,
        "vwap_deviation": 0.02,
    },
    "filusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 75.0,
        "rsi_oversold": 25.0,
        "atr_pct_min": 0.008,
        "vwap_deviation": 0.02,
    },
    "uniusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 74.0,
        "rsi_oversold": 23.0,
        "atr_pct_min": 0.007,
        "vwap_deviation": 0.02,
    },
    "aaveusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 74.0,
        "rsi_oversold": 23.0,
        "atr_pct_min": 0.007,
        "vwap_deviation": 0.02,
    },
    "hbarusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 75.0,
        "rsi_oversold": 25.0,
        "atr_pct_min": 0.008,
        "vwap_deviation": 0.02,
    },
    "dotusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 74.0,
        "rsi_oversold": 24.0,
        "atr_pct_min": 0.007,
        "vwap_deviation": 0.02,
    },
    "manausdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 76.0,
        "rsi_oversold": 24.0,
        "atr_pct_min": 0.008,
        "vwap_deviation": 0.03,
    },
    "sandusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 76.0,
        "rsi_oversold": 24.0,
        "atr_pct_min": 0.008,
        "vwap_deviation": 0.03,
    },
    "rndrusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 77.0,
        "rsi_oversold": 23.0,
        "atr_pct_min": 0.009,
        "vwap_deviation": 0.03,
    },
    "egldusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 75.0,
        "rsi_oversold": 25.0,
        "atr_pct_min": 0.009,
        "vwap_deviation": 0.03,
    },
    "twtusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 80.0,
        "rsi_oversold": 20.0,
        "atr_pct_min": 0.012,
        "vwap_deviation": 0.04,
    },
    "injusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 77.0,
        "rsi_oversold": 23.0,
        "atr_pct_min": 0.009,
        "vwap_deviation": 0.03,
    },
    "dydxusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 77.0,
        "rsi_oversold": 23.0,
        "atr_pct_min": 0.009,
        "vwap_deviation": 0.03,
    },
    "wldusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 80.0,
        "rsi_oversold": 20.0,
        "atr_pct_min": 0.012,
        "vwap_deviation": 0.04,
    },
    "apeusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 78.0,
        "rsi_oversold": 22.0,
        "atr_pct_min": 0.009,
        "vwap_deviation": 0.03,
    },
    "ldousdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 76.0,
        "rsi_oversold": 24.0,
        "atr_pct_min": 0.008,
        "vwap_deviation": 0.03,
    },
    "crvusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 76.0,
        "rsi_oversold": 24.0,
        "atr_pct_min": 0.008,
        "vwap_deviation": 0.03,
    },
    "minausdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 75.0,
        "rsi_oversold": 25.0,
        "atr_pct_min": 0.008,
        "vwap_deviation": 0.03,
    },
    "ftmusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 77.0,
        "rsi_oversold": 23.0,
        "atr_pct_min": 0.009,
        "vwap_deviation": 0.03,
    },
    "algousdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 75.0,
        "rsi_oversold": 25.0,
        "atr_pct_min": 0.008,
        "vwap_deviation": 0.03,
    },
    "axsusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 78.0,
        "rsi_oversold": 22.0,
        "atr_pct_min": 0.010,
        "vwap_deviation": 0.04,
    },
    "grtusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 78.0,
        "rsi_oversold": 22.0,
        "atr_pct_min": 0.010,
        "vwap_deviation": 0.04,
    },
    "thetausdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 74.0,
        "rsi_oversold": 26.0,
        "atr_pct_min": 0.008,
        "vwap_deviation": 0.03,
    },
    "chzusdt": {
        "vol_z_threshold": 2.5,
        "rsi_overbought": 74.0,
        "rsi_oversold": 26.0,
        "atr_pct_min": 0.008,
        "vwap_deviation": 0.03,
    },
    "galausdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 80.0,
        "rsi_oversold": 20.0,
        "atr_pct_min": 0.012,
        "vwap_deviation": 0.04,
    },
    "compusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 74.0,
        "rsi_oversold": 26.0,
        "atr_pct_min": 0.010,
        "vwap_deviation": 0.03,
    },
    "mkrusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 74.0,
        "rsi_oversold": 26.0,
        "atr_pct_min": 0.010,
        "vwap_deviation": 0.03,
    },
    "snxusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 74.0,
        "rsi_oversold": 26.0,
        "atr_pct_min": 0.010,
        "vwap_deviation": 0.03,
    },
    "icpusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 76.0,
        "rsi_oversold": 24.0,
        "atr_pct_min": 0.010,
        "vwap_deviation": 0.04,
    },
    "vetusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 76.0,
        "rsi_oversold": 24.0,
        "atr_pct_min": 0.010,
        "vwap_deviation": 0.04,
    },
    "stxusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 77.0,
        "rsi_oversold": 23.0,
        "atr_pct_min": 0.010,
        "vwap_deviation": 0.04,
    },
    "eosusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 75.0,
        "rsi_oversold": 25.0,
        "atr_pct_min": 0.010,
        "vwap_deviation": 0.04,
    },
    "woousdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 78.0,
        "rsi_oversold": 22.0,
        "atr_pct_min": 0.012,
        "vwap_deviation": 0.04,
    },
    "luncusdt": {
        "vol_z_threshold": 3.5,
        "rsi_overbought": 85.0,
        "rsi_oversold": 15.0,
        "atr_pct_min": 0.015,
        "vwap_deviation": 0.05,
    },
    "iotausdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 75.0,
        "rsi_oversold": 25.0,
        "atr_pct_min": 0.010,
        "vwap_deviation": 0.04,
    },
    "flowusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 75.0,
        "rsi_oversold": 25.0,
        "atr_pct_min": 0.010,
        "vwap_deviation": 0.04,
    },
    "imxusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 77.0,
        "rsi_oversold": 23.0,
        "atr_pct_min": 0.010,
        "vwap_deviation": 0.04,
    },
    "xtzusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 75.0,
        "rsi_oversold": 25.0,
        "atr_pct_min": 0.010,
        "vwap_deviation": 0.04,
    },
    "cfxusdt": {
        "vol_z_threshold": 3.5,
        "rsi_overbought": 80.0,
        "rsi_oversold": 20.0,
        "atr_pct_min": 0.012,
        "vwap_deviation": 0.05,
    },
    "maskusdt": {
        "vol_z_threshold": 3.5,
        "rsi_overbought": 80.0,
        "rsi_oversold": 20.0,
        "atr_pct_min": 0.012,
        "vwap_deviation": 0.05,
    },
    "gmxusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 78.0,
        "rsi_oversold": 22.0,
        "atr_pct_min": 0.010,
        "vwap_deviation": 0.04,
    },
    "fxsusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 74.0,
        "rsi_oversold": 26.0,
        "atr_pct_min": 0.010,
        "vwap_deviation": 0.04,
    },
    "cakeusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 76.0,
        "rsi_oversold": 24.0,
        "atr_pct_min": 0.010,
        "vwap_deviation": 0.04,
    },
    "kavausdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 76.0,
        "rsi_oversold": 24.0,
        "atr_pct_min": 0.010,
        "vwap_deviation": 0.04,
    },
    "blzusdt": {
        "vol_z_threshold": 3.5,
        "rsi_overbought": 82.0,
        "rsi_oversold": 18.0,
        "atr_pct_min": 0.015,
        "vwap_deviation": 0.05,
    },
    "sushiusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 75.0,
        "rsi_oversold": 25.0,
        "atr_pct_min": 0.010,
        "vwap_deviation": 0.04,
    },
    "runeusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 78.0,
        "rsi_oversold": 22.0,
        "atr_pct_min": 0.012,
        "vwap_deviation": 0.04,
    },
    "ksmusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 78.0,
        "rsi_oversold": 22.0,
        "atr_pct_min": 0.012,
        "vwap_deviation": 0.04,
    },
    "wavesusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 80.0,
        "rsi_oversold": 20.0,
        "atr_pct_min": 0.012,
        "vwap_deviation": 0.04,
    },
    "fetusdt": {
        "vol_z_threshold": 3.5,
        "rsi_overbought": 82.0,
        "rsi_oversold": 18.0,
        "atr_pct_min": 0.015,
        "vwap_deviation": 0.05,
    },
    "oceanusdt": {
        "vol_z_threshold": 3.5,
        "rsi_overbought": 82.0,
        "rsi_oversold": 18.0,
        "atr_pct_min": 0.015,
        "vwap_deviation": 0.05,
    },
    "cocosusdt": {
        "vol_z_threshold": 4.0,
        "rsi_overbought": 85.0,
        "rsi_oversold": 15.0,
        "atr_pct_min": 0.020,
        "vwap_deviation": 0.05,
    },
    "leverusdt": {
        "vol_z_threshold": 4.0,
        "rsi_overbought": 85.0,
        "rsi_oversold": 15.0,
        "atr_pct_min": 0.020,
        "vwap_deviation": 0.05,
    },
    "idusdt": {
        "vol_z_threshold": 4.0,
        "rsi_overbought": 85.0,
        "rsi_oversold": 15.0,
        "atr_pct_min": 0.020,
        "vwap_deviation": 0.05,
    },
    "ssvusdt": {
        "vol_z_threshold": 3.5,
        "rsi_overbought": 80.0,
        "rsi_oversold": 20.0,
        "atr_pct_min": 0.015,
        "vwap_deviation": 0.05,
    },
    "linausdt": {
        "vol_z_threshold": 3.5,
        "rsi_overbought": 80.0,
        "rsi_oversold": 20.0,
        "atr_pct_min": 0.015,
        "vwap_deviation": 0.05,
    },
    "phbusdt": {
        "vol_z_threshold": 4.0,
        "rsi_overbought": 85.0,
        "rsi_oversold": 15.0,
        "atr_pct_min": 0.020,
        "vwap_deviation": 0.05,
    },
    "rdntusdt": {
        "vol_z_threshold": 3.5,
        "rsi_overbought": 82.0,
        "rsi_oversold": 18.0,
        "atr_pct_min": 0.015,
        "vwap_deviation": 0.05,
    },
    "stblusdt": {
        "vol_z_threshold": 4.0,
        "rsi_overbought": 85.0,
        "rsi_oversold": 15.0,
        "atr_pct_min": 0.020,
        "vwap_deviation": 0.05,
    },
    "tutusdt": {
        "vol_z_threshold": 4.0,
        "rsi_overbought": 85.0,
        "rsi_oversold": 15.0,
        "atr_pct_min": 0.020,
        "vwap_deviation": 0.05,
    },
    "wlfiusdt": {
        "vol_z_threshold": 4.0,
        "rsi_overbought": 85.0,
        "rsi_oversold": 15.0,
        "atr_pct_min": 0.020,
        "vwap_deviation": 0.05,
    },
    "penguusdt": {
        "vol_z_threshold": 4.0,
        "rsi_overbought": 85.0,
        "rsi_oversold": 15.0,
        "atr_pct_min": 0.020,
        "vwap_deviation": 0.05,
    },
    "pumpusdt": {
        "vol_z_threshold": 4.0,
        "rsi_overbought": 85.0,
        "rsi_oversold": 15.0,
        "atr_pct_min": 0.020,
        "vwap_deviation": 0.05,
    },
    "enausdt": {
        "vol_z_threshold": 4.0,
        "rsi_overbought": 85.0,
        "rsi_oversold": 15.0,
        "atr_pct_min": 0.020,
        "vwap_deviation": 0.05,
    },
    "arpausdt": {
        "vol_z_threshold": 3.5,
        "rsi_overbought": 82.0,
        "rsi_oversold": 18.0,
        "atr_pct_min": 0.015,
        "vwap_deviation": 0.05,
    },
    "hookusdt": {
        "vol_z_threshold": 3.5,
        "rsi_overbought": 82.0,
        "rsi_oversold": 18.0,
        "atr_pct_min": 0.015,
        "vwap_deviation": 0.05,
    },
    "magicusdt": {
        "vol_z_threshold": 3.5,
        "rsi_overbought": 82.0,
        "rsi_oversold": 18.0,
        "atr_pct_min": 0.015,
        "vwap_deviation": 0.05,
    },
    "c98usdt": {
        "vol_z_threshold": 3.5,
        "rsi_overbought": 80.0,
        "rsi_oversold": 20.0,
        "atr_pct_min": 0.015,
        "vwap_deviation": 0.05,
    },
    "zilusdt": {
        "vol_z_threshold": 3.5,
        "rsi_overbought": 80.0,
        "rsi_oversold": 20.0,
        "atr_pct_min": 0.015,
        "vwap_deviation": 0.05,
    },
    "batusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 78.0,
        "rsi_oversold": 22.0,
        "atr_pct_min": 0.012,
        "vwap_deviation": 0.04,
    },
    "sfpusdt": {
        "vol_z_threshold": 3.5,
        "rsi_overbought": 80.0,
        "rsi_oversold": 20.0,
        "atr_pct_min": 0.015,
        "vwap_deviation": 0.05,
    },
    "aliceusdt": {
        "vol_z_threshold": 3.5,
        "rsi_overbought": 78.0,
        "rsi_oversold": 22.0,
        "atr_pct_min": 0.015,
        "vwap_deviation": 0.05,
    },
    "blurusdt": {
        "vol_z_threshold": 3.5,
        "rsi_overbought": 80.0,
        "rsi_oversold": 20.0,
        "atr_pct_min": 0.015,
        "vwap_deviation": 0.05,
    },
    "tlmusdt": {
        "vol_z_threshold": 3.5,
        "rsi_overbought": 78.0,
        "rsi_oversold": 22.0,
        "atr_pct_min": 0.015,
        "vwap_deviation": 0.05,
    },
    "yfiusdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 74.0,
        "rsi_oversold": 26.0,
        "atr_pct_min": 0.010,
        "vwap_deviation": 0.04,
    },
    "ankrusdt": {
        "vol_z_threshold": 3.5,
        "rsi_overbought": 78.0,
        "rsi_oversold": 22.0,
        "atr_pct_min": 0.015,
        "vwap_deviation": 0.05,
    },
    "klayusdt": {
        "vol_z_threshold": 3.5,
        "rsi_overbought": 78.0,
        "rsi_oversold": 22.0,
        "atr_pct_min": 0.015,
        "vwap_deviation": 0.05,
    },
    "neousdt": {
        "vol_z_threshold": 3.0,
        "rsi_overbought": 76.0,
        "rsi_oversold": 24.0,
        "atr_pct_min": 0.010,
        "vwap_deviation": 0.04,
    },
}
