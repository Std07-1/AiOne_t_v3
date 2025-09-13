"""QDE Core — рушій прийняття рішень (Stage2 ядро).

Призначення:
    • Перетворити Stage1 сигнал (stats + triggers) у вичерпний контекст ринку
    • Обчислити сценарії, confidence, рекомендацію, narrative та risk-параметри

Вхід (stage1_signal):
    {
        "symbol": str,
        "stats": {
            "current_price": float,
            "vwap": float,
            "atr": float,
            "daily_high": float,
            "daily_low": float,
            "rsi": float,
            "volume": float,
            "volume_z": float,
            "bid_ask_spread": float,
            # опційно: adx / correlation / sentiment_index / tick_size / closes
        },
        "trigger_reasons": [str, ...]
    }

Вихід: dict з ключами: market_context, confidence_metrics, anomaly_detection,
recommendation, narrative, risk_parameters.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple
import math
import logging
import numpy as np
import pandas as pd

from rich.console import Console
from rich.logging import RichHandler

# ── Logger ──
logger = logging.getLogger("app.stage2.qde_core")
if not logger.handlers:  # guard від повторної ініціалізації
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False


# ── Переліки ──
class Scenario:
    BULLISH_BREAKOUT = "BULLISH_BREAKOUT"
    BEARISH_REVERSAL = "BEARISH_REVERSAL"
    RANGE_BOUND = "RANGE_BOUND"
    BULLISH_CONTROL = "BULLISH_CONTROL"
    BEARISH_CONTROL = "BEARISH_CONTROL"
    HIGH_VOLATILITY = "HIGH_VOLATILITY"
    MANIPULATED = "MANIPULATED"
    UNCERTAIN = "UNCERTAIN"


# ── Конфіг ──
@dataclass
class QDEConfig:
    # пороги/дефолти
    volume_z_threshold: float = 1.2
    vwap_threshold: float = 0.001
    rsi_bounds: Tuple[float, float] = (30.0, 70.0)  # для нормалізації
    adx_bounds: Tuple[float, float] = (18.0, 35.0)
    min_confidence_abs: float = 0.35
    cooldown_seconds: int = 0  # не реалізуємо трекінг, залишено для сумісності
    # ризик
    base_rr: float = 1.8
    tp_steps: Tuple[float, float, float] = (0.6, 1.0, 1.6)  # у ATR
    sl_step: float = 0.8  # у ATR


# ── Утиліти ──
def _safe(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return default
        return v
    except Exception:  # broad except: універсальна санітизація вхідних значень stats
        return default


def _clamp01(x: float) -> float:
    return 0.0 if x < 0.0 else 1.0 if x > 1.0 else x


def _normalize(x: float, lo: float, hi: float) -> float:
    if hi == lo:
        return 0.0
    return _clamp01((x - lo) / (hi - lo))


# ── Валідація ──
def validate_stats(stats: Dict[str, Any]) -> Dict[str, Any]:
    required = ["current_price", "vwap", "atr", "daily_high", "daily_low"]
    for k in required:
        if k not in stats:
            raise ValueError(f"Відсутній ключ: {k}")
        v = _safe(stats[k], None)  # type: ignore[arg-type]
        if v is None or v <= 0:
            raise ValueError(f"Невірне значення {k}: {stats[k]}")
    if _safe(stats["daily_high"]) <= _safe(stats["daily_low"]):
        raise ValueError("daily_high <= daily_low")
    return stats


# ── Аномалії ──
def detect_anomalies(
    stats: Dict[str, Any], triggers: List[str], cfg: QDEConfig
) -> Dict[str, bool]:
    vol_z = _safe(stats.get("volume_z"))
    spread = _safe(stats.get("bid_ask_spread"), 0.0008)
    vwap = _safe(stats.get("vwap"), _safe(stats.get("current_price")))
    vwap_dev = abs(_safe(stats.get("current_price")) - vwap) / max(1e-9, vwap)
    anomalies = {
        "volume_spike": vol_z >= cfg.volume_z_threshold,
        "wide_spread": spread > 2.0 * _safe(stats.get("median_spread"), spread),
        "vwap_whipsaw": vwap_dev > 3.0 * cfg.vwap_threshold,
    }
    # підсилюємо з урахуванням зовнішніх trigger_reasons
    if "volume_spike" in triggers:
        anomalies["volume_spike"] = True
    return anomalies


# ── Коридор рівнів (лайт) ──
def build_corridor(stats: Dict[str, Any]) -> Dict[str, Optional[float]]:
    """
    Спрощений коридор: використовує daily_low/high як межі; якщо ціна поза — зсуває на ATR.
    """
    price = _safe(stats["current_price"])
    atr = _safe(stats["atr"])
    low = _safe(stats["daily_low"])
    high = _safe(stats["daily_high"])
    if high <= low:
        # синтетичний діапазон
        high = price * 1.01
        low = price * 0.99
    support = (
        low if price - low <= high - price else price - max(atr, (high - low) * 0.25)
    )
    resistance = (
        high if high - price < price - low else price + max(atr, (high - low) * 0.25)
    )
    mid = (support + resistance) / 2.0
    width_rel = abs(resistance - support) / max(1e-12, price)
    band_pct = min(5.0, width_rel * 100.0)
    conf = max(0.0, 1.0 - width_rel * 2.0)
    return {
        "support": support,
        "resistance": resistance,
        "mid": mid,
        "band_pct": band_pct,
        "confidence": conf,
        "dist_to_support_pct": abs(price - support) / price * 100.0,
        "dist_to_resistance_pct": abs(resistance - price) / price * 100.0,
    }


# ── Аналіз (мікро/мезо/макро) ──
def analyze_micro(
    stats: Dict[str, Any], cfg: QDEConfig, triggers: List[str]
) -> Dict[str, float]:
    price = _safe(stats["current_price"])
    last = _safe(stats.get("last_price"), price)
    vol = _safe(stats.get("volume"))
    vol_z = _safe(stats.get("volume_z"))
    spread = _safe(stats.get("bid_ask_spread"), 0.0008)
    vwap = _safe(stats.get("vwap"), price)

    price_change = abs(price - last) / max(1e-9, last)
    volatility_norm = max(1e-6, _safe(stats.get("atr")) / max(1e-9, price))
    price_shock = min(price_change / volatility_norm, 3.0)

    volume_anomaly = max(0.0, min(vol_z / max(1e-6, cfg.volume_z_threshold * 2.5), 1.0))
    median_spread = _safe(stats.get("median_spread"), spread)
    liquidity_gap = 1.0 if spread > max(1e-9, median_spread * 2.0) else 0.0

    vwap_dev = abs(price - vwap) / max(1e-9, vwap)
    vwap_pressure = min(vwap_dev / max(1e-6, cfg.vwap_threshold), 2.0)

    # якщо є явний тригер, легенько підсилюємо
    if "volume_spike" in triggers:
        volume_anomaly = min(1.0, volume_anomaly + 0.15)

    return {
        "price_shock": price_shock,
        "volume_anomaly": volume_anomaly,
        "liquidity_gap": liquidity_gap,
        "vwap_pressure": vwap_pressure,
    }


def analyze_meso(stats: Dict[str, Any], cfg: QDEConfig) -> Dict[str, float]:
    rsi = _safe(stats.get("rsi"), 50.0)
    adx = _safe(stats.get("adx"), 20.0)
    rsi_strength = _normalize(rsi, *cfg.rsi_bounds)
    adx_strength = _normalize(adx, *cfg.adx_bounds)
    volume_strength = min(max(_safe(stats.get("volume_z"), 0.0) / 2.5, 0.0), 1.0)
    # простий slope за 5 останніх цін, якщо передали масив
    closes = np.asarray(stats.get("closes", []), dtype=float)
    trend_slope = 0.5
    if closes.size >= 6:
        y = (closes[-60:] if closes.size > 60 else closes).astype(float)
        x = np.arange(len(y), dtype=float)
        x = (x - x.mean()) / (x.std() + 1e-9)
        y = (y - y.mean()) / (y.std() + 1e-9)
        beta = float(np.dot(x, y) / (np.dot(x, x) + 1e-9))
        trend_slope = max(0.0, min(1.0, 0.5 + beta / 2.0))

    trend_strength = (
        0.35 * rsi_strength
        + 0.25 * adx_strength
        + 0.25 * volume_strength
        + 0.15 * trend_slope
    )

    # якість рівнів (щільність у діапазоні + обсяги)
    low, high = _safe(stats.get("daily_low")), _safe(stats.get("daily_high"))
    levels = [low, high]
    in_range = [lv for lv in levels if low <= lv <= high]
    density = len(in_range) / max(2, len(levels))
    levels_quality = max(
        0.0,
        min(
            1.0, 0.6 * density + 0.4 * min(1.0, abs(_safe(stats.get("volume_z"))) / 3.0)
        ),
    )

    market_breadth = _safe(stats.get("market_breath"), 0.5)
    return {
        "trend_strength": max(0.0, min(1.0, trend_strength)),
        "levels_quality": levels_quality,
        "market_breadth": market_breadth,
    }


def analyze_macro(stats: Dict[str, Any]) -> Dict[str, float]:
    sector_health = _safe(stats.get("sector_health", {}).get("default", 0.5))
    correlation = _safe(stats.get("correlation"), 0.5)
    base_corr = 0.5
    correlation_shifts = abs(correlation - base_corr)
    macro_sentiment = _safe(stats.get("sentiment_index"), 0.0)
    return {
        "sector_health": sector_health,
        "correlation_shifts": correlation_shifts,
        "macro_sentiment": macro_sentiment,
    }


# ── Скоринг сценаріїв ──
def decision_matrix(
    micro: Dict[str, float], meso: Dict[str, float], macro: Dict[str, float]
) -> Dict[str, float]:
    dm = {
        Scenario.BULLISH_BREAKOUT: (
            0.3 * micro["volume_anomaly"]
            + 0.1 * (1 - micro["liquidity_gap"])
            + 0.2 * micro["vwap_pressure"]
            + 0.25 * meso["trend_strength"]
            + 0.15 * meso["levels_quality"]
            + 0.2 * macro["sector_health"]
            + 0.1 * max(0.0, macro["macro_sentiment"])
        ),
        Scenario.BEARISH_REVERSAL: (
            0.25 * micro["price_shock"]
            + 0.2 * micro["liquidity_gap"]
            + 0.15 * (1 - micro["vwap_pressure"])
            + 0.25 * (1 - meso["trend_strength"])
            + 0.15 * meso["levels_quality"]
            + 0.2 * (1 - macro["sector_health"])
        ),
        Scenario.RANGE_BOUND: (
            0.4 * (1 - micro["price_shock"])
            + 0.25 * (1 - meso["trend_strength"])
            + 0.15 * (1 - meso["levels_quality"])
            + 0.2
        ),
        Scenario.BULLISH_CONTROL: (
            0.2 * (1 - micro["liquidity_gap"])
            + 0.2 * (micro["vwap_pressure"] > 1.0)
            + 0.35 * meso["trend_strength"]
            + 0.25 * meso["levels_quality"]
            + 0.2 * (macro["sector_health"] + max(0.0, macro["macro_sentiment"]))
        ),
        Scenario.BEARISH_CONTROL: (
            0.25 * micro["liquidity_gap"]
            + 0.15 * (1 - micro["vwap_pressure"])
            + 0.2 * micro["price_shock"]
            + 0.25 * (1 - meso["trend_strength"])
            + 0.15 * (1 - meso["levels_quality"])
            + 0.2 * (1 - macro["sector_health"])
            + 0.1 * max(0.0, -macro["macro_sentiment"])
        ),
        Scenario.HIGH_VOLATILITY: (
            0.5 * min(1.0, micro["price_shock"] / 1.5)
            + 0.2 * micro["liquidity_gap"]
            + 0.3
            * (abs(macro["correlation_shifts"]) + (1 - macro["sector_health"]) / 2)
        ),
        Scenario.MANIPULATED: (
            0.35 * (micro["volume_anomaly"] > 0.7)
            + 0.25 * (micro["liquidity_gap"] > 0.0)
            + 0.25 * (abs(macro["correlation_shifts"]) > 0.2)
            + 0.15 * (micro["vwap_pressure"] > 1.5)
        ),
    }
    # Нормалізуємо в [0..1]
    return {k: float(_clamp01(v)) for k, v in dm.items()}


def select_scenario(dm: Dict[str, float], min_abs: float) -> Tuple[str, float]:
    ranked = sorted(dm.items(), key=lambda kv: kv[1], reverse=True)
    if not ranked:
        return Scenario.UNCERTAIN, 0.0
    top_scn, top = ranked[0]
    if len(ranked) == 1:
        return (top_scn if top >= min_abs else Scenario.UNCERTAIN, float(top))
    snd = ranked[1][1]
    rel = (top / max(1e-9, snd)) if snd > 0 else 2.0
    if top < min_abs:
        return Scenario.UNCERTAIN, float(top)
    if rel >= 1.25:
        return top_scn, float(top)
    if rel >= 1.1:
        return top_scn, float(max(0.0, min(1.0, top * 0.8)))
    return Scenario.UNCERTAIN, float(max(0.0, min(1.0, top * 0.6)))


# ── Confidence ──
def compute_confidence(ctx: Dict[str, Any]) -> Dict[str, float]:
    bp = ctx.get("breakout_probability")
    pp = ctx.get("pullback_probability")
    if bp is None or pp is None:
        # інферимо від сценарію + ширини коридору
        scn = ctx.get("scenario", Scenario.UNCERTAIN)
        band = (ctx.get("key_levels_meta") or {}).get("band_pct")
        if scn == Scenario.BULLISH_BREAKOUT:
            bp, pp = 0.75, 0.25
        elif scn == Scenario.BEARISH_REVERSAL:
            bp, pp = 0.25, 0.75
        elif scn == Scenario.RANGE_BOUND:
            if isinstance(band, (int, float)):
                if band < 0.08:
                    bp, pp = 0.20, 0.30
                elif band < 0.20:
                    bp, pp = 0.35, 0.35
                else:
                    bp, pp = 0.45, 0.40
            else:
                bp, pp = 0.30, 0.30
        else:
            bp, pp = 0.30, 0.30
    comp = 0.55 * _clamp01(ctx.get("confidence", 0.5)) + 0.45 * _clamp01(max(bp, pp))
    return {
        "breakout_probability": _clamp01(bp),
        "pullback_probability": _clamp01(pp),
        "composite_confidence": _clamp01(comp),
    }


# ── Рекомендація ──
def make_recommendation(ctx: Dict[str, Any], conf: Dict[str, float]) -> str:
    composite = conf.get("composite_confidence", 0.0)
    scn = ctx.get("scenario", Scenario.UNCERTAIN)
    km = ctx.get("key_levels_meta") or {}
    d_s = km.get("dist_to_support_pct", None)
    d_r = km.get("dist_to_resistance_pct", None)
    band = km.get("band_pct", None)

    if scn == Scenario.MANIPULATED:
        return "AVOID"
    if scn == Scenario.HIGH_VOLATILITY:
        return "AVOID_HIGH_RISK"
    if scn == Scenario.BULLISH_CONTROL:
        return "BUY_IN_DIPS" if isinstance(d_s, (int, float)) and d_s < 2.0 else "HOLD"
    if scn == Scenario.BEARISH_CONTROL:
        return (
            "SELL_ON_RALLIES" if isinstance(d_r, (int, float)) and d_r < 2.0 else "HOLD"
        )
    if scn == Scenario.RANGE_BOUND:
        if isinstance(band, (int, float)) and band < 0.08 and composite < 0.70:
            return "WAIT_FOR_CONFIRMATION"
        if isinstance(d_s, (int, float)) and d_s < 1.0:
            return "BUY_IN_DIPS"
        if isinstance(d_r, (int, float)) and d_r < 1.0:
            return "SELL_ON_RALLIES"
        if isinstance(band, (int, float)) and band < 1.2:
            return "RANGE_TRADE"
        return "WAIT_FOR_CONFIRMATION"

    if composite > 0.8:
        if "BULLISH" in scn:
            return "STRONG_BUY"
        if "BEARISH" in scn:
            return "STRONG_SELL"
    if composite > 0.65:
        if "BULLISH" in scn:
            return "BUY_IN_DIPS"
        if "BEARISH" in scn:
            return "SELL_ON_RALLIES"
        return "RANGE_TRADE"
    if composite > 0.5:
        return "WAIT_FOR_CONFIRMATION"
    return "AVOID"


# ── Наратив ──
def make_narrative(
    ctx: Dict[str, Any], anomalies: Dict[str, bool], lang: str = "UA"
) -> str:
    sym = ctx.get("symbol", "актив")
    scn = ctx.get("scenario", Scenario.UNCERTAIN)
    price = _safe(ctx.get("current_price"))
    km = ctx.get("key_levels_meta") or {}
    sup = (ctx.get("key_levels") or {}).get("immediate_support")
    res = (ctx.get("key_levels") or {}).get("immediate_resistance")

    scen_text = {
        "UA": {
            Scenario.BULLISH_BREAKOUT: f"{sym}: потенціал бичого пробою.",
            Scenario.BEARISH_REVERSAL: f"{sym}: ризик ведмежого розвороту.",
            Scenario.RANGE_BOUND: f"{sym}: торгівля у діапазоні.",
            Scenario.BULLISH_CONTROL: f"{sym}: бичий контроль.",
            Scenario.BEARISH_CONTROL: f"{sym}: ведмежий контроль.",
            Scenario.HIGH_VOLATILITY: f"{sym}: підвищена волатильність.",
            Scenario.MANIPULATED: f"{sym}: ознаки маніпуляцій.",
            Scenario.UNCERTAIN: f"{sym}: невизначеність.",
        },
        "EN": {
            Scenario.BULLISH_BREAKOUT: f"{sym}: bullish breakout potential.",
            Scenario.BEARISH_REVERSAL: f"{sym}: bearish reversal risk.",
            Scenario.RANGE_BOUND: f"{sym}: range-bound.",
            Scenario.BULLISH_CONTROL: f"{sym}: bullish control.",
            Scenario.BEARISH_CONTROL: f"{sym}: bearish control.",
            Scenario.HIGH_VOLATILITY: f"{sym}: high volatility.",
            Scenario.MANIPULATED: f"{sym}: possible manipulation.",
            Scenario.UNCERTAIN: f"{sym}: uncertain.",
        },
    }
    parts = [
        scen_text.get(lang, scen_text["UA"]).get(
            scn, scen_text["UA"][Scenario.UNCERTAIN]
        )
    ]
    if isinstance(sup, (int, float)) and isinstance(res, (int, float)):
        parts.append(
            {
                "UA": f"Ближні рівні: підтримка {sup:.6f}, опір {res:.6f}.",
                "EN": f"Key levels: support {sup:.6f}, resistance {res:.6f}.",
            }[lang]
        )
    if anomalies.get("volume_spike"):
        parts.append({"UA": "Сплеск обсягів.", "EN": "Volume spike."}[lang])
    if anomalies.get("wide_spread"):
        parts.append(
            {
                "UA": "Ризик ліквідності (широкий спред).",
                "EN": "Liquidity risk (wide spread).",
            }[lang]
        )
    if anomalies.get("vwap_whipsaw"):
        parts.append({"UA": "Нестабільність біля VWAP.", "EN": "VWAP whipsaw."}[lang])

    band = km.get("band_pct")
    if isinstance(band, (int, float)):
        if band < 0.08:
            parts.append(
                {
                    "UA": "Вузький діапазон — можливий імпульс.",
                    "EN": "Tight range — possible impulse.",
                }[lang]
            )
        elif band > 1.5:
            parts.append(
                {
                    "UA": "Широкий діапазон — ризиковані рухи.",
                    "EN": "Wide range — risky moves.",
                }[lang]
            )

    return " ".join(parts)


# ── Ризик ──
def make_risk(
    stats: Dict[str, Any], ctx: Dict[str, Any], cfg: QDEConfig
) -> Dict[str, Any]:
    price = _safe(stats["current_price"])
    atr = _safe(stats["atr"])
    if atr <= 0 or price <= 0:
        return {}
    sl = (
        price - cfg.sl_step * atr
        if "BULLISH" in ctx["scenario"]
        else price + cfg.sl_step * atr
    )
    tps = [
        price + k * atr if "BULLISH" in ctx["scenario"] else price - k * atr
        for k in cfg.tp_steps
    ]
    rr = (abs(tps[0] - price) / max(1e-9, abs(price - sl))) if tps else None
    return {"sl_level": sl, "tp_targets": tps, "risk_reward_ratio": rr}


# ── Ядро: QDEngine ──
class QDEngine:
    """Один клас, один виклик — повний цикл аналізу."""

    def __init__(self, config: Optional[QDEConfig] = None, lang: str = "UA"):
        self.cfg = config or QDEConfig()
        self.lang = lang

    def process(self, stage1_signal: Dict[str, Any]) -> Dict[str, Any]:
        stats = validate_stats(dict(stage1_signal.get("stats") or {}))
        symbol = str(stage1_signal.get("symbol", "UNKNOWN"))
        triggers = list(stage1_signal.get("trigger_reasons") or [])
        # 1) аномалії
        anomalies = detect_anomalies(stats, triggers, self.cfg)
        # 2) коридор/рівні
        corr = build_corridor(stats)
        key_levels = {
            "immediate_support": corr["support"],
            "immediate_resistance": corr["resistance"],
            "next_major_level": corr["mid"],
        }
        key_levels_meta = {
            k: corr[k]
            for k in (
                "band_pct",
                "confidence",
                "mid",
                "dist_to_support_pct",
                "dist_to_resistance_pct",
            )
        }
        # 3) аналіз
        micro = analyze_micro(stats, self.cfg, triggers)
        meso = analyze_meso(stats, self.cfg)
        macro = analyze_macro(stats)
        dm = decision_matrix(micro, meso, macro)
        scenario, conf_abs = select_scenario(dm, self.cfg.min_confidence_abs)
        # 4) context
        context = {
            "symbol": symbol,
            "current_price": _safe(stats["current_price"]),
            "scenario": scenario,
            "confidence": _clamp01(conf_abs),
            "decision_matrix": dm,
            "micro": micro,
            "meso": meso,
            "macro": macro,
            "key_levels": key_levels,
            "key_levels_meta": key_levels_meta,
        }
        # 5) confidence (композит)
        conf = compute_confidence(context)
        # 6) reco
        reco = make_recommendation(context, conf)
        # 7) narrative
        narr = make_narrative(context, anomalies, lang=self.lang)
        # 8) risk
        risk = make_risk(stats, context, self.cfg)
        # лог
        logger.debug(
            "[QDE] %s scen=%s comp=%.3f reco=%s",
            symbol,
            scenario,
            conf["composite_confidence"],
            reco,
        )
        return {
            "symbol": symbol,
            "market_context": context,
            "confidence_metrics": conf,
            "anomaly_detection": anomalies,
            "recommendation": reco,
            "narrative": narr,
            "risk_parameters": risk,
        }


# ── Приклад ──
if __name__ == "__main__":
    engine = QDEngine()
    sample = {
        "symbol": "BTCUSDT",
        "stats": {
            "current_price": 65000,
            "vwap": 64950,
            "atr": 400,
            "daily_high": 65500,
            "daily_low": 64000,
            "rsi": 58,
            "volume": 1200,
            "volume_z": 1.8,
            "bid_ask_spread": 0.9,
        },
        "trigger_reasons": ["volume_spike"],
    }
    out = engine.process(sample)
    print(pd.Series(out["confidence_metrics"]))
    print(out["recommendation"], "-", out["narrative"])

__all__ = [
    "QDEConfig",
    "QDEngine",
    "Scenario",
    "detect_anomalies",
    "build_corridor",
    "analyze_micro",
    "analyze_meso",
    "analyze_macro",
    "decision_matrix",
    "select_scenario",
    "compute_confidence",
    "make_recommendation",
    "make_narrative",
    "make_risk",
]
