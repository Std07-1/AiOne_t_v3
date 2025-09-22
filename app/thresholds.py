"""Порогові значення для сигналів (без динамічного калібрування).

Шлях: ``app/thresholds.py``

Завдання:
    • модель `Thresholds` з калібруванням на основі ATR та історії;
    • серіалізація/десеріалізація для кешу (Redis / файл);
    • допоміжні утиліти завантаження/збереження порогів.
"""

from __future__ import annotations

import json
import logging
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import pandas as pd
from rich.console import Console
from rich.logging import RichHandler

from config.config import CACHE_TTL_DAYS
from config.TOP100_THRESHOLDS import get_top100_threshold

if (
    TYPE_CHECKING
):  # pragma: no cover - лише для типів, щоб уникати імпорту під час рантайму
    from stage1.indicators.atr_indicator import ATRManager  # noqa: F401

# ───────────────────────────── Логування ─────────────────────────────
log = logging.getLogger("app.thresholds")
if not log.handlers:
    log.setLevel(logging.INFO)
    log.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    log.propagate = False


class Thresholds:
    """Статичні порогові значення для символу (спрощено, без ATR-калібрування)."""

    def __init__(
        self,
        symbol: str,
        config: dict,  # Конфігурація передається ззовні
        data: pd.DataFrame | None = None,  # ігнорується (калібрування вимкнено)
        atr_manager: ATRManager | None = None,  # ігнорується
        calibrated_params: dict | None = None,  # legacy (ігнорується)
    ):
        """
        symbol: Біржовий символ (наприклад "BTCUSDT")
        data: DataFrame з історичними даними (high, low, close)
        config: Словник конфігурації з параметрами
        atr_manager: Екземпляр ATRManager для інкрементальних розрахунків
        """
        self.symbol = symbol
        self.low_gate = config.get("low_gate", 0.006)
        self.high_gate = config.get("high_gate", 0.015)
        self.atr_target = config.get("atr_target", 0.5)
        # Canonical key: volume_z_threshold (backward compat: vol_z_threshold)
        self.vol_z_threshold = config.get(
            "volume_z_threshold",
            config.get("vol_z_threshold", 1.2),
        )
        self.rsi_oversold = config.get("rsi_oversold", 30.0)
        self.rsi_overbought = config.get("rsi_overbought", 70.0)
        # Додаткові параметри точності
        self.min_atr_percent = config.get(
            "min_atr_percent", config.get("atr_pct_min", 0.002)
        )  # 0.2% за замовчуванням
        self.vwap_deviation = config.get(
            "vwap_deviation", config.get("vwap_deviation_threshold", 0.02)
        )  # 2% за замовчуванням

        # Розширена конфігурація (необов'язкова): для state-aware порогів
        # Залишаємо як словники, щоб не ламати існуючі тести/АПІ
        self.signal_thresholds: dict[str, Any] = dict(
            config.get("signal_thresholds", {}) or {}
        )
        self.state_overrides: dict[str, dict[str, float]] = dict(
            config.get("state_overrides", {}) or {}
        )
        self.meta: dict[str, Any] = dict(config.get("meta", {}) or {})

        # Калібрування відключено – просто пост-обробка
        self._post_init()

    # _calibrate видалено – динамічне калібрування не підтримується

    def _post_init(self) -> None:
        """Автокорекція точності значень"""
        self.low_gate = round(self.low_gate, 4)
        self.high_gate = round(self.high_gate, 4)
        self.atr_target = round(self.atr_target, 2)
        self.vol_z_threshold = round(self.vol_z_threshold, 1)
        self.rsi_oversold = round(self.rsi_oversold, 1)
        self.rsi_overbought = round(self.rsi_overbought, 1)
        self.min_atr_percent = round(self.min_atr_percent, 4)
        self.vwap_deviation = round(self.vwap_deviation, 4)

        # Гарантоване співвідношення high_gate > low_gate
        if self.high_gate <= self.low_gate:
            self.high_gate = self.low_gate * 1.5

    @classmethod
    def from_mapping(cls, data: dict) -> Thresholds:
        """Створює Thresholds зі словника (symbol не передається у calibrated_params)"""
        symbol = data.get("symbol")
        # Перевірка наявності символу
        if not symbol or not isinstance(symbol, str):
            raise ValueError(
                f"[Thresholds] Недійсний symbol у from_mapping(): {symbol}"
            )

        # Видаляємо symbol із calibrated_params, щоб уникнути float('btcusdt')
        # (залишено як коментар: {k: v for k, v in data.items() if k != "symbol"})

        cfg: dict[str, Any] = {
            "low_gate": data.get("low_gate", 0.0015),  # Нижня межа ATR/price (0.15%)
            "high_gate": data.get("high_gate", 0.0134),  # Верхня межа ATR/price (1.34%)
            "atr_target": float(
                data.get("atr_target", 0.3)
            ),  # Цільовий ATR (float; раніше помилково список)
            "volume_z_threshold": data.get(
                "volume_z_threshold", data.get("vol_z_threshold", 1.2)
            ),  # сплеск обсягу ≥1.2σ (уніфіковано з fallback)
            "rsi_oversold": data.get(
                "rsi_oversold", 23.0
            ),  # Рівень перепроданності RSI (23%)
            "rsi_overbought": data.get(
                "rsi_overbought", 74.0
            ),  # Рівень перекупленості RSI (74%)
            "atr_period": 14,  # Період ATR (14)
            "min_atr_percent": data.get(
                "min_atr_percent", data.get("atr_pct_min", 0.002)
            ),  # Мінімальний ATR у відсотках
            "vwap_deviation": data.get(
                "vwap_deviation", data.get("vwap_deviation_threshold", 0.02)
            ),  # Поріг відхилення від VWAP
        }
        # Пропускаємо розширені поля як є (беккомпат):
        for k in ("signal_thresholds", "state_overrides", "meta"):
            if k in data:
                cfg[k] = data[k]

        return cls(
            symbol=symbol,
            config=cfg,
            calibrated_params=None,  # калібрування вимкнено
        )

    def to_dict(self) -> dict[str, float | str]:
        """Повертає словник з каліброваними значеннями (symbol не повертається)"""
        return {
            "low_gate": self.low_gate,
            "high_gate": self.high_gate,
            "atr_target": self.atr_target,
            "vol_z_threshold": self.vol_z_threshold,
            "rsi_oversold": self.rsi_oversold,
            "rsi_overbought": self.rsi_overbought,
            "min_atr_percent": self.min_atr_percent,
            "vwap_deviation": self.vwap_deviation,
        }

    # ────────────────────── State-aware resolve (мінімальна версія) ──────────────────────
    def effective_thresholds(
        self,
        *,
        market_state: str | None = None,
    ) -> dict[str, Any]:
        """
        Обчислює «ефективні» пороги з урахуванням state_overrides.

        Args:
            market_state: Простий класифікатор стану ринку
                ("range_bound" | "trend_strong" | "high_volatility" | None).

        Returns:
            dict[str, Any]: плаский базовий словник порогів + вкладені signal_thresholds
                (тільки невелика підмножина, яку використовує Stage1).

        Notes:
            - Мінімальні зміни: застосовуємо лише коригування для ключів, які
              безпосередньо використовує Stage1 сьогодні: vol_z_threshold, vwap_deviation.
            - Підтримуємо dot-path у overrides (наприклад, "volume_spike.z_score").
            - Для числових полів overrides трактуються як Δ (дельта): додаємо до бази.
        """
        # Базова копія з основних полів
        effective: dict[str, Any] = {
            "low_gate": float(self.low_gate),
            "high_gate": float(self.high_gate),
            "atr_target": float(self.atr_target),
            "vol_z_threshold": float(self.vol_z_threshold),
            "rsi_oversold": float(self.rsi_oversold),
            "rsi_overbought": float(self.rsi_overbought),
            "min_atr_percent": float(self.min_atr_percent),
            "vwap_deviation": float(self.vwap_deviation),
            # вкладені зони
            "signal_thresholds": {
                k: (v.copy() if isinstance(v, dict) else v)
                for k, v in (self.signal_thresholds or {}).items()
            },
        }

        # 1) Базове застосування atr_volatility → low/high gates (із signal_thresholds)
        try:
            st = effective.get("signal_thresholds", {}) or {}
            av = st.get("atr_volatility", {}) or {}

            def _to_fraction(val: Any, *, is_pct_hint: bool = False) -> float | None:
                """Нормалізує значення у частку від 1.

                - Якщо ключ має суфікс _pct або is_pct_hint=True → ділимо на 100.
                - Інакше: якщо val > 0.2 (ймовірно це відсоток) → поділити на 100,
                  інакше трактуємо як вже-нормалізовану частку.
                """
                try:
                    x = float(val)
                except Exception:
                    return None
                if is_pct_hint:
                    return round(x / 100.0, 6)
                # евристика: значення > 0.2 швидше за все задали у відсотках (наприклад 0.6% → 0.6)
                return round((x / 100.0) if x > 0.2 else x, 6)

            low_raw = None
            high_raw = None
            src = ""
            if "low_gate_pct" in av or "high_gate_pct" in av:
                low_raw = _to_fraction(av.get("low_gate_pct"), is_pct_hint=True)
                high_raw = _to_fraction(av.get("high_gate_pct"), is_pct_hint=True)
                src = "atr_volatility._pct"
            elif "low_gate" in av or "high_gate" in av:
                # TON-варіант конфігу: ключі без _pct, але значення у відсотках за коментарями
                low_raw = _to_fraction(av.get("low_gate"), is_pct_hint=False)
                high_raw = _to_fraction(av.get("high_gate"), is_pct_hint=False)
                src = "atr_volatility"

            applied: dict[str, float] = {}
            if isinstance(low_raw, float) and low_raw > 0:
                effective["low_gate"] = float(low_raw)
                applied["low_gate"] = float(low_raw)
            if isinstance(high_raw, float) and high_raw > 0:
                effective["high_gate"] = float(high_raw)
                applied["high_gate"] = float(high_raw)

            if applied:
                log.debug(
                    "effective_thresholds: застосовано atr_volatility",
                    extra={
                        "symbol": self.symbol,
                        "source": src,
                        "applied": applied,
                        "base": {
                            "low_gate": self.low_gate,
                            "high_gate": self.high_gate,
                        },
                    },
                )
        except Exception as e:  # не зривати пайплайн
            log.debug(
                "effective_thresholds: atr_volatility пропущено через помилку",
                extra={"symbol": self.symbol, "err": str(e)},
            )

        # 2) Якщо задано стан — застосовуємо overrides як дельти
        if market_state and market_state in self.state_overrides:
            overrides = self.state_overrides.get(market_state, {})
            for key, delta in overrides.items():
                try:
                    # Підтримка dot-path (наприклад, "signal_thresholds.breakout.band_pct_atr")
                    if "." in key:
                        parts = key.split(".")
                        node = effective
                        # Якщо це відомий тригер без префіксу, застосуємо до signal_thresholds
                        try:
                            st = effective.setdefault("signal_thresholds", {})
                            if parts[0] not in node and parts[0] in (st or {}):
                                node = st  # direct into signal_thresholds namespace
                        except Exception:
                            pass
                        for p in parts[:-1]:
                            node = node.setdefault(p, {})
                        leaf = parts[-1]
                        base_val = node.get(leaf)
                        if isinstance(base_val, (int, float)) and isinstance(
                            delta, (int, float)
                        ):
                            node[leaf] = float(base_val) + float(delta)
                        elif base_val is None and isinstance(delta, (int, float)):
                            node[leaf] = float(delta)
                        else:
                            # Якщо тип нечисловий — просто присвоюємо
                            node[leaf] = delta
                    else:
                        base_val = effective.get(key)
                        if isinstance(base_val, (int, float)) and isinstance(
                            delta, (int, float)
                        ):
                            effective[key] = float(base_val) + float(delta)
                        elif base_val is None and isinstance(delta, (int, float)):
                            effective[key] = float(delta)
                        else:
                            effective[key] = delta
                except Exception:
                    # Не зривати пайплайн через незнайомий ключ — пропускаємо
                    continue

        # 3) Гарантуємо коректність меж після можливих дельт
        try:
            if effective["high_gate"] <= effective["low_gate"]:
                effective["high_gate"] = effective["low_gate"] * 1.5
        except Exception:
            pass
        # 4) Синхронізуємо скорочення з вкладених порогів (якщо задані)
        try:
            st = effective.get("signal_thresholds", {}) or {}
            volz_nested = st.get("volume_spike", {}).get("z_score")
            if isinstance(volz_nested, (int, float)):
                effective["vol_z_threshold"] = float(volz_nested)
            vwap_nested = st.get("vwap_deviation", {}).get("threshold")
            if isinstance(vwap_nested, (int, float)) and not effective.get(
                "vwap_deviation"
            ):
                effective["vwap_deviation"] = float(vwap_nested)
        except Exception:
            pass
        return effective


# Redis-ключ
def _redis_key(symbol: str) -> str:
    return f"thresholds:{symbol}"


# Збереження та завантаження
async def save_thresholds(
    symbol: str,
    thr: Thresholds,
    cache_or_store: Any,
) -> None:
    """Зберігає калібровані параметри в Redis"""
    key = _redis_key(symbol)
    payload = json.dumps(
        {
            "low_gate": thr.low_gate,
            "high_gate": thr.high_gate,
            "atr_target": thr.atr_target,
            "vol_z_threshold": thr.vol_z_threshold,
            "rsi_oversold": thr.rsi_oversold,
            "rsi_overbought": thr.rsi_overbought,
            "min_atr_percent": thr.min_atr_percent,
            "vwap_deviation": thr.vwap_deviation,
            "symbol": symbol,
        },
        ensure_ascii=False,
    )

    # UnifiedDataStore path (has redis.jset)
    if hasattr(cache_or_store, "redis") and hasattr(cache_or_store.redis, "jset"):
        ttl = int(timedelta(days=CACHE_TTL_DAYS).total_seconds())
        await cache_or_store.redis.jset(
            "thresholds", symbol, value=json.loads(payload), ttl=ttl
        )
    # Legacy SimpleCacheHandler
    elif hasattr(cache_or_store, "store_in_cache"):
        await cache_or_store.store_in_cache(
            key,
            "global",
            payload,
            ttl=timedelta(days=CACHE_TTL_DAYS),
            raw=True,
        )
    else:  # fallback log
        log.warning("save_thresholds: Unsupported cache interface for %s", symbol)


async def load_thresholds(
    symbol: str,
    cache_or_store: Any,
    data: pd.DataFrame | None = None,
    config: dict | None = None,
    atr_manager: ATRManager | None = None,
) -> Thresholds:
    if not symbol or not isinstance(symbol, str):
        raise ValueError("load_thresholds: Некоректний символ")

    # 1. Спроба завантажити з thresholds-кешу
    key = f"thresholds:{symbol}"
    raw = None
    # UnifiedDataStore path
    if hasattr(cache_or_store, "redis") and hasattr(cache_or_store.redis, "jget"):
        rec = await cache_or_store.redis.jget("thresholds", symbol, default=None)
        if rec:
            raw = json.dumps(rec, ensure_ascii=False)
    elif hasattr(cache_or_store, "fetch_from_cache"):
        raw = await cache_or_store.fetch_from_cache(key, "global", raw=True)

    if raw:
        try:
            return Thresholds.from_mapping(json.loads(raw))
        except Exception as e:
            log.warning(f"[{symbol}] Помилка декодування thresholds: {e}")

    # 2. Калібрування та calib-кеш видалені – підтягуємо дефолти для Top100
    top100_cfg = None
    try:
        top100_cfg = get_top100_threshold(symbol)
    except Exception as e:
        log.debug("TOP100 defaults fetch failed for %s: %s", symbol, e)

    if top100_cfg:
        log.info(f"[{symbol}] Використання TOP100 дефолтів порогів")
        return Thresholds.from_mapping(top100_cfg)

    # 3. Фолбек на загальні дефолти
    log.info(f"[{symbol}] Використання загальних дефолтів порогів")
    if config is None:
        config = {}
    return Thresholds.from_mapping(
        {
            "symbol": symbol,
            "low_gate": config.get("low_gate", 0.006),
            "high_gate": config.get("high_gate", 0.015),
            "atr_target": config.get("atr_target", 0.5),
            "vol_z_threshold": config.get("vol_z_threshold", 1.2),
            "rsi_oversold": config.get("rsi_oversold", 30.0),
            "rsi_overbought": config.get("rsi_overbought", 70.0),
            "min_atr_percent": config.get("min_atr_percent", 0.002),
            "vwap_deviation": config.get("vwap_deviation", 0.02),
        }
    )
