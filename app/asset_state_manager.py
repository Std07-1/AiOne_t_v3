"""Менеджер стану активів (спрощена версія без калібрування).

Шлях: ``app/asset_state_manager.py``

Призначення:
    • централізоване зберігання стану активів (signal / thresholds / stats);
    • легкі геттер-и для UI/Stage2 (alerts, всі активи);
    • без історичної логіки калібрування (видалено).
"""

import json
import logging
from datetime import datetime
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

from config.config import (
    ASSET_STATE,
    K_SIGNAL,
    K_STATS,
    K_SYMBOL,
    K_TRIGGER_REASONS,
    STAGE2_STATUS,
)

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("app.asset_state_manager")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False


class AssetStateManager:
    """Централізований менеджер стану активів"""

    def __init__(
        self,
        initial_assets: list[str],
        cache_handler: Any | None = None,
        symbol_cfg: dict[str, Any] | None = None,
    ) -> None:
        self.state: dict[str, dict[str, Any]] = {}
        self.cache: Any | None = cache_handler
        self._symbol_cfg: dict[str, Any] = symbol_cfg or {}
        # Лічильники для UI (оновлюються продюсером кожен цикл)
        self.generated_signals: int = 0
        self.skipped_signals: int = 0
        # Накопичувальні лічильники блокувань / проходження ALERT після Stage2 гейтів
        self.blocked_alerts_lowvol: int = 0
        self.blocked_alerts_htf: int = 0
        self.blocked_alerts_lowconf: int = 0
        # Комбінований блок — одночасно low_volatility + low_confidence
        self.blocked_alerts_lowvol_lowconf: int = 0
        self.passed_alerts: int = 0  # фінально пройшли як ALERT_BUY/ALERT_SELL
        self.downgraded_alerts: int = 0  # були даунгрейджені до WAIT_FOR_CONFIRMATION
        # Сесії активних ALERT (інструментація якості)
        self.alert_sessions: dict[str, dict[str, Any]] = {}
        # Буфер для зразків composite_confidence (для перцентилів у UI)
        self.conf_samples: list[float] = (
            []
        )  # ковзне вікно обрізається під час додавання

        for asset in initial_assets:
            self.init_asset(asset)

    def set_cache_handler(self, cache_handler: Any) -> None:
        """Встановити обробник кешу/сховища порогів для збереження калібрування."""
        self.cache = cache_handler

    def set_symbol_config(self, symbol_cfg: dict[str, Any]) -> None:
        """Встановити локальну мапу конфігів порогів на символ (in-memory)."""
        self._symbol_cfg = symbol_cfg or {}

    def init_asset(self, symbol: str) -> None:
        """Ініціалізація базового стану для активу"""
        self.state[symbol] = {
            K_SYMBOL: symbol,
            K_SIGNAL: "NONE",
            K_TRIGGER_REASONS: [],
            "confidence": 0.0,
            "hints": ["Очікування даних..."],
            "tp": None,
            "sl": None,
            "cluster_factors": [],
            K_STATS: {},
            "state": ASSET_STATE["INIT"],
            "stage2": False,
            "stage2_status": STAGE2_STATUS["PENDING"],
            "last_updated": datetime.utcnow().isoformat(),
            "visible": True,
        }

    def update_asset(self, symbol: str, updates: dict[str, Any]) -> None:
        """Оновлення стану активу з мерджем існуючих даних"""
        if symbol not in self.state:
            self.init_asset(symbol)

        current = self.state[symbol]
        # Нормалізація trigger_reasons якщо приходить None
        if K_TRIGGER_REASONS in updates and updates[K_TRIGGER_REASONS] is None:
            updates[K_TRIGGER_REASONS] = []
        self.state[symbol] = {
            **current,
            **updates,
            "last_updated": datetime.utcnow().isoformat(),
        }

    def get_all_assets(self) -> list[dict[str, Any]]:
        """Отримати всі активи для відображення в UI"""
        if not self.state:
            logger.warning("Стан активів порожній, немає даних для відображення")
            return []

        return list(self.state.values())

    def get_alert_signals(self) -> list[dict[str, Any]]:
        """Отримати сигнали ALERT* (ALERT/ALERT_BUY/ALERT_SELL) для Stage2."""
        return [
            asset
            for asset in self.state.values()
            if str(asset.get(K_SIGNAL, "")).upper().startswith("ALERT")
        ]

    # ─────────────────────── Confidence перцентилі (збір зразків) ───────────────────────
    def add_confidence_sample(self, value: float | None, max_len: int = 500) -> None:
        """Додати зразок composite_confidence у ковзне вікно.

        Args:
            value: Значення впевненості (0..1). Ігнорується якщо не число.
            max_len: Максимальна довжина буфера (обрізається з початку).
        """
        if value is None:
            return
        try:
            v = float(value)
        except Exception:
            return
        if not (0 <= v <= 1.0):  # поза діапазоном — ігноруємо
            return
        self.conf_samples.append(v)
        if len(self.conf_samples) > max_len:
            # Видаляємо надлишок (можемо за один раз якщо виріс значно)
            overflow = len(self.conf_samples) - max_len
            if overflow > 0:
                del self.conf_samples[0:overflow]

    # ─────────────────────── Інструментація життєвого циклу ALERT ───────────────────────
    def start_alert_session(
        self,
        symbol: str,
        price: float | None,
        atr_pct: float | None,
        rsi: float | None = None,
        side: str | None = None,
    ) -> None:
        """Почати нову ALERT-сесію (фіксуємо стартові метрики)."""
        try:
            ts = datetime.utcnow().isoformat() + "Z"
        except Exception:
            ts = datetime.utcnow().isoformat()
        self.alert_sessions[symbol] = {
            "ts_alert": ts,
            "symbol": symbol,
            "side": side,
            "price_alert": price,
            "atr_alert": atr_pct,
            "rsi_alert": rsi,
            "max_high": price,
            "min_low": price,
            "bars": 0,
            "atr_path": [] if atr_pct is None else [atr_pct],
            "rsi_path": [] if rsi is None else [rsi],
            "htf_ok_path": [],
        }

    def update_alert_session(
        self,
        symbol: str,
        price: float | None,
        atr_pct: float | None = None,
        rsi: float | None = None,
        htf_ok: bool | None = None,
    ) -> None:
        """Оновити активну ALERT-сесію (max_high/min_low, ATR/RSI траєкторії)."""
        sess = self.alert_sessions.get(symbol)
        if not sess:
            return
        if price is not None:
            try:
                if sess.get("max_high") is None or price > sess["max_high"]:
                    sess["max_high"] = price
                if sess.get("min_low") is None or price < sess["min_low"]:
                    sess["min_low"] = price
            except Exception:
                pass
        sess["bars"] = int(sess.get("bars", 0)) + 1
        if atr_pct is not None:
            sess.setdefault("atr_path", []).append(atr_pct)
        if rsi is not None:
            sess.setdefault("rsi_path", []).append(rsi)
        if htf_ok is not None:
            sess.setdefault("htf_ok_path", []).append(htf_ok)

    def finalize_alert_session(
        self, symbol: str, downgrade_reason: str | None = None
    ) -> None:
        """Завершити ALERT-сесію і записати метрики у alerts_quality.jsonl."""
        sess = self.alert_sessions.pop(symbol, None)
        if not sess:
            return
        try:
            ts_end = datetime.utcnow().isoformat() + "Z"
            ts_alert = sess.get("ts_alert")
            try:
                end_dt = datetime.fromisoformat(ts_end.replace("Z", ""))
                start_dt = datetime.fromisoformat(str(ts_alert).replace("Z", ""))
                survival_s = int((end_dt - start_dt).total_seconds())
            except Exception:
                survival_s = 0
            price_alert = sess.get("price_alert")
            max_high = sess.get("max_high", price_alert)
            min_low = sess.get("min_low", price_alert)
            side = sess.get("side")
            mfe = None
            mae = None
            if price_alert is not None:
                try:
                    if side == "BUY":
                        mfe = (max_high or price_alert) - price_alert
                        mae = (min_low or price_alert) - price_alert
                    elif side == "SELL":
                        mfe = price_alert - (min_low or price_alert)
                        mae = price_alert - (max_high or price_alert)
                    else:
                        mfe = (max_high or price_alert) - price_alert
                        mae = (min_low or price_alert) - price_alert
                except Exception:
                    mfe = None
                    mae = None
            atr_alert = sess.get("atr_alert")
            aof = None
            if atr_alert:
                try:
                    if mfe is not None and atr_alert != 0:
                        aof = mfe / atr_alert
                except Exception:
                    aof = None
            rec = {
                "ts_alert": ts_alert,
                "ts_end": ts_end,
                "symbol": symbol,
                "side": side,
                "price_alert": price_alert,
                "atr_alert": atr_alert,
                "survival_s": survival_s,
                "mfe": mfe,
                "mae": mae,
                "aof": aof,
                "htf_ok_path": sess.get("htf_ok_path"),
                "downgrade_reason": downgrade_reason,
            }
            with open("alerts_quality.jsonl", "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        except Exception:
            try:
                logger.exception("Finalize ALERT session failed for %s", symbol)
            except Exception:
                pass

    # update_calibration видалено — калібрування не підтримується
