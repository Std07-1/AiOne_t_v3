"""Stage3 helper: відкриття угод на базі Stage2 сигналів.

Сортує сигнали за впевненістю, застосовує поріг і делегує відкриття
`TradeLifecycleManager`. Стиль уніфіковано (короткі секції, guard logger).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any

from rich.console import Console
from rich.logging import RichHandler

from config.config import STAGE3_TRADE_PARAMS
from stage3.trade_manager import TradeLifecycleManager
from utils.utils import safe_float

if TYPE_CHECKING:  # pragma: no cover
    from app.asset_state_manager import AssetStateManager


def _log_stage3_skip(signal: dict[str, Any], reason: str) -> None:
    """Записати причину пропуску відкриття угоди у JSONL (best-effort).

    Формат рядка: {
        ts, symbol, reason, confidence, signal_type,
        htf_ok, atr_pct, low_gate
    }
    """
    try:
        symbol = signal.get("symbol")
        ctx_meta = signal.get("context_metadata") or {}
        if not ctx_meta and isinstance(signal.get("market_context"), dict):
            ctx_meta = (signal.get("market_context", {}) or {}).get("meta", {})
        rec = {
            "ts": datetime.utcnow().isoformat() + "Z",
            "symbol": symbol,
            "reason": reason,
            "confidence": safe_float(signal.get("confidence")),
            "signal_type": signal.get("signal"),
            "htf_ok": ctx_meta.get("htf_ok"),
            "atr_pct": ctx_meta.get("atr_pct"),
            "low_gate": ctx_meta.get("low_gate"),
        }
        with open("stage3_skips.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        # ігноруємо будь-які помилки (не критично для пайплайну)
        pass


def _optional_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _ensure_alert_session(
    state_manager: AssetStateManager,
    symbol: str,
    signal: dict[str, Any],
) -> None:
    if symbol in state_manager.alert_sessions:
        return
    stats = signal.get("stats") or {}
    price_val = _optional_float(stats.get("current_price"))
    rsi_val = _optional_float(stats.get("rsi"))
    meta = signal.get("context_metadata") or {}
    if not meta and isinstance(signal.get("market_context"), dict):
        meta = (signal.get("market_context", {}) or {}).get("meta", {})
    atr_pct_val = _optional_float(meta.get("atr_pct"))
    sig_type = str(signal.get("signal", "")).upper()
    side_val: str | None = None
    if sig_type.startswith("ALERT_BUY"):
        side_val = "BUY"
    elif sig_type.startswith("ALERT_SELL"):
        side_val = "SELL"
    try:
        state_manager.start_alert_session(
            symbol,
            price_val,
            atr_pct_val,
            rsi_val,
            side_val,
        )
    except Exception:
        pass


def _finalize_alert_session(
    state_manager: AssetStateManager | None,
    signal: dict[str, Any],
    reason: str,
) -> None:
    if state_manager is None:
        return
    symbol = signal.get("symbol")
    if not isinstance(symbol, str):
        return
    sig_type = str(signal.get("signal", "")).upper()
    if symbol not in state_manager.alert_sessions and sig_type.startswith("ALERT"):
        _ensure_alert_session(state_manager, symbol, signal)
    if symbol in state_manager.alert_sessions:
        try:
            state_manager.finalize_alert_session(symbol, reason)
        except Exception:
            return
        try:
            state_manager.update_asset(symbol, {"signal": "NORMAL"})
        except Exception:
            pass


# ── Logger ───────────────────────────────────────────────────────────────────
logger = logging.getLogger("stage3.open_trades")
if not logger.handlers:  # guard від повторної ініціалізації
    logger.setLevel(logging.INFO)
    try:  # optional rich
        logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    except Exception:  # broad except: rich необов'язковий
        logger.addHandler(logging.StreamHandler())
    logger.propagate = False

# Мінімальна впевненість для відкриття угоди (централізована в config)
try:
    MIN_CONFIDENCE_TRADE = float(STAGE3_TRADE_PARAMS.get("min_confidence_trade", 0.75))
except Exception:
    MIN_CONFIDENCE_TRADE = 0.75


async def open_trades(
    signals: list[dict[str, Any]],
    trade_manager: TradeLifecycleManager,
    max_parallel: int,
    state_manager: AssetStateManager | None = None,
) -> None:
    """
    Відкриває угоди для найперспективніших сигналів:
    1. Сортує сигнали за впевненістю
    2. Обмежує кількість одночасних угод
    3. Відкриває угоди через TradeLifecycleManager
    """
    if not trade_manager or not signals:
        return

    # Формуємо кандидатів: ігноруємо лише ті, що явно validation_passed=False
    candidates: list[dict[str, Any]] = []
    for s in signals:
        if s.get("validation_passed") is False:
            try:
                logger.info("⛔️ Пропуск %s: validation_passed=False", s.get("symbol"))
            except Exception:
                pass
            continue
        candidates.append(s)
    if not candidates:
        logger.info("Stage3: немає кандидатів для відкриття (raw=%d)", len(signals))
        return

    # Вибір найкращих сигналів (за впевненістю)
    sorted_signals = sorted(
        candidates,
        key=lambda x: x.get("confidence", 0),
        reverse=True,
    )[:max_parallel]

    # Лічильники причин пропусків (для агрегованих логів)
    skipped_by_reason: dict[str, int] = {}

    # ── Iterate sorted signals ───────────────────────────────────────────────
    for signal in sorted_signals:
        symbol = signal["symbol"]
        confidence = safe_float(signal.get("confidence", 0))
        if confidence is None:
            confidence = 0.0

        # Лише INFO/DEBUG, відкриття не очікується для інших рекомендацій
        if confidence < MIN_CONFIDENCE_TRADE:
            logger.info(
                f"⛔️ Не відкриваємо угоду для {symbol}: впевненість {confidence:.3f} "
                f"< поріг {MIN_CONFIDENCE_TRADE}"
            )
            logger.debug(
                f"Деталі сигналу: {json.dumps(signal, ensure_ascii=False, default=str)}"
            )
            skipped_by_reason["low_confidence"] = (
                skipped_by_reason.get("low_confidence", 0) + 1
            )
            _log_stage3_skip(signal, "low_confidence")
            _finalize_alert_session(state_manager, signal, "stage3_low_confidence")
            continue

        # Додаткові перевірки (можна розширити)
        sig_type = str(signal.get("signal", "NONE")).upper()
        if sig_type not in ["ALERT_BUY", "ALERT_SELL"]:
            logger.info(
                f"⛔️ Не відкриваємо угоду для {symbol}: тип сигналу {signal.get('signal')} "
                "не є ALERT_BUY/ALERT_SELL"
            )
            logger.debug(
                f"Деталі сигналу: {json.dumps(signal, ensure_ascii=False, default=str)}"
            )
            skipped_by_reason["not_alert"] = skipped_by_reason.get("not_alert", 0) + 1
            _log_stage3_skip(signal, "not_alert")
            _finalize_alert_session(state_manager, signal, "stage3_not_alert")
            continue

        # Вимагаємо явний стан 'alert' якщо передається
        state_val = signal.get("state") or signal.get("status")
        if isinstance(state_val, dict):
            state_val = state_val.get("status") or state_val.get("state")
        if isinstance(state_val, str) and state_val.lower() != "alert":
            logger.info(f"⛔️ Пропуск відкриття {symbol}: state='{state_val}' ≠ 'alert'")
            skipped_by_reason["not_state_alert"] = (
                skipped_by_reason.get("not_state_alert", 0) + 1
            )
            _log_stage3_skip(signal, "not_state_alert")
            _finalize_alert_session(state_manager, signal, "stage3_not_state_alert")
            continue

        try:
            # Фінальний guard: перевірка HTF та ATR проти low_gate (якщо доступні метадані)
            try:
                ctx_meta = signal.get("context_metadata") or {}
                # допускаємо джерела: context_metadata або market_context.meta
                if not ctx_meta and isinstance(signal.get("market_context"), dict):
                    ctx_meta = (signal.get("market_context", {}) or {}).get("meta", {})
                htf_ok = ctx_meta.get("htf_ok")
                atr_pct = ctx_meta.get("atr_pct")
                low_gate = ctx_meta.get("low_gate")
                if isinstance(htf_ok, bool) and not htf_ok:
                    logger.info(
                        f"⛔️ Пропуск відкриття {symbol}: 1h не підтверджує (htf_ok=False)"
                    )
                    skipped_by_reason["htf_block"] = (
                        skipped_by_reason.get("htf_block", 0) + 1
                    )
                    _log_stage3_skip(signal, "htf_block")
                    _finalize_alert_session(state_manager, signal, "stage3_htf_block")
                    continue
                if isinstance(atr_pct, (int, float)) and isinstance(
                    low_gate, (int, float)
                ):
                    if float(atr_pct) < float(low_gate):
                        logger.info(
                            f"⛔️ Пропуск відкриття {symbol}: ATR%% {float(atr_pct)*100:.2f}% нижче порогу {float(low_gate)*100:.2f}%"
                        )
                        skipped_by_reason["low_atr"] = (
                            skipped_by_reason.get("low_atr", 0) + 1
                        )
                        _log_stage3_skip(signal, "low_atr")
                        _finalize_alert_session(state_manager, signal, "stage3_low_atr")
                        continue
            except Exception:
                pass

            # Захист від нульових значень ATR
            atr = safe_float(signal.get("atr"))
            if atr is None or atr < 0.0001:
                atr = 0.01
                logger.warning(
                    f"Коригування ATR для {symbol}: {signal.get('atr')} -> 0.01"
                )

            # Підготовка TP/SL: якщо відсутні топ-рівня, беремо з risk_parameters
            tp_val = safe_float(signal.get("tp"))
            sl_val = safe_float(signal.get("sl"))
            if tp_val is None or sl_val is None:
                rp = signal.get("risk_parameters") or {}
                if tp_val is None:
                    tp_fallback = safe_float(rp.get("take_profit"))
                    if tp_fallback is not None:
                        tp_val = tp_fallback
                        logger.debug(
                            "Fallback TP з risk_parameters для %s -> %s", symbol, tp_val
                        )
                if sl_val is None:
                    sl_fallback = safe_float(rp.get("stop_loss"))
                    if sl_fallback is not None:
                        sl_val = sl_fallback
                        logger.debug(
                            "Fallback SL з risk_parameters для %s -> %s", symbol, sl_val
                        )

            # Підготовка даних для відкриття угоди
            trade_data = {
                "symbol": symbol,
                "current_price": safe_float(signal.get("current_price")),
                "atr": safe_float(signal.get("atr")),
                "rsi": safe_float(signal.get("rsi")),
                "volume": safe_float(signal.get("volume_mean")),
                "tp": tp_val,
                "sl": sl_val,
                "confidence": confidence,
                "hints": signal.get("hints", []),
                "cluster_factors": signal.get("cluster_factors", []),
                "context_metadata": signal.get("context_metadata", {}),
                "strategy": "stage2_cluster",
            }

            # Відкриття угоди
            await trade_manager.open_trade(trade_data)
            logger.info(
                f"✅ Відкрито угоду для {symbol} (впевненість: {confidence:.2f})"
            )
        except Exception as e:  # broad except: відкриття угоди не критичне
            logger.error(f"Помилка відкриття угоди для {symbol}: {str(e)}")

    # Зведений лог причин пропусків (корисно для моніторингу)
    if skipped_by_reason:
        try:
            logger.info(
                "Stage3 пропуски: %s", json.dumps(skipped_by_reason, ensure_ascii=False)
            )
        except Exception:
            logger.info("Stage3 пропуски: %s", skipped_by_reason)


__all__ = ["open_trades"]
