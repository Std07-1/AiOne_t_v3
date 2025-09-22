"""Stage3 helper: відкриття угод на базі Stage2 сигналів.

Сортує сигнали за впевненістю, застосовує поріг і делегує відкриття
`TradeLifecycleManager`. Стиль уніфіковано (короткі секції, guard logger).
"""

from __future__ import annotations

import json
import logging
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

from config.config import STAGE3_TRADE_PARAMS
from stage3.trade_manager import TradeLifecycleManager
from utils.utils import safe_float

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
) -> None:
    """
    Відкриває угоди для найперспективніших сигналів:
    1. Сортує сигнали за впевненістю
    2. Обмежує кількість одночасних угод
    3. Відкриває угоди через TradeLifecycleManager
    """
    if not trade_manager or not signals:
        return

    # Вибір найкращих сигналів
    sorted_signals = sorted(
        [s for s in signals if s.get("validation_passed")],
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

        # Детальне логування причин, чому угода не відкривається
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

            # Підготовка даних для відкриття угоди
            trade_data = {
                "symbol": symbol,
                "current_price": safe_float(signal.get("current_price")),
                "atr": safe_float(signal.get("atr")),
                "rsi": safe_float(signal.get("rsi")),
                "volume": safe_float(signal.get("volume_mean")),
                "tp": safe_float(signal.get("tp")),
                "sl": safe_float(signal.get("sl")),
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
