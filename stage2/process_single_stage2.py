"""Stage2 одиночна обробка сигналу.

Модуль інкапсулює перетворення сирого Stage1 сигналу у розширений Stage2 результат
з урахуванням:
    • рекомендації (recommendation) та її даунгрейду (reco_original / reco_gate_reason);
    • злиття trigger_reasons з market_context;
    • збереження ALERT стану у колонці state навіть при даунгрейді до NORMAL;
    • інструментації життєвого циклу (start/update/finalize alert session);
    • накопичувальних лічильників проходження/блокування та семплів confidence;
    • JSONL-аудиту (stage2_decisions.jsonl) — best effort.

Side-effects: оновлює об'єкт AssetStateManager (мутабельний глобальний стан UI).
Контракти: НЕ змінює структуру Stage1Signal / Stage2Output, лише дописує службові
поля (reco_original, reco_gate_reason, stage1_alert_preserved тощо) згідно правил.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, TypedDict, cast

from rich.console import Console
from rich.logging import RichHandler

from config.config import ASSET_STATE, STAGE2_STATUS
from utils.utils import map_reco_to_signal as _map_reco_to_signal
from utils.utils import safe_float

logger = logging.getLogger("stage2.process_single_stage2")
if not logger.handlers:  # guard
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False

if TYPE_CHECKING:  # pragma: no cover
    from app.asset_state_manager import AssetStateManager
    from stage2.processor import Stage2Processor

__all__ = ["process_single_stage2"]


EDGE_THR_PCT = 0.25  # частка ширини коридору (0..1), яка вважається "near edge"


class SignalDict(TypedDict, total=False):  # легкий контракт для підказок IDE
    symbol: str
    signal: str
    stats: dict[str, Any]


async def process_single_stage2(
    signal: dict[str, Any],
    processor: Stage2Processor,
    state_manager: AssetStateManager,
) -> None:
    """Обчислює Stage2 для одного активу та оновлює глобальний стан.

    Args:
        signal: Вхідний Stage1/попередній сигнал (очікує ключі symbol, stats,...).
        processor: Stage2Processor (інкапсулює QDE / генерацію рекомендації).
        state_manager: Менеджер стану для агрегації результатів (мутабельний).

    Side Effects:
        - Модифікує state_manager.state[symbol] (додає stage2 поля, лічильники).
        - Пише рядок у stage2_decisions.jsonl (best effort, без винятків назовні).
        - Оновлює сесії alert lifecycle (start/update/finalize) та counters.

    Notes:
        - Функція гарантує, що якщо Stage1 був ALERT, а Stage2 понизив до NORMAL,
          в полі state залишиться ALERT (для консистентного UI відображення).
        - TP/SL НЕ нормалізуються тут — остаточно формуються Stage3.
    """
    symbol = signal["symbol"]

    try:

        prev_asset_state = state_manager.state.get(symbol, {}).copy()
        state_manager.update_asset(
            symbol, {"stage2_status": STAGE2_STATUS["PROCESSING"]}
        )
        logger.debug("[Stage2] %s processing started", symbol)

        # Stage2 (QDE_core під капотом)
        result: dict[str, Any] = await processor.process(signal)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "[Stage2] %s raw keys=%s",
                symbol,
                sorted(result.keys()),
            )

        update: dict[str, Any] = {
            "stage2": True,
            "stage2_status": STAGE2_STATUS["COMPLETED"],
            "last_updated": datetime.utcnow().isoformat(),
        }

        if "error" in result:
            err_text = str(result.get("error", "unknown"))
            update.update({"signal": "NONE", "hints": [f"Stage2 error: {err_text}"]})
            state_manager.update_asset(symbol, update)
            return

        market_ctx = result.get("market_context", {}) or {}
        scenario = market_ctx.get("scenario") or "UNCERTAIN"

        recommendation = result.get("recommendation")
        if not recommendation and scenario == "UNCERTAIN":
            recommendation = "WAIT"

        signal_type = _map_reco_to_signal(recommendation or "")

        risk_params = result.get("risk_parameters", {}) or {}
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("[Stage2] %s risk=%s", symbol, risk_params)
        # PR4: TP/SL не використовуються у продюсері для формування UI

        conf = result.get("confidence_metrics", {}) or {}
        composite_conf = conf.get("composite_confidence", 0.0)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("[Stage2] %s conf_metrics=%s", symbol, conf)

        raw_narr = (result.get("narrative") or "").strip()
        hints: list[str] = []
        if raw_narr:
            hints.append(raw_narr)
            try:
                logger.info("[NARR] %s %s", symbol, raw_narr.replace("\n", " "))
            except Exception:
                logger.debug("[NARR] %s (logging failed)", symbol)
        # Якщо рекомендація була даунгрейднута Stage2 гейтом — формуємо службовий тег
        try:
            original_reco = result.get("reco_original")
            if original_reco and recommendation and original_reco != recommendation:
                tag = f"downgraded:{original_reco}->{recommendation}"
                gate_reason = result.get("reco_gate_reason")
                if gate_reason:
                    tag = f"{tag}[{gate_reason}]"
                if tag not in hints:
                    hints.insert(0, tag)
                try:
                    logger.info(
                        "[Stage2] %s downgraded %s -> %s reason=%s",
                        symbol,
                        original_reco,
                        recommendation,
                        gate_reason,
                    )
                except Exception:
                    logger.debug("[Stage2] %s downgrade лог не записано", symbol)
        except Exception:
            pass

        anomaly_det = result.get("anomaly_detection")
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("[Stage2] %s anomaly=%s", symbol, anomaly_det)
        # Об'єднуємо джерела причин: основний результат + market_context
        tr_from_result = result.get("trigger_reasons") or []
        tr_from_ctx = (
            market_ctx.get("trigger_reasons") if isinstance(market_ctx, dict) else []
        )
        if not isinstance(tr_from_ctx, list):
            tr_from_ctx = []
        trigger_reasons_raw = list(tr_from_result) + list(tr_from_ctx)

        htf_flag = None
        try:
            htf_flag = (market_ctx.get("meta", {}) or {}).get("htf_ok")
        except Exception:
            htf_flag = None

        conf_val = (
            float(composite_conf) if isinstance(composite_conf, (int, float)) else 0.0
        )
        try:
            joined_triggers = (
                "+".join(str(t) for t in trigger_reasons_raw if t) or "none"
            )
            logger.debug(
                "[Stage2] %s scen=%s reco=%s signal=%s conf=%.3f triggers=%s htf_ok=%s",
                symbol,
                scenario,
                recommendation or "NONE",
                signal_type,
                conf_val,
                joined_triggers,
                htf_flag,
            )
        except Exception:
            logger.debug(
                "[Stage2] %s реко=%s сигнал=%s (логування невдале)",
                symbol,
                recommendation,
                signal_type,
            )

        update["signal"] = signal_type
        update["recommendation"] = recommendation or None
        update["scenario"] = scenario
        update["confidence"] = composite_conf
        if hints:
            update["hints"] = list(dict.fromkeys(hints))
        # Нормалізація TP/SL щоб уникнути кейсів типу TP нижче SL для BUY
        # PR4: TP/SL формуються і коригуються Stage3; продюсер їх не нормалізує
        trigger_add = None

        # ── Збагачення corridor meta для Stage3/UI ────────────────────────
        corridor_meta_payload: dict[str, Any] = {}
        try:
            stats_src = signal.get("stats") if isinstance(signal, dict) else None
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "[Stage2] %s stats snapshot=%s",
                    symbol,
                    {
                        "price": (stats_src or {}).get("current_price"),
                        "atr": (stats_src or {}).get("atr"),
                        "daily_low": (stats_src or {}).get("daily_low"),
                        "daily_high": (stats_src or {}).get("daily_high"),
                        "rsi": (stats_src or {}).get("rsi"),
                        "volume": (stats_src or {}).get("volume"),
                    },
                )
            price_val = (
                safe_float((stats_src or {}).get("current_price"))
                if isinstance(stats_src, dict)
                else None
            )

            key_levels = (
                market_ctx.get("key_levels") if isinstance(market_ctx, dict) else {}
            )
            if not isinstance(key_levels, dict):
                key_levels = {}

            support_val = safe_float(key_levels.get("immediate_support"))
            resistance_val = safe_float(key_levels.get("immediate_resistance"))

            key_levels_meta = (
                market_ctx.get("key_levels_meta")
                if isinstance(market_ctx, dict)
                else {}
            )
            if not isinstance(key_levels_meta, dict):
                key_levels_meta = {}
                if isinstance(market_ctx, dict):
                    market_ctx["key_levels_meta"] = key_levels_meta

            band_pct_val = safe_float(key_levels_meta.get("band_pct"))
            dist_support_pct = safe_float(key_levels_meta.get("dist_to_support_pct"))
            dist_resistance_pct = safe_float(
                key_levels_meta.get("dist_to_resistance_pct")
            )

            if band_pct_val is not None:
                corridor_meta_payload["band_pct"] = band_pct_val
                corridor_meta_payload["edge_threshold_ratio"] = EDGE_THR_PCT
            if dist_support_pct is not None:
                corridor_meta_payload["dist_to_support_pct"] = dist_support_pct
            if dist_resistance_pct is not None:
                corridor_meta_payload["dist_to_resistance_pct"] = dist_resistance_pct

            nearest_edge: str | None = None
            dist_to_edge_pct: float | None = None
            is_near_edge: bool | None = None
            dist_to_edge_ratio: float | None = None

            if dist_support_pct is not None or dist_resistance_pct is not None:
                candidates: list[tuple[str, float]] = []
                if dist_support_pct is not None:
                    candidates.append(("support", float(dist_support_pct)))
                if dist_resistance_pct is not None:
                    candidates.append(("resistance", float(dist_resistance_pct)))
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "[Stage2] %s corridor raw distances support=%s resistance=%s",
                        symbol,
                        dist_support_pct,
                        dist_resistance_pct,
                    )
                if candidates:
                    candidates.sort(key=lambda item: item[1])
                    nearest_edge, dist_to_edge_pct = candidates[0]
                    corridor_meta_payload["dist_to_edge_pct"] = dist_to_edge_pct
                    if band_pct_val and band_pct_val > 0:
                        dist_to_edge_ratio = dist_to_edge_pct / band_pct_val
                        corridor_meta_payload["dist_to_edge_ratio"] = dist_to_edge_ratio
                        is_near_edge = dist_to_edge_ratio <= EDGE_THR_PCT
                        corridor_meta_payload["is_near_edge"] = is_near_edge
                    if is_near_edge:
                        corridor_meta_payload["near_edge"] = nearest_edge
                    corridor_meta_payload["nearest_edge"] = nearest_edge

            within_corridor: bool | None = None
            if (
                price_val is not None
                and support_val is not None
                and resistance_val is not None
                and resistance_val >= support_val
            ):
                within_corridor = support_val <= price_val <= resistance_val
                corridor_meta_payload["within_corridor"] = within_corridor

            if corridor_meta_payload and isinstance(market_ctx, dict):
                # Оновлюємо key_levels_meta утримуючи існуючі значення
                kl_meta_ref = market_ctx.setdefault("key_levels_meta", {})
                if not isinstance(kl_meta_ref, dict):
                    kl_meta_ref = {}
                    market_ctx["key_levels_meta"] = kl_meta_ref
                for key, value in corridor_meta_payload.items():
                    if value is not None:
                        kl_meta_ref[key] = value

                meta_ref = market_ctx.setdefault("meta", {})
                corridor_ref = meta_ref.setdefault("corridor", {})
                if not isinstance(corridor_ref, dict):
                    corridor_ref = {}
                    meta_ref["corridor"] = corridor_ref
                for key, value in corridor_meta_payload.items():
                    if value is not None:
                        corridor_ref[key] = value
                try:
                    logger.debug(
                        "[Stage2] %s corridor band_pct=%s dist_edge_pct=%s ratio=%s within=%s near_edge=%s edge=%s",
                        symbol,
                        corridor_meta_payload.get("band_pct"),
                        corridor_meta_payload.get("dist_to_edge_pct"),
                        corridor_meta_payload.get("dist_to_edge_ratio"),
                        corridor_meta_payload.get("within_corridor"),
                        corridor_meta_payload.get("is_near_edge"),
                        corridor_meta_payload.get("nearest_edge"),
                    )
                except Exception:
                    logger.debug("[Stage2] %s corridor лог не записано", symbol)
        except Exception:
            corridor_meta_payload = {}

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("[Stage2] %s update_market_ctx=%s", symbol, market_ctx)

        update["market_context"] = market_ctx or None
        update["risk_parameters"] = risk_params or None
        update["confidence_metrics"] = conf or None
        update["anomaly_detection"] = anomaly_det or None
        # Проксі службових полів від Stage2 (якщо були змінені гейтом)
        if "reco_original" in result and result.get("reco_original") != recommendation:
            update["reco_original"] = result.get("reco_original")
        if "reco_gate_reason" in result:
            update["reco_gate_reason"] = result.get("reco_gate_reason")
        existing = prev_asset_state.get("trigger_reasons") or []
        extra_triggers: list[str] = []
        if trigger_add:
            extra_triggers.append(trigger_add)
        # Обережно приводимо типи до list[str]
        existing_list: list[str] = list(cast(list[str], existing))
        tr_list: list[str] = (
            list(cast(list[str], trigger_reasons_raw)) if trigger_reasons_raw else []
        )
        merged_triggers = list(dict.fromkeys(existing_list + tr_list + extra_triggers))
        if signal_type.startswith("ALERT") and not merged_triggers:
            merged_triggers = ["signal_generated"]
        update["trigger_reasons"] = merged_triggers
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("[Stage2] %s merged_triggers=%s", symbol, merged_triggers)
        # Зберігаємо Stage1 alert у статусі (state) навіть якщо Stage2 понизив сигнал до NORMAL
        prev_state = prev_asset_state.get("state")
        if signal_type.startswith("ALERT"):
            update["state"] = ASSET_STATE["ALERT"]
        elif signal_type == "NORMAL":
            if prev_state == ASSET_STATE["ALERT"]:
                # Stage1 ставив ALERT, Stage2 понизив — відображаємо ALERT у колонці "Статус"
                update["state"] = ASSET_STATE["ALERT"]
                update["stage1_alert_preserved"] = True
            else:
                update["state"] = ASSET_STATE["NORMAL"]
        else:
            update["state"] = update.get("state") or ASSET_STATE["NO_TRADE"]
        update["narrative"] = raw_narr or None
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("[Stage2] %s state_update=%s", symbol, update)
        state_manager.update_asset(symbol, update)

        # ── Інструментація життєвого циклу ALERT ──
        try:
            prev_signal_raw = prev_asset_state.get("signal") or ""
            prev_signal = (
                str(prev_signal_raw).upper() if isinstance(prev_signal_raw, str) else ""
            )
            # Поточні базові метрики
            price_val = signal.get("stats", {}).get("current_price")
            rsi_val = signal.get("stats", {}).get("rsi")
            atr_pct_val = None
            htf_ok_val = None
            band_pct_val: float | None = None
            near_edge_val: str | None = None
            low_gate_val: float | None = None
            meta_ctx: dict[str, Any] = {}
            corridor_meta_ctx: dict[str, Any] = {}
            try:
                raw_meta_ctx = (
                    market_ctx.get("meta", {}) if isinstance(market_ctx, dict) else {}
                ) or {}
                if isinstance(raw_meta_ctx, dict):
                    meta_ctx = raw_meta_ctx
                atr_pct_val = meta_ctx.get("atr_pct")
                htf_ok_val = meta_ctx.get("htf_ok")
                low_gate_val = safe_float(meta_ctx.get("low_gate"))

                corridor_candidate = meta_ctx.get("corridor")
                if isinstance(corridor_candidate, dict):
                    corridor_meta_ctx = corridor_candidate
                elif isinstance(market_ctx, dict):
                    key_levels_meta = market_ctx.get("key_levels_meta")
                    if isinstance(key_levels_meta, dict):
                        corridor_meta_ctx = key_levels_meta

                if corridor_meta_ctx:
                    band_pct_val = safe_float(corridor_meta_ctx.get("band_pct"))
                    near_edge_raw = corridor_meta_ctx.get("near_edge")
                    if isinstance(near_edge_raw, str):
                        near_edge_val = near_edge_raw
                    else:
                        nearest_edge_candidate = corridor_meta_ctx.get("nearest_edge")
                        is_near_edge_val = corridor_meta_ctx.get("is_near_edge")
                        if isinstance(nearest_edge_candidate, str) and bool(
                            is_near_edge_val
                        ):
                            near_edge_val = nearest_edge_candidate
            except Exception:
                pass
            side_val: str | None = None
            sig_u = signal_type.upper() if isinstance(signal_type, str) else ""
            if sig_u.startswith("ALERT_BUY"):
                side_val = "BUY"
            elif sig_u.startswith("ALERT_SELL"):
                side_val = "SELL"
            # Перехід NORMAL/іншого → ALERT
            if (not prev_signal.startswith("ALERT")) and sig_u.startswith("ALERT"):
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "[Stage2] %s alert_session start price=%s atr_pct=%s rsi=%s side=%s",
                        symbol,
                        price_val,
                        atr_pct_val,
                        rsi_val,
                        side_val,
                    )
                state_manager.start_alert_session(
                    symbol,
                    price_val,
                    atr_pct_val,
                    rsi_val,
                    side_val,
                    band_pct_val,
                    low_gate_val,
                    near_edge_val,
                )
            # Продовження ALERT
            elif prev_signal.startswith("ALERT") and sig_u.startswith("ALERT"):
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "[Stage2] %s alert_session update price=%s atr_pct=%s rsi=%s htf_ok=%s",
                        symbol,
                        price_val,
                        atr_pct_val,
                        rsi_val,
                        htf_ok_val,
                    )
                state_manager.update_alert_session(
                    symbol,
                    price_val,
                    atr_pct_val,
                    rsi_val,
                    htf_ok_val,
                    band_pct_val,
                    low_gate_val,
                    near_edge_val,
                )
            # Вихід із ALERT
            elif prev_signal.startswith("ALERT") and (not sig_u.startswith("ALERT")):
                downgrade_list: list[str] = []
                try:
                    if (
                        isinstance(atr_pct_val, (int, float))
                        and isinstance(low_gate_val, (int, float))
                        and float(atr_pct_val) < float(low_gate_val)
                    ):
                        downgrade_list.append("low_volatility")
                    if htf_ok_val is not None and htf_ok_val is False:
                        downgrade_list.append("htf_block")
                except Exception:
                    pass
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "[Stage2] %s alert_session finalize reasons=%s",
                        symbol,
                        downgrade_list,
                    )
                state_manager.finalize_alert_session(
                    symbol, "+".join(downgrade_list) if downgrade_list else None
                )
        except Exception:
            pass

        # ── Накопичувальні метрики проходження/блокування ALERT ──
        try:
            if signal_type.startswith("ALERT_BUY") or signal_type.startswith(
                "ALERT_SELL"
            ):
                state_manager.passed_alerts += 1
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "[Stage2] %s counters passed_alerts=%s",
                        symbol,
                        state_manager.passed_alerts,
                    )
            elif signal_type == "NORMAL" and result.get("reco_original"):
                # був даунгрейд → класифікуємо причини
                state_manager.downgraded_alerts += 1
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "[Stage2] %s counters downgraded_alerts=%s",
                        symbol,
                        state_manager.downgraded_alerts,
                    )
                gr = (
                    (result.get("reco_gate_reason") or "").split("+")
                    if result.get("reco_gate_reason")
                    else []
                )
                if any(r == "low_volatility" for r in gr):
                    state_manager.blocked_alerts_lowvol += 1
                if any(r == "htf_block" for r in gr):
                    state_manager.blocked_alerts_htf += 1
                if any(r == "low_confidence" for r in gr):
                    state_manager.blocked_alerts_lowconf += 1
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "[Stage2] %s counters lowvol=%s htf=%s lowconf=%s",
                        symbol,
                        state_manager.blocked_alerts_lowvol,
                        state_manager.blocked_alerts_htf,
                        state_manager.blocked_alerts_lowconf,
                    )
                # Комбінований випадок low_volatility + low_confidence
                if any(r == "low_volatility" for r in gr) and any(
                    r == "low_confidence" for r in gr
                ):
                    state_manager.blocked_alerts_lowvol_lowconf += 1
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(
                            "[Stage2] %s counters lowvol_lowconf=%s",
                            symbol,
                            state_manager.blocked_alerts_lowvol_lowconf,
                        )
            # Записуємо семпл confidence (незалежно від типу сигналу)
            try:
                if isinstance(composite_conf, (int, float)):
                    state_manager.add_confidence_sample(float(composite_conf))
            except Exception:
                pass
        except Exception:
            pass

        # JSONL аудит Stage2 рішень (best‑effort, без винятків)
        try:
            corridor_meta = (
                (market_ctx.get("key_levels_meta", {}) or {})
                if isinstance(market_ctx, dict)
                else {}
            )
            audit = {
                "ts": datetime.utcnow().isoformat() + "Z",
                "symbol": symbol,
                "scenario": scenario,
                "recommendation": recommendation,
                "signal": signal_type,
                "confidence": composite_conf,
                "htf_ok": (market_ctx.get("meta", {}) or {}).get("htf_ok"),
                "atr_pct": (market_ctx.get("meta", {}) or {}).get("atr_pct"),
                "low_gate": (market_ctx.get("meta", {}) or {}).get("low_gate"),
                "corridor_band_pct": corridor_meta.get("band_pct"),
                "corridor_dist_to_support_pct": corridor_meta.get(
                    "dist_to_support_pct"
                ),
                "corridor_dist_to_resistance_pct": corridor_meta.get(
                    "dist_to_resistance_pct"
                ),
                "corridor_dist_to_edge_pct": corridor_meta.get("dist_to_edge_pct"),
                "corridor_dist_to_edge_ratio": corridor_meta.get("dist_to_edge_ratio"),
                "corridor_is_near_edge": corridor_meta.get("is_near_edge"),
                "corridor_near_edge": corridor_meta.get("near_edge"),
                "corridor_within": corridor_meta.get("within_corridor"),
                "triggers": merged_triggers,
            }
            line = json.dumps(audit, ensure_ascii=False)
            with open("stage2_decisions.jsonl", "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

    except Exception:
        logger.exception("Stage2 помилка для %s", symbol)
        state_manager.update_asset(
            symbol,
            {
                "stage2_status": STAGE2_STATUS["ERROR"],
                "error": "Stage2 exception (див. логи)",
            },
        )
