"""Публікація агрегованого стану активів у Redis (UI snapshot).

Шлях: ``UI/publish_full_state.py``

Винос з `app.screening_producer` для розділення відповідальностей:
    • збір та нормалізація стану (producer)
    • публікація / форматування для UI (цей модуль)

Формат payload (type = REDIS_CHANNEL_ASSET_STATE):
    {
        "type": REDIS_CHANNEL_ASSET_STATE,
        "meta": {"ts": ISO8601UTC},
        "counters": {"assets": N, "alerts": A},
        "assets": [ { ... нормалізовані поля ... } ]
    }

Примітка: Форматовані рядкові значення (`price_str`, `volume_str`, `tp_sl`) додаються
щоб UI не перевизначав бізнес-логіку форматування.
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from datetime import datetime
from typing import Any, Protocol

from rich.console import Console
from rich.logging import RichHandler

from config.config import (
    REDIS_CHANNEL_ASSET_STATE,
    REDIS_CHANNEL_UI_ASSET_STATE,
    REDIS_SNAPSHOT_KEY,
    REDIS_SNAPSHOT_UI_KEY,
    UI_DUAL_PUBLISH,
    UI_PAYLOAD_SCHEMA_VERSION,
    UI_SNAPSHOT_TTL_SEC,
    UI_TP_SL_FROM_STAGE3_ENABLED,
    UI_USE_V2_NAMESPACE,
)
from utils.utils import format_price as fmt_price_stage1
from utils.utils import format_volume_usd
from utils.utils import map_reco_to_signal as _map_reco_to_signal

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("ui.publish_full_state")
if not logger.handlers:  # guard від повторної ініціалізації
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False

# Монотонний sequence для meta (у межах процесу)
_SEQ: int = 0


def _safe_float(value: Any) -> float | None:
    """Конвертує значення у float або повертає None."""

    try:
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str) and value.strip():
            return float(value)
    except Exception:
        return None
    return None


class RedisLike(Protocol):
    async def publish(
        self, channel: str, message: str
    ) -> int:  # pragma: no cover - типізація
        ...

    async def set(self, key: str, value: str) -> object:  # pragma: no cover - типізація
        ...


class AssetStateManagerProto(Protocol):
    def get_all_assets(self) -> list[dict[str, Any]]:  # pragma: no cover - типізація
        ...


async def publish_full_state(
    state_manager: AssetStateManagerProto, cache_handler: object, redis_conn: RedisLike
) -> None:
    """Публікує агрегований стан активів у Redis одним повідомленням.

    Формат payload (type = REDIS_CHANNEL_ASSET_STATE):
        {
            "type": REDIS_CHANNEL_ASSET_STATE,
            "meta": {"ts": ISO8601UTC},
            "counters": {"assets": N, "alerts": A},
            "assets": [ ... нормалізовані поля ... ]
        }

    UI може брати заголовок зі ``counters``, а таблицю — з ``assets``.

    Args:
        state_manager: Постачальник станів активів (має метод ``get_all_assets()``).
        cache_handler: Резервний параметр для майбутнього кешу (не використовується).
        redis_conn: Підключення до Redis із методами ``publish`` та ``set``.

    Returns:
        None: Побічно публікує повідомлення у канал і зберігає снапшот у Redis.

    Raises:
        Винятки драйвера Redis або серіалізації зазвичай перехоплюються та логуються,
        оскільки виконання обгорнуто у блок ``try`` (best‑effort).
    """
    try:
        all_assets = state_manager.get_all_assets()  # список dict
        serialized_assets: list[dict[str, Any]] = []
        band_samples: list[float] = []
        dist_edge_samples: list[float] = []
        edge_ratio_samples: list[float] = []
        low_gate_samples: list[float] = []
        atr_meta_samples: list[float] = []
        atr_vs_low_gate_samples: list[float] = []
        near_edge_counter: Counter[str] = Counter()
        near_edge_alerts = 0
        near_edge_total = 0
        within_true = 0
        within_false = 0
        low_vol_assets = 0
        low_vol_alerts = 0

        # Попередньо завантажимо core:trades для TP/SL таргетів (best-effort)
        core_trades: dict[str, Any] | None = None
        try:
            # cache_handler може бути UnifiedStore із redis.jget; якщо ні — пропускаємо
            redis_attr = getattr(cache_handler, "redis", None)
            jget = getattr(redis_attr, "jget", None) if redis_attr is not None else None
            if callable(jget):
                core_doc = await jget("core", default=None)
                if isinstance(core_doc, dict):
                    core_trades = core_doc.get("trades")
        except Exception:
            core_trades = None

        # Витягнемо мапу targets із core_trades (символ → {tp,sl})
        targets_map: dict[str, dict[str, float]] = {}
        try:
            if isinstance(core_trades, dict) and isinstance(
                core_trades.get("targets"), dict
            ):
                # Нормалізуємо ключі символів до upper
                for k, v in core_trades["targets"].items():
                    if isinstance(k, str) and isinstance(v, dict):
                        sym = k.upper()
                        tpv = v.get("tp")
                        slv = v.get("sl")
                        if isinstance(tpv, (int, float)) and isinstance(
                            slv, (int, float)
                        ):
                            targets_map[sym] = {"tp": float(tpv), "sl": float(slv)}
        except Exception:
            targets_map = {}

        for asset in all_assets:
            # Захист: stats має бути dict
            if not isinstance(asset.get("stats"), dict):
                asset["stats"] = {}
            # числові поля для рядка таблиці
            for key in ["tp", "sl", "rsi", "volume", "atr", "confidence"]:
                if key in asset:
                    try:
                        asset[key] = (
                            float(asset[key])
                            if asset[key] not in [None, "", "NaN"]
                            else 0.0
                        )
                    except (TypeError, ValueError):
                        asset[key] = 0.0

            # ціна для UI: форматування виконується нижче через fmt_price_stage1

            # нормалізуємо базові статс (лише якщо ключ існує; не вводимо штучні 0.0)
            if "stats" in asset:
                for stat_key in [
                    "current_price",
                    "atr",
                    "volume_mean",
                    "open_interest",
                    "rsi",
                    "rel_strength",
                    "btc_dependency_score",
                ]:
                    if stat_key in asset["stats"]:
                        try:
                            val = asset["stats"][stat_key]
                            asset["stats"][stat_key] = (
                                float(val) if val not in [None, "", "NaN"] else None
                            )
                        except (TypeError, ValueError):  # narrow: очікувана валідація
                            asset["stats"][stat_key] = None

            # ── UI flattening layer ────────────────────────────────────────
            stats = asset.get("stats") or {}
            # Уніфіковані кореневі ключі, щоб UI не мав додаткових мапперів
            # Ціну ВСІГДА беремо зі stats.current_price (джерело правди).
            cp = stats.get("current_price")
            try:
                cp_f = float(cp) if cp is not None else None
            except Exception:
                cp_f = None
            if cp_f is not None and cp_f > 0:
                asset["price"] = cp_f
                try:
                    asset["price_str"] = fmt_price_stage1(
                        float(asset["price"]), str(asset.get("symbol", "")).lower()
                    )
                except Exception:
                    asset.pop("price_str", None)
            else:
                # Поточна ціна невалідна → прибираємо застаріле форматування
                asset.pop("price", None)
                asset.pop("price_str", None)
            # Raw volume_mean (кількість контрактів/штук) — оновлюємо КОЖЕН цикл
            vm = stats.get("volume_mean")
            try:
                if isinstance(vm, (int, float)):
                    asset["raw_volume"] = float(vm)
                else:
                    asset.pop("raw_volume", None)
            except Exception:
                asset.pop("raw_volume", None)
            # Обчислюємо оборот у USD (notional) = raw_volume * current_price (переобчислюємо кожен раз)
            cp_val = stats.get("current_price")
            try:
                cp_f2 = float(cp_val) if cp_val is not None else None
            except Exception:
                cp_f2 = None
            if (
                isinstance(asset.get("raw_volume"), (int, float))
                and cp_f2 is not None
                and cp_f2 > 0
            ):
                asset["volume"] = float(asset["raw_volume"]) * float(cp_f2)
                try:
                    asset["volume_str"] = format_volume_usd(float(asset["volume"]))
                except Exception:
                    asset.pop("volume_str", None)
            else:
                asset.pop("volume", None)
                asset.pop("volume_str", None)
            # ATR% (для UI) — перераховуємо завжди (може змінюватися ATR або ціна)
            atr_v = stats.get("atr")
            cp_for_atr = stats.get("current_price")
            try:
                atr_f = float(atr_v) if atr_v is not None else None
            except Exception:
                atr_f = None
            try:
                cp_f_atr = float(cp_for_atr) if cp_for_atr is not None else None
            except Exception:
                cp_f_atr = None
            if atr_f is not None and cp_f_atr is not None and cp_f_atr > 0:
                asset["atr_pct"] = float(atr_f) / float(cp_f_atr) * 100.0
            else:
                # Якщо більше невалідно — прибираємо, щоб не залишався застарілий відсоток
                asset.pop("atr_pct", None)
            # RSI — перезаписуємо якщо присутній у stats; не тримаємо старе значення
            rsi_v = stats.get("rsi")
            try:
                rsi_f = float(rsi_v) if rsi_v is not None else None
            except Exception:
                rsi_f = None
            if rsi_f is not None:
                asset["rsi"] = rsi_f
            else:
                asset.pop("rsi", None)
            # status: перераховуємо щоразу, щоб не застрягав у 'init'
            status_val = asset.get("state")
            if isinstance(status_val, dict):  # захист
                status_val = status_val.get("status") or status_val.get("state")
            if not isinstance(status_val, str) or not status_val:
                status_val = (
                    asset.get("scenario") or asset.get("stage2_status") or "normal"
                )
            # Більше НЕ замінюємо 'init' на 'initializing' – коротка форма
            asset["status"] = status_val

            # Узгодження сигналу зі Stage2 recommendation: якщо rec → ALERT*,
            # форсуємо signal і статус 'alert', аби уникнути розсинхрону зі стейтом
            try:
                rec_val = asset.get("recommendation")
                sig_from_rec = _map_reco_to_signal(rec_val)
                # Сигнал у колонці "Сигнал" = Stage2 мапований;
                # Статус (state) не форсуємо — якщо Stage1 виставив ALERT і Stage2 понизив, залишаємо ALERT.
                if sig_from_rec in ("ALERT_BUY", "ALERT_SELL"):
                    asset["signal"] = sig_from_rec
                # Якщо сигнали нейтральні, не чіпаємо asset['state'] / status
            except Exception:
                pass

            # tp_sl: береться виключно зі Stage3 (core:trades.targets), без локальних розрахунків
            # Можна вимкнути повністю через feature‑flag UI_TP_SL_FROM_STAGE3_ENABLED
            if not UI_TP_SL_FROM_STAGE3_ENABLED:
                asset["tp_sl"] = "-"
            else:
                try:
                    sym_up = str(asset.get("symbol", "")).upper()
                    tgt = targets_map.get(sym_up)
                    if (
                        tgt
                        and isinstance(tgt.get("tp"), (int, float))
                        and isinstance(tgt.get("sl"), (int, float))
                    ):
                        fmt_tp = fmt_price_stage1(float(tgt["tp"]), sym_up.lower())
                        fmt_sl = fmt_price_stage1(float(tgt["sl"]), sym_up.lower())
                        asset["tp_sl"] = f"TP: {fmt_tp} | SL: {fmt_sl}"
                    else:
                        asset["tp_sl"] = "-"
                except Exception:
                    asset["tp_sl"] = "-"
            # гарантуємо signal (для UI фільтра)
            if not asset.get("signal"):
                asset["signal"] = "NONE"
            # видимість (fallback True якщо не задано)
            if "visible" in asset and asset["visible"] is False:
                pass  # залишаємо як є
            else:
                asset.setdefault("visible", True)

            # Проксі метаданих HTF та коридорної аналітики для UI
            mc_raw = asset.get("market_context")
            mc = mc_raw if isinstance(mc_raw, dict) else {}
            meta_candidate = mc.get("meta") if isinstance(mc, dict) else {}
            meta = meta_candidate if isinstance(meta_candidate, dict) else {}
            analytics_bucket = asset.get("analytics")
            if not isinstance(analytics_bucket, dict):
                analytics_bucket = {}

            try:
                if "htf_alignment" in meta and "htf_alignment" not in asset:
                    val = meta.get("htf_alignment")
                    if isinstance(val, (int, float)):
                        asset["htf_alignment"] = float(val)
                if "htf_ok" in meta and "htf_ok" not in asset:
                    hov = meta.get("htf_ok")
                    if isinstance(hov, bool):
                        asset["htf_ok"] = hov
                if "htf_ok" in meta:
                    hov = meta.get("htf_ok")
                    if isinstance(hov, bool):
                        analytics_bucket.setdefault("htf_ok", hov)
            except Exception:
                pass

            corridor_meta: dict[str, Any] = {}
            corridor_candidate = (
                meta.get("corridor") if isinstance(meta, dict) else None
            )
            if isinstance(corridor_candidate, dict):
                corridor_meta = corridor_candidate
            else:
                km = mc.get("key_levels_meta") if isinstance(mc, dict) else {}
                if isinstance(km, dict):
                    corridor_meta = km

            signal_upper = str(asset.get("signal", "")).upper()
            was_near_edge_asset = False

            low_gate_val = _safe_float(meta.get("low_gate"))
            if low_gate_val is not None:
                analytics_bucket["low_gate"] = low_gate_val
                low_gate_samples.append(low_gate_val)

            atr_meta_val = _safe_float(meta.get("atr_pct"))
            if atr_meta_val is not None:
                analytics_bucket["atr_pct_stage2"] = atr_meta_val
                atr_meta_samples.append(atr_meta_val)

            atr_vs_low_gate = None
            if (
                atr_meta_val is not None
                and low_gate_val is not None
                and low_gate_val > 0
            ):
                atr_vs_low_gate = atr_meta_val / low_gate_val
                analytics_bucket["atr_vs_low_gate_ratio"] = atr_vs_low_gate
                atr_vs_low_gate_samples.append(atr_vs_low_gate)

            low_vol_flag: bool | None = None
            if atr_meta_val is not None and low_gate_val is not None:
                low_vol_flag = atr_meta_val < low_gate_val
                if low_vol_flag:
                    low_vol_assets += 1
                    if signal_upper.startswith("ALERT"):
                        low_vol_alerts += 1
                analytics_bucket["low_volatility_flag"] = low_vol_flag

            band_val = _safe_float(corridor_meta.get("band_pct"))
            if band_val is not None:
                analytics_bucket["corridor_band_pct"] = band_val
                band_samples.append(band_val)

            dist_edge_pct = _safe_float(corridor_meta.get("dist_to_edge_pct"))
            if dist_edge_pct is not None:
                analytics_bucket["corridor_dist_to_edge_pct"] = dist_edge_pct
                dist_edge_samples.append(dist_edge_pct)

            dist_edge_ratio = _safe_float(corridor_meta.get("dist_to_edge_ratio"))
            if dist_edge_ratio is not None:
                analytics_bucket["corridor_dist_to_edge_ratio"] = dist_edge_ratio
                edge_ratio_samples.append(dist_edge_ratio)

            nearest_edge = corridor_meta.get("nearest_edge")
            if isinstance(nearest_edge, str):
                analytics_bucket["corridor_nearest_edge"] = nearest_edge

            near_edge_val = corridor_meta.get("near_edge")
            if isinstance(near_edge_val, str):
                analytics_bucket["corridor_near_edge"] = near_edge_val
                near_edge_counter[near_edge_val] += 1
                was_near_edge_asset = True

            is_near_edge = corridor_meta.get("is_near_edge")
            if isinstance(is_near_edge, bool):
                analytics_bucket["corridor_is_near_edge"] = is_near_edge
                if is_near_edge:
                    was_near_edge_asset = True

            within_corridor = corridor_meta.get("within_corridor")
            if isinstance(within_corridor, bool):
                analytics_bucket["corridor_within"] = within_corridor
                if within_corridor:
                    within_true += 1
                else:
                    within_false += 1

            if was_near_edge_asset:
                near_edge_total += 1
                if signal_upper.startswith("ALERT"):
                    near_edge_alerts += 1

            if analytics_bucket:
                asset["analytics"] = analytics_bucket
            else:
                asset.pop("analytics", None)

            serialized_assets.append(asset)

        analytics_summary: dict[str, Any] = {}
        total_assets = len(serialized_assets)
        if band_samples:
            analytics_summary["corridor_band_pct"] = {
                "avg": round(sum(band_samples) / len(band_samples), 5),
                "min": round(min(band_samples), 5),
                "max": round(max(band_samples), 5),
                "count": len(band_samples),
            }
        if dist_edge_samples:
            analytics_summary["corridor_dist_to_edge_pct"] = {
                "avg": round(sum(dist_edge_samples) / len(dist_edge_samples), 5),
                "min": round(min(dist_edge_samples), 5),
                "max": round(max(dist_edge_samples), 5),
                "count": len(dist_edge_samples),
            }
        if edge_ratio_samples:
            analytics_summary["corridor_dist_to_edge_ratio"] = {
                "avg": round(sum(edge_ratio_samples) / len(edge_ratio_samples), 5),
                "min": round(min(edge_ratio_samples), 5),
                "max": round(max(edge_ratio_samples), 5),
                "count": len(edge_ratio_samples),
            }
        if low_gate_samples:
            analytics_summary["low_gate"] = {
                "avg": round(sum(low_gate_samples) / len(low_gate_samples), 5),
                "min": round(min(low_gate_samples), 5),
                "max": round(max(low_gate_samples), 5),
                "count": len(low_gate_samples),
            }
        if atr_meta_samples:
            analytics_summary["atr_pct_stage2"] = {
                "avg": round(sum(atr_meta_samples) / len(atr_meta_samples), 5),
                "min": round(min(atr_meta_samples), 5),
                "max": round(max(atr_meta_samples), 5),
                "count": len(atr_meta_samples),
            }
        if atr_vs_low_gate_samples:
            analytics_summary["atr_vs_low_gate_ratio"] = {
                "avg": round(
                    sum(atr_vs_low_gate_samples) / len(atr_vs_low_gate_samples), 5
                ),
                "min": round(min(atr_vs_low_gate_samples), 5),
                "max": round(max(atr_vs_low_gate_samples), 5),
                "count": len(atr_vs_low_gate_samples),
            }
        if near_edge_counter:
            analytics_summary["near_edge_counts"] = dict(near_edge_counter)
        if near_edge_total:
            analytics_summary["near_edge_assets"] = int(near_edge_total)
            if total_assets:
                analytics_summary["near_edge_assets_share"] = round(
                    near_edge_total / total_assets, 3
                )
        if near_edge_alerts:
            analytics_summary["near_edge_alerts"] = int(near_edge_alerts)
        if within_true or within_false:
            analytics_summary["within_corridor"] = {
                "true": int(within_true),
                "false": int(within_false),
            }
        if low_vol_assets or low_vol_alerts:
            summary_block = {
                "assets": int(low_vol_assets),
            }
            if total_assets:
                summary_block["assets_share"] = round(low_vol_assets / total_assets, 3)
            if low_vol_alerts:
                summary_block["alerts"] = int(low_vol_alerts)
            analytics_summary["low_volatility"] = summary_block

        # counters для хедера (+ базові агрегати Stage3‑гейтів)
        alerts_list = [
            a
            for a in serialized_assets
            if str(a.get("signal", "")).upper().startswith("ALERT")
        ]
        htf_blocks = 0
        lowatr_blocks = 0
        alerts_buy = 0
        alerts_sell = 0
        for a in alerts_list:
            sig = str(a.get("signal", "")).upper()
            if sig == "ALERT_BUY":
                alerts_buy += 1
            elif sig == "ALERT_SELL":
                alerts_sell += 1
            # Оцінка потенційних блоків Stage3: якщо meta доступна
            try:
                meta = (a.get("market_context") or {}).get("meta", {})
                if isinstance(meta, dict):
                    if meta.get("htf_ok") is False:
                        htf_blocks += 1
                    atr_pct = meta.get("atr_pct")
                    low_gate = meta.get("low_gate")
                    if (
                        isinstance(atr_pct, (int, float))
                        and isinstance(low_gate, (int, float))
                        and float(atr_pct) < float(low_gate)
                    ):
                        lowatr_blocks += 1
            except Exception:
                pass
        # Додаткові лічильники (best-effort): скільки згенеровано/пропущено за цикл
        # Якщо state_manager надає ці значення, використаємо їх; інакше не включаємо
        generated_signals = None
        skipped_signals = None
        try:
            generated_signals = getattr(state_manager, "generated_signals", None)
            skipped_signals = getattr(state_manager, "skipped_signals", None)
        except Exception:
            pass

        # counters (int-only): агрегати для хедера UI; метрики з плаваючою точкою (percentiles) виносимо в confidence_stats
        counters: dict[str, int] = {}
        counters["assets"] = int(len(serialized_assets))
        counters["alerts"] = int(len(alerts_list))
        counters["alerts_buy"] = int(alerts_buy)
        counters["alerts_sell"] = int(alerts_sell)
        counters["htf_blocked"] = int(htf_blocks)
        counters["lowatr_blocked"] = int(lowatr_blocks)
        if isinstance(generated_signals, int):
            counters["generated_signals"] = generated_signals
        if isinstance(skipped_signals, int):
            counters["skipped_signals"] = skipped_signals
        # Додаємо накопичувальні лічильники блокувань / проходжень ALERT (якщо є у state_manager)
        try:
            blocked_lv = getattr(state_manager, "blocked_alerts_lowvol", None)
            blocked_htf = getattr(state_manager, "blocked_alerts_htf", None)
            blocked_lc = getattr(state_manager, "blocked_alerts_lowconf", None)
            blocked_lv_lc = getattr(
                state_manager, "blocked_alerts_lowvol_lowconf", None
            )
            passed_total = getattr(state_manager, "passed_alerts", None)
            downgraded_total = getattr(state_manager, "downgraded_alerts", None)
            if isinstance(blocked_lv, int):
                counters["blocked_alerts_lowvol"] = blocked_lv
            if isinstance(blocked_htf, int):
                counters["blocked_alerts_htf"] = blocked_htf
            if isinstance(blocked_lc, int):
                counters["blocked_alerts_lowconf"] = blocked_lc
            if isinstance(blocked_lv_lc, int):
                counters["blocked_alerts_lowvol_lowconf"] = blocked_lv_lc
            if isinstance(passed_total, int):
                counters["passed_alerts"] = passed_total
            if isinstance(downgraded_total, int):
                counters["downgraded_alerts"] = downgraded_total
        except Exception:
            pass

        # Confidence перцентилі (best-effort) — окремо від counters (щоб counters залишались int-only для сумісності)
        confidence_stats: dict[str, float] | None = None
        try:
            samples = getattr(state_manager, "conf_samples", [])
            if isinstance(samples, list) and len(samples) >= 5:
                import math

                sorted_vals = [v for v in samples if isinstance(v, (int, float))]
                sorted_vals.sort()
                if sorted_vals:

                    def _pct(p: float) -> float:
                        k = (len(sorted_vals) - 1) * p
                        f = math.floor(k)
                        c = math.ceil(k)
                        if f == c:
                            return float(sorted_vals[int(k)])
                        d0 = sorted_vals[f] * (c - k)
                        d1 = sorted_vals[c] * (k - f)
                        return float(d0 + d1)

                    confidence_stats = {
                        "p50": round(_pct(0.50), 3),
                        "p75": round(_pct(0.75), 3),
                        "p90": round(_pct(0.90), 3),
                        "count": float(len(sorted_vals)),  # для дебагу/контексту
                    }
        except Exception:
            confidence_stats = None

        # Нормалізуємо символи для UI (єдиний формат UPPER)
        for a in serialized_assets:
            if isinstance(a, dict) and "symbol" in a:
                try:
                    a["symbol"] = str(a["symbol"]).upper()
                except Exception:  # broad except: upper-case sanitation
                    pass

        # Оновлюємо sequence (проста монотонність у межах процесу)
        global _SEQ
        _SEQ = (_SEQ + 1) if _SEQ < 2**31 - 1 else 1

        payload = {
            "type": REDIS_CHANNEL_ASSET_STATE,
            "meta": {
                "ts": datetime.utcnow().isoformat() + "Z",
                "seq": _SEQ,
                "schema_version": UI_PAYLOAD_SCHEMA_VERSION,
            },
            "counters": counters,
            "assets": serialized_assets,
        }
        if analytics_summary:
            payload["analytics"] = analytics_summary
        if confidence_stats:
            payload["confidence_stats"] = confidence_stats

        try:
            if serialized_assets:
                first_keys = list(serialized_assets[0].keys())
            else:
                first_keys = []
            logger.debug(
                "Publish payload counters=%s assets_len=%d first_asset_keys=%s",
                counters,
                len(serialized_assets),
                first_keys,
            )
        except Exception:
            pass

        payload_json = json.dumps(payload, default=str)
        # Вибір namespace для публікації (PR6): v1 або v2, та опційний dual‑publish
        # Зчитуємо фіче-флаги один раз (можливий override через ENV поза тестами)
        use_v2 = bool(UI_USE_V2_NAMESPACE)
        dual_publish = bool(UI_DUAL_PUBLISH)

        primary_snapshot = REDIS_SNAPSHOT_UI_KEY if use_v2 else REDIS_SNAPSHOT_KEY
        primary_channel = (
            REDIS_CHANNEL_UI_ASSET_STATE if use_v2 else REDIS_CHANNEL_ASSET_STATE
        )
        secondary_snapshot = REDIS_SNAPSHOT_KEY if use_v2 else REDIS_SNAPSHOT_UI_KEY
        secondary_channel = (
            REDIS_CHANNEL_ASSET_STATE if use_v2 else REDIS_CHANNEL_UI_ASSET_STATE
        )

        # Спочатку snapshot → потім publish (щоб listener мав консистентний снапшот)
        async def _set_with_ttl(key: str) -> None:
            try:
                await redis_conn.set(key, payload_json)
                try:
                    await redis_conn.expire(key, UI_SNAPSHOT_TTL_SEC)  # type: ignore[attr-defined]
                except Exception:
                    pass
            except Exception:
                logger.debug("Не вдалося записати snapshot key=%s", key, exc_info=True)

        await _set_with_ttl(primary_snapshot)
        if dual_publish:
            await _set_with_ttl(secondary_snapshot)

        # Публікуємо у основний канал та, за потреби, в обидва
        await redis_conn.publish(primary_channel, payload_json)
        if dual_publish:
            try:
                await redis_conn.publish(secondary_channel, payload_json)
            except Exception:
                logger.debug(
                    "Dual publish у %s не вдався", secondary_channel, exc_info=True
                )

        logger.info(f"✅ Опубліковано стан {len(serialized_assets)} активів")

    except Exception as e:  # broad except: публікація best-effort
        logger.error(f"Помилка публікації стану: {str(e)}")


__all__ = ["publish_full_state"]
# -*- coding: utf-8 -*-
