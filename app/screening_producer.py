"""Stage1→Stage2→State публішер (формування агрегованого стану активів).

Шлях: ``app/screening_producer.py``

Призначення:
    • періодичний збір даних через UnifiedDataStore і Stage1 монітор;
    • нормалізація та уніфікація сигналів (confidence / tp/sl / triggers);
    • публікація повного snapshot у Redis (канал і ключ) для UI;
    • оновлення життєвого циклу трейдів через Stage3 `TradeLifecycleManager`.
"""

# ───────────── Стандартна бібліотека ─────────────
import asyncio
import json
import logging
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, cast

# ───────────── Third-party ─────────────
from rich.console import Console
from rich.logging import RichHandler

from app.utils.helper import (
    estimate_atr_pct,
    resample_5m,
    store_to_dataframe,
)
from config.config import (
    ASSET_STATE,
    DEFAULT_LOOKBACK,
    DEFAULT_TIMEFRAME,
    MAX_PARALLEL_STAGE2,
    MIN_READY_PCT,
    STAGE2_STATUS,
    TRADE_REFRESH_INTERVAL,
)
from stage1.asset_monitoring import AssetMonitorStage1
from stage2.level_manager import LevelManager
from stage2.processor import Stage2Processor
from stage3.open_trades import open_trades
from stage3.trade_manager import TradeLifecycleManager
from UI.publish_full_state import RedisLike, publish_full_state
from utils.utils import (
    create_error_signal,
    create_no_data_signal,
    first_not_none,
    get_tick_size,
    map_reco_to_signal,
    normalize_result_types,
)

from .asset_state_manager import AssetStateManager

# ───────────── Внутрішні модулі ─────────────


if TYPE_CHECKING:  # pragma: no cover - only for type hints
    from data.unified_store import UnifiedDataStore

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("app.screening_producer")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False


async def process_asset_batch(
    symbols: list[str],
    monitor: AssetMonitorStage1,
    store: "UnifiedDataStore",
    timeframe: str,
    lookback: int,
    state_manager: AssetStateManager,
) -> None:
    """Обробляє батч символів через UnifiedDataStore.

    Очікується: store.get_df(symbol, interval, limit=lookback) -> DataFrame з open_time.
    """
    for symbol in symbols:
        try:
            df = await store.get_df(symbol, timeframe, limit=lookback)
            if df is None or df.empty or len(df) < 5:
                state_manager.update_asset(symbol, create_no_data_signal(symbol))
                continue
            if "open_time" in df.columns and "timestamp" not in df.columns:
                df = df.rename(columns={"open_time": "timestamp"})
            # ── Базові метрики оновлюємо КОЖЕН цикл (щоб UI не «застирав») ──
            try:
                current_price = (
                    float(df["close"].iloc[-1]) if "close" in df.columns else None
                )
            except Exception:
                current_price = None
            try:
                volume_last = (
                    float(df["volume"].iloc[-1]) if "volume" in df.columns else None
                )
            except Exception:
                volume_last = None
            last_ts_val = None
            if "timestamp" in df.columns:
                try:
                    last_ts_val = df["timestamp"].iloc[-1]
                except Exception:
                    last_ts_val = None

            signal = await monitor.check_anomalies(symbol, df)
            if not isinstance(signal, dict):  # захист від невалідного повернення
                signal = {"symbol": symbol.lower(), "signal": "NONE", "stats": {}}

            # Гарантуємо наявність контейнера stats
            stats_container = signal.get("stats")
            if not isinstance(stats_container, dict):
                stats_container = {}
                signal["stats"] = stats_container

            # ВАЖЛИВО: ці базові метрики ОНОВЛЮЄМО КОЖЕН ЦИКЛ (інакше UI «зависає» на першому значенні)
            # Раніше тут було set-if-missing, що призводило до застиглих price/volume/ts → повертаємо always-overwrite.
            if current_price is not None:
                stats_container["current_price"] = current_price
            if volume_last is not None:
                stats_container["volume"] = volume_last
            if last_ts_val is not None:
                stats_container["timestamp"] = last_ts_val

            # Нормалізуємо типи (існуючі метрики збережуться)
            normalized = normalize_result_types(signal)
            # Переконуємось, що нормалізація не втратила базові stats
            try:
                norm_stats = normalized.get("stats")
                if not isinstance(norm_stats, dict):
                    normalized["stats"] = stats_container
                else:
                    for k, v in stats_container.items():
                        norm_stats.setdefault(k, v)
            except Exception:
                normalized["stats"] = stats_container

            state_manager.update_asset(symbol, normalized)
        except Exception as e:
            logger.error(f"Помилка AssetMonitor для {symbol}: {str(e)}")
            state_manager.update_asset(symbol, create_error_signal(symbol, str(e)))


_map_reco_to_signal = map_reco_to_signal
_first_not_none = first_not_none
# PR4: Міграція TP/SL на Stage3 — не нормалізуємо TP/SL у продюсері


async def process_single_stage2(
    signal: dict[str, Any],
    processor: "Stage2Processor",
    state_manager: "AssetStateManager",
) -> None:
    """Обробляє один Stage2-сигнал і оновлює стан активу."""
    symbol = signal["symbol"]
    try:
        state_manager.update_asset(
            symbol, {"stage2_status": STAGE2_STATUS["PROCESSING"]}
        )

        # Stage2 (QDE_core під капотом)
        result: dict[str, Any] = await processor.process(signal)

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
        # PR4: TP/SL не використовуються у продюсері для формування UI

        conf = result.get("confidence_metrics", {}) or {}
        composite_conf = conf.get("composite_confidence", 0.0)

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
        except Exception:
            pass

        anomaly_det = result.get("anomaly_detection")
        # Об'єднуємо джерела причин: основний результат + market_context
        tr_from_result = result.get("trigger_reasons") or []
        tr_from_ctx = (
            market_ctx.get("trigger_reasons") if isinstance(market_ctx, dict) else []
        )
        if not isinstance(tr_from_ctx, list):
            tr_from_ctx = []
        trigger_reasons_raw = list(tr_from_result) + list(tr_from_ctx)

        update["signal"] = signal_type
        update["recommendation"] = recommendation or None
        update["scenario"] = scenario
        update["confidence"] = composite_conf
        if hints:
            update["hints"] = list(dict.fromkeys(hints))
        # Нормалізація TP/SL щоб уникнути кейсів типу TP нижче SL для BUY
        # PR4: TP/SL формуються і коригуються Stage3; продюсер їх не нормалізує
        trigger_add = None

        update["market_context"] = market_ctx or None
        update["risk_parameters"] = risk_params or None
        update["confidence_metrics"] = conf or None
        update["anomaly_detection"] = anomaly_det or None
        # Проксі службових полів від Stage2 (якщо були змінені гейтом)
        if "reco_original" in result and result.get("reco_original") != recommendation:
            update["reco_original"] = result.get("reco_original")
        if "reco_gate_reason" in result:
            update["reco_gate_reason"] = result.get("reco_gate_reason")
        existing = state_manager.state.get(symbol, {}).get("trigger_reasons") or []
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
        # Зберігаємо Stage1 alert у статусі (state) навіть якщо Stage2 понизив сигнал до NORMAL
        prev_state = (
            state_manager.state.get(symbol, {}).get("state")
            if symbol in state_manager.state
            else None
        )
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
        state_manager.update_asset(symbol, update)

        # ── Накопичувальні метрики проходження/блокування ALERT ──
        try:
            if signal_type.startswith("ALERT_BUY") or signal_type.startswith(
                "ALERT_SELL"
            ):
                state_manager.passed_alerts += 1
            elif signal_type == "NORMAL" and result.get("reco_original"):
                # був даунгрейд → класифікуємо причини
                state_manager.downgraded_alerts += 1
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
        except Exception:
            pass

        # JSONL аудит Stage2 рішень (best‑effort, без винятків)
        try:
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
                "near_edge": (
                    (market_ctx.get("key_levels_meta", {}) or {}).get("band_pct")
                    if isinstance(market_ctx, dict)
                    else None
                ),
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


async def process_single_stage2_with_semaphore(
    signal: dict[str, Any],
    processor: "Stage2Processor",
    semaphore: asyncio.Semaphore,
    state_manager: "AssetStateManager",
) -> None:
    async with semaphore:
        await process_single_stage2(signal, processor, state_manager)


async def screening_producer(
    monitor: AssetMonitorStage1,
    store: "UnifiedDataStore",
    store_fast_symbols: "UnifiedDataStore",
    assets: list[str],
    redis_conn: RedisLike,
    trade_manager: TradeLifecycleManager | None = None,
    reference_symbol: str = "BTCUSDT",
    timeframe: str = DEFAULT_TIMEFRAME,
    lookback: int = DEFAULT_LOOKBACK,
    interval_sec: int = TRADE_REFRESH_INTERVAL,
    min_ready_pct: float = MIN_READY_PCT,
    state_manager: AssetStateManager | None = None,
    level_manager: LevelManager | None = None,
    user_lang: str = "UA",
    user_style: str = "explain",
) -> None:
    logger.info(
        (
            "🚀 Старт screening_producer: %d активів, таймфрейм %s, глибина %d, "
            "оновлення кожні %d сек"
        ),
        len(assets),
        timeframe,
        lookback,
        interval_sec,
    )
    _last_levels_update_ts: dict[str, int] = {}
    levels_update_every = 25
    if state_manager is None:
        assets_current = [s.lower() for s in (assets or [])]
        state_manager = AssetStateManager(assets_current)
    else:
        assets_current = list(state_manager.state.keys())
    for sym in assets_current:
        state_manager.init_asset(sym)
    ref = (reference_symbol or "BTCUSDT").lower()
    if ref not in state_manager.state:
        state_manager.init_asset(ref)
    logger.info(f"Ініціалізовано стан для {len(assets_current)} активів")
    processor = Stage2Processor(
        timeframe=timeframe,
        state_manager=state_manager,
        level_manager=level_manager,
        user_lang=user_lang,
        user_style=user_style,
    )
    # Забезпечуємо доступ Stage2 до UnifiedDataStore через state_manager.cache (для публікацій у Redis)
    try:
        if getattr(state_manager, "cache", None) is None:
            state_manager.set_cache_handler(store)
    except Exception:
        pass
    stage2_semaphore = asyncio.Semaphore(MAX_PARALLEL_STAGE2)
    await publish_full_state(state_manager, store, redis_conn)
    while True:
        start_time = time.time()
        try:
            new_assets_raw = await store_fast_symbols.get_fast_symbols()
            if new_assets_raw:
                new_assets = [s.lower() for s in new_assets_raw]
                current_set = set(assets_current)
                new_set = set(new_assets)
                added = new_set - current_set
                removed = current_set - new_set
                for symbol in added:
                    state_manager.init_asset(symbol)
                assets_current = list(new_set)
                for symbol in removed:
                    state_manager.state.pop(symbol, None)
                if added or removed:
                    logger.info(
                        "🔄 Оновлено список активів: +%d/-%d (загалом: %d)",
                        len(added),
                        len(removed),
                        len(assets_current),
                    )
            else:
                logger.debug(
                    "get_fast_symbols() повернув порожньо — тримаємо попередній список (%d).",
                    len(assets_current),
                )
        except Exception as e:
            logger.error(f"Помилка оновлення активів: {str(e)}")
        ready_assets: list[str] = []
        ref_ready = False
        for symbol in assets_current:
            try:
                df_tmp = await store.get_df(symbol, timeframe, limit=lookback)
                if df_tmp is not None and not df_tmp.empty and len(df_tmp) >= lookback:
                    ready_assets.append(symbol)
            except Exception:
                continue
        try:
            ref_df = await store.get_df(
                reference_symbol.lower(), timeframe, limit=lookback
            )
            ref_ready = bool(
                ref_df is not None and not ref_df.empty and len(ref_df) >= lookback
            )
        except Exception:
            ref_ready = False
        ready_count = len(ready_assets)
        min_ready = max(1, int(len(assets_current) * min_ready_pct))
        if ready_count < min_ready:
            logger.warning(
                "⏳ Недостатньо даних: %d/%d активів готові. Очікування %d сек...",
                ready_count,
                min_ready,
                interval_sec,
            )
            # Встановлюємо явний стан NO_DATA для неготових активів,
            # щоб UI не зависав у стані 'init'.
            try:
                not_ready = [s for s in assets_current if s not in ready_assets]
                for symbol in not_ready:
                    state_manager.update_asset(symbol, create_no_data_signal(symbol))
                if not_ready:
                    logger.info(
                        "📭 NO_DATA для неготових активів: %d (публікація проміжного стану)",
                        len(not_ready),
                    )
                # Публікуємо частковий стан, щоб UI одразу побачив NO_DATA
                await publish_full_state(state_manager, store, redis_conn)
            except Exception as e:
                logger.error("Помилка під час оновлення NO_DATA: %s", str(e))
            await asyncio.sleep(interval_sec)
            # Переходимо до наступної ітерації while True
            continue
        logger.info(
            f"📊 Дані готові для {ready_count}/{len(assets_current)} активів"
            + (" (+reference ready)" if ref_ready else "")
        )
        now_ts = int(time.time())
        for symbol in ready_assets:
            last_ts = _last_levels_update_ts.get(symbol, 0)
            if (now_ts - last_ts) < levels_update_every:
                continue
            df_1m = await store_to_dataframe(store, symbol, limit=500)
            if df_1m is None or df_1m.empty:
                continue
            df_5m = resample_5m(df_1m)
            atr_pct = estimate_atr_pct(df_1m)
            price_hint = float(df_1m["close"].iloc[-1])
            tick_size = get_tick_size(symbol, price_hint=price_hint)
            if level_manager is not None:
                level_manager.update_meta(symbol, atr_pct=atr_pct, tick_size=tick_size)
                level_manager.update_from_bars(symbol, df_1m=df_1m, df_5m=df_5m)
            _last_levels_update_ts[symbol] = now_ts
        try:
            batch_size = 20
            tasks: list[asyncio.Task[Any]] = []
            for i in range(0, len(ready_assets), batch_size):
                batch = ready_assets[i : i + batch_size]
                tasks.append(
                    asyncio.create_task(
                        process_asset_batch(
                            batch, monitor, store, timeframe, lookback, state_manager
                        )
                    )
                )
            if tasks:
                await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Критична помилка Stage1: {str(e)}")
        alert_signals = state_manager.get_alert_signals()
        if alert_signals:
            logger.info(f"[Stage2] Обробка {len(alert_signals)} сигналів...")
            # Оновлюємо лічильник згенерованих сигналів для UI counters
            try:
                state_manager.generated_signals = int(
                    getattr(state_manager, "generated_signals", 0)
                ) + len(alert_signals)
            except Exception:
                pass
            tasks = [
                asyncio.create_task(
                    process_single_stage2_with_semaphore(
                        signal, processor, stage2_semaphore, state_manager
                    )
                )
                for signal in alert_signals
            ]
            await asyncio.gather(*tasks)
            logger.info(f"[Stage2] Завершено обробку {len(alert_signals)} сигналів")
        else:
            logger.info("[Stage2] Немає сигналів ALERT для обробки")
            # Якщо нічого не оброблялось — вважаємо це пропущеними для метрики 'skipped'
            try:
                state_manager.skipped_signals = (
                    int(getattr(state_manager, "skipped_signals", 0)) + 1
                )
            except Exception:
                pass
        logger.info("📢 Публікація стану активів...")
        await publish_full_state(state_manager, store, redis_conn)

        # Відкриття угод
        if trade_manager and alert_signals:
            logger.info("💼 Відкриття угод для Stage2 сигналів...")
            max_trades = getattr(trade_manager, "max_parallel_trades", 3)
            if max_trades is None or max_trades <= 0:
                max_trades = 3
            logger.info(f"Максимальна кількість угод: {max_trades}")
            await open_trades(alert_signals, trade_manager, max_trades)
        else:
            logger.info(
                "💼 Торгівля Stage2 вимкнена або немає сигналів для відкриття угод"
            )

        processing_time = time.time() - start_time
        logger.info(f"⏳ Час обробки циклу: {processing_time:.2f} сек")
        if processing_time < 1:
            logger.warning(
                "Час обробки циклу менше 1 секунди — система працює дуже швидко"
            )
        sleep_time = (
            1
            if processing_time >= interval_sec
            else max(1, interval_sec - int(processing_time))
        )
        logger.info(f"⏱ Час очікування до наступного циклу: {sleep_time} сек")
        await asyncio.sleep(sleep_time)
