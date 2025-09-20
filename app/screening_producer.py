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
from stage3.trade_manager import TradeLifecycleManager
from UI.publish_full_state import RedisLike, publish_full_state
from utils.utils import (
    create_error_signal,
    create_no_data_signal,
    ensure_timestamp_column,
    first_not_none,
    get_tick_size,
    map_reco_to_signal,
    normalize_result_types,
    normalize_tp_sl,
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
            df = ensure_timestamp_column(df)
            signal = await monitor.check_anomalies(symbol, df)
            normalized = normalize_result_types(signal)
            state_manager.update_asset(symbol, normalized)
        except Exception as e:
            logger.error(f"Помилка AssetMonitor для {symbol}: {str(e)}")
            state_manager.update_asset(symbol, create_error_signal(symbol, str(e)))


_map_reco_to_signal = map_reco_to_signal
_first_not_none = first_not_none
_normalize_tp_sl = normalize_tp_sl


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
        tp0 = _first_not_none(risk_params.get("tp_targets"))
        sl = risk_params.get("sl_level")
        # Поточна ціна (для логіки перевірки – необов'язкова)
        current_price = None
        try:
            current_price = signal.get("stats", {}).get("current_price")
        except Exception:
            current_price = None

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
        norm_tp, norm_sl, swapped, note = _normalize_tp_sl(
            tp0, sl, recommendation, current_price
        )
        if norm_tp is not None:
            update["tp"] = norm_tp
        if norm_sl is not None:
            update["sl"] = norm_sl
        if swapped:
            # додаємо службову причину, щоб було видно у UI (якщо користувач захоче)
            trigger_add = note or "tp_sl_swapped"
        else:
            trigger_add = None

        update["market_context"] = market_ctx or None
        update["risk_parameters"] = risk_params or None
        update["confidence_metrics"] = conf or None
        update["anomaly_detection"] = anomaly_det or None
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
        if signal_type.startswith("ALERT"):
            update["state"] = ASSET_STATE["ALERT"]
        elif signal_type == "NORMAL":
            update["state"] = ASSET_STATE["NORMAL"]
        else:
            update["state"] = update.get("state") or ASSET_STATE["NO_TRADE"]
        update["narrative"] = raw_narr or None
        state_manager.update_asset(symbol, update)

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
        logger.info("📢 Публікація стану активів...")
        await publish_full_state(state_manager, store, redis_conn)
        processing_time = time.time() - start_time
        # Метрика часу циклу (Prometheus якщо увімкнено)
        try:
            store.metrics.put_latency.labels(layer="screening_cycle").observe(
                processing_time
            )
        except Exception:
            pass
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
