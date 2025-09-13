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
import json  # TODO: видалити якщо не використовується
import logging
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional

# ───────────── Third-party ─────────────
from rich.console import Console
from rich.logging import RichHandler

# ───────────── Внутрішні модулі ─────────────

from config.config import (
    DEFAULT_LOOKBACK,
    DEFAULT_TIMEFRAME,
    MIN_READY_PCT,
    MAX_PARALLEL_STAGE2,
    TRADE_REFRESH_INTERVAL,
    STAGE2_STATUS,
    ASSET_STATE,
)
from stage1.asset_monitoring import AssetMonitorStage1
from stage3.trade_manager import TradeLifecycleManager
from UI.publish_full_state import publish_full_state
from utils.utils import (
    ensure_timestamp_column,
    map_reco_to_signal,
    first_not_none,
    normalize_tp_sl,
    normalize_result_types,
    create_no_data_signal,
    create_error_signal,
)
from stage2.processor import Stage2Processor
from stage2.level_manager import LevelManager
from app.utils.helper import (
    store_to_dataframe,
    resample_5m,
    estimate_atr_pct,
)
from utils.utils import get_tick_size
from .asset_state_manager import AssetStateManager

if TYPE_CHECKING:  # pragma: no cover - only for type hints
    from data.unified_store import UnifiedDataStore

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("app.screening_producer")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False


async def process_asset_batch(
    symbols: list,
    monitor: AssetMonitorStage1,
    store: "UnifiedDataStore",
    timeframe: str,
    lookback: int,
    state_manager: AssetStateManager,
):
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
    signal: Dict[str, Any],
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
        result: Dict[str, Any] = await processor.process(signal)

        update: Dict[str, Any] = {
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
        hints: List[str] = []
        if raw_narr:
            hints.append(raw_narr)
            try:
                logger.info("[NARR] %s %s", symbol, raw_narr.replace("\n", " "))
            except Exception:
                logger.debug("[NARR] %s (logging failed)", symbol)

        anomaly_det = result.get("anomaly_detection")
        trigger_reasons = result.get("trigger_reasons") or market_ctx.get(
            "trigger_reasons"
        )

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
        extra_triggers: List[str] = []
        if trigger_add:
            extra_triggers.append(trigger_add)
        merged_triggers = list(
            dict.fromkeys(list(existing) + list(trigger_reasons or []) + extra_triggers)
        )
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
    signal: Dict[str, Any],
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
    assets: List[str],
    redis_conn: Any,
    trade_manager: Optional[TradeLifecycleManager] = None,
    reference_symbol: str = "BTCUSDT",
    timeframe: str = DEFAULT_TIMEFRAME,
    lookback: int = DEFAULT_LOOKBACK,
    interval_sec: int = TRADE_REFRESH_INTERVAL,
    min_ready_pct: float = MIN_READY_PCT,
    state_manager: AssetStateManager = None,
    level_manager: LevelManager = None,
    user_lang: str = "UA",
    user_style: str = "explain",
) -> None:
    logger.info(
        f"🚀 Старт screening_producer: {len(assets)} активів, таймфрейм {timeframe}, глибина {lookback}, оновлення кожні {interval_sec} сек"
    )
    _last_levels_update_ts: Dict[str, int] = {}
    LEVELS_UPDATE_EVERY = 25
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
                        f"🔄 Оновлено список активів: +{len(added)}/-{len(removed)} (загалом: {len(assets_current)})"
                    )
            else:
                logger.debug(
                    "get_fast_symbols() повернув порожньо — тримаємо попередній список (%d).",
                    len(assets_current),
                )
        except Exception as e:
            logger.error(f"Помилка оновлення активів: {str(e)}")
        ready_assets: List[str] = []
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
                f"⏳ Недостатньо даних: {ready_count}/{min_ready} активів готові. Очікування {interval_sec} сек..."
            )
            await asyncio.sleep(interval_sec)
            continue
        logger.info(
            f"📊 Дані готові для {ready_count}/{len(assets_current)} активів"
            + (" (+reference ready)" if ref_ready else "")
        )
        now_ts = int(time.time())
        for symbol in ready_assets:
            last_ts = _last_levels_update_ts.get(symbol, 0)
            if (now_ts - last_ts) < LEVELS_UPDATE_EVERY:
                continue
            df_1m = await store_to_dataframe(store, symbol, limit=500)
            if df_1m is None or df_1m.empty:
                continue
            df_5m = resample_5m(df_1m)
            atr_pct = estimate_atr_pct(df_1m)
            price_hint = float(df_1m["close"].iloc[-1])
            tick_size = get_tick_size(symbol, price_hint=price_hint)
            level_manager.update_meta(symbol, atr_pct=atr_pct, tick_size=tick_size)
            level_manager.update_from_bars(symbol, df_1m=df_1m, df_5m=df_5m)
            _last_levels_update_ts[symbol] = now_ts
        try:
            batch_size = 20
            tasks: List[asyncio.Task] = []
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
