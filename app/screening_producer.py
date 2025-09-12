# app/screening_producer.py
# -*- coding: utf-8 -*-

import pandas as pd
import logging
import asyncio
import json
import time
from typing import Any, Dict, List, Optional
from datetime import datetime

from rich.console import Console
from rich.logging import RichHandler

from stage1.asset_monitoring import AssetMonitorStage1
from stage3.trade_manager import TradeLifecycleManager
from utils.utils_1_2 import safe_float
from stage2.processor import Stage2Processor
from utils.utils_1_2 import ensure_timestamp_column
from stage2.level_manager import LevelManager
from app.utils.helper import (
    buffer_to_dataframe,
    resample_5m,
    estimate_atr_pct,
    get_tick_size,
)
from .asset_state_manager import AssetStateManager

# --- Налаштування логування ---
logger = logging.getLogger("app.screening_producer")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False

# --- Глобальні константи ---
DEFAULT_LOOKBACK = 20
DEFAULT_TIMEFRAME = "1m"
MIN_READY_PCT = 0.1
MAX_PARALLEL_STAGE2 = 10
MIN_CONFIDENCE_TRADE = 0.5
TRADE_REFRESH_INTERVAL = 60
BUY_SET = {"STRONG_BUY", "BUY_IN_DIPS"}
SELL_SET = {"STRONG_SELL", "SELL_ON_RALLIES"}


def normalize_result_types(result: dict) -> dict:
    """Нормалізує типи даних та додає стан для UI"""
    numeric_fields = [
        "confidence",
        "tp",
        "sl",
        "current_price",
        "atr",
        "rsi",
        "volume",
        "volume_mean",
        "volume_usd",
        "volume_z",
        "open_interest",
        "btc_dependency_score",
    ]

    if "calibrated_params" in result:
        result["calibrated_params"] = {
            k: float(v) for k, v in result["calibrated_params"].items()
        }

    for field in numeric_fields:
        if field in result:
            result[field] = safe_float(result[field])
        elif "stats" in result and field in result["stats"]:
            result["stats"][field] = safe_float(result["stats"][field])

    # Визначення стану сигналу
    signal_type = result.get("signal", "NONE").upper()
    if signal_type == "ALERT" or signal_type.startswith("ALERT_"):
        result["state"] = "alert"
    elif signal_type == "NORMAL":
        result["state"] = "normal"
    else:
        result["state"] = "no_trade"

    result["visible"] = True
    return result


def make_serializable_safe(data) -> Any:
    """Робить вкладені структури JSON-сумісними."""
    if isinstance(data, pd.DataFrame):
        return data.to_dict(orient="records")
    if hasattr(data, "to_dict") and not isinstance(data, dict):
        return data.to_dict()
    if isinstance(data, dict):
        return {k: make_serializable_safe(v) for k, v in data.items()}
    if isinstance(data, list):
        return [make_serializable_safe(x) for x in data]
    return data


async def process_asset_batch(
    symbols: list,
    monitor: AssetMonitorStage1,
    buffer: Any,
    timeframe: str,
    lookback: int,
    state_manager: AssetStateManager,
):
    """Обробляє батч символів та оновлює стан"""
    for symbol in symbols:
        bars = buffer.get(symbol, timeframe, lookback)
        if not bars or len(bars) < 5:
            state_manager.update_asset(symbol, create_no_data_signal(symbol))
            continue

        try:
            df = pd.DataFrame(bars)
            df = ensure_timestamp_column(df)
            signal = await monitor.check_anomalies(symbol, df)
            normalized = normalize_result_types(signal)
            state_manager.update_asset(symbol, normalized)
        except Exception as e:
            logger.error(f"Помилка AssetMonitor для {symbol}: {str(e)}")
            state_manager.update_asset(symbol, create_error_signal(symbol, str(e)))


def create_no_data_signal(symbol: str) -> Dict[str, Any]:
    return normalize_result_types(
        {
            "symbol": symbol,
            "signal": "NONE",
            "trigger_reasons": ["Недостатньо даних"],
            "confidence": 0.0,
            "hints": ["Недостатньо даних для аналізу"],
            "state": "no_data",
            "stage2_status": "skipped",
        }
    )


def create_error_signal(symbol: str, error: str) -> Dict[str, Any]:
    return normalize_result_types(
        {
            "symbol": symbol,
            "signal": "NONE",
            "trigger_reasons": ["Помилка обробки"],
            "confidence": 0.0,
            "hints": [f"Помилка: {error}"],
            "state": "error",
            "stage2_status": "error",
        }
    )


async def publish_full_state(
    state_manager: AssetStateManager, cache_handler: Any, redis_conn: Any
) -> None:
    """
    Публікує у Redis ОДНИМ повідомленням:
      {
        "meta": {...},
        "counters": {"assets": N, "alerts": A, "calibrated": 0, "queued": 0},
        "assets": [ ... рядки таблиці ... ]
      }
    UI може брати заголовок зі counters, а таблицю — з assets.
    """
    try:
        all_assets = state_manager.get_all_assets()  # список dict
        serialized_assets: List[Dict[str, Any]] = []

        for asset in all_assets:
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

            # ціна для UI
            if "stats" in asset and "current_price" in asset["stats"]:
                asset["price_str"] = str(asset["stats"]["current_price"])

            # нормалізуємо базові статс
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
                            asset["stats"][stat_key] = (
                                float(asset["stats"][stat_key])
                                if asset["stats"][stat_key] not in [None, "", "NaN"]
                                else 0.0
                            )
                        except (TypeError, ValueError):
                            asset["stats"][stat_key] = 0.0

            serialized_assets.append(asset)

        # counters для хедера
        counters = {
            "assets": len(serialized_assets),
            "alerts": len(
                [
                    a
                    for a in serialized_assets
                    if str(a.get("signal", "")).upper().startswith("ALERT")
                ]
            ),
        }

        payload = {
            "meta": {"ts": datetime.utcnow().isoformat() + "Z"},
            "counters": counters,
            "assets": serialized_assets,
        }

        await redis_conn.publish("asset_state_update", json.dumps(payload, default=str))
        logger.info(f"✅ Опубліковано стан {len(serialized_assets)} активів")

    except Exception as e:
        logger.error(f"Помилка публікації стану: {str(e)}")


def _map_reco_to_signal(recommendation: str) -> str:
    """Stage2 recommendation → тип сигналу для таблиці."""
    if recommendation in BUY_SET:
        return "ALERT_BUY"
    if recommendation in SELL_SET:
        return "ALERT_SELL"
    return "NORMAL"


def _first_not_none(seq: List[Optional[float]]) -> Optional[float]:
    for x in seq or []:
        if x is not None:
            return x
    return None


async def process_single_stage2(
    signal: Dict[str, Any],
    processor: "Stage2Processor",
    state_manager: "AssetStateManager",
) -> None:
    """Обробляє один Stage2-сигнал і оновлює стан активу."""
    symbol = signal["symbol"]
    try:
        state_manager.update_asset(symbol, {"stage2_status": "processing"})

        # Stage2 (QDE_core під капотом)
        result: Dict[str, Any] = await processor.process(signal)

        update: Dict[str, Any] = {
            "stage2": True,
            "stage2_status": "completed",
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
        if tp0 is not None:
            update["tp"] = tp0
        if sl is not None:
            update["sl"] = sl

        update["market_context"] = market_ctx or None
        update["risk_parameters"] = risk_params or None
        update["confidence_metrics"] = conf or None
        update["anomaly_detection"] = anomaly_det or None
        update["trigger_reasons"] = list(trigger_reasons or [])
        update["narrative"] = raw_narr or None

        state_manager.update_asset(symbol, update)

    except Exception:
        logger.exception("Stage2 помилка для %s", symbol)
        state_manager.update_asset(
            symbol, {"stage2_status": "error", "error": "Stage2 exception (див. логи)"}
        )


async def process_single_stage2_with_semaphore(
    signal: Dict[str, Any],
    processor: Stage2Processor,
    semaphore: asyncio.Semaphore,
    state_manager: AssetStateManager,
) -> None:
    """Обробка сигналу Stage2 з обмеженням через семафор"""
    async with semaphore:
        await process_single_stage2(signal, processor, state_manager)


async def screening_producer(
    monitor: AssetMonitorStage1,
    buffer: Any,
    cache_handler: Any,
    assets: List[str],
    redis_conn: Any,
    fetcher: Any,
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
    """
    Основний цикл генерації сигналів з централізованим станом (без калібрування).
    """
    logger.info(
        f"🚀 Старт screening_producer: {len(assets)} активів, таймфрейм {timeframe}, "
        f"глибина {lookback}, оновлення кожні {interval_sec} сек"
    )

    # throttle для оновлення рівнів (раз на 20–30 сек)
    _last_levels_update_ts: Dict[str, int] = {}
    LEVELS_UPDATE_EVERY = 25  # сек

    # Ініціалізація менеджера стану
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

    # Створюємо Stage2Processor (легка версія на QDE_core; LevelManager зберігаємо)
    processor = Stage2Processor(
        timeframe=timeframe,
        state_manager=state_manager,
        level_manager=level_manager,
        user_lang=user_lang,
        user_style=user_style,
    )

    # Семафор для Stage2
    stage2_semaphore = asyncio.Semaphore(MAX_PARALLEL_STAGE2)

    # Початкова публікація стану
    await publish_full_state(state_manager, cache_handler, redis_conn)

    # Основний цикл
    while True:
        start_time = time.time()

        # 1) Оновлення списку активів (НЕ обнуляємо, якщо кеш пустий)
        try:
            new_assets_raw = await cache_handler.get_fast_symbols()
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

        # 2) Перевірка готовності даних
        ready_assets: List[str] = []
        for symbol in assets_current + [reference_symbol.lower()]:
            bars = buffer.get(symbol, timeframe, lookback)
            if bars and len(bars) >= lookback:
                ready_assets.append(symbol)

        ready_count = len(ready_assets)
        min_ready = max(1, int(len(assets_current) * min_ready_pct))
        if ready_count < min_ready:
            logger.warning(
                f"⏳ Недостатньо даних: {ready_count}/{min_ready} активів готові. "
                f"Очікування {interval_sec} сек..."
            )
            await asyncio.sleep(interval_sec)
            continue

        logger.info(f"📊 Дані готові для {ready_count}/{len(assets_current)} активів")

        # 3) Оновлення LevelSystem v2
        now_ts = int(time.time())
        for symbol in ready_assets:
            last_ts = _last_levels_update_ts.get(symbol, 0)
            if (now_ts - last_ts) < LEVELS_UPDATE_EVERY:
                continue

            df_1m = buffer_to_dataframe(buffer, symbol, limit=500)
            if df_1m is None or df_1m.empty:
                continue

            df_5m = resample_5m(df_1m)
            atr_pct = estimate_atr_pct(df_1m)
            price_hint = float(df_1m["close"].iloc[-1])
            tick_size = get_tick_size(symbol, price_hint=price_hint)

            level_manager.update_meta(symbol, atr_pct=atr_pct, tick_size=tick_size)
            level_manager.update_from_bars(
                symbol, df_1m=df_1m, df_5m=df_5m
            )  # df_1d не обов'язковий

            _last_levels_update_ts[symbol] = now_ts

        # 4) Генерація Stage1 сигналів
        try:
            batch_size = 20
            tasks = []
            for i in range(0, len(ready_assets), batch_size):
                batch = ready_assets[i : i + batch_size]
                tasks.append(
                    process_asset_batch(
                        batch, monitor, buffer, timeframe, lookback, state_manager
                    )
                )
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Критична помилка Stage1: {str(e)}")

        # 5) Інтеграція Stage2 (QDE_core)
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

        # 6) Публікація стану активів
        logger.info("📢 Публікація стану активів...")
        await publish_full_state(state_manager, cache_handler, redis_conn)

        # 7) (за потреби) керування угодами — наразі вимкнено
        # if trade_manager and alert_signals:
        #     ...

        # 8) Цикл очікування
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
