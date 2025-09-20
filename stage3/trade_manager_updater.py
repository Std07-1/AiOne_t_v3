"""Stage3 utility task: periodic trade state refresh.

Оновлює активні угоди, підтягує агреговані статистики зі Stage1 замість
сирих барів (ATR/RSI/Volume) і логгує кількість active/closed.

"""

from __future__ import annotations

import asyncio

# ── Imports ──────────────────────────────────────────────────────────────────
import logging
import math
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

from app.settings import load_datastore_cfg
from stage1.asset_monitoring import AssetMonitorStage1
from stage3.trade_manager import TradeLifecycleManager

try:  # optional Prometheus
    from prometheus_client import Gauge, Histogram  # type: ignore
except Exception:  # pragma: no cover
    Gauge = None  # type: ignore

# ── Logger ───────────────────────────────────────────────────────────────────
logger = logging.getLogger("stage3.trade_manager_updater")
if not logger.handlers:  # guard щоб не дублювати хендлери
    logger.setLevel(logging.INFO)
    try:  # optional rich
        logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    except Exception:  # broad except: rich може бути недоступний у середовищі
        logger.addHandler(logging.StreamHandler())
    logger.propagate = False


async def trade_manager_updater(
    trade_manager: TradeLifecycleManager,
    store: Any,
    monitor: AssetMonitorStage1,
    timeframe: str = "1m",
    lookback: int = 20,
    interval_sec: int = 30,
    log_interval_sec: int | None = None,
    log_on_change: bool = True,
    max_backoff_sec: int = 300,
    backoff_multiplier: float | None = None,
    publish_ui: bool = True,
    ui_ttl: int = 90,
    skipped_ewma_alpha: float | None = None,
):
    """Фонове оновлення стану угод.

    Параметри:
        timeframe: таймфрейм барів для оновлення метрик.
        lookback: скільки барів брати для розрахунку поточних stats.
        interval_sec: базовий інтервал циклу (poll).
        log_interval_sec: мінімальний інтервал між логами (override log_on_change).
        log_on_change: логувати тільки якщо змінилась кількість активних/закритих.
    """
    last_log_ts = 0.0
    last_counts: tuple[int, int] | None = None
    dynamic_interval = float(interval_sec)
    skipped_symbols = 0
    skip_reason_counts: dict[str, int] = {}
    last_warn_drift_ts = 0.0
    last_warn_pressure_ts = 0.0

    # Centralized config (best-effort)
    try:
        ds_cfg = load_datastore_cfg()
        tu_cfg = ds_cfg.trade_updater
    except Exception:
        tu_cfg = None
    if backoff_multiplier is None:
        backoff_multiplier = getattr(tu_cfg, "backoff_multiplier", 1.5)
    max_backoff_sec = getattr(tu_cfg, "max_backoff_sec", max_backoff_sec)

    # Prometheus gauges (optional) with safe registrar to avoid duplicate registration
    active_g: Any | None = None
    closed_g: Any | None = None
    cycle_time_g: Any | None = None
    cycle_time_h: Any | None = None  # Histogram
    drift_ratio_g: Any | None = None
    dynamic_interval_g: Any | None = None
    last_update_ts_g: Any | None = None
    skipped_ewma_g: Any | None = None
    pressure_g: Any | None = None
    pressure_norm_g: Any | None = None
    consecutive_drift_high_g: Any | None = None
    consecutive_pressure_high_g: Any | None = None
    skipped_ewma: float = 0.0
    # smoothing factor (конфігурований): або параметр, або ENV TRADE_UPDATER_SKIPPED_ALPHA, дефолт 0.3
    if skipped_ewma_alpha is None:
        try:
            import os

            env_val = os.getenv("TRADE_UPDATER_SKIPPED_ALPHA")
            if env_val is not None:
                skipped_ewma_alpha = float(env_val)
            elif tu_cfg is not None:
                skipped_ewma_alpha = float(getattr(tu_cfg, "skipped_ewma_alpha", 0.3))
            else:
                skipped_ewma_alpha = 0.3
        except Exception:
            skipped_ewma_alpha = 0.3
    skipped_alpha = max(0.01, min(0.95, float(skipped_ewma_alpha)))

    # локальний кеш створених Gauge щоб уникнути повторної реєстрації
    _gauge_cache: dict[str, Any] = {}

    def _register_gauge(name: str, description: str) -> Any | None:
        """Idempotent gauge creator.

        Avoids ValueError on module reload / multi-start by caching locally.
        If already created in this function scope, returns cached instance.
        (Prometheus client does not expose a simple public fetch-by-name API,
        so on duplicate registration we just swallow and reuse local cache.)
        """
        if not Gauge:
            return None
        if name in _gauge_cache:
            return _gauge_cache[name]
        try:
            g = Gauge(name, description)
            _gauge_cache[name] = g
            return g
        except Exception:  # pragma: no cover - duplicate or registry error
            return _gauge_cache.get(name)

    if Gauge:
        active_g = _register_gauge(
            "trade_active_total", "Number of active trades (Stage3 updater)"
        )
        closed_g = _register_gauge(
            "trade_closed_total", "Number of closed trades (Stage3 updater)"
        )
        cycle_time_g = _register_gauge(
            "trade_updater_cycle_seconds",
            "Cycle wall time of trade updater loop (last)",
        )
        drift_ratio_g = _register_gauge(
            "trade_updater_drift_ratio", "cycle_time / configured_interval ratio"
        )
        # Histogram (best-effort) – використовуємо окрему реєстрацію бо потрібні buckets
        if "Histogram" in globals() and Histogram:
            try:
                buckets_cfg = None
                if tu_cfg is not None:
                    buckets_cfg = getattr(tu_cfg, "cycle_histogram_buckets", None)
                if not buckets_cfg:
                    buckets_cfg = [0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300]
                cycle_time_h = Histogram(
                    "trade_updater_cycle_seconds_bucket",
                    "Distribution of trade updater cycle durations",
                    buckets=tuple(buckets_cfg),
                )
            except Exception:
                cycle_time_h = None
        dynamic_interval_g = _register_gauge(
            "trade_updater_dynamic_interval_seconds", "Current adaptive sleep interval"
        )
        last_update_ts_g = _register_gauge(
            "trade_updater_last_update_ts",
            "Last successful update timestamp (monotonic)",
        )
        skipped_ewma_g = _register_gauge(
            "trade_updater_skipped_symbols_ewma", "EWMA of skipped symbols per cycle"
        )
        pressure_g = _register_gauge(
            "trade_updater_pressure",
            "Skipped symbols pressure = skipped_ewma / active_trades",
        )
        pressure_norm_g = _register_gauge(
            "trade_updater_pressure_norm",
            "Normalized pressure log1p(skipped_ewma / active_trades)",
        )
        consecutive_drift_high_g = _register_gauge(
            "trade_updater_consecutive_drift_high",
            "Consecutive cycles with drift_ratio above high threshold",
        )
        consecutive_pressure_high_g = _register_gauge(
            "trade_updater_consecutive_pressure_high",
            "Consecutive cycles with pressure above threshold",
        )
    if log_interval_sec is None:
        # за замовчуванням = interval_sec (раз на цикл) якщо немає режиму only-on-change
        log_interval_sec = interval_sec if not log_on_change else 0

    consecutive_high_drift = 0
    consecutive_high_pressure = 0
    drift_normal_counter = 0  # cycles below high threshold to trigger reset
    drift_reset_cycles = 3  # configurable if needed later

    while True:
        loop_time = asyncio.get_event_loop().time()
        cycle_start = loop_time

        # 1) Оновити активні угоди (best-effort)
        active_list = await trade_manager.get_active_trades()
        for tr in active_list:
            sym = tr["symbol"]
            try:
                df = await store.get_df(sym, timeframe, limit=lookback)
                if (
                    df is not None
                    and not df.empty
                    and "open_time" in df.columns
                    and "timestamp" not in df.columns
                ):
                    df = df.rename(columns={"open_time": "timestamp"})
            except Exception as e:  # broad-except: I/O / кеш / мережа не критичні
                logger.debug(f"Failed to fetch bars for {sym}: {e}")
                continue
            if df is None or df.empty or len(df) < lookback:
                skipped_symbols += 1
                skip_reason_counts["insufficient_bars"] = (
                    skip_reason_counts.get("insufficient_bars", 0) + 1
                )
                continue
            # Try to obtain current stats from monitor if such API exists
            try:
                if hasattr(monitor, "get_current_stats"):
                    stats = await monitor.get_current_stats(sym)  # type: ignore[attr-defined]
                else:
                    stats = {}
            except Exception:
                stats = {}
            market_data = {
                "price": stats.get("current_price", 0),
                "atr": stats.get("atr", 0),
                "rsi": stats.get("rsi", 0),
                "volume": stats.get("volume_mean", 0),
                "context_break": stats.get("context_break", False),
            }
            await trade_manager.update_trade(tr["id"], market_data)

        # 2) Лічильники після оновлення
        active = await trade_manager.get_active_trades()
        closed = await trade_manager.get_closed_trades()
        now = asyncio.get_event_loop().time()
        counts = (len(active), len(closed))
        last_success_ts = int(now)
        should_log = False
        if log_on_change and last_counts is not None and counts != last_counts:
            should_log = True
        elif log_on_change and last_counts is None:
            # перший лог обов'язково
            should_log = True
        if log_interval_sec and (now - last_log_ts) >= log_interval_sec:
            # якщо заданий інтервал — поважаємо його (може співіснувати з on_change)
            should_log = should_log or True

        if should_log:
            logger.info(
                f"🟢 Active trades: {counts[0]}    🔴 Closed trades: {counts[1]}"
            )
            last_log_ts = now
            last_counts = counts

        # Publish metrics (Prometheus gauges)
        if active_g:
            try:
                active_g.set(counts[0])
                if closed_g:
                    closed_g.set(counts[1])
                if cycle_time_g:
                    # expose skipped_symbols via gauge labels would need Histogram; keep simple via UI publish
                    pass
            except Exception:  # pragma: no cover
                pass

        # 3) Поточний час виконання циклу (elapsed) ДО формування payload щоб мати drift_ratio
        elapsed = asyncio.get_event_loop().time() - cycle_start

        # Update EWMA for skipped symbols before publishing (exclude cycles with zero to preserve decay behaviour)
        if skipped_symbols > 0:
            skipped_ewma = (
                skipped_alpha * skipped_symbols + (1 - skipped_alpha) * skipped_ewma
            )
        else:
            # light decay toward 0 (optional): multiply by (1 - alpha/4)
            skipped_ewma *= 1 - skipped_alpha / 4

        # Pressure ratio (avoid div by zero)
        active_trades_count = counts[0]
        pressure_ratio = (
            skipped_ewma / active_trades_count if active_trades_count > 0 else 0.0
        )
        pressure_norm = math.log1p(pressure_ratio) if pressure_ratio > 0 else 0.0
        if pressure_g:
            try:
                pressure_g.set(pressure_ratio)
            except Exception:
                pass
        if pressure_norm_g:
            try:
                pressure_norm_g.set(pressure_norm)
            except Exception:
                pass

        # Drift warnings (rate limited) & pressure warnings (rate limited)
        drift_ratio_value = elapsed / max(1e-6, interval_sec)
        now_wall = asyncio.get_event_loop().time()
        if tu_cfg is not None:
            try:
                # Drift thresholds
                # Idle mode detection (no trades) – suppress counting & reset
                idle_mode = counts[0] == 0 and len(closed) == 0
                low_pressure = pressure_ratio < 0.01
                if idle_mode or low_pressure:
                    if consecutive_high_drift:
                        logger.debug(
                            "Reset consecutive_drift_high due to idle/low-pressure (was %s)",
                            consecutive_high_drift,
                        )
                    consecutive_high_drift = 0
                    drift_normal_counter = 0
                if idle_mode:
                    # Skip drift anomaly logic entirely when idle
                    pass
                elif drift_ratio_value > tu_cfg.drift_warn_high:
                    consecutive_high_drift += 1
                    drift_normal_counter = 0
                    if (now_wall - last_warn_drift_ts) > 60:
                        logger.warning(
                            f"Drift ratio warning: {drift_ratio_value:.2f} (>{tu_cfg.drift_warn_high})"
                        )
                        last_warn_drift_ts = now_wall
                elif drift_ratio_value < tu_cfg.drift_warn_low and not idle_mode:
                    # treat *low* drift anomaly similarly (still 'high' counter for consecutive anomalous cycles)
                    consecutive_high_drift += 1
                    drift_normal_counter = 0
                    if (now_wall - last_warn_drift_ts) > 60:
                        logger.warning(
                            f"Drift ratio low warning: {drift_ratio_value:.2f} (<{tu_cfg.drift_warn_low})"
                        )
                        last_warn_drift_ts = now_wall
                else:
                    # within normal band → increment normal counter
                    if not idle_mode:
                        drift_normal_counter += 1
                        if (
                            drift_normal_counter >= drift_reset_cycles
                            and consecutive_high_drift
                        ):
                            logger.info(
                                f"Drift back to normal for {drift_normal_counter} cycles – resetting consecutive_drift_high ({consecutive_high_drift} -> 0)"
                            )
                            consecutive_high_drift = 0
                            drift_normal_counter = 0

                # Pressure threshold
                if pressure_ratio > tu_cfg.pressure_warn and not idle_mode:
                    consecutive_high_pressure += 1
                    if (now_wall - last_warn_pressure_ts) > 60:
                        logger.warning(
                            f"Pressure high: {pressure_ratio:.2f} (>{tu_cfg.pressure_warn})"
                        )
                        last_warn_pressure_ts = now_wall
                else:
                    consecutive_high_pressure = 0
                if consecutive_drift_high_g:
                    consecutive_drift_high_g.set(consecutive_high_drift)
                if consecutive_pressure_high_g:
                    consecutive_pressure_high_g.set(consecutive_high_pressure)

                # ── Adaptive interval scaling (optional) ──
                if getattr(tu_cfg, "auto_interval_scale_enabled", False):
                    try:
                        if consecutive_high_pressure >= getattr(
                            tu_cfg, "auto_interval_scale_cycles", 3
                        ) and dynamic_interval < getattr(
                            tu_cfg, "auto_interval_scale_cap", 900.0
                        ):
                            factor = max(
                                1.01,
                                float(
                                    getattr(tu_cfg, "auto_interval_scale_factor", 1.25)
                                ),
                            )
                            new_interval = min(
                                dynamic_interval * factor,
                                getattr(tu_cfg, "auto_interval_scale_cap", 900.0),
                            )
                            if new_interval > dynamic_interval:
                                logger.warning(
                                    f"Adaptive interval scaling: {dynamic_interval:.1f}s -> {new_interval:.1f}s (pressure sustained)"
                                )
                                dynamic_interval = new_interval
                                # reset pressure counter to avoid runaway escalation
                                consecutive_high_pressure = 0
                    except Exception:
                        pass

                # ── Adaptive skipped_ewma_alpha (optional) ──
                if getattr(tu_cfg, "auto_alpha_enabled", False):
                    try:
                        turbulent = drift_ratio_value >= getattr(
                            tu_cfg, "alpha_turbulence_drift", 2.0
                        ) or pressure_ratio >= getattr(
                            tu_cfg, "alpha_turbulence_pressure", 1.5
                        )
                        calm = drift_ratio_value <= getattr(
                            tu_cfg, "alpha_calm_drift", 1.05
                        ) and pressure_ratio <= getattr(
                            tu_cfg, "alpha_calm_pressure", 0.5
                        )
                        # maintain calm counter across cycles
                        if "calm_counter" not in locals():
                            calm_counter = 0  # type: ignore
                        if turbulent:
                            calm_counter = 0  # type: ignore
                            # increase alpha (shorter memory)
                            new_alpha = min(
                                getattr(tu_cfg, "alpha_max", 0.6),
                                skipped_alpha + getattr(tu_cfg, "alpha_step", 0.05),
                            )
                            if abs(new_alpha - skipped_alpha) > 1e-9:
                                logger.info(
                                    f"Adaptive α increase: {skipped_alpha:.3f} -> {new_alpha:.3f} (turbulence)"
                                )
                                skipped_alpha = new_alpha
                        elif calm:
                            calm_counter += 1  # type: ignore
                            if calm_counter >= getattr(tu_cfg, "alpha_calm_cycles", 5):
                                new_alpha = max(
                                    getattr(tu_cfg, "alpha_min", 0.05),
                                    skipped_alpha - getattr(tu_cfg, "alpha_step", 0.05),
                                )
                                if abs(new_alpha - skipped_alpha) > 1e-9:
                                    logger.info(
                                        f"Adaptive α decrease: {skipped_alpha:.3f} -> {new_alpha:.3f} (calm)"
                                    )
                                    skipped_alpha = new_alpha
                                calm_counter = 0  # type: ignore
                        else:
                            # neither calm nor turbulent resets calm counter
                            pass
                    except Exception:
                        pass
            except Exception:
                pass

        # UI publish (Redis JSON) for centralized consumer (two keys for backward compat)
        if publish_ui:
            payload_trades = {
                "active": counts[0],
                "closed": counts[1],
                "ts": last_success_ts,
                "interval": timeframe,
            }
            core_payload = {
                "trades": payload_trades,
                "skipped": skipped_symbols,
                "skipped_ewma": round(skipped_ewma, 4),
                "last_update_ts": last_success_ts,
                "cycle_interval": interval_sec,
                "dynamic_interval": dynamic_interval,
                "drift_ratio": (elapsed / max(1e-6, interval_sec)),
                "pressure": round(pressure_ratio, 4),
                "pressure_norm": round(pressure_norm, 5),
                "thresholds": {
                    "drift_high": getattr(tu_cfg, "drift_warn_high", None),
                    "drift_low": getattr(tu_cfg, "drift_warn_low", None),
                    "pressure": getattr(tu_cfg, "pressure_warn", None),
                },
                "consecutive": {
                    "drift_high": consecutive_high_drift,
                    "pressure_high": consecutive_high_pressure,
                },
                "alpha": round(skipped_alpha, 4),
            }
            # top skip reasons (optional)
            if tu_cfg is not None and getattr(tu_cfg, "publish_skip_reasons", False):
                try:
                    if skip_reason_counts:
                        top_n = int(getattr(tu_cfg, "skip_reasons_top_n", 5))
                        sorted_reasons = sorted(
                            skip_reason_counts.items(),
                            key=lambda kv: kv[1],
                            reverse=True,
                        )[:top_n]
                        core_payload["skip_reasons"] = dict(sorted_reasons)
                except Exception:
                    pass
            try:
                # legacy key
                await store.redis.jset(
                    "stats", "trades", value=payload_trades, ttl=ui_ttl
                )
            except Exception:
                pass
            try:
                await store.redis.jset("stats", "core", value=core_payload, ttl=ui_ttl)
            except Exception as e:  # pragma: no cover
                logger.debug(f"UI publish core failed: {e}")

            # Health heartbeat key (short TTL)
            try:
                hb_payload = {
                    "ts": last_success_ts,
                    "active_trades": counts[0],
                    "drift_ratio": round(drift_ratio_value, 4),
                    "pressure": round(pressure_ratio, 4),
                }
                hb_ttl = max(5, int(interval_sec * 0.9))
                await store.redis.jset("stats", "health", value=hb_payload, ttl=hb_ttl)
            except Exception:
                pass

        # Cycle time metric & drift ratio & other gauges
        if cycle_time_g:
            try:
                cycle_time_g.set(elapsed)
            except Exception:  # pragma: no cover
                pass
        if "cycle_time_h" in locals() and cycle_time_h is not None:
            try:
                cycle_time_h.observe(elapsed)
            except Exception:
                pass
        if drift_ratio_g:
            try:
                drift_ratio_g.set(elapsed / max(1e-6, interval_sec))
            except Exception:  # pragma: no cover
                pass
        if dynamic_interval_g:
            try:
                dynamic_interval_g.set(dynamic_interval)
            except Exception:  # pragma: no cover
                pass
        if last_update_ts_g:
            try:
                last_update_ts_g.set(last_success_ts)
            except Exception:  # pragma: no cover
                pass
        if skipped_ewma_g:
            try:
                skipped_ewma_g.set(skipped_ewma)
            except Exception:  # pragma: no cover
                pass

        # Exponential backoff if cycle took longer than current interval (reset skipped counter per cycle)
        skipped_symbols = 0
        skip_reason_counts.clear()
        if elapsed > dynamic_interval:
            mul = float(backoff_multiplier) if backoff_multiplier is not None else 1.5
            dynamic_interval = min(dynamic_interval * mul, float(max_backoff_sec))
        else:
            if dynamic_interval > interval_sec:
                div = (
                    float(backoff_multiplier) if backoff_multiplier is not None else 1.5
                )
                dynamic_interval = max(float(interval_sec), dynamic_interval / div)

        sleep_for = max(0.0, dynamic_interval - elapsed)
        await asyncio.sleep(sleep_for)


__all__ = ["trade_manager_updater"]
