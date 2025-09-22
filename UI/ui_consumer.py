import asyncio
import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Literal, cast

import redis.asyncio as redis
from rich.box import ROUNDED
from rich.console import Console
from rich.live import Live
from rich.logging import RichHandler
from rich.table import Table

from config.config import (
    K_SIGNAL,
    K_STATS,
    K_SYMBOL,
    K_TRIGGER_REASONS,
    REDIS_CHANNEL_ASSET_STATE,
    REDIS_SNAPSHOT_KEY,
    STATS_CORE_KEY,
)
from utils.utils import format_price

ui_console = Console(stderr=False)

ui_logger = logging.getLogger("ui_consumer")
ui_logger.setLevel(logging.INFO)
ui_logger.handlers.clear()
ui_logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
ui_logger.propagate = False


class AlertAnimator:
    def __init__(self) -> None:
        self.active_alerts: dict[str, float] = {}

    def add_alert(self, symbol: str) -> None:
        self.active_alerts[symbol] = time.time()

    def should_highlight(self, symbol: str) -> bool:
        ts = self.active_alerts.get(symbol)
        if ts is None:
            return False
        if (time.time() - ts) < 8.0:
            return True
        self.active_alerts.pop(symbol, None)
        return False


class UIConsumer:
    def __init__(self, vol_z_threshold: float = 2.5, low_atr_threshold: float = 0.005):
        self.vol_z_threshold = vol_z_threshold
        self.low_atr_threshold = low_atr_threshold
        self.alert_animator = AlertAnimator()
        self.last_update_time: float = time.time()
        self._last_counters: dict[str, Any] = {}
        self._display_results: list[dict[str, Any]] = (
            []
        )  # кеш останнього непорожнього списку
        self._blink_state = False  # для миготіння pressure
        self._pressure_alert_active = False

    def _format_price(self, price: float | None, symbol: str) -> str:
        """Форматує ціну для відображення у таблиці.

        Якщо значення відсутнє або непозитивне — повертає "-";
        інакше використовує utils.format_price.
        """
        try:
            if price is None:
                return "-"
            p = float(price)
            if p <= 0:
                return "-"
            return format_price(p, symbol)
        except Exception:
            return "-"

    def _get_rsi_color(self, rsi: float) -> str:
        if rsi < 30:
            return "green"
        if rsi < 50:
            return "light_green"
        if rsi < 70:
            return "yellow"
        return "red"

    def _get_atr_color(self, atr_pct: float) -> str:
        if atr_pct < self.low_atr_threshold:
            return "red"
        if atr_pct > 0.02:
            return "yellow"
        return ""

    def _get_signal_icon(self, signal: str) -> str:
        icons = {
            "ALERT": "🔴",
            "NORMAL": "🟢",
            "ALERT_BUY": "🟢↑",
            "ALERT_SELL": "🔴↓",
            "NONE": "⚪",
        }
        return icons.get(signal, "❓")

    def _get_recommendation_icon(self, recommendation: str) -> str:
        icons = {
            "STRONG_BUY": "🟢↑↑",
            "BUY_IN_DIPS": "🟢↑",
            "HOLD": "🟡",
            "SELL_ON_RALLIES": "🔴↓",
            "STRONG_SELL": "🔴↓↓",
            "AVOID": "⚫",
            "WAIT": "⚪",
        }
        return icons.get(recommendation, "")

    async def redis_consumer(
        self,
        redis_url: str | None = None,
        channel: str = REDIS_CHANNEL_ASSET_STATE,
        refresh_rate: float = 0.8,
        loading_delay: float = 1.5,
        smooth_delay: float = 0.05,
    ) -> None:
        """
        Слухає канал Redis Pub/Sub, приймає payload {"meta","counters","assets"}
        і рендерить таблицю.
        """
        # Підтримка конфігів з ENV, щоб не промахнутись по інстансу Redis
        redis_url = (
            redis_url
            or os.getenv("REDIS_URL")
            or f"redis://{os.getenv('REDIS_HOST','localhost')}:{os.getenv('REDIS_PORT','6379')}/0"
        )

        # Ініціалізація збереженого списку результатів (instance-level) з типом
        if not hasattr(self, "_last_results"):
            self._last_results: list[dict[str, Any]] = []

        redis_client = redis.from_url(
            redis_url, decode_responses=True, encoding="utf-8"
        )
        pubsub = redis_client.pubsub()

        # Спроба початкового снапшоту перед підпискою + нові core-статистики
        try:
            snapshot_raw = await redis_client.get(REDIS_SNAPSHOT_KEY)
            if snapshot_raw:
                snap = json.loads(snapshot_raw)
                if isinstance(snap, dict) and isinstance(snap.get("assets"), list):
                    self._last_results = snap.get("assets") or []
                    if self._last_results:
                        self._display_results = self._last_results
                    self._last_counters = snap.get("counters", {}) or {}
                    meta_ts = snap.get("meta", {}).get("ts")
                    if meta_ts:
                        try:
                            self.last_update_time = datetime.fromisoformat(
                                meta_ts.replace("Z", "")
                            ).timestamp()
                        except Exception:
                            pass
                    ui_logger.info(
                        "📥 Завантажено початковий снапшот: %d активів",
                        len(self._last_results),
                    )
            # Нові агреговані трейд-метрики (stats:core)
            try:
                core_raw = await redis_client.get(STATS_CORE_KEY)
                if core_raw:
                    core = json.loads(core_raw)
                    if not isinstance(core, dict):
                        core = {}
                    trades_part = core.get("trades", {})
                    # кешуємо як counters.* для заголовку
                    if trades_part:
                        self._last_counters.update(
                            {
                                "active_trades": trades_part.get("active"),
                                "closed_trades": trades_part.get("closed"),
                                "skipped": core.get("skipped"),
                                "skipped_ewma": core.get("skipped_ewma"),
                                "drift_ratio": core.get("drift_ratio"),
                                "dynamic_interval": core.get("dynamic_interval"),
                                "pressure": core.get("pressure"),
                                "pressure_norm": core.get("pressure_norm"),
                                "alpha": core.get("alpha"),
                            }
                        )
                        thresholds = core.get("thresholds") or {}
                        consecutive = core.get("consecutive") or {}
                        self._last_counters["th_drift_high"] = thresholds.get(
                            "drift_high"
                        )
                        self._last_counters["th_drift_low"] = thresholds.get(
                            "drift_low"
                        )
                        self._last_counters["th_pressure"] = thresholds.get("pressure")
                        self._last_counters["consec_drift_high"] = consecutive.get(
                            "drift_high"
                        )
                        self._last_counters["consec_pressure_high"] = consecutive.get(
                            "pressure_high"
                        )
                        if "skip_reasons" in core and isinstance(
                            core.get("skip_reasons"), dict
                        ):
                            self._last_counters["skip_reasons"] = core.get(
                                "skip_reasons"
                            )
                        # last_update_ts можна відобразити як heartbeat
                        if core.get("last_update_ts"):
                            self.last_update_time = float(core["last_update_ts"])
            except Exception:
                pass
        except Exception:  # broad-except: початковий снапшот не критичний
            ui_logger.debug("Не вдалося завантажити початковий снапшот", exc_info=True)

        await asyncio.sleep(loading_delay)
        await pubsub.subscribe(channel)
        ui_logger.info(f"🔗 Підключено до Redis ({redis_url}), канал '{channel}'...")

        # Початкове відображення: якщо вже є снапшот, показуємо його одразу
        initial_results = self._display_results if self._display_results else []
        with Live(
            self._build_signal_table(
                initial_results, loading=not bool(initial_results)
            ),
            console=ui_console,
            refresh_per_second=refresh_rate,
            screen=False,
            transient=False,
        ) as live:
            while True:
                try:
                    # Періодичний fallback: якщо >7s без оновлень і маємо порожній
                    # live список, пробуємо перезчитати снапшот
                    if (
                        (time.time() - self.last_update_time) > 7
                        and not self._last_results
                        and self._display_results
                    ):
                        try:
                            snapshot_raw = await redis_client.get(REDIS_SNAPSHOT_KEY)
                            if snapshot_raw:
                                snap = json.loads(snapshot_raw)
                                assets_snap = (
                                    snap.get("assets")
                                    if isinstance(snap, dict)
                                    else None
                                )
                                if isinstance(assets_snap, list) and assets_snap:
                                    self._display_results = assets_snap
                                    ui_logger.info(
                                        "♻️ Fallback snapshot reload: %d активів",
                                        len(assets_snap),
                                    )
                        except Exception:  # broad except: fallback reload best-effort
                            pass
                    message = await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=1.0
                    )
                    if message:
                        try:
                            data = json.loads(message["data"])
                        except Exception:
                            ui_logger.error(
                                "Невдача json.loads для отриманого повідомлення"
                            )
                            data = None

                        # ✅ Новий коректний парсинг продюсера: очікуємо dict з 'assets'
                        if isinstance(data, dict) and "assets" in data:
                            try:
                                assets_field = data.get("assets")
                                assets_len = (
                                    len(assets_field)
                                    if isinstance(assets_field, list)
                                    else None
                                )
                                ui_logger.debug(
                                    "UI recv keys=%s counters=%s assets_len=%s type=%s",
                                    list(data.keys()),
                                    data.get("counters"),
                                    assets_len,
                                    data.get("type"),
                                )
                                assets_dbg = data.get("assets")
                                if (
                                    isinstance(assets_dbg, list)
                                    and assets_dbg
                                    and isinstance(assets_dbg[0], dict)
                                ):
                                    ui_logger.debug(
                                        "UI first asset keys=%s",
                                        list(assets_dbg[0].keys()),
                                    )
                            except Exception:
                                pass
                            parsed_assets = data.get("assets") or []
                            # Якщо прийшов порожній список, але вже маємо попередні
                            # дані — ігноруємо очищення
                            if not parsed_assets and self._display_results:
                                ui_logger.debug(
                                    "Ignore empty assets update; keeping %d cached rows",
                                    len(self._display_results),
                                )
                            else:
                                self._last_results = parsed_assets
                                if parsed_assets:
                                    self._display_results = parsed_assets
                            # meta.ts → час оновлення
                            meta_ts = data.get("meta", {}).get("ts")
                            if meta_ts:
                                try:
                                    incoming_ts = datetime.fromisoformat(
                                        meta_ts.replace("Z", "")
                                    ).timestamp()
                                    # Оновлюємо лише якщо новіше значення
                                    if incoming_ts >= self.last_update_time:
                                        self.last_update_time = incoming_ts
                                except Exception:
                                    pass
                            else:
                                # Heartbeat без meta.ts — оновлюємо час лише якщо
                                # давно не оновлювалось (>5s)
                                if time.time() - self.last_update_time > 5:
                                    self.last_update_time = time.time()
                            # counters → для заголовку
                            self._last_counters = data.get("counters", {}) or {}
                            # Додатковий лог узгодженості
                            ui_logger.debug(
                                "Post-assign last_results_len=%d counters_assets=%s display_len=%d",
                                len(self._last_results),
                                self._last_counters.get("assets"),
                                len(self._display_results),
                            )
                        # (сумісність зі старим форматом)
                        elif isinstance(data, list):
                            # Legacy format: whole message is just the assets list
                            if data:
                                self._last_results = data
                                self._display_results = data
                                self.last_update_time = time.time()
                            else:
                                ui_logger.debug(
                                    "Legacy empty list ignored; keeping cached results"
                                )

                    # Підсвітка для всіх ALERT*
                    for r in self._last_results:
                        sig = str(r.get("signal", "")).upper()
                        if sig.startswith("ALERT"):
                            self.alert_animator.add_alert(r.get("symbol", ""))

                    # Якщо counters каже >0, а список порожній — лог/діагностика
                    # Вибір списку для відображення: або поточний, або останній непорожній
                    results_for_render = (
                        self._last_results
                        if self._last_results
                        else self._display_results
                    )
                    if (
                        not self._last_results
                        and self._last_counters.get("assets", 0) > 0
                        and self._display_results
                    ):
                        ui_logger.warning(
                            "Using cached results_for_render len=%d (last empty, "
                            "counters.assets=%s)",
                            len(self._display_results),
                            self._last_counters.get("assets"),
                        )
                    elif not results_for_render:
                        ui_logger.debug(
                            "Render with empty results_for_render; counters.assets=%s",
                            self._last_counters.get("assets"),
                        )
                    ui_logger.debug(
                        "Render: last=%d cached=%d render=%d last_update_age=%.1fs",
                        len(self._last_results),
                        len(self._display_results),
                        len(results_for_render),
                        time.time() - self.last_update_time,
                    )
                    table = self._build_signal_table(results_for_render)
                    live.update(table)
                    await asyncio.sleep(smooth_delay)

                except (ConnectionError, TimeoutError) as e:
                    ui_logger.error(f"Помилка з'єднання: {e}")
                    await asyncio.sleep(3)
                    try:
                        await pubsub.reset()
                        await pubsub.subscribe(channel)
                        ui_logger.info("✅ Перепідключено до Redis")
                    except Exception as reconnect_err:
                        ui_logger.error(f"Помилка перепідключення: {reconnect_err}")
                except Exception as e:
                    ui_logger.error(f"Невідома помилка: {e}")
                    await asyncio.sleep(1)

    def _build_signal_table(
        self, results: list[dict[str, Any]], loading: bool = False
    ) -> Table:
        """Побудова таблиці з сигналами та метриками системи."""
        # counters з payloadу, якщо є
        # Спершу беремо фактичну кількість рядків (що реально відображаються)
        total_assets = len(results)
        # ALERT беремо з counters якщо є, інакше перерахуємо локально
        alert_count = self._last_counters.get("alerts")
        if alert_count is None:
            alert_count = sum(
                1
                for r in results
                if str(r.get("signal", "")).upper().startswith("ALERT")
            )

        last_update = datetime.fromtimestamp(self.last_update_time).strftime("%H:%M:%S")

        # Нові трейд-метрики з core (якщо були підвантажені)
        active_trades = self._last_counters.get("active_trades")
        closed_trades = self._last_counters.get("closed_trades")
        skipped = self._last_counters.get("skipped")
        skipped_ewma = self._last_counters.get("skipped_ewma")
        drift_ratio = self._last_counters.get("drift_ratio")
        dynamic_interval = self._last_counters.get("dynamic_interval")
        pressure = self._last_counters.get("pressure")
        pressure_norm = self._last_counters.get("pressure_norm")
        th_drift_high = self._last_counters.get("th_drift_high")
        th_drift_low = self._last_counters.get("th_drift_low")
        th_pressure = self._last_counters.get("th_pressure")
        consec_drift = self._last_counters.get("consec_drift_high")
        consec_pressure = self._last_counters.get("consec_pressure_high")
        alpha_val = self._last_counters.get("alpha")
        skip_reasons = self._last_counters.get("skip_reasons")

        # Форматуємо drift (якщо буде передаватися через stats:core у майбутньому)
        if drift_ratio is not None:
            try:
                drift_val = float(drift_ratio)
                # Якщо thresholds доступні – використовуємо їх
                if th_drift_high is not None and th_drift_low is not None:
                    if drift_val < float(th_drift_low):
                        drift_color = (
                            "yellow"  # занадто повільно / мало часу? (нижній поріг)
                        )
                    elif drift_val > float(th_drift_high):
                        drift_color = "red"
                    else:
                        drift_color = "green"
                else:
                    if drift_val < 0.9:
                        drift_color = "green"
                    elif drift_val <= 1.2:
                        drift_color = "yellow"
                    else:
                        drift_color = "red"
                drift_fragment = f" | Drift: [{drift_color}]{drift_val:.2f}[/]"
            except Exception:
                drift_fragment = ""
        else:
            drift_fragment = ""

        trades_fragment = ""
        if active_trades is not None or closed_trades is not None:
            trades_fragment = (
                f" | Trades: 🟢{active_trades or 0}/🔴{closed_trades or 0}"
            )
        # Форматування skipped / ewma
        if skipped is not None:
            skipped_fragment = f" | Skipped: {skipped}"
            if skipped_ewma is not None:
                try:
                    skipped_ewma_val = float(skipped_ewma)
                    color = (
                        "green"
                        if skipped_ewma_val < 1
                        else ("yellow" if skipped_ewma_val < 3 else "red")
                    )
                    skipped_fragment += f" (EWMA: [{color}]{skipped_ewma_val:.2f}[/])"
                except Exception:
                    pass
        else:
            skipped_fragment = ""

        if dynamic_interval is not None:
            try:
                dyn_val = float(dynamic_interval)
                dyn_color = (
                    "green"
                    if dyn_val
                    <= 1.1 * (self._last_counters.get("cycle_interval") or dyn_val)
                    else (
                        "yellow"
                        if dyn_val
                        <= 2.0 * (self._last_counters.get("cycle_interval") or dyn_val)
                        else "red"
                    )
                )
                dynamic_fragment = f" | ΔInterval: [{dyn_color}]{dyn_val:.1f}s[/]"
            except Exception:
                dynamic_fragment = f" | ΔInterval: {dynamic_interval}"
        else:
            dynamic_fragment = ""

        blink_fragment = ""
        if pressure is not None:
            try:
                p_val = float(pressure)
                if th_pressure is not None:
                    th_pressure_f = float(th_pressure)
                    if p_val > th_pressure_f:
                        p_color = "red"
                        self._pressure_alert_active = True
                    elif p_val > th_pressure_f * 0.7:
                        p_color = "yellow"
                        self._pressure_alert_active = False
                    else:
                        p_color = "green"
                        self._pressure_alert_active = False
                else:
                    p_color = (
                        "green" if p_val < 0.5 else ("yellow" if p_val < 1.5 else "red")
                    )
                    self._pressure_alert_active = p_color == "red"
                pressure_fragment = f" | Pressure: [{p_color}]{p_val:.2f}[/]"
                if pressure_norm is not None:
                    try:
                        pn = float(pressure_norm)
                        pressure_fragment += f"(n={pn:.2f})"
                    except Exception:
                        pass
                # Миготіння
                if self._pressure_alert_active:
                    self._blink_state = not self._blink_state
                    if self._blink_state:
                        blink_fragment = " [blink][red]⚠[/][/blink]"
            except Exception:
                pressure_fragment = f" | Pressure: {pressure}"
        else:
            pressure_fragment = ""

        consec_fragment = ""
        if (consec_drift or consec_pressure) and (consec_drift or 0) + (
            consec_pressure or 0
        ) > 0:
            consec_fragment = (
                f" | Seq(drift/press): {consec_drift or 0}/{consec_pressure or 0}"
            )
        alpha_fragment = (
            f" | α={alpha_val:.2f}" if isinstance(alpha_val, (int, float)) else ""
        )
        skip_reasons_fragment = ""
        if isinstance(skip_reasons, dict) and skip_reasons:
            # take first 3 reasons for compact display
            top_pairs = list(skip_reasons.items())[:3]
            compact = ",".join(f"{k}:{v}" for k, v in top_pairs)
            skip_reasons_fragment = f" | SkipReasons[{compact}]"

        title = (
            f"[bold]Система моніторингу AiOne_t[/bold] | "
            f"Активи: [green]{total_assets}[/green] | "
            f"ALERT: [red]{alert_count}[/red] | "
            f"Оновлено: [cyan]{last_update}[/cyan]"
            f"{trades_fragment}{skipped_fragment}{drift_fragment}{dynamic_fragment}{pressure_fragment}{consec_fragment}{alpha_fragment}{skip_reasons_fragment}{blink_fragment}"
        )

        table = Table(
            title=title,
            box=ROUNDED,
            show_header=True,
            header_style="bold magenta",
            expand=True,
        )

        columns = [
            ("Символ", "left"),
            ("Ціна", "right"),
            ("Оборот USD", "right"),
            ("ATR%", "right"),
            ("RSI", "right"),
            ("Статус", "center"),
            ("Причини", "left"),
            ("Сигнал", "center"),
            ("Conf%", "right"),
            ("Рекомендація", "left"),
            ("TP/SL", "right"),
        ]
        for header, justify in columns:
            j = (
                "left"
                if justify == "left"
                else "right" if justify == "right" else "center"
            )
            table.add_column(
                header,
                justify=cast(Literal["default", "left", "center", "right", "full"], j),
            )

        if loading or not results:
            # Маркап Rich має відповідати: відкрили [cyan] — закрили [/cyan]
            table.add_row(
                "[cyan]🔄 Очікування даних...[/cyan]", *[""] * (len(columns) - 1)
            )
            return table

        def priority_key(r: dict) -> tuple:
            stats = r.get(K_STATS, {})
            reasons = set(r.get(K_TRIGGER_REASONS, []))
            is_alert = str(r.get(K_SIGNAL, "")).upper().startswith("ALERT")
            anomaly = (stats.get("volume_z", 0.0) or 0.0) >= self.vol_z_threshold
            warning = (not is_alert) and bool(reasons)
            if is_alert and "volume_spike" in reasons:
                cat = 0
            elif is_alert:
                cat = 1
            elif anomaly:
                cat = 2
            elif warning:
                cat = 3
            else:
                cat = 4
            return (cat, -(stats.get("volume_mean", 0.0) or 0.0))

        try:
            sorted_results = sorted(results, key=priority_key)
        except Exception as e:
            ui_logger.debug("Sorting failed: %s", e)
            sorted_results = results

        for asset in sorted_results:
            symbol = str(asset.get(K_SYMBOL, "")).upper()
            stats = asset.get(K_STATS, {}) or {}

            # Перевага плоских ключів якщо вони вже розраховані продюсером
            if "price_str" in asset and isinstance(asset.get("price_str"), str):
                price_str = asset["price_str"]
                try:
                    cp_raw = asset.get("price", stats.get("current_price"))
                    current_price = float(cp_raw) if cp_raw is not None else None
                except Exception:
                    current_price = None
            else:
                try:
                    if "price" in asset and isinstance(
                        asset.get("price"), (int, float)
                    ):
                        price_val = asset.get("price")
                        current_price = (
                            float(price_val) if price_val is not None else None
                        )
                    else:
                        cp_raw = stats.get("current_price")
                        current_price = float(cp_raw) if cp_raw is not None else None
                except Exception:
                    current_price = None
                price_str = self._format_price(current_price, symbol)

            volume = asset.get("volume")
            if not isinstance(volume, (int, float)):
                volume = stats.get("volume_mean", 0.0) or 0.0
            volume_z = stats.get("volume_z", 0.0) or 0.0
            if "volume_str" in asset and isinstance(asset.get("volume_str"), str):
                volume_str = asset["volume_str"]
            else:
                volume_str = f"{volume:,.0f}"
            if volume_z > self.vol_z_threshold:
                volume_str = f"[bold magenta]{volume_str}[/]"

            if "atr_pct" in asset and isinstance(asset.get("atr_pct"), (int, float)):
                atr_pct = float(asset.get("atr_pct") or 0.0)
            else:
                atr_raw = stats.get("atr")
                try:
                    atr = float(atr_raw) if atr_raw is not None else None
                except Exception:
                    atr = None
                atr_pct = (
                    (float(atr) / float(current_price) * 100.0)
                    if (
                        atr is not None
                        and current_price is not None
                        and current_price > 0
                    )
                    else 0.0
                )
            atr_color = self._get_atr_color(atr_pct)
            if atr_color:
                atr_str = f"[{atr_color}]{atr_pct:.2f}%[/]"
            else:
                atr_str = f"{atr_pct:.2f}%"

            rsi_val = asset.get("rsi")
            if not isinstance(rsi_val, (int, float)):
                rsi_val = stats.get("rsi")
            try:
                rsi_f = float(rsi_val) if rsi_val is not None else None
            except Exception:
                rsi_f = None
            if rsi_f is None:
                rsi_str = "-"
            else:
                rsi_color = self._get_rsi_color(float(rsi_f))
                if rsi_color:
                    rsi_str = f"[{rsi_color}]{float(rsi_f):.1f}[/]"
                else:
                    rsi_str = f"{float(rsi_f):.1f}"

            status = asset.get("status") or asset.get("state", "normal")
            if status == "normal":
                status_icon = "🟢"
            elif status == "init":
                status_icon = "🟨"
            else:
                status_icon = "🔴"
            status_str = f"{status_icon} {status}"

            signal = str(asset.get(K_SIGNAL, "NONE")).upper()
            signal_str = f"{self._get_signal_icon(signal)} {signal}"

            # Нова колонка впевненості (confidence)
            conf_val = asset.get("confidence")
            if not isinstance(conf_val, (int, float)):
                conf_val = 0.0
            conf_str = f"{float(conf_val)*100:.1f}%"

            # ✅ Рекомендація береться з кореня, а не з stage2_result
            recommendation = str(asset.get("recommendation", "-"))
            rec_str = (
                f"{self._get_recommendation_icon(recommendation)} {recommendation}"
            )

            if "tp_sl" in asset:
                tp_sl_str = asset.get("tp_sl") or "-"
            else:
                tp = asset.get("tp")
                sl = asset.get("sl")
                tp_sl_str = (
                    f"TP: {self._format_price(tp, symbol)}\nSL: {self._format_price(sl, symbol)}"
                    if tp and sl
                    else "-"
                )

            # Підсвітка для ALERT*
            row_style = (
                "bold red"
                if signal.startswith("ALERT")
                and self.alert_animator.should_highlight(symbol)
                else ""
            )

            tags = []
            for reason in asset.get(K_TRIGGER_REASONS, []) or []:
                tags.append(
                    "[magenta]Сплеск обсягу[/]"
                    if reason == "volume_spike"
                    else f"[yellow]{reason}[/]"
                )
            reasons = "  ".join(tags) or "-"

            table.add_row(
                symbol,
                price_str,
                volume_str,
                atr_str,
                rsi_str,
                status_str,
                reasons,
                signal_str,
                conf_str,
                rec_str,
                tp_sl_str,
                style=row_style,
            )

        return table


async def main() -> None:
    consumer = UIConsumer()
    await consumer.redis_consumer()


if __name__ == "__main__":
    asyncio.run(main())
