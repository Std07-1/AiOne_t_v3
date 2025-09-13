# UI/ui_consumer.py
# -*- coding: utf-8 -*-
import os
import redis.asyncio as redis
import json
import logging
import asyncio
import time
from datetime import datetime
from typing import Any, Dict, List

from rich.console import Console
from rich.logging import RichHandler
from rich.live import Live
from rich.table import Table
from rich.box import ROUNDED

ui_console = Console(stderr=False)

ui_logger = logging.getLogger("ui_consumer")
ui_logger.setLevel(logging.INFO)
ui_logger.handlers.clear()
ui_logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
ui_logger.propagate = False


class AlertAnimator:
    def __init__(self) -> None:
        self.active_alerts: Dict[str, float] = {}

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


class UI_Consumer:
    def __init__(self, vol_z_threshold: float = 2.5, low_atr_threshold: float = 0.005):
        self.vol_z_threshold = vol_z_threshold
        self.low_atr_threshold = low_atr_threshold
        self.alert_animator = AlertAnimator()
        self.last_update_time: float = time.time()
        self._last_counters: Dict[str, Any] = {}
        self._display_results: List[Dict[str, Any]] = (
            []
        )  # кеш останнього непорожнього списку

    def _format_price(self, price: float) -> str:
        if price >= 1000:
            return f"{price:,.2f}"
        return f"{price:.4f}"

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
        redis_url: str = None,
        channel: str = "asset_state_update",
        refresh_rate: float = 0.8,
        loading_delay: float = 1.5,
        smooth_delay: float = 0.05,
    ) -> None:
        """
        Слухає канал Redis Pub/Sub, приймає payload {"meta","counters","assets"} і рендерить таблицю.
        """
        # Підтримка конфігів з ENV, щоб не промахнутись по інстансу Redis
        redis_url = (
            redis_url
            or os.getenv("REDIS_URL")
            or f"redis://{os.getenv('REDIS_HOST','localhost')}:{os.getenv('REDIS_PORT','6379')}/0"
        )

        # Ініціалізація збереженого списку результатів (instance-level)
        if not hasattr(self, "_last_results"):
            self._last_results = []  # type: ignore[attr-defined]

        redis_client = redis.from_url(
            redis_url, decode_responses=True, encoding="utf-8"
        )
        pubsub = redis_client.pubsub()

        # Спроба початкового снапшоту перед підпискою
        try:
            snapshot_raw = await redis_client.get("asset_state_snapshot")
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
        except Exception:
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
                    # Періодичний fallback: якщо >7s без оновлень і маємо порожній live список, пробуємо перезчитати снапшот
                    if (
                        (time.time() - self.last_update_time) > 7
                        and not self._last_results
                        and self._display_results
                    ):
                        try:
                            snapshot_raw = await redis_client.get(
                                "asset_state_snapshot"
                            )
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
                                ui_logger.debug(
                                    "UI recv keys=%s counters=%s assets_len=%s type=%s",
                                    list(data.keys()),
                                    data.get("counters"),
                                    (
                                        None
                                        if data.get("assets") is None
                                        else len(data.get("assets"))
                                    ),
                                    data.get("type"),
                                )
                                if data.get("assets"):
                                    ui_logger.debug(
                                        "UI first asset keys=%s",
                                        list(data.get("assets")[0].keys()),
                                    )
                            except Exception:
                                pass
                            parsed_assets = data.get("assets") or []
                            # Якщо прийшов порожній список, але ми вже маємо попередні дані – ігноруємо очищення
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
                                # Heartbeat без meta.ts – оновлюємо час лише якщо давно не оновлювалось (>5s)
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
                            "Using cached results_for_render len=%d (last empty, counters.assets=%s)",
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

    def _build_signal_table(self, results: List[dict], loading: bool = False) -> Table:
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

        title = (
            f"[bold]Система моніторингу AiOne_t[/bold] | "
            f"Активи: [green]{total_assets}[/green] | "
            f"ALERT: [red]{alert_count}[/red] | "
            f"Оновлено: [cyan]{last_update}[/cyan]"
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
            table.add_column(header, justify=justify)

        if loading or not results:
            # Маркап Rich має відповідати: відкрили [cyan] — закрили [/cyan]
            table.add_row(
                "[cyan]🔄 Очікування даних...[/cyan]", *[""] * (len(columns) - 1)
            )
            return table

        def priority_key(r: dict) -> tuple:
            stats = r.get("stats", {})
            reasons = set(r.get("trigger_reasons", []))
            is_alert = str(r.get("signal", "")).upper().startswith("ALERT")
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
            symbol = str(asset.get("symbol", "")).upper()
            stats = asset.get("stats", {}) or {}

            # Перевага плоских ключів якщо вони вже розраховані продюсером
            if "price_str" in asset and isinstance(asset.get("price_str"), str):
                price_str = asset["price_str"]
                current_price = float(
                    asset.get("price", stats.get("current_price", 0.0)) or 0.0
                )
            else:
                if "price" in asset and isinstance(asset.get("price"), (int, float)):
                    current_price = float(asset.get("price") or 0.0)
                else:
                    current_price = stats.get("current_price", 0.0) or 0.0
                price_str = self._format_price(float(current_price))

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
                atr = stats.get("atr", 0.0) or 0.0
                atr_pct = (
                    (float(atr) / float(current_price) * 100.0)
                    if current_price
                    else 0.0
                )
            atr_color = self._get_atr_color(atr_pct)
            if atr_color:
                atr_str = f"[{atr_color}]{atr_pct:.2f}%[/]"
            else:
                atr_str = f"{atr_pct:.2f}%"

            rsi_val = asset.get("rsi")
            if not isinstance(rsi_val, (int, float)):
                rsi_val = stats.get("rsi", 0.0) or 0.0
            rsi_color = self._get_rsi_color(float(rsi_val))
            if rsi_color:
                rsi_str = f"[{rsi_color}]{float(rsi_val):.1f}[/]"
            else:
                rsi_str = f"{float(rsi_val):.1f}"

            status = asset.get("status") or asset.get("state", "normal")
            if status == "normal":
                status_icon = "🟢"
            elif status == "init":
                status_icon = "🟨"
            else:
                status_icon = "🔴"
            status_str = f"{status_icon} {status}"

            signal = str(asset.get("signal", "NONE")).upper()
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
                    f"TP: {self._format_price(tp)}\nSL: {self._format_price(sl)}"
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
            for reason in asset.get("trigger_reasons", []) or []:
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
    consumer = UI_Consumer()
    await consumer.redis_consumer()


if __name__ == "__main__":
    asyncio.run(main())
