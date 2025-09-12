# monitor/asset_monitoring.py
# -*- coding: utf-8 -*-
"""
Модуль Stage1 для AiOne_t — швидкий реальний моніторинг 1m/5m WS-барів,
визначення аномалій і формування сирих сигналів для Stage2.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List
import pandas as pd
import numpy as np

from utils.utils_1_2 import ensure_timestamp_column

from app.thresholds import load_thresholds, Thresholds

from stage1.asset_triggers import (
    volume_spike_trigger,
    breakout_level_trigger,
    volatility_spike_trigger,
    rsi_divergence_trigger,
)

from stage1.indicators import (
    RSIManager,
    format_rsi,
    compute_rsi,
    VWAPManager,
    vwap_deviation_trigger,
    ATRManager,
    VolumeZManager,
)

from stage1.utils import normalize_trigger_reasons

from rich.console import Console
from rich.logging import RichHandler

# --- Логування ---
logger = logging.getLogger("stage1_monitor")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False


class AssetMonitorStage1:
    """
    Stage1: Моніторинг крипто-активів у реальному часі на основі WS-барів.
    Основні тригери:
      • Сплеск обсягу (volume_z)
      • Динамічний RSI (overbought/oversold)
      • Локальні рівні підтримки/опору
      • VWAP
      • ATR-коридор (волатильність)
    """

    def __init__(
        self,
        cache_handler: Any,
        state_manager: Any = None,
        *,
        vol_z_threshold: float = 2.0,
        rsi_overbought: Optional[float] = None,
        rsi_oversold: Optional[float] = None,
        dynamic_rsi_multiplier: float = 1.1,
        min_reasons_for_alert: int = 2,
        enable_stats: bool = True,
        feature_switches: dict | None = None,
    ):
        self.cache_handler = cache_handler
        self.vol_z_threshold = vol_z_threshold
        self.rsi_manager = RSIManager(period=14)
        self.atr_manager = ATRManager(period=14)
        self.vwap_manager = VWAPManager(window=30)
        self.volumez_manager = VolumeZManager(window=20)
        self.global_levels: Dict[str, List[float]] = {}
        self.rsi_overbought = rsi_overbought
        self.rsi_oversold = rsi_oversold
        self.dynamic_rsi_multiplier = dynamic_rsi_multiplier
        self.min_reasons_for_alert = min_reasons_for_alert
        self.enable_stats = enable_stats
        self.asset_stats: Dict[str, Dict[str, Any]] = {}
        self._symbol_cfg: Dict[str, Thresholds] = {}
        self.state_manager = state_manager
        # Статистики для anti-spam/визначення частоти тригерів можна додати тут, якщо потрібно
        self.feature_switches = feature_switches or {}
        self._sw_triggers = self.feature_switches.get("triggers") or {}

    def update_params(
        self,
        vol_z_threshold: Optional[float] = None,
        rsi_overbought: Optional[float] = None,
        rsi_oversold: Optional[float] = None,
    ) -> None:
        """
        Оновлює параметри монітора під час бектесту
        """
        if vol_z_threshold is not None:
            self.vol_z_threshold = vol_z_threshold
        if rsi_overbought is not None:
            self.rsi_overbought = rsi_overbought
        if rsi_oversold is not None:
            self.rsi_oversold = rsi_oversold

        logger.debug(
            f"Оновлено параметри Stage1: vol_z={vol_z_threshold}, "
            f"rsi_ob={rsi_overbought}, rsi_os={rsi_oversold}"
        )

    async def ensure_symbol_cfg(self, symbol: str) -> Thresholds:
        """
        Завантажує індивідуальні пороги (з Redis або дефолтні).
        Додає захист від ситуації, коли замість Thresholds приходить рядок (наприклад, symbol).
        """
        import traceback

        if symbol not in self._symbol_cfg:
            thr = await load_thresholds(symbol, self.cache_handler)
            # Захист: якщо thr — це рядок, а не Thresholds
            if isinstance(thr, str):
                logger.error(
                    f"[{symbol}] load_thresholds повернув рядок замість Thresholds: {thr}"
                )
                logger.error(traceback.format_stack())
                raise TypeError(
                    f"[{symbol}] load_thresholds повернув рядок замість Thresholds: {thr}"
                )
            if thr is None:
                logger.warning(
                    f"[{symbol}] Не знайдено порогів у Redis, використовую стандартні"
                )
                thr = Thresholds(symbol=symbol, config={})
            self._symbol_cfg[symbol] = thr
            logger.debug(
                f"[{symbol}] Завантажено пороги: {getattr(thr, 'to_dict', lambda: thr)()}"
            )
        return self._symbol_cfg[symbol]

    async def update_statistics(
        self,
        symbol: str,
        df: pd.DataFrame,
    ) -> Dict[str, Any]:
        """
        Оновлення базових метрик для швидкого моніторингу (1m/5m, максимум 1-3 години).
        Забезпечує стандартизацію формату, коректний розрахунок RSI (інкрементально),
        крос-метрики для UI та тригерів.
        """
        # 1. Стандартизація формату (завжди уніфіковані колонки)
        df = ensure_timestamp_column(df)
        if df.empty:
            raise ValueError(f"[{symbol}] Передано порожній DataFrame для статистики!")

        # 2. Основні ціни/зміни
        price = df["close"].iloc[-1]
        first = df["close"].iloc[0]
        price_change = (price / first - 1) if first else 0.0

        # 3. Денні high/low/range з цього ж df
        daily_high = df["high"].max()
        daily_low = df["low"].min()
        daily_range = daily_high - daily_low

        # 4. Volume statistics
        vol_mean = df["volume"].mean()
        vol_std = df["volume"].std(ddof=0) or 1.0
        volume_z = (df["volume"].iloc[-1] - vol_mean) / vol_std

        # 5. RSI (інкрементально) O(1) (RAM-fast)
        self.rsi_manager.ensure_state(symbol, df["close"])  # на всяк випадок при старті

        # RSI (RAM-fast, seed-based)
        rsi = self.rsi_manager.update(symbol, price)
        rsi_bar = format_rsi(rsi, symbol=symbol)
        rsi_s = compute_rsi(
            df["close"], symbol=symbol
        )  # Для статистики (векторний, не обов’язково на кожен бар)

        # 6. VWAP (інкрементально) (FIFO)
        # seed-буфер із всіх, крім останнього бару
        self.vwap_manager.ensure_buffer(symbol, df.iloc[:-1])
        # додаємо новий бар у буфер
        volume = df["volume"].iloc[-1]
        self.vwap_manager.update(symbol, price, volume)
        # 3) розраховуємо VWAP вже по оновленому буферу
        vwap = self.vwap_manager.compute_vwap(symbol)

        # 7. ATR (інкрементально) (O(1)!)
        self.atr_manager.ensure_state(symbol, df)
        high = df["high"].iloc[-1]
        low = df["low"].iloc[-1]
        close = df["close"].iloc[-1]
        atr = self.atr_manager.update(symbol, high, low, close)

        # 8. Volume Z-score (інкрементально) (RAM-fast)
        self.volumez_manager.ensure_buffer(symbol, df)
        volume = df["volume"].iloc[-1]
        volume_z = self.volumez_manager.update(symbol, volume)

        # 10. Динамічні пороги RSI
        avg_rsi = rsi_s.mean()

        # Якщо не задані константи, використовуй динаміку
        over = getattr(self, "rsi_overbought", None) or min(
            avg_rsi * getattr(self, "dynamic_rsi_multiplier", 1.25), 90
        )
        under = getattr(self, "rsi_oversold", None) or max(
            avg_rsi / getattr(self, "dynamic_rsi_multiplier", 1.25), 10
        )

        # 11. Збираємо всі метрики в один словник для UI і тригерів
        stats = {
            "current_price": float(price),
            "price_change": float(price_change),
            "daily_high": float(daily_high),
            "daily_low": float(daily_low),
            "daily_range": float(daily_range),
            "volume_mean": float(vol_mean),
            "volume_std": float(vol_std),
            "volume_z": float(volume_z),
            "rsi": float(rsi) if rsi is not None else np.nan,
            "rsi_bar": str(rsi_bar),
            "dynamic_overbought": float(over) if over is not None else np.nan,
            "dynamic_oversold": float(under) if under is not None else np.nan,
            "vwap": float(vwap) if vwap is not None else np.nan,
            "atr": float(atr) if atr is not None else np.nan,
            "volume_z": float(volume_z) if volume_z is not None else np.nan,
            "last_updated": datetime.now(timezone.utc).isoformat(),
            # Опціонально: можна додати median, quantile, trend, etc.
        }

        # 12. Зберігаємо в кеші монітора та лог
        self.asset_stats[symbol] = stats
        if getattr(self, "enable_stats", False):
            logger.debug(f"[{symbol}] Оновлено статистику: {stats}")
        return stats

    async def check_anomalies(
        self,
        symbol: str,
        df: pd.DataFrame,
        stats: Optional[Dict[str, Any]] = None,
        trigger_reasons: List[str] = [],
    ) -> Dict[str, Any]:
        """
        Аналізує основні тригери та формує raw signal.
        Додає захист від ситуації, коли пороги некоректні (наприклад, рядок).
        """
        import traceback

        # Завжди оновлюємо метрики по новому df
        stats = await self.update_statistics(symbol, df)
        price = stats["current_price"]

        anomalies: list[str] = []
        reasons: list[str] = []

        thr = await self.ensure_symbol_cfg(symbol)
        # Захист: якщо thr — це рядок, а не Thresholds
        if isinstance(thr, str):
            logger.error(
                f"[{symbol}] ensure_symbol_cfg повернув рядок замість Thresholds: {thr}"
            )
            logger.error(traceback.format_stack())
            raise TypeError(
                f"[{symbol}] ensure_symbol_cfg повернув рядок замість Thresholds: {thr}"
            )
        logger.debug(
            f"[{symbol}] Пороги: low={thr.low_gate*100:.2f}%, high={thr.high_gate*100:.2f}%"
        )

        # Отримання каліброваних параметрів
        calibrated_params = None
        if self.state_manager and symbol in self.state_manager.state:
            asset_state = self.state_manager.state[symbol]
            calibrated_params = asset_state.get("calibrated_params")
            logger.debug(
                f"[{symbol}] Отримано калібровані параметри: {calibrated_params}"
            )

        # Оновлення порогів
        if calibrated_params:
            thr.low_gate = calibrated_params.get("low_gate", thr.low_gate)
            thr.high_gate = calibrated_params.get("high_gate", thr.high_gate)
            thr.vol_z_threshold = calibrated_params.get(
                "volume_z_threshold", thr.vol_z_threshold
            )
            thr.rsi_oversold = calibrated_params.get("rsi_oversold", thr.rsi_oversold)
            thr.rsi_overbought = calibrated_params.get(
                "rsi_overbought", thr.rsi_overbought
            )

        logger.debug(
            f"[check_anomalies] {symbol} | Параметри застосовані: "
            f"lg={thr.low_gate:.4f}, hg={thr.high_gate:.4f}, volz={thr.vol_z_threshold:.2f}, "
            f"rsi_os={thr.rsi_oversold}, rsi_ob={thr.rsi_overbought}"
        )

        def _add(reason: str, text: str) -> None:
            anomalies.append(text)
            reasons.append(reason)

        # ————— Перевірка ATR —————
        atr_pct = stats["atr"] / price

        # Ініціалізація змінних
        low_atr_flag = False  # Флаг для визначення, чи ринок спокійний

        over = stats.get("dynamic_overbought", 70)
        under = stats.get("dynamic_oversold", 30)

        # ————— Якщо ATR занадто низький — просто позначаємо low_atr, але не перериваємо логіку
        if atr_pct < thr.low_gate:
            logger.debug(
                f"[{symbol}] ATR={atr_pct:.4f} < поріг low_gate — ринок спокійний, але продовжуємо аналіз.."
            )
            low_atr_flag = True
            _add("low_volatility", "📉 Низька волатильність")

        # Додаткове логування для зневадження
        logger.debug(
            f"[{symbol}] Перевірка тригерів:"
            f" price={price:.4f}"
            f" - ATR={atr_pct:.4f} (поріг low={thr.low_gate:.4f}, high={thr.high_gate:.4f})"
            f" - VolumeZ: {stats['volume_z']:.2f} (поріг {thr.vol_z_threshold:.2f})"
            f" - RSI: {stats['rsi']:.2f} (OB {over:.2f}, OS {under:.2f})"
            # f" - VWAP: {stats['vwap']:.4f} (поріг відхилення {thr.vwap_threshold:.2f})"
        )

        # ————— ІНТЕГРАЦІЯ ВСІХ СУЧАСНИХ ТРИГЕРІВ —————
        # 1. Сплеск обсягу
        if self._sw_triggers.get("volume_spike", True):
            if volume_spike_trigger(df, z_thresh=thr.vol_z_threshold):
                _add("volume_spike", f"📈 Сплеск обсягу (Z>{thr.vol_z_threshold:.2f})")
                logger.debug(
                    f"[{symbol}] Volume spike detected: {stats['volume_z']:.2f} > {thr.vol_z_threshold:.2f}"
                )

        # if stats["volume_z"] > thr.vol_z_threshold:  # Використовувати оновлені stats
        # _add("volume_spike", f"📈 Сплеск обсягу (Z>{thr.vol_z_threshold:.2f})")
        # logger.debug(
        #    f"[{symbol}] Volume spike detected: {stats['volume_z']:.2f} > {thr.vol_z_threshold:.2f}"
        # )

        # 2. Пробій рівнів (локальний breakout, підхід до рівня)
        if self._sw_triggers.get("breakout", True):
            breakout = breakout_level_trigger(
                df,
                stats,
                window=20,
                near_threshold=0.005,
                near_daily_threshold=0.5,  # наприклад, 0.5%
                symbol=symbol,
            )
            if breakout["breakout_up"]:
                _add("breakout_up", "🔺 Пробій вгору локального максимуму")
            if breakout["breakout_down"]:
                _add("breakout_down", "🔻 Пробій вниз локального мінімуму")
            if breakout["near_high"]:
                _add("near_high", "📈 Підхід до локального максимуму")
            if breakout["near_low"]:
                _add("near_low", "📉 Підхід до локального мінімуму")
            if breakout["near_daily_support"]:
                _add("near_daily_support", "🟢 Підхід до денного рівня підтримки")
            if breakout["near_daily_resistance"]:
                _add("near_daily_resistance", "🔴 Підхід до денного рівня опору")

        # 3. Сплеск волатильності
        if self._sw_triggers.get("volatility_spike", True):
            if volatility_spike_trigger(df, window=14, threshold=2.0):
                _add("volatility_spike", "⚡️ Сплеск волатильності (ATR/TR)")

        # 4. RSI + дивергенції
        if self._sw_triggers.get("rsi", True):
            rsi_res = rsi_divergence_trigger(df, rsi_period=14)
            if rsi_res.get("rsi") is not None:
                # Замість фіксованих 70/30 — динамічні з stats
                over = stats["dynamic_overbought"]
                under = stats["dynamic_oversold"]
                if rsi_res["rsi"] > over:
                    _add(
                        "rsi_overbought",
                        f"🔺 RSI перекупленість ({rsi_res['rsi']:.1f} > {over:.1f})",
                    )
                elif rsi_res["rsi"] < under:
                    _add(
                        "rsi_oversold",
                        f"🔻 RSI перепроданість ({rsi_res['rsi']:.1f} < {under:.1f})",
                    )
                if rsi_res.get("bearish_divergence"):
                    _add("bearish_div", "🦀 Ведмежа дивергенція RSI/ціна")
                if rsi_res.get("bullish_divergence"):
                    _add("bullish_div", "🦅 Бичача дивергенція RSI/ціна")

        # 5. Відхилення від VWAP
        if self._sw_triggers.get("vwap_deviation", True):
            vwap_trig = vwap_deviation_trigger(
                self.vwap_manager, symbol, price, threshold=0.005
            )
            if vwap_trig["trigger"]:
                _add(
                    "vwap_deviation",
                    f"⚖️ Відхилення від VWAP на {vwap_trig['deviation']*100:.2f}%",
                )

        # 6. Сплеск відкритого інтересу (OI)
        #    if open_interest_spike_trigger(df, z_thresh=3.0):
        #        _add("oi_spike", "🆙 Сплеск відкритого інтересу (OI)")

        # 7. Додатково: ATR-коридор (волатильність)
        if atr_pct > thr.high_gate:
            _add("high_atr", f"📊 ATR > {thr.high_gate:.2%}")
        elif low_atr_flag:
            _add("low_atr", f"📉 ATR < {thr.low_gate:.2%}")

        # Зберігаємо причини тригерів для подальшої обробки
        raw_reasons = list(reasons)  # зберігаємо «як є» для діагностики

        # Нормалізуємо причини тригерів
        trigger_reasons = normalize_trigger_reasons(raw_reasons)

        # Мінімум 2 причини — це "ALERT"
        signal = (
            "ALERT" if len(trigger_reasons) >= self.min_reasons_for_alert else "NORMAL"
        )

        logger.debug(
            f"[{symbol}] SIGNAL={signal}, тригери={trigger_reasons}, ціна={price:.4f}"
        )

        return {
            "symbol": symbol,
            "current_price": price,
            "anomalies": anomalies,
            "signal": signal,
            "trigger_reasons": trigger_reasons,  # повертаємо канонічні імена
            "raw_trigger_reasons": raw_reasons,  # опційно: залишимо для дебагу
            "stats": stats,
            "calibrated_params": thr.to_dict(),
            "thresholds": thr.to_dict(),
        }


"""
    # ─────────── Відправка Telegram ───────────
    async def send_alert(self, symbol: str, price: float, trigger_reasons: List[str], **extra: Any, ) -> None:
        
        Надсилає повідомлення в Telegram,
        якщо сигнал пройшов загальний cooldown для цього символу.
        
        now = datetime.now(timezone.utc)
        last = self.last_alert_time.get(symbol)
        if last and now - last < self.cooldown_period:
            return

        text = _build_telegram_text(symbol, price, trigger_reasons, **extra)
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            "chat_id": ADMIN_ID,
            "text": text,
            "parse_mode": "Markdown"
        }
        async with aiohttp.ClientSession() as sess:
            async with sess.post(url, json=payload) as resp:
                if resp.status == 200:
                    self.last_alert_time[symbol] = now
                else:
                    logger.error("Telegram error %s: %s", resp.status, await resp.text())


# ─────────── Utility: Telegram API ───────────
def _build_telegram_text(symbol: str, price: float, reasons: List[str], **extra: Any ) -> str:
    
    Формує зрозуміле повідомлення для Telegram:
     • символ і ціна
     • маркований список з описами причин
     • підказка, на що звернути увагу
    
    lines = [
        f"🔔 *Сигнал:* `{symbol}` @ *{price:.4f} USD*",
        "",
        "*Причини сигналу:*"
    ]
    # Додаємо всі додаткові поля, якщо вони є
    if extra:
        lines.append("*Додаткові дані:*")
        for k, v in extra.items():
            lines.append(f"• {k}: `{v}`")
        lines.append("")
    for code in reasons:
        desc = _REASON_DESCRIPTIONS.get(code, code)
        lines.append(f"• {desc}")
    lines.append("")
    return "\n".join(lines)

"""
