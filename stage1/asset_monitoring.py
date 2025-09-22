"""Stage1 моніторинг потокових барів (1m/5m) та генерація сирих сигналів.

Шлях: ``stage1/asset_monitoring.py``

Призначення:
    • підтримка інкрементальної статистики (RSI, VWAP, ATR, VolumeZ);
    • агрегація тригерів (volume / breakout / volatility / RSI / VWAP deviation);
    • нормалізація причин (`normalize_trigger_reasons`) і формування сигналу ALERT/NORMAL.

Особливості:
    • lazy ініціалізація порогів (Redis / дефолти);
    • динамічні RSI пороги (over/under) із історії;
    • можливість каліброваних параметрів через state_manager.
"""

import asyncio
import datetime as dt
import logging
from typing import Any

import numpy as np
import pandas as pd
from rich.console import Console
from rich.logging import RichHandler

from app.thresholds import Thresholds, load_thresholds
from config.config import (  # додано USE_RSI_DIV, USE_VWAP_DEVIATION
    K_SIGNAL,
    K_STATS,
    K_SYMBOL,
    K_TRIGGER_REASONS,
    USE_VOL_ATR,
)
from stage1.asset_triggers import (
    breakout_level_trigger,
    rsi_divergence_trigger,
    volatility_spike_trigger,
    volume_spike_trigger,
)
from stage1.indicators import (
    ATRManager,
    RSIManager,
    VolumeZManager,
    VWAPManager,
    format_rsi,
    vwap_deviation_trigger,
)
from utils.utils import (
    normalize_trigger_reasons,
)

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("app.stage1.asset_monitoring")
if not logger.handlers:  # guard від подвійного підключення
    logger.setLevel(logging.DEBUG)
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
        rsi_overbought: float | None = None,
        rsi_oversold: float | None = None,
        dynamic_rsi_multiplier: float = 1.1,
        min_reasons_for_alert: int = 2,
        enable_stats: bool = True,
        feature_switches: dict | None = None,
        on_alert: Any | None = None,
    ):
        self.cache_handler = cache_handler
        self.vol_z_threshold = vol_z_threshold
        self.rsi_manager = RSIManager(period=14)
        self.atr_manager = ATRManager(period=14)
        self.vwap_manager = VWAPManager(window=30)
        self.volumez_manager = VolumeZManager(window=20)
        self.global_levels: dict[str, list[float]] = {}
        self.rsi_overbought = rsi_overbought
        self.rsi_oversold = rsi_oversold
        self.dynamic_rsi_multiplier = dynamic_rsi_multiplier
        self.min_reasons_for_alert = min_reasons_for_alert
        self.enable_stats = enable_stats
        self.asset_stats: dict[str, dict[str, Any]] = {}
        self._symbol_cfg: dict[str, Thresholds] = {}
        self.state_manager = state_manager
        # Статистики для anti-spam/визначення частоти тригерів можна додати тут, якщо потрібно
        self.feature_switches = feature_switches or {}
        self._sw_triggers = self.feature_switches.get("triggers") or {}
        # Stage2 trigger callback (async function expected). Signature: (signal: dict) -> Awaitable[None]
        self._on_alert_cb = on_alert
        # Службові маркери для дедуплікації обробки барів
        self._last_processed_last_ts: dict[str, float] = {}
        # Пер-символьні замки для реактивної обробки
        # Per-symbol reactive lock to avoid overlapping processing
        self._locks: dict[str, asyncio.Lock] = {}
        # Тогл для OR-гілки Vol/ATR у volume_spike
        self.use_vol_atr: bool = USE_VOL_ATR

        # Можливий оверрайд через feature_switches
        try:
            sw = (feature_switches or {}).get("volume_spike", {})
            if isinstance(sw, dict) and "use_vol_atr" in sw:
                self.use_vol_atr = bool(sw["use_vol_atr"])
        except Exception:
            pass

        logger.debug("[Stage1] use_vol_atr=%s", self.use_vol_atr)

    def _detect_market_state(self, symbol: str, stats: dict[str, Any]) -> str | None:
        """Грубе евристичне визначення стану ринку.

        Повертає один з: "range_bound" | "trend_strong" | "high_volatility" | None

        Heuristics (мінімально інвазивно):
          - high_volatility: ATR% > high_gate
          - range_bound: ATR% < low_gate і |price_change| < 1%
          - trend_strong: |price_change| >= 2% або RSI далеко від 50 (>|60| або <|40|)
        """
        try:
            price = float(stats.get("current_price") or 0.0)
            atr = float(stats.get("atr") or 0.0)
            price_change = float(stats.get("price_change") or 0.0)
            rsi = float(stats.get("rsi") or 50.0)
            thr = self._symbol_cfg.get(symbol)
            low_gate = getattr(thr, "low_gate", 0.006) if thr else 0.006
            high_gate = getattr(thr, "high_gate", 0.015) if thr else 0.015
            atr_pct = (atr / price) if price else 0.0
            if atr_pct > high_gate:
                return "high_volatility"
            if atr_pct < low_gate and abs(price_change) < 0.01:
                return "range_bound"
            if abs(price_change) >= 0.02 or rsi >= 60 or rsi <= 40:
                return "trend_strong"
        except Exception:
            return None
        return None

    def update_params(
        self,
        vol_z_threshold: float | None = None,
        rsi_overbought: float | None = None,
        rsi_oversold: float | None = None,
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
    ) -> dict[str, Any]:
        """
        Оновлення базових метрик для швидкого моніторингу (1m/5m, максимум 1-3 години).
        Забезпечує стандартизацію формату, коректний розрахунок RSI (інкрементально),
        крос-метрики для UI та тригерів.
        """
        # Не виконуємо конвертацію часу: працюємо з наданим df як є
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
        # Уникаємо повного перерахунку RSI кожен раз; беремо історію з менеджера
        rsi_hist = list(self.rsi_manager.history_map.get(symbol, []))
        rsi_s = (
            pd.Series(rsi_hist[-min(len(rsi_hist), 120) :])
            if rsi_hist
            else pd.Series([rsi])
        )

        # 6. VWAP (інкрементально) (FIFO)
        # seed-буфер із всіх, крім останнього бару
        # ініціалізація буфера відбувається лише якщо він відсутній (без перезаливки кожен крок)
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
        # ініціалізація буфера лише за потреби (без перезаливки)
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
            "rsi": float(rsi) if rsi is not None else np.nan,
            "rsi_bar": str(rsi_bar),
            "dynamic_overbought": float(over) if over is not None else np.nan,
            "dynamic_oversold": float(under) if under is not None else np.nan,
            "vwap": float(vwap) if vwap is not None else np.nan,
            "atr": float(atr) if atr is not None else np.nan,
            "volume_z": float(volume_z) if volume_z is not None else np.nan,
            "last_updated": dt.datetime.now(dt.UTC).isoformat(),
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
        stats: dict[str, Any] | None = None,
        trigger_reasons: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Аналізує основні тригери та формує raw signal.
        Додає захист від ситуації, коли пороги некоректні (наприклад, рядок).
        """
        import traceback

        # Нормалізація mutable default
        if trigger_reasons is None:
            trigger_reasons = []

        # Boundary log: отримано DataFrame для аналізу (лише raw numeric значення)
        try:
            n = len(df)
            if "timestamp" in df.columns:
                t_head = (
                    pd.to_numeric(df["timestamp"], errors="coerce")
                    .astype("Int64")
                    .head(3)
                    .dropna()
                    .astype("int64")
                    .tolist()
                )
                t_tail = (
                    pd.to_numeric(df["timestamp"], errors="coerce")
                    .astype("Int64")
                    .tail(3)
                    .dropna()
                    .astype("int64")
                    .tolist()
                )
                logger.debug(
                    "[Stage1 RECEIVE] %s | rows=%d timestamp head=%s tail=%s",
                    symbol,
                    n,
                    t_head,
                    t_tail,
                )
        except Exception:
            pass

        # Додатково: лог сирих open_time/close_time як приходять (інт/рядки)
        try:
            if "open_time" in df.columns:
                ot = pd.to_numeric(df["open_time"], errors="coerce").astype("Int64")
                logger.debug(
                    "[check_anomalies] %s | RAW open_time head=%s tail=%s",
                    symbol,
                    ot.head(3).dropna().astype("int64").tolist(),
                    ot.tail(3).dropna().astype("int64").tolist(),
                )
            if "close_time" in df.columns:
                ct = pd.to_numeric(df["close_time"], errors="coerce").astype("Int64")
                logger.debug(
                    "[check_anomalies] %s | RAW close_time head=%s tail=%s",
                    symbol,
                    ct.head(3).dropna().astype("int64").tolist(),
                    ct.tail(3).dropna().astype("int64").tolist(),
                )
        except Exception:
            pass

        # Не конвертуємо час — лишаємо raw numeric логіку вище

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

        # Калібровані параметри видалені — використовуються лише завантажені/дефолтні thresholds

        # Визначаємо стан ринку і ефективні пороги (мінімальні зміни)
        market_state = self._detect_market_state(symbol, stats)
        try:
            effective = thr.effective_thresholds(market_state=market_state)
        except Exception:
            effective = thr.to_dict()
        logger.debug(
            f"[check_anomalies] {symbol} | Застосовано пороги: "
            f"lg={effective.get('low_gate'):.4f}, hg={effective.get('high_gate'):.4f}, "
            f"volz={effective.get('vol_z_threshold'):.2f}, "
            f"rsi_os={effective.get('rsi_oversold')}, rsi_ob={effective.get('rsi_overbought')}, "
            f"state={market_state}"
        )
        # Інформативний лог на INFO-рівні (нечасто): показати зміну стану
        try:
            # Лог лише коли стан змінюється (зберігаємо попередній у self.asset_stats)
            prev_state = self.asset_stats.get(symbol, {}).get("_market_state")
            if prev_state != market_state:
                logger.info(
                    "[%s] Ринковий стан: %s → ефективні пороги: volZ=%.2f, vwap=%.3f, gates=[%.3f..%.3f]",
                    symbol,
                    market_state,
                    float(effective.get("vol_z_threshold", float("nan"))),
                    float(effective.get("vwap_deviation", float("nan"))),
                    float(effective.get("low_gate", float("nan"))),
                    float(effective.get("high_gate", float("nan"))),
                )
            # збережемо стан для наступного порівняння
            self.asset_stats.setdefault(symbol, {})["_market_state"] = market_state
        except Exception:
            pass

        def _add(reason: str, text: str) -> None:
            anomalies.append(text)
            reasons.append(reason)

        # ————— Перевірка ATR —————
        atr_pct = stats["atr"] / price

        # Ініціалізація змінних
        low_atr_flag = False  # Флаг для визначення, чи ринок спокійний

        over = stats.get("dynamic_overbought", 70)
        under = stats.get("dynamic_oversold", 30)

        # ————— Якщо ATR занадто низький — позначаємо low_atr і готуємо gate
        if atr_pct < thr.low_gate:
            logger.debug(
                f"[{symbol}] ATR={atr_pct:.4f} < поріг low_gate — ринок спокійний, але продовжуємо аналіз.."
            )
            low_atr_flag = True
            _add("low_volatility", "📉 Низька волатильність")

        logger.debug(
            f"[{symbol}] Перевірка тригерів:"
            f" price={price:.4f}"
            f" - ATR={atr_pct:.4f} (поріг low={effective.get('low_gate'):.4f}, high={effective.get('high_gate'):.4f})"
            f" - VolumeZ: {stats['volume_z']:.2f} (поріг {effective.get('vol_z_threshold'):.2f})"
            f" - RSI: {stats['rsi']:.2f} (OB {over:.2f}, OS {under:.2f})"
        )

        # ————— ІНТЕГРАЦІЯ ВСІХ СУЧАСНИХ ТРИГЕРІВ —————
        # 1. Сплеск обсягу (використовуємо виключно Z‑score, vol/atr шлях опційний)
        if self._sw_triggers.get("volume_spike", True):
            volz = float(
                effective.get("vol_z_threshold", getattr(thr, "vol_z_threshold", 2.0))
            )
            # За замовчуванням використовуємо лише Z-score (use_vol_atr=False)
            if volume_spike_trigger(
                df,
                z_thresh=volz,
                symbol=symbol,
                use_vol_atr=self.use_vol_atr,
            ):
                # Визначаємо напрям бару, щоб розрізняти бичий/ведмежий сплеск
                try:
                    last_open = float(df["open"].iloc[-1])
                    last_close = float(df["close"].iloc[-1])
                    upward = last_close > last_open
                except Exception:
                    upward = True  # якщо неможливо визначити — припускаємо вгору
                # Визначимо, яка саме умова спрацювала, щоб лог не вводив в оману
                try:
                    z_val = float(stats.get("volume_z", 0.0))
                except Exception:
                    z_val = 0.0
                # (VOL/ATR гілка вимкнена за замовчуванням)
                if upward:
                    reason_txt = (
                        f"📈 Бичий сплеск обсягу (Z>{volz:.2f})"
                        if z_val >= volz
                        else "📈 Бичий сплеск обсягу (VOL/ATR)"
                    )
                    _add("bull_volume_spike", reason_txt)
                    logger.debug(
                        f"[{symbol}] Bull volume spike | Z={z_val:.2f} thr={volz:.2f} use_vol_atr={self.use_vol_atr}"
                    )
                else:
                    reason_txt = (
                        f"📉 Ведмежий сплеск обсягу (Z>{volz:.2f})"
                        if z_val >= volz
                        else "📉 Ведмежий сплеск обсягу (VOL/ATR)"
                    )
                    _add("bear_volume_spike", reason_txt)
                    logger.debug(
                        f"[{symbol}] Bear volume spike | Z={z_val:.2f} thr={volz:.2f} use_vol_atr={self.use_vol_atr}"
                    )

        # 2. Пробій рівнів (локальний breakout, підхід до рівня)
        if self._sw_triggers.get("breakout", True):
            # Налаштування breakout із конфігурації (state-aware)
            br_cfg: dict[str, Any] = {}
            try:
                st = (
                    effective.get("signal_thresholds", {})
                    if isinstance(effective, dict)
                    else {}
                )
                if isinstance(st, dict):
                    br_cfg = st.get("breakout", {}) or {}
            except Exception:
                br_cfg = {}

            band_pct_atr = br_cfg.get("band_pct_atr", br_cfg.get("band_pct"))
            confirm_bars = int(br_cfg.get("confirm_bars", 1) or 1)
            min_retests = int(br_cfg.get("min_retests", 0) or 0)

            # Обчислимо поріг близькості як частку від ціни: band_pct_atr * (ATR/price)
            try:
                atr_pct_local = float(stats.get("atr", 0.0)) / float(price)
            except Exception:
                atr_pct_local = 0.0
            if isinstance(band_pct_atr, (int, float)) and atr_pct_local > 0:
                near_thr = float(band_pct_atr) * atr_pct_local
                # Клапани безпеки
                near_thr = float(min(0.03, max(0.001, near_thr)))
            else:
                near_thr = 0.005

            logger.debug(
                "[%s] Breakout cfg: band_pct_atr=%s → near_thr=%.5f, confirm_bars=%d, min_retests=%d",
                symbol,
                band_pct_atr,
                near_thr,
                confirm_bars,
                min_retests,
            )

            breakout = breakout_level_trigger(
                df,
                stats,
                window=20,
                near_threshold=float(near_thr),
                near_daily_threshold=0.5,  # у % (0.5% за замовчуванням)
                symbol=symbol,
                confirm_bars=confirm_bars,
                min_retests=min_retests,
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
                # Замість фіксованих 70/30 — динамічні з stats, із clamp від конфігу (за наявності)
                over = stats["dynamic_overbought"]
                under = stats["dynamic_oversold"]
                # Застосуємо обмеження (стеля/підлога) з signal_thresholds.rsi_trigger
                try:
                    st = (
                        effective.get("signal_thresholds", {})
                        if isinstance(effective, dict)
                        else {}
                    )
                    rsi_cfg = st.get("rsi_trigger", {}) if isinstance(st, dict) else {}
                    clamp_over = rsi_cfg.get("overbought")
                    clamp_under = rsi_cfg.get("oversold")
                    over_eff = (
                        float(min(float(over), float(clamp_over)))
                        if isinstance(clamp_over, (int, float))
                        else float(over)
                    )
                    under_eff = (
                        float(max(float(under), float(clamp_under)))
                        if isinstance(clamp_under, (int, float))
                        else float(under)
                    )
                    if over_eff != over or under_eff != under:
                        logger.debug(
                            "[%s] RSI clamp застосовано",
                            symbol,
                            extra={
                                "base": {"over": float(over), "under": float(under)},
                                "clamp": {"over": clamp_over, "under": clamp_under},
                                "effective": {"over": over_eff, "under": under_eff},
                            },
                        )
                    over = over_eff
                    under = under_eff
                except Exception:
                    pass
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

        # 5. Відхилення від VWAP (порог з thresholds)
        if self._sw_triggers.get("vwap_deviation", True):
            vwap_thr = float(
                effective.get("vwap_deviation", getattr(thr, "vwap_deviation", 0.02))
                or 0.02
            )
            vwap_trig = vwap_deviation_trigger(
                self.vwap_manager, symbol, price, threshold=float(vwap_thr)
            )
            if vwap_trig["trigger"]:
                _add(
                    "vwap_deviation",
                    f"⚖️ Відхилення від VWAP на {vwap_trig['deviation']*100:.2f}% (поріг {float(vwap_thr)*100:.2f}%)",
                )

        # 6. Сплеск відкритого інтересу (OI)
        #    if open_interest_spike_trigger(df, z_thresh=3.0):
        #        _add("oi_spike", "🆙 Сплеск відкритого інтересу (OI)")

        # 7. Додатково: ATR-коридор (волатильність) з урахуванням мінімального ATR
        min_atr_pct = float(getattr(thr, "min_atr_percent", 0.0) or 0.0)
        if atr_pct > thr.high_gate:
            _add("high_atr", f"📊 ATR > {thr.high_gate:.2%}")
        elif low_atr_flag or (min_atr_pct and atr_pct < min_atr_pct):
            _add("low_atr", f"📉 ATR < {thr.low_gate:.2%}")

        # Зберігаємо причини тригерів для подальшої обробки
        raw_reasons = list(reasons)  # зберігаємо «як є» для діагностики

        # Нормалізуємо причини тригерів
        trigger_reasons = normalize_trigger_reasons(raw_reasons)

        # Gate: якщо ринок спокійний (low ATR) і немає сильних тригерів — не ескалюємо до ALERT
        strong_trigs = {"breakout_up", "breakout_down", "vwap_deviation"}
        has_strong = any(t in strong_trigs for t in trigger_reasons)
        if low_atr_flag and not has_strong:
            signal = "NORMAL"
        else:
            # Мінімум 2 причини — це "ALERT"
            signal = (
                "ALERT"
                if len(trigger_reasons) >= self.min_reasons_for_alert
                else "NORMAL"
            )

        logger.debug(
            f"[{symbol}] SIGNAL={signal}, тригери={trigger_reasons}, ціна={price:.4f}"
        )

        return {
            K_SYMBOL: symbol,
            "current_price": price,
            "anomalies": anomalies,
            K_SIGNAL: signal,
            K_TRIGGER_REASONS: trigger_reasons,  # повертаємо канонічні імена
            "raw_trigger_reasons": raw_reasons,  # опційно: залишимо для дебагу
            K_STATS: stats,
            "calibrated_params": thr.to_dict(),
            "thresholds": thr.to_dict(),
        }


"""
    # ─────────── Відправка Telegram ───────────
    async def send_alert(self, symbol: str, price: float, trigger_reasons: list[str], **extra: Any, ) -> None:

        Надсилає повідомлення в Telegram,
        якщо сигнал пройшов загальний cooldown для цього символу.

    now = datetime.now(datetime.UTC)
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
def _build_telegram_text(symbol: str, price: float, reasons: list[str], **extra: Any ) -> str:

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
