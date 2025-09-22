"""Stage2 (lite) — мінімальний процесор.

Потік:
    Stage1 stats → QDE Core → corridor (LevelManager) → evidence → результат.

Призначення:
    • Формування компактного результату (recommendation / confidence / risk / narrative)
    • Інʼєкція коридору рівнів (LevelManager v2) у context
    • Повністю без калібрування та легасі модулів

Особливості:
    • Валідація мінімально необхідних полів
    • Обережне оновлення LevelSystem (throttling)
    • Логування короткого REC-підсумку
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import datetime
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

from config.config import (
    K_CONFIDENCE_METRICS,
    K_MARKET_CONTEXT,
    K_RECOMMENDATION,
    K_RISK_PARAMETERS,
    K_STATS,
    K_SYMBOL,
    K_TRIGGER_REASONS,
)
from utils.utils import safe_number

# Локальні імпорти (Stage2)
from .level_manager import LevelManager
from .qde_core import QDEConfig, QDEngine

# Логування
logger = logging.getLogger("app.stage2.processor")
if not logger.handlers:  # захист від повторної ініціалізації
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False


def _safe(val: Any, default: float = 0.0) -> float:
    """Адаптер для сумісності з існуючим кодом; делегує до utils.safe_number."""
    return safe_number(val, default)


class Stage2Processor:
    """Легкий Stage2-процесор.

    Фокусується лише на необхідному пайплайні:
        QDE Core (context/confidence/reco/narrative/risk) + LevelManager corridor.

    Відсутнє:
        • Калібрування
        • Легасі anomaly/risk overlays
        • Будь-які кеш-адаптери
    """

    def __init__(
        self,
        calib_queue: Any | None = None,
        timeframe: str = "1m",
        state_manager: Any = None,
        level_manager: LevelManager | None = None,
        bars_1m: dict[str, Any] | None = None,
        bars_5m: dict[str, Any] | None = None,
        bars_1d: dict[str, Any] | None = None,
        get_bars_1m: Callable[[str, int], Any] | None = None,
        get_bars_5m: Callable[[str, int], Any] | None = None,
        get_bars_1d: Callable[[str], Any] | None = None,
        user_lang: str = "UA",
        user_style: str = "explain",
        levels_update_every: int = 25,
    ) -> None:
        # Зберігаємо (опціонально) посилання на чергу/кеш калібрувань для сумісності з тестами
        self.calib_queue = calib_queue
        self.user_lang = user_lang
        self.user_style = user_style
        self.timeframe = timeframe
        self._state_manager = state_manager

        # Лишаємо LevelManager
        self.level_manager: LevelManager = level_manager or LevelManager()

        # QDE Core (один інстанс достатньо; він безстанний у нашій реалізації)
        self.engine = QDEngine(config=QDEConfig(), lang=self.user_lang)

        # Джерела барів для LevelManager (без змін)
        self.bars_1m = bars_1m or {}
        self.bars_5m = bars_5m or {}
        self.bars_1d = bars_1d or {}
        self.get_bars_1m = get_bars_1m
        self.get_bars_5m = get_bars_5m
        self.get_bars_1d = get_bars_1d

        # Тротлінг оновлень рівнів
        self._levels_last_update: dict[str, int] = {}
        self.levels_update_every = max(5, int(levels_update_every))

        logger.debug("Stage2Processor (lite) ініціалізовано, TF=%s", timeframe)

        # Підрахунок часу між викликами (без метрик Prometheus)
        self._last_process_wall: float | None = None

    # Внутрішні допоміжні
    def _maybe_fetch_bars(self, symbol: str) -> tuple[Any, Any, Any]:
        """Повертає (df_1m, df_5m, df_1d), якщо доступні; інакше (None, None, None)."""
        df_1m = (
            self.get_bars_1m(symbol, 500)
            if callable(self.get_bars_1m)
            else self.bars_1m.get(symbol)
        )
        df_5m = (
            self.get_bars_5m(symbol, 500)
            if callable(self.get_bars_5m)
            else self.bars_5m.get(symbol)
        )
        df_1d = (
            self.get_bars_1d(symbol)
            if callable(self.get_bars_1d)
            else self.bars_1d.get(symbol)
        )
        return df_1m, df_5m, df_1d

    def _update_levels_if_needed(self, symbol: str, stats: dict[str, Any]) -> None:
        """Оновлює LevelSystem v2 з тротлінгом (безпечний try/except)."""
        now_ts = int(time.time())
        last = self._levels_last_update.get(symbol, 0)
        if (now_ts - last) < self.levels_update_every:
            return

        try:
            price = float(stats.get("current_price") or 0.0)
            atr = float(stats.get("atr") or 0.0)
            atr_pct = (atr / price) * 100.0 if price > 0 else 0.5
            tick_size = stats.get("tick_size")

            # мета-оновлення
            self.level_manager.update_meta(symbol, atr_pct=atr_pct, tick_size=tick_size)

            # оновлення з барів
            df_1m, df_5m, df_1d = self._maybe_fetch_bars(symbol)
            self.level_manager.update_from_bars(
                symbol, df_1m=df_1m, df_5m=df_5m, df_1d=df_1d
            )

            self._levels_last_update[symbol] = now_ts
            # метрики вимкнено
        except (
            Exception
        ) as e:  # broad except: оновлення рівнів не критичне, пропускаємо
            logger.debug("Level update skipped for %s: %s", symbol, e)

    # Основний пайплайн
    async def process(self, stage1_signal: dict[str, Any]) -> dict[str, Any]:
        """
        Мінімалістичний потік:
        Stage1 stats --> QDE Core --> corridor v2 injection (LevelManager)
        --> evidence --> результат
        """
        # Початок відліку (залишено на випадок локальних замірів)
        try:
            stats = dict(stage1_signal.get(K_STATS) or {})
            # self._update_levels_if_needed(symbol, stats)
            symbol: str = str(
                stage1_signal.get(K_SYMBOL, stats.get(K_SYMBOL, "UNKNOWN"))
            )
            triggers: list[str] = list(stage1_signal.get(K_TRIGGER_REASONS) or [])

            # Заповнення критично необхідних полів (якщо Stage1 не поклав)
            cp = float(stats.get("current_price", 0) or 0)
            if cp <= 0:
                return {
                    "error": "no_price",
                    "symbol": symbol,
                    "recommendation": "AVOID",
                    "market_context": {"scenario": "INVALID_DATA"},
                    "narrative": (
                        "Помилка: невизначений стан — невизначені дані — "
                        "відсутня поточна ціна"
                    ),
                }
            stats.setdefault("vwap", stats.get("vwap", cp))
            stats.setdefault("atr", stats.get("atr", max(cp * 0.005, 1e-6)))
            dl = float(stats.get("daily_low", 0) or 0)
            dh = float(stats.get("daily_high", 0) or 0)
            if not dh or not dl or dh <= dl:
                stats["daily_low"] = cp * 0.99
                stats["daily_high"] = cp * 1.01

            # 1) QDE Core — єдине джерело context/confidence/reco/narrative/risk
            result = self.engine.process(
                {
                    K_SYMBOL: symbol,
                    K_STATS: stats,
                    K_TRIGGER_REASONS: triggers,
                }
            )
            # метрики вимкнено

            # 2) Corridor v2 із LevelManager (override key_levels у context)
            corr = (
                self.level_manager.get_corridor(
                    symbol=symbol,
                    price=stats["current_price"],
                    daily_low=stats.get("daily_low"),
                    daily_high=stats.get("daily_high"),
                )
                if self.level_manager
                else {}
            )

            ctx = result.get(K_MARKET_CONTEXT, {}) or {}
            kl = ctx.get("key_levels") or {}
            ctx["key_levels"] = {
                "immediate_support": kl.get("immediate_support") or corr.get("support"),
                "immediate_resistance": kl.get("immediate_resistance")
                or corr.get("resistance"),
                "next_major_level": kl.get("next_major_level") or corr.get("mid"),
            }
            ctx["key_levels_meta"] = {
                "band_pct": corr.get("band_pct"),
                "confidence": corr.get("confidence"),
                "mid": corr.get("mid"),
                "dist_to_support_pct": corr.get("dist_to_support_pct"),
                "dist_to_resistance_pct": corr.get("dist_to_resistance_pct"),
            }
            # 2.1) Обчислюємо atr_pct і low_gate (якщо передані пороги) для прозорості/гейтів
            try:
                price_v = float(stats.get("current_price") or 0.0)
                atr_v = float(stats.get("atr") or 0.0)
                atr_pct = (atr_v / price_v) if price_v > 0 else 0.0
            except Exception:
                atr_pct = 0.0
            thresholds = (
                stage1_signal.get("thresholds")
                if isinstance(stage1_signal, dict)
                else None
            )
            low_gate = None
            try:
                if isinstance(thresholds, dict):
                    lg = thresholds.get("low_gate")
                    if isinstance(lg, (int, float)):
                        low_gate = float(lg)
            except Exception:
                low_gate = None
            # Прапор підтвердження HTF: витягуємо з meso.htf_alignment як [0..1]
            meso = ctx.get("meso") or result.get(K_MARKET_CONTEXT, {}).get("meso") or {}
            htf_align = meso.get("htf_alignment") if isinstance(meso, dict) else None
            try:
                htf_ok = bool(htf_align is not None and float(htf_align) >= 0.5)
            except Exception:
                htf_ok = None  # невідомо
            # Прив’язуємо у context.meta для подальших етапів / UI
            ctx.setdefault("meta", {})
            ctx["meta"].update(
                {
                    "atr_pct": atr_pct,
                    "low_gate": low_gate,
                    "htf_alignment": htf_align,
                    "htf_ok": htf_ok,
                }
            )
            result[K_MARKET_CONTEXT] = ctx  # поклали назад

            # 3) Evidence біля рівнів (як і раніше)
            try:
                sup = ctx["key_levels"].get("immediate_support")
                res = ctx["key_levels"].get("immediate_resistance")
                s_ev = (
                    self.level_manager.evidence_around(symbol, sup, pct_window=0.12)
                    if isinstance(sup, (int, float))
                    else {}
                )
                r_ev = (
                    self.level_manager.evidence_around(symbol, res, pct_window=0.12)
                    if isinstance(res, (int, float))
                    else {}
                )
                ctx["level_evidence"] = {"support": s_ev, "resistance": r_ev}
            except Exception:  # broad except: evidence необов'язкова, тихо фолбек
                ctx["level_evidence"] = {"support": {}, "resistance": {}}
            # метрики вимкнено

            # 4) Лог короткого підсумку (сумісний)
            conf = result.get(K_CONFIDENCE_METRICS, {}) or {}
            risk = result.get(K_RISK_PARAMETERS, {}) or {}
            tp_str = (
                ",".join(f"{tp:.6f}" for tp in (risk.get("tp_targets") or [])[:3])
                if risk
                else ""
            )
            sl_val = risk.get("sl_level") if isinstance(risk, dict) else None
            rr_val = risk.get("risk_reward_ratio") if isinstance(risk, dict) else None
            sl_str = (
                f"{float(sl_val):.6f}" if isinstance(sl_val, (int, float)) else "nan"
            )
            rr_str = (
                f"{float(rr_val):.2f}" if isinstance(rr_val, (int, float)) else "nan"
            )
            logger.info(
                "[REC] %s scenario=%s composite=%.3f reco=%s tp=%s sl=%s rr=%s",
                symbol,
                ctx.get("scenario"),
                float(conf.get("composite_confidence", 0.0)),
                result.get(K_RECOMMENDATION),
                tp_str,
                sl_str,
                rr_str,
            )

            # 4.1) Action-gate (обережний): застосовуємо тільки якщо low_gate відомий
            try:
                conf = float(
                    (result.get(K_CONFIDENCE_METRICS) or {}).get(
                        "composite_confidence", 0.0
                    )
                )
            except Exception:
                conf = 0.0
            if isinstance(low_gate, float):
                if (
                    (atr_pct < low_gate)
                    or (isinstance(htf_ok, bool) and not htf_ok)
                    or (conf < 0.75)
                ):
                    # помʼякшуємо до WAIT_FOR_CONFIRMATION, не ламаючи структуру результату
                    result[K_RECOMMENDATION] = "WAIT_FOR_CONFIRMATION"

            # 5) Додаткові технічні поля як і раніше
            result.update(
                {
                    "symbol": symbol,
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "processing_time": datetime.utcnow().isoformat(),
                }
            )
            # Gap & last success timestamp (без Prometheus)
            wall_now = time.time()
            # no-op: прометеус вимкнено
            self._last_process_wall = wall_now
            return result

        except (
            Exception
        ) as e:  # broad except: гарантуємо повернення без падіння пайплайну
            logger.exception("Stage2Processor failure: %s", e)
            # метрики вимкнено
            return {
                "error": "SYSTEM_FAILURE",
                K_SYMBOL: stage1_signal.get(K_SYMBOL, "UNKNOWN"),
                K_RECOMMENDATION: "AVOID",
                "scenario": "SYSTEM_FAILURE",
                "narrative": "Критична системна помилка",
            }
