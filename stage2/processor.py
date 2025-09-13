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
import math
import time
from datetime import datetime
from typing import Any, Dict, Optional, Tuple, Callable, List

from rich.console import Console
from rich.logging import RichHandler

from config.config import STAGE2_CONFIG

# Підключаємо незалежний QDE Core
from .qde_core import QDEngine, QDEConfig
from .level_manager import LevelManager


# ── Логування ──
logger = logging.getLogger("app.stage2.processor")
if not logger.handlers:  # захист від повторної ініціалізації
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False


def _safe(val: Any, default: float = 0.0) -> float:
    """Безпечне перетворення у float з фільтром NaN/Inf."""
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except (
        Exception
    ):  # broad except: захист від будь-яких типів/об'єктів, що не кастяться у float
        return default


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
        timeframe: str = "1m",
        state_manager: Any = None,
        level_manager: Optional[LevelManager] = None,
        bars_1m: Optional[Dict[str, Any]] = None,
        bars_5m: Optional[Dict[str, Any]] = None,
        bars_1d: Optional[Dict[str, Any]] = None,
        get_bars_1m: Optional[Callable[[str, int], Any]] = None,
        get_bars_5m: Optional[Callable[[str, int], Any]] = None,
        get_bars_1d: Optional[Callable[[str], Any]] = None,
        user_lang: str = "UA",
        user_style: str = "explain",
        levels_update_every: int = 25,
    ) -> None:
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
        self._levels_last_update: Dict[str, int] = {}
        self.levels_update_every = max(5, int(levels_update_every))

        logger.debug("Stage2Processor (lite) ініціалізовано, TF=%s", timeframe)

    # ── Внутрішні допоміжні ──
    def _maybe_fetch_bars(self, symbol: str) -> Tuple[Any, Any, Any]:
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

    def _update_levels_if_needed(self, symbol: str, stats: Dict[str, Any]) -> None:
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
        except (
            Exception
        ) as e:  # broad except: оновлення рівнів не критичне, пропускаємо
            logger.debug("Level update skipped for %s: %s", symbol, e)

    # ── Основний пайплайн ──
    async def process(self, stage1_signal: Dict[str, Any]) -> Dict[str, Any]:
        """
        Мінімалістичний потік:
        Stage1 stats --> QDE Core --> corridor v2 injection (LevelManager) --> evidence --> результат
        """
        try:
            stats = dict(stage1_signal.get("stats") or {})
            # self._update_levels_if_needed(symbol, stats)
            symbol: str = str(
                stage1_signal.get("symbol", stats.get("symbol", "UNKNOWN"))
            )
            triggers: List[str] = list(stage1_signal.get("trigger_reasons") or [])

            # Заповнення критично необхідних полів (якщо Stage1 не поклав)
            cp = float(stats.get("current_price", 0) or 0)
            if cp <= 0:
                return {
                    "error": "no_price",
                    "symbol": symbol,
                    "recommendation": "AVOID",
                    "market_context": {"scenario": "INVALID_DATA"},
                    "narrative": "Відсутня поточна ціна",
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
                    "symbol": symbol,
                    "stats": stats,
                    "trigger_reasons": triggers,
                }
            )

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

            ctx = result.get("market_context", {}) or {}
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
            result["market_context"] = ctx  # поклали назад

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

            # 4) Лог короткого підсумку (сумісний)
            conf = result.get("confidence_metrics", {}) or {}
            risk = result.get("risk_parameters", {}) or {}
            tp_str = (
                ",".join(f"{tp:.6f}" for tp in (risk.get("tp_targets") or [])[:3])
                if risk
                else ""
            )
            sl_str = (
                f"{float(risk.get('sl_level')):.6f}"
                if risk and risk.get("sl_level") is not None
                else "nan"
            )
            rr_str = (
                f"{float(risk.get('risk_reward_ratio')):.2f}"
                if risk and risk.get("risk_reward_ratio") is not None
                else "nan"
            )
            logger.info(
                "[REC] %s scenario=%s composite=%.3f reco=%s tp=%s sl=%s rr=%s",
                symbol,
                ctx.get("scenario"),
                float(conf.get("composite_confidence", 0.0)),
                result.get("recommendation"),
                tp_str,
                sl_str,
                rr_str,
            )

            # 5) Додаткові технічні поля як і раніше
            result.update(
                {
                    "symbol": symbol,
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "processing_time": datetime.utcnow().isoformat(),
                }
            )
            return result

        except (
            Exception
        ) as e:  # broad except: фінальний бар'єр Stage2, гарантуємо повернення без падіння пайплайну
            logger.exception("Stage2Processor failure: %s", e)
            return {
                "error": "SYSTEM_FAILURE",
                "symbol": stage1_signal.get("symbol", "UNKNOWN"),
                "recommendation": "AVOID",
                "scenario": "SYSTEM_FAILURE",
                "narrative": "Критична системна помилка",
            }
