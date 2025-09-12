# stage3/trade_manager.py
# -*- coding: utf-8 -*-
"""
Stage3: TradeLifecycleManager — управління життєвим циклом угоди.
Містить модель угоди (Trade), правила оновлення (TradeRule) і асинхронний менеджер.
"""

import asyncio
import logging
import json
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional
from utils.utils_1_2 import safe_float

# --- Логування ---
logger = logging.getLogger("trade_manager")
logger.setLevel(logging.DEBUG)

# ───── Статуси угод ─────
TRADE_STATUS: Dict[str, str] = {
    "OPEN": "open",
    "CLOSED_TP": "closed_tp",
    "CLOSED_SL": "closed_sl",
    "CLOSED_MANUAL": "closed_manual",
    "CLOSED_TIMEOUT": "closed_timeout",
    "CLOSED_BY_SIGNAL": "closed_by_signal",
    "CLOSED_BY_CLUSTER": "closed_by_cluster",
}


def utc_now() -> str:
    """Повертає поточний час в UTC у форматі ISO із суфіксом 'Z'."""
    return datetime.utcnow().isoformat() + "Z"


class Trade:
    """
    Модель торгової угоди.

    Attributes:
        id: Унікальний ідентифікатор.
        symbol: Торговий інструмент.
        entry_price: Ціна входу.
        tp: Take Profit.
        sl: Stop Loss.
        status: Поточний статус.
        open_time: Час відкриття.
        close_time: Час закриття.
        exit_reason: Причина закриття.
        result: Фінальний P&L (%).
        strategy: Ім'я стратегії.
        confidence: Рівень впевненості сигналу.
        indicators: ATR, RSI, Volume на вході.
        updates: Історія подій (open, update, trailing_stop тощо).
        current_price: Остання відома ціна.
        close_price: Ціна закриття.
        predicted_profit: Прогнозований профіт (%) на момент відкриття.
    """

    def __init__(self, signal: Dict[str, Any], strategy: str = "default") -> None:
        # Унікальний ідентифікатор угоди
        self.id: str = f"{signal.get('symbol','?')}_{uuid.uuid4().hex}"
        # Основні атрибути
        self.symbol: str = signal.get("symbol", "")
        self.entry_price: float = safe_float(
            signal.get("current_price"), name="current_price"
        )
        self.tp: float = safe_float(signal.get("tp"), name="tp")
        self.sl: float = safe_float(signal.get("sl"), name="sl")
        self.strategy: str = strategy
        self.confidence: float = safe_float(
            signal.get("confidence", 0.0), name="confidence"
        )
        # Кластерні фактори, знайдені патерни та підтвердження контексту
        self.cluster_factors: List[str] = signal.get("cluster_factors", [])
        self.patterns: List[str] = signal.get("patterns", [])
        self.context_confirmations: List[str] = signal.get("context_confirmations", [])
        # Статус та часові мітки
        self.status: str = TRADE_STATUS["OPEN"]
        self.open_time: str = utc_now()
        self.close_time: Optional[str] = None
        self.exit_reason: Optional[str] = None
        # Ціни та індикатори
        self.current_price: float = self.entry_price
        self.close_price: Optional[float] = None
        self.indicators: Dict[str, float] = {
            "atr": safe_float(signal.get("atr"), name="atr"),
            "rsi": safe_float(signal.get("rsi"), name="rsi"),
            "volume": safe_float(signal.get("volume"), name="volume"),
        }
        # Прогнозований прибуток (%) на момент відкриття
        if self.tp >= self.entry_price:
            self.predicted_profit = (
                (self.tp - self.entry_price) / self.entry_price * 100
            )
        else:
            self.predicted_profit = (
                (self.entry_price - self.tp) / self.entry_price * 100
            )

        # Фінальний P&L (%) — спочатку None, встановиться при закритті
        self.result: Optional[float] = None

        # Історія подій (open, update, trailing_stop тощо)
        self.updates: List[Dict[str, Any]] = []
        self._log_event("open", self._snapshot())
        logger.info(
            "🔔 Відкрито угоду %s: factors=%s patterns=%s conf=%.2f TP=%.4f SL=%.4f",
            self.id,
            self.cluster_factors,
            self.patterns,
            self.confidence,
            self.tp,
            self.sl,
        )

    def _snapshot(self) -> Dict[str, Any]:
        """Поточний зріз стану угоди (для логування)."""
        return {
            "symbol": self.symbol,
            "side": self.side,
            "entry_price": self.entry_price,
            "tp": self.tp,
            "sl": self.sl,
            "status": self.status,
            "open_time": self.open_time,
            "current_price": self.current_price,
            "max_profit": self.max_profit,
            "cluster_factors": self.cluster_factors,
            "patterns": self.patterns,
            "context_confirmations": self.context_confirmations,
        }

    @property
    def side(self) -> str:
        """Напрямок угоди (buy якщо TP>=entry, інакше sell)."""
        return "buy" if self.tp >= self.entry_price else "sell"

    @property
    def max_profit(self) -> float:
        """Максимальний профіт (%) від відкриття до теперішньої ціни."""
        if self.side == "buy":
            return (self.current_price - self.entry_price) / self.entry_price * 100
        return (self.entry_price - self.current_price) / self.entry_price * 100

    def _log_event(self, event: str, data: Dict[str, Any]) -> None:
        """Додає запис в історію подій, фіксує поточний SL/TP."""
        data["sl"] = self.sl  # Фіксуємо поточний SL
        data["tp"] = self.tp  # Фіксуємо поточний TP
        record = {"event": event, "timestamp": utc_now(), **data}
        self.updates.append(record)

    def to_dict(self) -> Dict[str, Any]:
        """Повертає повне представлення угоди для запису в лог."""
        base = self._snapshot()
        base.update(
            {
                "id": self.id,
                "strategy": self.strategy,
                "confidence": self.confidence,
                "predicted_profit": self.predicted_profit,
                "close_time": self.close_time,
                "exit_reason": self.exit_reason,
                "result": self.result,
                "close_price": self.close_price,
                "indicators": self.indicators,
                "updates": self.updates,
            }
        )
        return base


class TradeRule:
    """Інтерфейс правила для оновлення угоди."""

    async def __call__(self, trade: Trade, market: Dict[str, Any]) -> None:
        raise NotImplementedError


class ContextExitRule(TradeRule):
    """Правило закриття при зміні ринкового контексту."""

    async def __call__(self, trade: Trade, market: Dict[str, Any]) -> None:
        # Якщо market містить прапорець контр-тренду → закрити
        if market.get("context_break", False):
            trade.status = TRADE_STATUS["CLOSED_BY_SIGNAL"]
            trade.exit_reason = "context_break"
            trade._log_event("exit_context", {"reason": "context_break"})
            logger.info("❌ Угода %s закрита через контекст (context_break)", trade.id)


class TrailingStopRule(TradeRule):
    """
    Правило перенесення SL у бік ціни входу, тільки після
    значного руху (buffer_size).

    Логіка:
      для buy:
        - якщо price < entry_price + buffer_size → трейл не активується
        - target_sl = price - buffer_size
        - new_sl = max(old_sl, target_sl), але не вище entry_price
      для sell:
        - якщо price > entry_price - buffer_size → трейл не активується
        - target_sl = price + buffer_size
        - new_sl = min(old_sl, target_sl), але не нижче entry_price
    """

    def __init__(self, atr_buffer: float = 0.3) -> None:
        """
        Args:
            atr_buffer: множник від ATR, який визначає
                        дистанцію перед першим трейлом
        """
        self.atr_buffer = atr_buffer
        self.logger = logging.getLogger(f"{__name__}.TrailingStopRule")
        logger.setLevel(logging.DEBUG)

    async def __call__(self, trade: Trade, market: Dict[str, Any]) -> None:
        # Ігноруємо неактивні угоди
        if trade.status != TRADE_STATUS["OPEN"]:
            return

        # Забираємо дані
        price = safe_float(market.get("price"), name="price")
        atr = trade.indicators.get("atr", 0.0)
        if atr <= 0:
            return

        buffer_size = atr * self.atr_buffer
        old_sl = trade.sl

        if trade.side == "buy":
            # Старт трейла — тільки після price ≥ entry_price + buffer_size
            if price < trade.entry_price + buffer_size:
                return
            target_sl = price - buffer_size
            # Переміщаємо стоп вгору, але не вище entry_price
            new_sl = max(old_sl, target_sl)
            new_sl = min(new_sl, trade.entry_price)

        else:  # sell
            if price > trade.entry_price - buffer_size:
                return
            target_sl = price + buffer_size
            # Переміщаємо стоп вниз, але не нижче entry_price
            new_sl = min(old_sl, target_sl)
            new_sl = max(new_sl, trade.entry_price)

        # Якщо SL змінився — лог і подія
        if new_sl != old_sl:
            trade.sl = new_sl
            trade._log_event(
                "trailing_stop",
                {"old_sl": old_sl, "new_sl": new_sl, "timestamp": utc_now()},
            )
            self.logger.debug(
                "🛡 TRAIL-STOP %s: %.6f → %.6f (price=%.6f, atr=%.6f, buffer=%.6f)",
                trade.id,
                old_sl,
                new_sl,
                price,
                atr,
                buffer_size,
            )


class EarlyExitRule(TradeRule):
    """Правило дострокового закриття за зворотною зміною обсягу/RSI."""

    async def __call__(self, trade: Trade, market: Dict[str, Any]) -> None:
        vol = safe_float(market.get("volume"), name="volume")
        rsi = safe_float(market.get("rsi"), name="rsi")
        if trade.side == "buy" and vol < trade.indicators["volume"] * 0.7 and rsi < 50:
            trade.status = TRADE_STATUS["CLOSED_BY_SIGNAL"]
            trade.exit_reason = "early_exit"
            trade._log_event("early_exit", {"vol": vol, "rsi": rsi})
            logger.info("🔻 Угода %s early exit (vol drop & RSI)", trade.id)
        if trade.side == "sell" and vol < trade.indicators["volume"] * 0.7 and rsi > 50:
            trade.status = TRADE_STATUS["CLOSED_BY_SIGNAL"]
            trade.exit_reason = "early_exit"
            trade._log_event("early_exit", {"vol": vol, "rsi": rsi})
            logger.info("🔺 Угода %s early exit (vol drop & RSI)", trade.id)


class TradeLifecycleManager:
    """
    Асинхронний менеджер життєвого циклу угоди.

    Використовує asyncio.Lock для потокобезпечності,
    cooldown для повторного відкриття
    та збирає підсумкову статистику кожної угоди.
    """

    def __init__(
        self,
        log_file: str = "trade_log.jsonl",
        summary_file: str = "summary_log.jsonl",
        reopen_cooldown: float = 60.0,  # секунди
        max_parallel_trades: int = 3,  # максимальна кількість одночасних угод
    ) -> None:
        self.active_trades: Dict[str, Trade] = {}
        self.closed_trades: List[Dict[str, Any]] = []
        self.reopen_cooldown = reopen_cooldown
        self.max_parallel_trades = max_parallel_trades
        self.recently_closed: Dict[str, str] = {}  # symbol → ISO close_time
        self.log_file = log_file
        self.summary_file = summary_file
        # Оновлені правила включають контекстний вихід
        self.rules: List[TradeRule] = [
            ContextExitRule(),
            TrailingStopRule(),
            EarlyExitRule(),
        ]
        self.lock = asyncio.Lock()

    async def open_trade(
        self, signal: Dict[str, Any], strategy: str = "default"
    ) -> Optional[str]:
        """
        Відкриває угоду, якщо для символа нема open-угоди
        і якщо не в cooldown після останнього закриття.
        Додає обмеження на кількість одночасних угод.

        Returns:
            id відкритої або існуючої угоди, або None якщо пропущено.
        """
        async with self.lock:
            sym = signal["symbol"]
            logger.debug("Спроба відкриття угоди для %s зі сигналом: %s", sym, signal)

            # 0) обмеження на кількість одночасних угод
            if len(self.active_trades) >= self.max_parallel_trades:
                logger.info(
                    "SKIP OPEN ❌ %s: досягнуто ліміту одночасних угод (%d)",
                    sym,
                    self.max_parallel_trades,
                )
                return None

            # 1) cooldown після закриття
            last = self.recently_closed.get(sym)
            if last:
                t0 = datetime.fromisoformat(last.rstrip("Z"))
                if (datetime.utcnow() - t0).total_seconds() < self.reopen_cooldown:
                    logger.info("SKIP OPEN ❌ %s: в cooldown (закрита %s)", sym, last)
                    return None

            # 2) якщо вже є open-угода — не відкриваємо нову
            for tr in self.active_trades.values():
                if tr.symbol == sym and tr.status == TRADE_STATUS["OPEN"]:
                    logger.info(
                        "SKIP OPEN ❌ %s: вже має відкриту угоду id=%s",
                        sym,
                        tr.id,
                    )
                    return tr.id

            # 3) інакше відкриваємо
            trade = Trade(signal, strategy)
            self.active_trades[trade.id] = trade

            # Лог файлу
            await self._persist(self.log_file, trade.to_dict())

            logger.info(
                "OPENED ✅ %s: id=%s, entry_price=%.6f, tp=%.6f, sl=%.6f",
                sym,
                trade.id,
                trade.entry_price,
                trade.tp,
                trade.sl,
            )
            logger.debug("OPEN DETAIL ▶ %s", trade.to_dict())

            return trade.id

    async def update_trade(self, trade_id: str, market: Dict[str, Any]) -> bool:
        """
        Оновлює стан угоди: індикатори, правила, TP/SL, timeout.

        Returns:
            True якщо угода закрилася в цьому оновленні.
        """
        async with self.lock:
            tr = self.active_trades.get(trade_id)
            if not tr or tr.status != TRADE_STATUS["OPEN"]:
                logger.debug(
                    "UPDATE SKIP 🔄 %s: не знайдено відкриту угоду або status≠OPEN",
                    trade_id,
                )
                return False

            tr.current_price = safe_float(market.get("price"), name="price")
            logger.debug(
                "UPDATE ► %s: нова поточна ціна = %.6f",
                trade_id,
                tr.current_price,
            )

            # застосування кожного правила
            for rule in self.rules:
                logger.debug(
                    "Застосовуємо правило %s до %s", rule.__class__.__name__, trade_id
                )
                await rule(tr, market)

            closed = await self._check_exit(tr)
            logger.debug(
                "RESULT ▶ %s: status=%s, closed=%s",
                trade_id,
                tr.status,
                closed,
            )

            # лог події
            tr._log_event("update", {"price": tr.current_price, "status": tr.status})
            await self._persist(self.log_file, tr.to_dict())
            logger.debug("UPDATED ► %s", tr.to_dict())

            if closed:
                # прибираємо з активних
                self.active_trades.pop(trade_id, None)
                logger.info(
                    "TRADE CLOSED ✅ %s: причина='%s'", trade_id, tr.exit_reason
                )

            return closed

    async def close_trade(self, trade_id: str, price: float, reason: str) -> None:
        """
        Ручне закриття угоди.

        Args:
            price: Ціна закриття.
            reason: Причина закриття.
        """
        async with self.lock:
            tr = self.active_trades.pop(trade_id, None)
            if not tr:
                logger.warning("CLOSE SKIP ⚠️ %s: угода не знайдена", trade_id)
                return

            tr.status = TRADE_STATUS["CLOSED_MANUAL"]
            tr.exit_reason = reason
            tr.close_price = price
            tr.close_time = utc_now()
            tr.result = TradeLifecycleManager.calculate_profit(tr, price)

            # зберігаємо час для cooldown
            self.recently_closed[tr.symbol] = tr.close_time

            logger.info(
                "CLOSE ◀ %s: price=%.6f, reason=%s, result=%.2f%%",
                trade_id,
                price,
                reason,
                tr.result,
            )
            logger.debug("CLOSE DETAIL ◀ %s", tr.to_dict())

            # запис повного логу
            await self._persist(self.log_file, tr.to_dict())

            # запис summary
            summary = self._make_summary(tr)
            await self._persist(self.summary_file, summary)
            logger.info("SUMMARY ✍️ %s", summary)

    async def _check_exit(self, tr: Trade) -> bool:
        """
        Перевіряє TP/SL та інші автоматичні статуси,
        застосовує timeout і записує summary при закритті.
        """
        p = tr.current_price
        now = datetime.utcnow()

        # TP
        if tr.side == "buy" and p >= tr.tp:
            tr.status, tr.exit_reason = TRADE_STATUS["CLOSED_TP"], "TP"
        elif tr.side == "sell" and p <= tr.tp:
            tr.status, tr.exit_reason = TRADE_STATUS["CLOSED_TP"], "TP"
        # SL
        elif tr.side == "buy" and p <= tr.sl:
            tr.status, tr.exit_reason = TRADE_STATUS["CLOSED_SL"], "SL"
        elif tr.side == "sell" and p >= tr.sl:
            tr.status, tr.exit_reason = TRADE_STATUS["CLOSED_SL"], "SL"
        # timeout
        elif (now - datetime.fromisoformat(tr.open_time.rstrip("Z"))).seconds > 3600:
            tr.status, tr.exit_reason = TRADE_STATUS["CLOSED_TIMEOUT"], "timeout"
        else:
            if tr.status != TRADE_STATUS["OPEN"]:
                # правило змінило статус вручну
                pass
            else:
                return False

        # заповнюємо поля закриття
        tr.close_price = p
        tr.close_time = utc_now()
        tr.result = TradeLifecycleManager.calculate_profit(tr, p)
        # cooldown
        self.recently_closed[tr.symbol] = tr.close_time

        # запис full-detail
        await self._persist(self.log_file, tr.to_dict())
        # запис summary
        await self._persist(self.summary_file, self._make_summary(tr))

        # додаємо в closed_trades для внутрішнього зберігання
        self.closed_trades.append(tr.to_dict())
        return True

    @staticmethod
    def calculate_profit(tr: Trade, price: float) -> float:
        """Profit (%) для buy/sell."""
        if tr.side == "buy":
            return (price - tr.entry_price) / tr.entry_price * 100
        return (tr.entry_price - price) / tr.entry_price * 100

    def _make_summary(self, tr: Trade) -> Dict[str, Any]:
        """
        Формує підсумковий запис для summary_log.jsonl
        """
        return {
            "id": tr.id,
            "symbol": tr.symbol,
            "strategy": tr.strategy,
            "confidence": tr.confidence,
            "open_time": tr.open_time,
            "entry_price": tr.entry_price,
            "predicted_profit": tr.predicted_profit,
            "close_time": tr.close_time,
            "close_price": tr.close_price,
            "exit_reason": tr.exit_reason,
            "realized_profit": tr.result,
            "events_count": len(tr.updates),
        }

    async def _persist(self, file_path: str, data: Dict[str, Any]) -> None:
        """Асинхронно записує JSONL у вказаний файл."""
        loop = asyncio.get_event_loop()
        line = json.dumps(data, ensure_ascii=False) + "\n"
        await loop.run_in_executor(None, self._write_sync, file_path, line)

    def _write_sync(self, file_path: str, line: str) -> None:
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(line)

    async def get_active_trades(self) -> List[Dict[str, Any]]:
        """Повертає копію активних угод."""
        async with self.lock:
            return [tr.to_dict() for tr in self.active_trades.values()]

    async def get_closed_trades(self) -> List[Dict[str, Any]]:
        """Повертає копію закритих угод."""
        async with self.lock:
            return list(self.closed_trades)


class EnhancedContextAwareTradeManager(TradeLifecycleManager):
    def __init__(self, context_engine, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.context_engine = context_engine
        # Додаткові параметри для керування чутливістю
        self.volatility_threshold = 0.005
        self.phase_change_threshold = 0.5

    async def manage_active_trades(self):
        """Періодична перевірка активних угод з урахуванням контексту"""
        while True:
            for trade_id in list(self.active_trades.keys()):
                trade = self.active_trades[trade_id]
                try:
                    # Отримання контексту з обробкою помилок
                    context = await self.context_engine.evaluate_context(trade.symbol)

                    # Перевірка зміни контексту
                    if self.has_context_changed_significantly(trade, context):
                        await self.close_trade(
                            trade_id, trade.current_price, "context_change"
                        )
                        continue

                    # Адаптація параметрів угоди
                    self.adapt_trade_parameters(trade, context)

                    # Оновлення ринковими даними
                    market_data = self.get_market_data(trade.symbol)
                    await self.update_trade(trade_id, market_data)

                except Exception as e:
                    logger.error(f"Error managing trade {trade_id}: {e}")

            await asyncio.sleep(60)

    def has_context_changed_significantly(self, trade, new_context: dict) -> bool:
        """Визначає чи зміна контексту вимагає закриття угоди"""
        old_context = getattr(trade, "context", {})
        old_phase = old_context.get("market_phase", "")
        new_phase = new_context["market_phase"]

        # Критичні зміни між протилежними станами
        critical_changes = {
            ("strong_uptrend", "strong_downtrend"),
            ("strong_downtrend", "strong_uptrend"),
            ("accumulation_phase", "distribution"),
            ("volatility_compression", "volatility_expansion"),
            ("price_compression", "price_expansion"),
        }

        # Перевірка критичних переходів
        if (old_phase, new_phase) in critical_changes:
            return True

        # Перевірка зсуву ключових рівнів
        old_levels = set(old_context.get("key_levels", []))
        new_levels = set(new_context["key_levels"])

        if old_levels and new_levels:
            # Розрахунок середньої зміни рівнів
            avg_change = sum(
                abs(new - old)
                for new, old in zip(sorted(new_levels), sorted(old_levels))
            ) / len(old_levels)

            if avg_change / trade.entry_price > 0.03:
                return True

        # Перевірка різкої зміни волатильності
        old_volatility = old_context.get("volatility", 0)
        new_volatility = new_context["volatility"]
        if abs(new_volatility - old_volatility) > self.volatility_threshold * 3:
            return True

        return False

    def adapt_trade_parameters(self, trade, context: dict):
        """Адаптація параметрів угоди до нового контексту"""
        new_volatility = context["volatility"]
        old_context = getattr(trade, "context", {})
        old_volatility = old_context.get("volatility", 0)
        phase = context["market_phase"]

        # Корекція тільки при значній зміні волатильності
        if abs(new_volatility - old_volatility) > self.volatility_threshold:
            # Розраховуємо коефіцієнт коригування
            volatility_ratio = (
                new_volatility / old_volatility if old_volatility > 0 else 1.0
            )

            # Для трендових станів - більш агресивна корекція
            if "trend" in phase:
                tp_adjust = volatility_ratio**0.8
                sl_adjust = volatility_ratio**1.2
            # Для консолідації - консервативна корекція
            else:
                tp_adjust = volatility_ratio**0.5
                sl_adjust = volatility_ratio**0.8

            # Застосовуємо корекцію до TP/SL
            trade.tp = trade.entry_price + (trade.tp - trade.entry_price) * tp_adjust
            trade.sl = trade.entry_price - (trade.entry_price - trade.sl) * sl_adjust

            trade._log_event(
                "parameters_adjusted",
                {
                    "reason": "volatility_change",
                    "new_volatility": new_volatility,
                    "old_volatility": old_volatility,
                    "tp_adjust": tp_adjust,
                    "sl_adjust": sl_adjust,
                },
            )

        # Оновлення контексту в угоді
        trade.context = {
            "market_phase": phase,
            "key_levels": context["key_levels"],
            "volatility": new_volatility,
            "cluster_indicators": context["cluster_indicators"],
            "sentiment": context.get("sentiment", 0),
            "timestamp": datetime.utcnow().isoformat(),
        }

        # Додаткова корекція для стиснених ринків
        if "compression" in phase:
            # Зменшуємо TP та розширюємо SL для більш консервативної стратегії
            trade.tp = trade.entry_price + (trade.tp - trade.entry_price) * 0.8
            trade.sl = trade.entry_price - (trade.entry_price - trade.sl) * 1.2
            trade._log_event(
                "compression_adjust",
                {
                    "reason": "market_compression",
                    "new_tp": trade.tp,
                    "new_sl": trade.sl,
                },
            )

    def get_market_data(self, symbol: str) -> dict:
        """Покращене отримання ринкових даних з реального контексту/буфера/біржі"""
        # Спробуємо отримати останній бар з context_engine (якщо є метод)
        try:
            # Припускаємо, що context_engine має метод get_last_bar або load_data
            if hasattr(self.context_engine, "get_last_bar"):
                bar = self.context_engine.get_last_bar(symbol)
            else:
                # Fallback: load_data повертає DataFrame
                df = asyncio.get_event_loop().run_until_complete(
                    self.context_engine.load_data(symbol, "1m")
                )
                bar = df.iloc[-1] if not df.empty else {}
        except Exception as e:
            logger.error(
                f"[TradeManager] Не вдалося отримати ринкові дані для {symbol}: {e}"
            )
            bar = {}
        return {
            "price": float(bar.get("close", 0.0)),
            "volume": float(bar.get("volume", 0.0)),
            "rsi": float(bar.get("rsi", 0.0)),
            "bid_ask_spread": (
                float(bar.get("ask", 0.0)) - float(bar.get("bid", 0.0))
                if bar.get("ask") and bar.get("bid")
                else 0.0
            ),
        }

    def get_current_price(self, symbol: str) -> float:
        """Отримання поточної ціни з context_engine (остання ціна close)"""
        try:
            df = self.context_engine.load_data(symbol)
            if df is not None and not df.empty:
                return float(df.iloc[-1]["close"])
        except Exception as e:
            logger.error(f"get_current_price error for {symbol}: {e}")
        return 0.0

    def get_current_volume(self, symbol: str) -> float:
        """Отримання поточного обсягу з context_engine (остній bar volume)"""
        try:
            df = self.context_engine.load_data(symbol)
            if df is not None and not df.empty:
                return float(df.iloc[-1]["volume"])
        except Exception as e:
            logger.error(f"get_current_volume error for {symbol}: {e}")
        return 0.0

    def get_current_rsi(self, symbol: str) -> float:
        """Отримання поточного RSI з context_engine (остній bar rsi)"""
        try:
            df = self.context_engine.load_data(symbol)
            if df is not None and not df.empty and "rsi" in df.columns:
                return float(df.iloc[-1]["rsi"])
        except Exception as e:
            logger.error(f"get_current_rsi error for {symbol}: {e}")
        return 0.0

    def get_bid_ask_spread(self, symbol: str) -> float:
        """Отримання спреду з context_engine (bid/ask якщо є, інакше 0)"""
        try:
            df = self.context_engine.load_data(symbol)
            if (
                df is not None
                and not df.empty
                and "bid" in df.columns
                and "ask" in df.columns
            ):
                bid = float(df.iloc[-1]["bid"])
                ask = float(df.iloc[-1]["ask"])
                return abs(ask - bid)
        except Exception as e:
            logger.error(f"get_bid_ask_spread error for {symbol}: {e}")
        return 0.0
