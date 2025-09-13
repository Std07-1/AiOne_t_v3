import logging
import json
from typing import Any, Dict, List
from rich.console import Console
from rich.logging import RichHandler
from stage3.trade_manager import TradeLifecycleManager
from utils.utils import safe_float

# --- Налаштування логування ---
logger = logging.getLogger("app.screening_producer")
logger.setLevel(logging.INFO)  # Змінено на INFO для зменшення шуму
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False

MIN_CONFIDENCE_TRADE = 0.6  # Мінімальна впевненість для відкриття угоди


async def open_trades(
    signals: List[Dict[str, Any]],
    trade_manager: TradeLifecycleManager,
    max_parallel: int,
) -> None:
    """
    Відкриває угоди для найперспективніших сигналів:
    1. Сортує сигнали за впевненістю
    2. Обмежує кількість одночасних угод
    3. Відкриває угоди через TradeLifecycleManager
    """
    if not trade_manager or not signals:
        return

    # Вибір найкращих сигналів
    sorted_signals = sorted(
        [s for s in signals if s.get("validation_passed")],
        key=lambda x: x.get("confidence", 0),
        reverse=True,
    )[:max_parallel]

    for signal in sorted_signals:
        symbol = signal["symbol"]
        confidence = safe_float(signal.get("confidence", 0))

        # Детальне логування причин, чому угода не відкривається
        if confidence < MIN_CONFIDENCE_TRADE:
            logger.info(
                f"⛔️ Не відкриваємо угоду для {symbol}: впевненість {confidence:.3f} < поріг {MIN_CONFIDENCE_TRADE}"
            )
            logger.debug(
                f"Деталі сигналу: {json.dumps(signal, ensure_ascii=False, default=str)}"
            )
            continue

        # Додаткові перевірки (можна розширити)
        if signal.get("signal", "NONE").upper() not in [
            "ALERT",
            "ALERT_BUY",
            "ALERT_SELL",
        ]:
            logger.info(
                f"⛔️ Не відкриваємо угоду для {symbol}: тип сигналу {signal.get('signal')} не є ALERT"
            )
            logger.debug(
                f"Деталі сигналу: {json.dumps(signal, ensure_ascii=False, default=str)}"
            )
            continue

        try:
            # Захист від нульових значень ATR
            atr = safe_float(signal.get("atr"))
            if atr is None or atr < 0.0001:
                atr = 0.01
                logger.warning(
                    f"Коригування ATR для {symbol}: {signal.get('atr')} -> 0.01"
                )

            # Підготовка даних для відкриття угоди
            trade_data = {
                "symbol": symbol,
                "current_price": safe_float(signal.get("current_price")),
                "atr": safe_float(signal.get("atr")),
                "rsi": safe_float(signal.get("rsi")),
                "volume": safe_float(signal.get("volume_mean")),
                "tp": safe_float(signal.get("tp")),
                "sl": safe_float(signal.get("sl")),
                "confidence": confidence,
                "hints": signal.get("hints", []),
                "cluster_factors": signal.get("cluster_factors", []),
                "context_metadata": signal.get("context_metadata", {}),
                "strategy": "stage2_cluster",
            }

            # Відкриття угоди
            await trade_manager.open_trade(trade_data)
            logger.info(
                f"✅ Відкрито угоду для {symbol} (впевненість: {confidence:.2f})"
            )
        except Exception as e:
            logger.error(f"Помилка відкриття угоди для {symbol}: {str(e)}")
