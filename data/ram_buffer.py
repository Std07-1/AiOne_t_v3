import logging
from collections import defaultdict, deque

from rich.console import Console
from rich.logging import RichHandler

logger = logging.getLogger("ram_buffer")
log_console = Console(stderr=True)  # тепер логування піде в stderr
rich_handler = RichHandler(console=log_console, level="DEBUG")
logging.getLogger("ram_buffer").addHandler(rich_handler)


class RAMBuffer:
    """
    In-memory буфер для зберігання останніх барів для кожного символу/таймфрейму.
    Застосування: швидкий доступ для stage1 моніторингу без Redis.

    Особливість: один bar на кожен timestamp (оновлення partial‑бару inplace).
    """

    def __init__(self, max_bars: int = 120):
        self.data = defaultdict(lambda: defaultdict(lambda: deque(maxlen=max_bars)))
        self.max_bars = max_bars
        logger.info("RAMBuffer ініціалізовано (max_bars=%d)", max_bars)

    def add(self, symbol: str, timeframe: str, bar: dict):
        """
        Додає або оновлює бар у RAMBuffer:
        - Якщо bar з тим же timestamp вже є — оновлює inplace (partial‑update).
        - Якщо новий timestamp — додає новий бар у tail.
        """
        symbol = symbol.lower()
        tf_deque = self.data[symbol][timeframe]
        if tf_deque and tf_deque[-1].get("timestamp") == bar.get("timestamp"):
            # Оновлюємо останній (partial) бар inplace
            tf_deque[-1].update(bar)
            logger.debug(
                "[RAMBuffer] Partial update для %s/%s (ts=%s)",
                symbol,
                timeframe,
                bar.get("timestamp"),
            )
        else:
            prev_count = len(tf_deque)
            tf_deque.append(bar)
            logger.debug(
                "[RAMBuffer] Додано новий бар для %s/%s (було=%d → стало=%d), ts=%s",
                symbol,
                timeframe,
                prev_count,
                len(tf_deque),
                bar.get("timestamp"),
            )

    def get(self, symbol: str, timeframe: str, count: int) -> list:
        symbol = symbol.lower()
        bars = list(self.data[symbol][timeframe])[-count:]
        logger.debug(
            "[RAMBuffer] Витягнуто %d/%d бар(ів) для %s/%s (загалом=%d)",
            len(bars),
            count,
            symbol,
            timeframe,
            len(self.data[symbol][timeframe]),
        )
        return bars

    def stats(self) -> dict:
        """
        Повертає зведення: скільки символів, скільки барів по кожному symbol/tf.
        """
        stat = {
            "symbols": len(self.data),
            "by_symbol": {
                sym: {tf: len(self.data[sym][tf]) for tf in self.data[sym]}
                for sym in self.data
            },
        }
        logger.info(
            "[RAMBuffer] Статистика: %d символів, %s",
            stat["symbols"],
            {k: sum(v.values()) for k, v in stat["by_symbol"].items()},
        )
        return stat

    def symbols_with_min_bars(self, timeframe: str, min_bars: int) -> list:
        """
        Повертає список symbol, у яких у заданому tf є не менше min_bars барів.
        """
        result = [
            sym for sym in self.data if len(self.data[sym][timeframe]) >= min_bars
        ]
        logger.info(
            "[RAMBuffer] %d символів мають ≥%d бар(ів) у %s",
            len(result),
            min_bars,
            timeframe,
        )
        return result
