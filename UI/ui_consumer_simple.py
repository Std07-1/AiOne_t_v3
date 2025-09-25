import asyncio
import json
import logging
from datetime import datetime
from typing import Any

import redis.asyncio as redis
from rich.console import Console
from rich.live import Live
from rich.table import Table


class SimpleUIConsumer:
    def __init__(self):
        self.console = Console()
        self.last_data = {}
        self.sequence_num = 0

    async def start(self, redis_url: str = "redis://localhost:6379/0"):
        """Спрощена версія UI, яка читає тільки останній снапшот"""
        redis_client = redis.from_url(redis_url, decode_responses=True)

        table = Table(title="AiOne_t Monitoring - SIMPLE MODE")
        for col in ["Symbol", "Price", "Volume", "RSI", "Signal", "Status"]:
            table.add_column(col)

        with Live(table, refresh_per_second=4) as live:
            while True:
                try:
                    # Читаємо тільки снапшот, не підписуємось на канал
                    snapshot_data = await redis_client.get("asset_state_snapshot")

                    if snapshot_data:
                        data = json.loads(snapshot_data)
                        assets = data.get("assets", [])

                        # Оновлюємо таблицю тільки якщо є нові дані
                        if assets != self.last_data.get("assets"):
                            self.last_data = data
                            self._update_table(live, assets)

                    await asyncio.sleep(0.5)  # Частіше оновлення

                except Exception as e:
                    logging.error(f"Помилка: {e}")
                    await asyncio.sleep(2)

    def _update_table(self, live, assets: list[dict[str, Any]]):
        """Оновлює таблицю один раз при зміні даних"""
        new_table = Table(
            title=f"AiOne_t - {len(assets)} активів | {datetime.now().strftime('%H:%M:%S')}"
        )

        for col in ["Symbol", "Price", "Volume", "RSI", "ATR%", "Signal", "Status"]:
            new_table.add_column(col)

        for asset in assets[:20]:  # Обмежити кількість рядків
            symbol = asset.get("symbol", "N/A")
            price = asset.get("price_str", "-")
            volume = asset.get("volume_str", "-")
            rsi = str(asset.get("rsi", "-"))
            atr = f"{asset.get('atr_pct', 0):.2f}%" if asset.get("atr_pct") else "-"
            signal = asset.get("signal", "NONE")
            status = asset.get("status", "unknown")

            new_table.add_row(symbol, price, volume, rsi, atr, signal, status)

        live.update(new_table)


async def main():
    consumer = SimpleUIConsumer()
    await consumer.start()


if __name__ == "__main__":
    asyncio.run(main())
