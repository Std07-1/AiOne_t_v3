# UI/ui_consumer_entry.py

import asyncio
import logging
import os
from rich.console import Console
from rich.logging import RichHandler

from UI.ui_consumer import UI_Consumer

# --- Налаштування логування ---
logger = logging.getLogger("ui_consumer_entry")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False


async def main():
    # Додаємо low_atr_threshold як у конструкторі UI_Consumer
    ui = UI_Consumer(
        vol_z_threshold=2.5, low_atr_threshold=0.005  # Додано новий параметр
    )

    logger.info("🚀 Запуск UI Consumer з оптимізованим інтерфейсом...")

    # Використовуємо оптимізовані параметри оновлення
    await ui.redis_consumer(
        # Єдиний спосіб формувати Redis URL (ENV сумісний з app/main.py)
        redis_url=(
            os.getenv("REDIS_URL")
            or f"redis://{os.getenv('REDIS_HOST','localhost')}:{os.getenv('REDIS_PORT','6379')}/0"
        ),
        channel="asset_state_update",
        refresh_rate=0.8,  # Оптимальна частота оновлення
        loading_delay=1.5,  # Скорочений час завантаження
        smooth_delay=0.05,  # Мінімальна затримка для плавності
    )


if __name__ == "__main__":
    asyncio.run(main())
