import asyncio
import logging
import os

from rich.console import Console
from rich.logging import RichHandler

from config.config import REDIS_CHANNEL_ASSET_STATE  # SIMPLE_UI_MODE fallback
from UI.ui_consumer import UIConsumer
from UI.ui_consumer_simple import SimpleUIConsumer

# ── Налаштування логування ─────────────────────────────────────────────────
logger = logging.getLogger("ui_consumer_entry")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False


async def main():
    # Додаємо low_atr_threshold як у конструкторі UI_Consumer
    # Отримуємо SIMPLE_UI_MODE динамічно (fallback False для сумісності зі старими версіями)
    try:
        from config import config as _cfg  # локальний імпорт щоб уникнути циклів

        simple_mode = bool(getattr(_cfg, "SIMPLE_UI_MODE", False))
    except Exception:
        simple_mode = False

    if simple_mode:
        # Експериментальний простий режим (snapshot only)
        logger.info("🚀 Запуск SimpleUIConsumer (snapshot-only режим)...")
        simple = SimpleUIConsumer()

        logger.info(
            "Коротке пояснення: \n"
            "Blocks: lowvol|htf|lowconf|OK = A|B|C|D \n"
            "A = blocked_alerts_lowvol (накопичено)\n"
            "B = blocked_alerts_htf\n"
            "C = blocked_alerts_lowconf\n"
            "D = passed_alerts (ALERT, що дійшли без даунгрейду)\n"
            "Downgraded: загальна кількість випадків, коли первинна рекомендація була змінена.\n"
            "Gen: кумулятивно скільки разів Stage2 реально отримав пакет alert_signals (скільки сигналів оброблено)."
            "Skip: скільки циклів без жодного Stage1 ALERT."
        )
        await simple.start(
            redis_url=(
                os.getenv("REDIS_URL")
                or f"redis://{os.getenv('REDIS_HOST','localhost')}:{os.getenv('REDIS_PORT','6379')}/0"
            )
        )
    else:
        ui = UIConsumer(vol_z_threshold=2.5, low_atr_threshold=0.005)
        logger.info("🚀 Запуск UI Consumer з оптимізованим інтерфейсом...")
        await ui.redis_consumer(
            redis_url=(
                os.getenv("REDIS_URL")
                or f"redis://{os.getenv('REDIS_HOST','localhost')}:{os.getenv('REDIS_PORT','6379')}/0"
            ),
            channel=REDIS_CHANNEL_ASSET_STATE,
            refresh_rate=0.8,
            loading_delay=1.5,
            smooth_delay=0.05,
        )


if __name__ == "__main__":
    asyncio.run(main())
