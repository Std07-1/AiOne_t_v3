# conftest.py
import logging
import os
import sys
import gc
import pytest


@pytest.fixture(autouse=True, scope="session")
def _silence_everything():
    """
    Глушить RichHandler/stream handlers, опускає рівень до CRITICAL
    та вимикає пропагацію для всіх наших модулів під час тестів.
    """
    # максимально тиха Python’ова конфігурація
    logging.basicConfig(level=logging.CRITICAL)
    for name in list(logging.root.manager.loggerDict.keys()):
        lg = logging.getLogger(name)
        lg.setLevel(logging.CRITICAL)
        lg.propagate = False
        # прибираємо дорогі хендлери (Rich, Stream форматери і т.п.)
        for h in list(lg.handlers):
            lg.removeHandler(h)

    # на всяк випадок — повністю відключити root
    logging.getLogger().disabled = True

    # трошки стабільності бенчмарків
    if gc.isenabled():
        gc.collect()
