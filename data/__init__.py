"""Пакет data: адаптери сховища та фетчери.

Експортуємо OptimizedDataFetcher для тестів пагінації.
"""

from .raw_data import OptimizedDataFetcher  # noqa: F401

__all__ = ["OptimizedDataFetcher"]
