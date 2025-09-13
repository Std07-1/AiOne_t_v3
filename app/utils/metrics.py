"""Колектор метрик (Counters / Histograms / Gauges) для внутрішнього моніторингу.

Шлях: ``app/utils/metrics.py``

Призначення:
    • Легка in-process альтернативa Prometheus для локальної діагностики
    • Вимірювання часу (контекстні менеджери timer / async_timer)
    • Квантилі / p95 без зовнішніх залежностей
"""

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional


# ──────────────────── Класи для метрик ────────────────────────
@dataclass
class CounterMetric:
    value: int = 0
    labels: Dict[str, str] = None


@dataclass
class HistogramMetric:
    buckets: List[float] = None
    values: List[float] = None
    count: int = 0
    sum: float = 0.0


@dataclass
class GaugeMetric:
    value: float = 0.0
    labels: Dict[str, str] = None


class MetricsCollector:
    """
    Система збору метрик для моніторингу продуктивності.

    Підтримує три типи метрик:
    1. COUNTER: монотонно зростаючий лічильник
    2. HISTOGRAM: розподіл значень по бакетах
    3. GAUGE: миттєве значення
    """

    def __init__(self):
        self._counters = defaultdict(lambda: CounterMetric())
        self._histograms = defaultdict(
            lambda: HistogramMetric(buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60], values=[])
        )
        self._gauges = defaultdict(lambda: GaugeMetric())
        self._lock = asyncio.Lock()
        self._start_time = time.time()

    # ──────────────── Публічний API ────────────────────────────
    def inc(self, name: str, value: int = 1, labels: Optional[Dict] = None) -> None:
        """Збільшує лічильник метрики."""
        key = self._get_key(name, labels)
        self._counters[key].value += value

    def observe(self, name: str, value: float, labels: Optional[Dict] = None) -> None:
        """Додає значення до гістограми."""
        key = self._get_key(name, labels)
        metric = self._histograms[key]

        metric.values.append(value)
        metric.count += 1
        metric.sum += value

        # Автоматичне очищення історії для довгоживучих процесів
        if len(metric.values) > 1000:
            metric.values = metric.values[-500:]

    def gauge(self, name: str, value: float, labels: Optional[Dict] = None) -> None:
        """Встановлює значення датчика."""
        key = self._get_key(name, labels)
        self._gauges[key].value = value

    def timer(self, name: str, labels: Optional[Dict] = None):
        """Контекстний менеджер для вимірювання часу виконання."""
        return TimerContext(self, name, labels)

    async def async_timer(self, name: str, labels: Optional[Dict] = None):
        """Асинхронний контекстний менеджер для вимірювання часу."""
        return AsyncTimerContext(self, name, labels)

    def collect(self) -> Dict[str, Dict]:
        """Повертає всі метрики у форматі, придатному для експорту."""
        return {
            "counters": self._serialize_counters(),
            "histograms": self._serialize_histograms(),
            "gauges": self._serialize_gauges(),
            "uptime": time.time() - self._start_time,
        }

    # ──────────────── Внутрішня логіка ────────────────────────
    @staticmethod
    def _get_key(name: str, labels: Optional[Dict]) -> str:
        """Створює унікальний ключ для метрики з мітками."""
        if not labels:
            return name
        return f"{name}:{','.join(f'{k}={v}' for k, v in sorted(labels.items()))}"

    def _serialize_counters(self) -> Dict[str, int]:
        """Серіалізує лічильники у простий словник."""
        return {key: metric.value for key, metric in self._counters.items()}

    def _serialize_histograms(self) -> Dict[str, Dict]:
        """Серіалізує гістограми з розрахунком квантилів."""
        result = {}
        for key, metric in self._histograms.items():
            if not metric.values:
                continue

            sorted_vals = sorted(metric.values)
            result[key] = {
                "count": metric.count,
                "sum": metric.sum,
                "avg": metric.sum / metric.count if metric.count else 0,
                "min": sorted_vals[0],
                "max": sorted_vals[-1],
                "median": sorted_vals[len(sorted_vals) // 2],
                "p95": sorted_vals[int(len(sorted_vals) * 0.95)],
                "values": sorted_vals[-10:],  # Останні 10 значень
            }
        return result

    def _serialize_gauges(self) -> Dict[str, float]:
        """Серіалізує значення датчиків."""
        return {key: metric.value for key, metric in self._gauges.items()}


class TimerContext:
    """Синхронний контекстний менеджер для вимірювання часу."""

    def __init__(self, collector: MetricsCollector, name: str, labels: Optional[Dict]):
        self.collector = collector
        self.name = name
        self.labels = labels
        self.start_time = None

    def __enter__(self):
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.perf_counter() - self.start_time
        self.collector.observe(self.name, duration, self.labels)


class AsyncTimerContext:
    """Асинхронний контекстний менеджер для вимірювання часу."""

    def __init__(self, collector: MetricsCollector, name: str, labels: Optional[Dict]):
        self.collector = collector
        self.name = name
        self.labels = labels
        self.start_time = None

    async def __aenter__(self):
        self.start_time = time.perf_counter()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        duration = time.perf_counter() - self.start_time
        self.collector.observe(self.name, duration, self.labels)
