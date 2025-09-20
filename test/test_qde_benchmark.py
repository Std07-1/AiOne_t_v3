"""
QDE micro-benchmark: тестує продуктивність QuantumDecisionEngine на великій кількості викликів.

Навіщо цей тест:
- Перевіряє, що середній час одного виклику QDE не перевищує 0.5 мс (p95 < 1.0 мс)
- Дає гарантію, що система витримає навантаження 350+ активів у реальному часі
- Виявляє регресії продуктивності після змін у логіці/структурі QDE

Як запускати:
    pytest -q test/test_qde_benchmark.py -s

    короткий підсумок після edge‑cases:
    pytest -s -vv test_qde_edgecases.py


Як інтерпретувати:
- Якщо avg_ms < 0.5 та p95_ms < 1.0 — все ок
- Якщо перевищено — оптимізуйте hot-path QDE
"""

import gc
import os
import platform
import statistics as st
import sys
from time import perf_counter, perf_counter_ns

from QDE.quantum_decision_engine import QuantumDecisionEngine


# імпорт вашого двигуна
def _engine():
    # фабрика QDE для бенчмарка
    return QuantumDecisionEngine(
        symbol="bench",
        normal_state={"volatility": 0.012, "spread": 0.0008, "correlation": 0.5},
        historical_window=64,
    )


def _p95(values):
    if not values:
        return 0.0
    k = int(0.95 * (len(values) - 1))
    return sorted(values)[k]


def test_qde_micro_benchmark():
    """
    QDE micro-benchmark:
      • ціль: avg < 0.10 ms, p95 < 0.30 ms (на звичайному CPU)
      • вимикаємо логування (див. conftest.py)
      • міряємо батчами, щоб мінімізувати накладні perf_counter_ns
    """
    # 0) стабілізація середовища
    was_enabled = gc.isenabled()
    if was_enabled:
        gc.disable()

    qde = _engine()
    # 1) прогрів (уникаємо cold-start: JIT кеші, гілки, пам’ять)
    s = {
        "current_price": 100.0,
        "volume": 10.0,
        "volume_z": 0.1,
        "rsi": 52.0,
        "adx": 25.0,
        "bid_ask_spread": 0.0009,
        "sentiment_index": 0.0,
        "correlation": 0.5,
    }
    for _ in range(512):
        qde.quantum_decision(s, [])

    # 2) основний бенчмарк — батчами для зменшення накладних вимірювання
    N = 8000
    BATCH = 50  # вимір у блоках по 50 викликів
    times = []  # час ОДНОГО виклику (ms), зібраний на кожному батчі
    mutate = s  # не створюємо нові dict — тільки мутуємо значення

    t0 = perf_counter()
    for i in range(0, N, BATCH):
        # легка псевдо-випадкова варіація, щоб уникати “константних” гілок CPU
        base = i
        t1 = perf_counter_ns()
        for k in range(BATCH):
            j = base + k
            mutate["current_price"] = 100.0 + (j % 17) * 0.01
            mutate["volume_z"] = ((j % 23) - 11) / 10.0
            mutate["bid_ask_spread"] = 0.0008 + ((j % 7) * 1e-5)
            qde.quantum_decision(mutate, [])
        t2 = perf_counter_ns()
        per_call_ms = (t2 - t1) / 1_000_000.0 / BATCH
        times.append(per_call_ms)

    total_ms = (perf_counter() - t0) * 1000.0
    avg_ms = st.mean(times)
    p95_ms = _p95(times)
    p99_ms = _p95(times[int(0.80 * len(times)) :])  # груба оцінка хвоста

    # 3) інформативний друк
    print(
        f"\nQDE benchmark: avg={avg_ms:.3f}ms, p95={p95_ms:.3f}ms, "
        f"p99≈{p99_ms:.3f}ms, total={total_ms:.1f}ms, batches={len(times)}, N={N}\n"
        f"Python {sys.version.split()[0]} | {platform.platform()} | "
        f"Processor: {platform.processor() or platform.machine()}"
    )

    # 4) критерії (з невеликим запасом для різних CPU/середовищ)
    target_avg = float(os.getenv("QDE_BENCH_AVG_MS", "0.12"))
    target_p95 = float(os.getenv("QDE_BENCH_P95_MS", "0.35"))
    assert avg_ms < target_avg, f"Середній час {avg_ms:.3f} ms > {target_avg:.3f} ms"
    assert p95_ms < target_p95, f"P95 {p95_ms:.3f} ms > {target_p95:.3f} ms"

    if was_enabled:
        gc.enable()
