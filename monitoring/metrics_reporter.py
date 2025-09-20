"""Одноразовий (one‑shot) репортер метрик.

Генерує знімок (Markdown / Text / JSON) поточного стану:
    1. Garbage Collection – зібрані об'єкти та цикли
    2. Python Runtime – версія інтерпретатора
    3. DataStore Latency – квантилі p50/p90/p99 get/put
    4. Cache Efficiency – hit ratio, RAM usage
    5. Trade Updater – drift / pressure / α / лічильники аномалій
    6. System – CPU%, RSS, uptime
    7. Stage1 Feed – lag / missing bars (data freshness Stage1)
    8. Stage2 Processor – latency / errors / reco дистрибуція
    9. Health – «свіжість» оновлення Stage3 (пороги налаштовуються)
   10. Observations – евристичні попередження
   11. Conclusions – структуровані підсумки та поради

Приклад:
        python -m monitoring.metrics_reporter \
                --prom-url http://localhost:9109/metrics \
                --redis-url redis://localhost:6379/0 \
                --format markdown --explain

Ключі:
    --format markdown|text|json
    --fresh-sec / --late-sec / --stale-sec – пороги віковості
    --explain – додати короткі пояснення до секцій (markdown/text)

Приклади використання:
    python -m monitoring.metrics_reporter --format markdown --explain
    python -m monitoring.metrics_reporter \
        --format text --fresh-sec 90 --late-sec 300 \
        --stale-sec 1800 --explain
    python -m monitoring.metrics_reporter --format json

JSON режим повертає «сирі» дані + блок conclusions (машиночитний).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import aiohttp
import psutil
import redis.asyncio as redis


# ── Data Structures ──────────────────────────────────────────────────────────
@dataclass
class Histogram:
    buckets: list[tuple[float, float]]  # (le, cumulative_count)
    count: float
    summation: float

    def quantile(self, q: float) -> float | None:
        if not self.buckets or self.count <= 0:
            return None
        target = q * self.count
        prev_le = 0.0
        prev_count = 0.0
        for le, c in self.buckets:
            if c >= target:
                # linear interpolate inside bucket (rough)
                bucket_count = c - prev_count
                if bucket_count <= 0:
                    return le
                ratio_in_bucket = (target - prev_count) / bucket_count
                # assume uniform distribution; approximate bucket lower bound
                lower = prev_le
                upper = le
                return lower + (upper - lower) * ratio_in_bucket
            prev_le = le
            prev_count = c
        return self.buckets[-1][0]


@dataclass
class MetricsSnapshot:
    """Застарілий контейнер-заготовка (не використовується).

    Залишено для сумісності на випадок імпортів у зовнішньому коді;
    функціонально не застосовується в модулі. Може бути видалений у
    майбутніх версіях після повної перевірки використань.
    """

    raw: dict[str, Any] = field(default_factory=dict)


# ── Prometheus Parsing ───────────────────────────────────────────────────────
METRIC_LINE_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?P<labels>\{.*?\})?\s+(?P<value>[-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?)"
)
LABEL_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')


def parse_prometheus_text(text: str) -> dict[str, list[dict[str, Any]]]:
    metrics: dict[str, list[dict[str, Any]]] = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        m = METRIC_LINE_RE.match(line.strip())
        if not m:
            continue
        name = m.group("name")
        value = float(m.group("value"))
        labels_text = m.group("labels")
        labels: dict[str, str] = {}
        if labels_text:
            for lm in LABEL_RE.finditer(labels_text):
                labels[lm.group(1)] = lm.group(2)
        metrics.setdefault(name, []).append({"labels": labels, "value": value})
    return metrics


# ── Histogram Reconstruction ─────────────────────────────────────────────────


def build_histogram(
    metrics: dict[str, list[dict[str, Any]]], base_name: str
) -> Histogram | None:
    bucket_name = base_name + "_bucket"
    count_name = base_name + "_count"
    sum_name = base_name + "_sum"
    if bucket_name not in metrics:
        return None
    buckets_raw = metrics[bucket_name]
    # we ignore labels variations other than 'le'
    buckets: list[tuple[float, float]] = []
    for item in buckets_raw:
        le_raw = item["labels"].get("le")
        if le_raw is None:
            continue
        try:
            le = float("inf") if le_raw == "+Inf" else float(le_raw)
        except ValueError:
            continue
        buckets.append((le, item["value"]))
    buckets.sort(key=lambda x: x[0])
    count = metrics.get(count_name, [{"value": 0.0}])[0]["value"]
    summation = metrics.get(sum_name, [{"value": 0.0}])[0]["value"]
    return Histogram(buckets=buckets, count=count, summation=summation)


# ── Extraction Helpers ──────────────────────────────────────────────────────


def first_val(metrics: dict[str, list[dict[str, Any]]], name: str) -> float | None:
    arr = metrics.get(name)
    if not arr:
        return None
    try:
        return float(arr[0]["value"])
    except Exception:
        return None


## Примітка: раніше існувала утиліта sum_by_label(...), але вона не використовувалась
## і створювала шум для лінтера. Видалено без зміни логіки.


# ── Report Builders ─────────────────────────────────────────────────────────


def fmt_bytes(num: float) -> str:
    if num < 1024:
        return f"{num:.0f} B"
    for unit in ["KB", "MB", "GB", "TB"]:
        num /= 1024
        if num < 1024:
            return f"{num:.1f} {unit}"
    return f"{num:.1f} PB"


def build_observations(sections: dict[str, Any]) -> list[str]:
    obs: list[str] = []
    trade = sections.get("trade_updater", {})
    cache = sections.get("cache", {})
    latency = sections.get("latency", {})
    gc = sections.get("gc", {})
    health = sections.get("health", {})
    system = sections.get("system", {})
    stage1 = sections.get("stage1", {})
    stage2 = sections.get("stage2", {})
    deltas = sections.get("deltas", {})

    def tag(level: str, msg: str) -> str:
        return f"[{level}] {msg}"

    # Heuristics summary (maintenance note):
    # - Drift WARN suppressed when no trades (idle) to avoid noise
    # - Pressure percent thresholds: >120% WARN, >95% INFO
    # - Stage2 error rate: WARN >5%, INFO >0%
    # - Stage2 latency: WARN p99>0.08s, INFO p99>0.04s (min sample size 30)
    # Extend carefully: keep messages concise & tagged.

    # Drift warning (приглушуємо якщо повний idle без угод: active==0 & closed==0)
    if trade.get("consecutive_drift_high", 0) >= 3 and (
        trade.get("active", 0) or trade.get("closed", 0)
    ):
        obs.append(
            tag(
                "WARN",
                (
                    f"Drift high {trade['consecutive_drift_high']} cycles – "
                    f"investigate blocking I/O or heavy compute."
                ),
            )
        )
    if trade.get("pressure", 0) > 2.0:
        obs.append(
            tag(
                "WARN",
                (
                    f"High pressure {trade['pressure']:.2f} – "
                    f"consider interval scaling or symbol throttling."
                ),
            )
        )
        # Sustained pressure warning (аналог дрейфу, тільки для навантаження)
        if trade.get("consecutive_pressure_high", 0) >= 3 and (
            trade.get("active", 0) or trade.get("closed", 0)
        ):
            obs.append(
                tag(
                    "WARN",
                    (
                        f"Pressure high sustained "
                        f"{int(trade['consecutive_pressure_high'])} cycles – "
                        f"consider interval scaling or throttling."
                    ),
                )
            )
    pt = trade.get("pressure_percent")
    if pt is not None:
        if pt > 120:
            obs.append(tag("WARN", f"Pressure {pt:.0f}% of threshold – severe load"))
        elif pt > 95:
            obs.append(tag("INFO", f"Pressure {pt:.0f}% of threshold – nearing limit"))
    if cache.get("redis_hit_ratio") == 0 and cache.get("ram_hit_ratio", 0) > 0.8:
        obs.append(
            tag(
                "INFO",
                "Redis unused – verify if intended (cost saving) or misconfiguration.",
            )
        )
    if latency.get("get_p99", 0) and latency.get("get_p99", 0) > 2.0:
        obs.append(tag("WARN", "Slow 99th percentile read latency."))
    if gc.get("uncollectable_total", 0) > 0:
        obs.append(
            tag(
                "WARN",
                "Uncollectable GC objects present – potential ref cycle with __del__.",
            )
        )
    # Trade updater inactivity / stale
    age = health.get("age_seconds")
    stale_sec = health.get("thresholds", {}).get("stale")
    if age is not None and stale_sec and age > max(stale_sec, 3600):
        hours = age / 3600.0
        if hours > 48:
            age_str = ">48h"
        elif hours > 1:
            age_str = f"{hours:.1f}h"
        else:
            age_str = f"{age:.0f}s"
        obs.append(tag("WARN", f"Trade updater very stale age={age_str}"))
    if (
        trade.get("active", 0) == 0
        and trade.get("closed", 0) == 0
        and (age is None or age > 300)
    ):
        obs.append(
            tag(
                "INFO",
                "Trade updater has no active/closed trades (idle or not started).",
            )
        )
    # System level
    rss = system.get("rss_bytes")
    if rss and rss > 1_000_000_000:  # ~1GB
        obs.append(tag("WARN", f"High RSS memory {rss/1024/1024:.0f}MB"))
    cpu = system.get("cpu_percent")
    if cpu and cpu > 95:
        obs.append(tag("WARN", f"CPU saturated {cpu:.0f}%"))
    elif cpu and cpu > 85:
        obs.append(tag("INFO", f"High CPU load {cpu:.0f}%"))

    # Stage2 error rate
    s2_proc = stage2.get("processed_total") or 0
    s2_err = stage2.get("errors_total") or 0
    if s2_proc >= 10:  # avoid noise for tiny counts
        err_rate = (s2_err / s2_proc) * 100 if s2_proc else 0
        if err_rate > 5:
            obs.append(
                tag(
                    "WARN",
                    f"Stage2 error rate {err_rate:.1f}% ({int(s2_err)}/{int(s2_proc)})",
                )
            )
        elif err_rate > 0:
            obs.append(
                tag(
                    "INFO",
                    f"Stage2 errors {int(s2_err)}/{int(s2_proc)} ({err_rate:.2f}%)",
                )
            )

    # Stage1 feed lag heuristics (if normalized available)
    s1_lag = stage1.get("feed_lag_normalized") or stage1.get("feed_lag_seconds")
    if isinstance(s1_lag, (int, float)) and s1_lag is not None:
        if s1_lag > 10:
            obs.append(tag("WARN", f"Stage1 feed lag {s1_lag:.1f}s > 10s"))
        elif s1_lag > 3:
            obs.append(tag("INFO", f"Stage1 feed lag {s1_lag:.1f}s > 3s"))

    # Stage1 missing bars delta
    mb_delta = deltas.get("stage1_missing_bars_delta")
    if isinstance(mb_delta, (int, float)) and mb_delta is not None and mb_delta > 0:
        lvl = "WARN" if mb_delta >= 3 else "INFO"
        obs.append(tag(lvl, f"Stage1 missing bars +{int(mb_delta)} vs prev"))

    # Stage2 latency heuristics (p99 thresholds)
    s2_lat = stage2.get("latency") or {}
    p99 = s2_lat.get("p99")
    if isinstance(p99, (int, float)) and s2_proc >= 30:  # need some sample size
        if p99 > 0.08:
            obs.append(tag("WARN", f"Stage2 latency p99 {p99:.3f}s > 0.08s"))
        elif p99 > 0.04:
            obs.append(tag("INFO", f"Stage2 latency p99 {p99:.3f}s > 0.04s"))

    # Stage2 regression deltas
    p99_ch = deltas.get("stage2_latency_p99_change_pct")
    if isinstance(p99_ch, (int, float)) and p99_ch is not None:
        if p99_ch > 40:
            obs.append(tag("WARN", f"Stage2 p99 latency Δ{p99_ch:.1f}%"))
        elif p99_ch > 15:
            obs.append(tag("INFO", f"Stage2 p99 latency Δ{p99_ch:.1f}%"))
    err_ch = deltas.get("stage2_error_rate_change_pp")
    if isinstance(err_ch, (int, float)) and err_ch is not None and err_ch > 2:
        # 2 percentage points – mild regression
        level = "WARN" if err_ch > 5 else "INFO"
        obs.append(tag(level, f"Stage2 error rate +{err_ch:.2f}pp"))

    if not obs:
        obs.append(tag("OK", "No critical anomalies detected."))
    return obs


def build_conclusions(sections: dict[str, Any]) -> dict[str, Any]:
    """Структуровані висновки з короткими порадами.

    Формат:
        {
          domain: {
             status: OK|INFO|WARN,
             summary: str,
             recommendation: Optional[str]
          }, ...
        }
    """
    out: dict[str, dict[str, Any]] = {}

    def pack(domain: str, status: str, summary: str, recommendation: str | None = None):
        out[domain] = {
            "status": status,
            "summary": summary,
            "recommendation": recommendation,
        }

    gc = sections.get("gc", {})
    if gc.get("uncollectable_total", 0) == 0:
        pack("gc", "OK", "Немає непридатних (uncollectable) об'єктів")
    else:
        pack(
            "gc",
            "WARN",
            f"Uncollectable={gc.get('uncollectable_total')}",
            "Перевірити циклічні посилання з __del__ або закриття ресурсів",
        )

    latency = sections.get("latency", {})
    get_p99 = latency.get("get_p99") or 0
    if get_p99 < 0.05:
        pack("datastore", "OK", f"Read p99 {get_p99:.3f}s < 0.05s")
    elif get_p99 < 0.2:
        pack(
            "datastore",
            "INFO",
            f"Read p99 {get_p99:.3f}s (degradation)",
            "Перевірити I/O або кешування",
        )
    else:
        pack(
            "datastore",
            "WARN",
            f"Read p99 {get_p99:.3f}s (very slow)",
            "Профілювати I/O / блокуючі операції",
        )

    cache = sections.get("cache", {})
    ram_hit = cache.get("ram_hit_ratio") or 0
    redis_hit = cache.get("redis_hit_ratio") or 0
    if ram_hit > 0.8 and redis_hit == 0:
        pack(
            "cache",
            "INFO",
            f"RAM hit {ram_hit*100:.1f}% / Redis 0%",
            "Якщо Redis має бути задіяний – перевірити налаштування",
        )
    elif ram_hit > 0.8:
        pack("cache", "OK", f"RAM hit {ram_hit*100:.1f}%")
    else:
        pack(
            "cache",
            "INFO",
            f"RAM hit {ram_hit*100:.1f}% нижче бажаного",
            "Оптимізувати гарячі ключі / збільшити RAM",
        )

    trade = sections.get("trade_updater", {})
    drift = trade.get("drift_ratio") or 0
    pressure = trade.get("pressure") or 0
    consec_drift = trade.get("consecutive_drift_high") or 0
    if drift < 1.2:
        drift_state = ("OK", f"Drift {drift:.2f}")
    elif drift < 1.5:
        drift_state = ("INFO", f"Drift {drift:.2f} (elevated)")
    else:
        drift_state = ("WARN", f"Drift {drift:.2f} (high)")
    rec = None
    if drift_state[0] != "OK":
        rec = "Перевірити блокуючі виклики / збільшити інтервал"
    if pressure > 2.0:
        pack(
            "trade_updater",
            "WARN",
            f"Pressure {pressure:.2f}; {drift_state[1]}",
            "Зменшити частоту або відфільтрувати активи",
        )
    else:
        status = drift_state[0]
        summary = f"{drift_state[1]} Pressure {pressure:.2f}"
        pack("trade_updater", status, summary, rec)
    if consec_drift >= 3 and out["trade_updater"]["status"] != "WARN":
        # escalate
        out["trade_updater"]["status"] = "WARN"
        out["trade_updater"]["recommendation"] = (
            out["trade_updater"].get("recommendation") or "Профілювати цикл оновлення"
        )
        out["trade_updater"]["summary"] += f"; consecutive drift={consec_drift}"

    system = sections.get("system", {})
    cpu = system.get("cpu_percent") or 0
    if cpu < 80:
        pack("system_cpu", "OK", f"CPU {cpu:.0f}%")
    elif cpu < 95:
        pack(
            "system_cpu",
            "INFO",
            f"CPU {cpu:.0f}% high",
            "Розглянути оптимізацію / шардінг",
        )
    else:
        pack(
            "system_cpu",
            "WARN",
            f"CPU {cpu:.0f}% saturated",
            "Зменшити навантаження / масштабування",
        )
    rss = system.get("rss_bytes") or 0
    if rss and rss / (1024 * 1024) > 1200:
        pack("system_mem", "WARN", f"RSS {rss/1024/1024:.0f}MB", "Пошук витоків")
    else:
        pack("system_mem", "OK", f"RSS {rss/1024/1024:.0f}MB")

    stage1 = sections.get("stage1", {})
    lag = stage1.get("feed_lag_normalized") or stage1.get("feed_lag_seconds") or 0
    missing = stage1.get("missing_bars_total") or 0
    if lag < 1.5:
        pack("stage1_feed", "OK", f"Lag {lag:.2f}s missing={int(missing)}")
    elif lag < 5:
        pack("stage1_feed", "INFO", f"Lag {lag:.2f}s", "Перевірити затримки мережі")
    else:
        pack(
            "stage1_feed",
            "WARN",
            f"Lag {lag:.2f}s",
            "Відновити джерело / перевірити WS",
        )

    stage2 = sections.get("stage2", {})
    s2_proc = stage2.get("processed_total") or 0
    s2_err = stage2.get("errors_total") or 0
    s2_rate = (s2_err / s2_proc) * 100 if s2_proc else 0
    p99 = (stage2.get("latency") or {}).get("p99") or 0
    if s2_rate == 0 and p99 < 0.04:
        pack("stage2", "OK", f"err=0 p99={p99:.3f}s")
    else:
        status = "OK"
        rec_s: list[str] = []
        summary_bits: list[str] = []
        if s2_rate > 5:
            status = "WARN"
            summary_bits.append(f"err {s2_rate:.1f}%")
            rec_s.append("Перевірити логіку stage2/исключення")
        elif s2_rate > 0:
            status = "INFO"
            summary_bits.append(f"err {s2_rate:.2f}%")
        if p99 > 0.08:
            status = "WARN"
            summary_bits.append(f"p99 {p99:.3f}s")
            rec_s.append("Оптимізувати важкі обчислення")
        elif p99 > 0.04:
            if status != "WARN":
                status = max(
                    status, "INFO", key=lambda s: ["OK", "INFO", "WARN"].index(s)
                )
            summary_bits.append(f"p99 {p99:.3f}s")
        pack(
            "stage2",
            status,
            ", ".join(summary_bits) or f"p99 {p99:.3f}s",
            "; ".join(rec_s) or None,
        )

    health = sections.get("health", {})
    age = health.get("age_seconds") or 0
    status_h = "fresh"
    fresh_t = (health.get("thresholds") or {}).get("fresh", 120)
    late_t = (health.get("thresholds") or {}).get("late", 600)
    stale_t = (health.get("thresholds") or {}).get("stale", 3600)
    if age >= stale_t:
        status_h = "very-stale" if age > stale_t * 2 else "stale"
    elif age >= late_t:
        status_h = "late"
    elif age < fresh_t:
        status_h = "fresh"
    if status_h == "fresh":
        pack("health", "OK", f"age={age:.1f}s")
    elif status_h == "late":
        pack(
            "health",
            "INFO",
            f"age={age:.1f}s late",
            "Перевірити чи цикл працює стабільно",
        )
    else:
        pack(
            "health",
            "WARN",
            f"age={age:.1f}s {status_h}",
            "Перезапустити або діагностувати зависання",
        )

    return out


# ── Delta / Regression Support ─────────────────────────────────────────────
def compute_deltas(current: dict[str, Any], previous: dict[str, Any]) -> dict[str, Any]:
    """Обчислення ключових дельт між поточним і попереднім snapshot.

    Повертає:
        stage1_missing_bars_delta – різниця missing bars
        stage2_latency_p99_change_pct – % зміна p99 (від prev до current)
        stage2_error_rate_change_pp – зміна error rate у процентних пунктах
    """
    out: dict[str, Any] = {}
    try:
        cur_s1 = current.get("stage1", {}) or {}
        prev_s1 = previous.get("stage1", {}) or {}
        out["stage1_missing_bars_delta"] = (cur_s1.get("missing_bars_total") or 0) - (
            prev_s1.get("missing_bars_total") or 0
        )
    except Exception:
        pass
    try:
        cur_s2 = current.get("stage2", {}) or {}
        prev_s2 = previous.get("stage2", {}) or {}
        cur_p99 = ((cur_s2.get("latency") or {}).get("p99") or 0.0) or 0.0
        prev_p99 = ((prev_s2.get("latency") or {}).get("p99") or 0.0) or 0.0
        if prev_p99 > 0:
            out["stage2_latency_p99_change_pct"] = (
                (cur_p99 - prev_p99) / prev_p99 * 100.0
            )
        cur_proc = cur_s2.get("processed_total") or 0
        cur_err = cur_s2.get("errors_total") or 0
        prev_proc = prev_s2.get("processed_total") or 0
        prev_err = prev_s2.get("errors_total") or 0
        cur_err_rate = (cur_err / cur_proc * 100.0) if cur_proc > 0 else 0.0
        prev_err_rate = (prev_err / prev_proc * 100.0) if prev_proc > 0 else 0.0
        out["stage2_error_rate_change_pp"] = cur_err_rate - prev_err_rate
    except Exception:
        pass
    return out


def enrich_conclusions_with_deltas(
    conclusions: dict[str, Any], deltas: dict[str, Any]
) -> None:
    """Ескалація статусів з урахуванням регресій (оновлює на місці)."""
    try:
        stage2_block = conclusions.get("stage2")
        if stage2_block:
            p99_ch = deltas.get("stage2_latency_p99_change_pct")
            if isinstance(p99_ch, (int, float)) and p99_ch is not None:
                if p99_ch > 40:
                    stage2_block["status"] = "WARN"
                elif p99_ch > 15 and stage2_block.get("status") == "OK":
                    stage2_block["status"] = "INFO"
                if p99_ch > 15:
                    stage2_block["summary"] += f"; p99 Δ{p99_ch:+.1f}%"
                    if not stage2_block.get("recommendation"):
                        stage2_block["recommendation"] = (
                            "Профілювати Stage2 або оптимізувати важкі ділянки"
                        )
            err_ch = deltas.get("stage2_error_rate_change_pp")
            if isinstance(err_ch, (int, float)) and err_ch is not None and err_ch > 2:
                if err_ch > 5 and stage2_block.get("status") != "WARN":
                    stage2_block["status"] = "WARN"
                elif stage2_block.get("status") == "OK":
                    stage2_block["status"] = "INFO"
                stage2_block["summary"] += f"; err Δ{err_ch:+.2f}pp"
                if not stage2_block.get("recommendation"):
                    stage2_block["recommendation"] = (
                        "Перевірити логіку обробки помилок Stage2"
                    )
    except Exception:
        pass


# ── Async Fetchers ──────────────────────────────────────────────────────────
async def fetch_prometheus(url: str, timeout: float = 3.0) -> str:
    async with aiohttp.ClientSession() as session:
        async with session.get(
            url, timeout=aiohttp.ClientTimeout(total=timeout)
        ) as resp:
            resp.raise_for_status()
            return await resp.text()


async def fetch_redis_keys(r: redis.Redis) -> dict[str, Any]:
    out: dict[str, Any] = {}
    from config.config import STATS_CORE_KEY, STATS_HEALTH_KEY

    for key in [STATS_CORE_KEY, STATS_HEALTH_KEY]:
        try:
            raw = await r.get(key)
            if raw:
                out[key] = json.loads(raw)
        except Exception:
            continue
    return out


# ── Section Builders ────────────────────────────────────────────────────────


def section_gc(metrics: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    collected = {
        g["labels"].get("generation", "?"): g["value"]
        for g in metrics.get("python_gc_objects_collected_total", [])
    }
    uncollectable = sum(
        item["value"]
        for item in metrics.get("python_gc_objects_uncollectable_total", [])
    )
    collections = {
        g["labels"].get("generation", "?"): g["value"]
        for g in metrics.get("python_gc_collections_total", [])
    }
    return {
        "collected": collected,
        "uncollectable_total": uncollectable,
        "collections": collections,
    }


def section_python(metrics: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    info_items = metrics.get("python_info", [])
    if info_items:
        labels = info_items[0]["labels"]
    else:
        labels = {}
    version = labels.get("python_version") or labels.get("version")
    if not version:
        # fallback to runtime
        try:
            import platform

            version = platform.python_version()
        except Exception:
            version = None
    return {"version": version, "implementation": labels.get("implementation")}


def section_latency(metrics: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    get_hist = build_histogram(metrics, "ds_get_latency_seconds")
    put_hist = build_histogram(metrics, "ds_put_latency_seconds")

    def quant_pack(h: Histogram | None) -> dict[str, Any]:
        if not h:
            return {}
        return {
            "count": h.count,
            "sum": h.summation,
            "p50": h.quantile(0.50),
            "p90": h.quantile(0.90),
            "p99": h.quantile(0.99),
        }

    data = {
        "get": quant_pack(get_hist),
        "put": quant_pack(put_hist),
    }
    # flatten convenience
    flat = {}
    for prefix, block in data.items():
        for k, v in block.items():
            flat[f"{prefix}_{k}"] = v
    return flat


def section_cache(metrics: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    ram_hit = first_val(metrics, "ds_ram_hit_ratio") or 0.0
    redis_hit = first_val(metrics, "ds_redis_hit_ratio") or 0.0
    bytes_ram = first_val(metrics, "ds_bytes_in_ram") or 0.0
    flush_backlog = first_val(metrics, "ds_flush_backlog") or 0.0
    evictions = first_val(metrics, "ds_evictions_total") or 0.0
    errors = first_val(metrics, "ds_errors_total") or 0.0
    return {
        "ram_hit_ratio": ram_hit,
        "redis_hit_ratio": redis_hit,
        "bytes_in_ram": bytes_ram,
        "bytes_in_ram_h": fmt_bytes(bytes_ram),
        "flush_backlog": flush_backlog,
        "evictions_total": evictions,
        "errors_total": errors,
    }


def section_trade_updater(
    metrics: dict[str, list[dict[str, Any]]], redis_data: dict[str, Any]
) -> dict[str, Any]:
    active = first_val(metrics, "trade_active_total") or 0
    closed = first_val(metrics, "trade_closed_total") or 0
    drift = first_val(metrics, "trade_updater_drift_ratio") or 0.0
    dyn_interval = first_val(metrics, "trade_updater_dynamic_interval_seconds") or 0.0
    consec_drift = first_val(metrics, "trade_updater_consecutive_drift_high") or 0
    consec_pressure = first_val(metrics, "trade_updater_consecutive_pressure_high") or 0
    pressure = first_val(metrics, "trade_updater_pressure") or 0.0
    pressure_norm = first_val(metrics, "trade_updater_pressure_norm") or 0.0
    # Alpha та skip_reasons беремо з Redis core payload
    from config.config import STATS_CORE_KEY

    core = redis_data.get(STATS_CORE_KEY, {})
    alpha = core.get("alpha")
    skip_reasons = core.get("skip_reasons", {}) if isinstance(core, dict) else {}
    thresholds = core.get("thresholds", {}) if isinstance(core, dict) else {}
    pressure_threshold = thresholds.get("pressure")
    pressure_percent = None
    if pressure_threshold and pressure_threshold > 0:
        try:
            pressure_percent = pressure / pressure_threshold * 100.0
        except Exception:
            pressure_percent = None
    return {
        "active": active,
        "closed": closed,
        "drift_ratio": drift,
        "dynamic_interval": dyn_interval,
        "consecutive_drift_high": consec_drift,
        "consecutive_pressure_high": consec_pressure,
        "pressure": pressure,
        "pressure_norm": pressure_norm,
        "alpha": alpha,
        "skip_reasons": skip_reasons,
        "pressure_threshold": pressure_threshold,
        "pressure_percent": pressure_percent,
    }


def section_health(
    redis_data: dict[str, Any], fresh_sec: int, late_sec: int, stale_sec: int
) -> dict[str, Any]:
    from config.config import STATS_CORE_KEY, STATS_HEALTH_KEY

    core = redis_data.get(STATS_CORE_KEY, {}) or {}
    health = redis_data.get(STATS_HEALTH_KEY, {}) or {}
    now = time.time()
    last_ts = core.get("last_update_ts") or health.get("ts") or 0
    try:
        # Auto-detect ms timestamps ( > ~ Nov 2001 * 1e12 )
        if isinstance(last_ts, (int, float)) and last_ts > 1_000_000_000_000:
            last_ts = last_ts / 1000.0
    except Exception:
        pass
    age = now - last_ts if last_ts else None
    status = "unknown"
    if age is None:
        status = "missing"
    else:
        if age < fresh_sec:
            status = "fresh"
        elif age < late_sec:
            status = "late"
        elif age < stale_sec:
            status = "stale"
        else:
            status = "very-stale"
    return {
        "last_update_ts": last_ts,
        "age_seconds": age,
        "status": status,
        "drift_ratio": core.get("drift_ratio"),
        "pressure": core.get("pressure"),
        "thresholds": {
            "fresh": fresh_sec,
            "late": late_sec,
            "stale": stale_sec,
        },
    }


def section_system() -> dict[str, Any]:
    proc = psutil.Process(os.getpid())
    try:
        cpu = psutil.cpu_percent(interval=0.05)
    except Exception:
        cpu = None
    try:
        rss = proc.memory_info().rss
    except Exception:
        rss = None
    try:
        boot_time = psutil.boot_time()
        uptime = time.time() - boot_time
    except Exception:
        uptime = None
    return {
        "cpu_percent": cpu,
        "rss_bytes": rss,
        "rss_h": fmt_bytes(rss) if rss is not None else None,
        "uptime_seconds": uptime,
    }


def section_stage2(metrics: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    """Агрегація Stage2 Prometheus метрик (якщо доступні).

    Очікувані метрики:
        stage2_process_total
        stage2_errors_total
        stage2_process_latency_seconds_* (histogram)
        stage2_recommendation_total{recommendation="..."}
        stage2_level_updates_total
        stage2_level_update_skips_total
    """
    processed = first_val(metrics, "stage2_process_total") or 0.0
    errors = first_val(metrics, "stage2_errors_total") or 0.0
    level_updates = first_val(metrics, "stage2_level_updates_total") or 0.0
    level_skips = first_val(metrics, "stage2_level_update_skips_total") or 0.0
    latency_hist = build_histogram(metrics, "stage2_process_latency_seconds")
    latency = {}
    if latency_hist:
        latency = {
            "count": latency_hist.count,
            "p50": latency_hist.quantile(0.50),
            "p90": latency_hist.quantile(0.90),
            "p99": latency_hist.quantile(0.99),
        }
    # Recommendation distribution
    reco_dist: dict[str, float] = {}
    for item in metrics.get("stage2_recommendation_total", []):
        label_reco = item["labels"].get("recommendation", "?")
        reco_dist[label_reco] = reco_dist.get(label_reco, 0.0) + item["value"]
    return {
        "processed_total": processed,
        "errors_total": errors,
        "latency": latency,
        "recommendation_dist": reco_dist,
        "level_updates_total": level_updates,
        "level_update_skips_total": level_skips,
    }


def section_stage1(metrics: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    """Агрегація Stage1 метрик (лаг та пропущені бари).

    Очікувані метрики:
        stage1_feed_lag_seconds (gauge)
        stage1_missing_bars_total (counter)
    """
    feed_lag = first_val(metrics, "stage1_feed_lag_seconds")
    missing = first_val(metrics, "stage1_missing_bars_total") or 0.0
    normalized = None
    anomaly_flag = False
    if feed_lag is not None:
        # Якщо значення виглядає як epoch timestamp (≈1.7e9) або надто велике –
        # інтерпретуємо як timestamp
        now_sec = time.time()
        if feed_lag > 60 * 60 * 24 * 30:  # >30 діб в секундах – точно не lag
            # Можливо це timestamp (epoch). Якщо так – обчислимо відставання.
            if feed_lag > 1_000_000_000:  # epoch seconds
                normalized = max(0.0, now_sec - feed_lag)
                anomaly_flag = True
            else:
                normalized = feed_lag  # залишимо як є
        else:
            normalized = feed_lag
    return {
        "feed_lag_seconds": feed_lag,
        "feed_lag_normalized": normalized,
        "feed_lag_anomalous": anomaly_flag,
        "missing_bars_total": missing,
    }


# ── Markdown Rendering ──────────────────────────────────────────────────────


def md_section(title: str, lines: list[str], explain: str | None = None) -> str:
    body = "\n".join(lines)
    if explain:
        body = f"{body}\n\n> {explain}"
    return f"### {title}\n{body}\n\n"


def render_markdown(sections: dict[str, Any], explain: bool = False) -> str:
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    gc = sections["gc"]
    python_info = sections["python"]
    latency = sections["latency"]
    cache = sections["cache"]
    trade = sections["trade_updater"]
    system = sections["system"]
    stage1 = sections.get("stage1", {})
    stage2 = sections.get("stage2", {})
    health = sections["health"]
    obs = sections["observations"]

    parts: list[str] = [f"# AiOne Metrics Snapshot\n_Collected: {ts}_\n"]

    # 1 GC
    c0 = int(gc["collected"].get("0", 0))
    c1 = int(gc["collected"].get("1", 0))
    c2 = int(gc["collected"].get("2", 0))
    col0 = int(gc["collections"].get("0", 0))
    col1 = int(gc["collections"].get("1", 0))
    col2 = int(gc["collections"].get("2", 0))
    gc_lines = [
        f"Collected: gen0={c0} gen1={c1} gen2={c2}",
        f"Collections: gen0={col0} gen1={col1} gen2={col2}",
        f"Uncollectable: {int(gc['uncollectable_total'])}",
    ]
    parts.append(
        md_section(
            "1. Garbage Collection",
            gc_lines,
            "GC частота та наявність непридатних об'єктів (ref cycles)",
        )
        if explain
        else md_section("1. Garbage Collection", gc_lines)
    )

    # 2 Python
    py_lines = [
        f"Version: {python_info.get('version')}",
        f"Implementation: {python_info.get('implementation')}",
    ]
    parts.append(
        md_section(
            "2. Python Runtime",
            py_lines,
            "Версія важлива для сумісності та performance аналізу",
        )
        if explain
        else md_section("2. Python Runtime", py_lines)
    )

    # 3 Latency
    lat_lines = []
    if latency:
        for k in [
            "get_count",
            "get_p50",
            "get_p90",
            "get_p99",
            "put_count",
            "put_p50",
            "put_p90",
            "put_p99",
        ]:
            v = latency.get(k)
            if v is not None:
                lat_lines.append(f"{k}: {v:.4f}")
    parts.append(
        md_section(
            "3. DataStore Latency (s)",
            lat_lines or ["No data"],
            "Квантилі p50/p90/p99 допомагають помітити хвости затримок",
        )
        if explain
        else md_section("3. DataStore Latency (s)", lat_lines or ["No data"])
    )

    # 4 Cache
    cache_lines = [
        (
            f"RAM Hit Ratio: {cache['ram_hit_ratio']*100:.1f}%  "
            f"Redis Hit Ratio: {cache['redis_hit_ratio']*100:.1f}%"
        ),
        (
            f"Bytes in RAM: {cache['bytes_in_ram_h']}  "
            f"Flush backlog: {int(cache['flush_backlog'])}"
        ),
        (
            f"Evictions: {int(cache['evictions_total'])}  "
            f"Errors: {int(cache['errors_total'])}"
        ),
    ]
    parts.append(
        md_section(
            "4. Cache Efficiency",
            cache_lines,
            "RAM hit > Redis hit може бути нормою (Redis поки не прогрітий)",
        )
        if explain
        else md_section("4. Cache Efficiency", cache_lines)
    )

    # 5 Trade Updater
    trade_lines = [
        f"Active/Closed: {int(trade['active'])}/{int(trade['closed'])}",
        (
            f"Drift Ratio: {trade['drift_ratio']:.3f}  "
            f"Pressure: {trade['pressure']:.3f} (norm {trade['pressure_norm']:.3f})"
        ),
        f"Dynamic Interval: {trade['dynamic_interval']:.2f}s  α: {trade.get('alpha')}",
        (
            f"Consecutive High Drift: {int(trade['consecutive_drift_high'])}  "
            f"High Pressure: {int(trade['consecutive_pressure_high'])}"
        ),
    ]
    skip_r = trade.get("skip_reasons") or {}
    if skip_r:
        sr = ", ".join(f"{k}:{v}" for k, v in skip_r.items())
        trade_lines.append(f"Skip Reasons: {sr}")
    else:
        trade_lines.append("Skip Reasons: (none)")
    parts.append(
        md_section(
            "5. Trade Updater",
            trade_lines,
            "Drift = фактичний_час/базовий; Pressure ~ skipped_ewma/active",
        )
        if explain
        else md_section("5. Trade Updater", trade_lines)
    )

    # 6 System
    if system.get("uptime_seconds"):
        sys_line = (
            f"CPU: {system.get('cpu_percent')}%  "
            f"RSS: {system.get('rss_h')}  "
            f"Uptime: {system.get('uptime_seconds')/3600:.2f}h"
        )
    else:
        sys_line = f"CPU: {system.get('cpu_percent')}%  " f"RSS: {system.get('rss_h')}"
    sys_lines = [sys_line]
    parts.append(
        md_section(
            "6. System",
            sys_lines,
            "CPU% миттєве; RSS – resident memory процесу; uptime ОС",
        )
        if explain
        else md_section("6. System", sys_lines)
    )

    # 7 Stage1 Feed
    s1_lines: list[str] = []
    if stage1:
        lag = stage1.get("feed_lag_normalized") or stage1.get("feed_lag_seconds")
        missing = stage1.get("missing_bars_total")
        if lag is not None:
            if stage1.get("feed_lag_anomalous"):
                s1_lines.append(f"Feed Lag: {lag:.2f}s (normalized from raw epoch)")
            else:
                s1_lines.append(f"Feed Lag: {lag:.2f}s (max observed)")
        else:
            s1_lines.append("Feed Lag: n/a")
        s1_lines.append(f"Missing Bars (total): {int(missing or 0)}")
    else:
        s1_lines.append("No Stage1 metrics")
    parts.append(
        md_section(
            "7. Stage1 Feed",
            s1_lines,
            (
                "Lag = наскільки останній бар відстає від поточного часу; "
                "Missing Bars – кількість пропусків хронології"
            ),
        )
        if explain
        else md_section("7. Stage1 Feed", s1_lines)
    )

    # 8 Stage2
    s2_lines: list[str] = []
    if stage2:
        s2_lines.append(
            f"Processed: {int(stage2.get('processed_total',0))}  "
            f"Errors: {int(stage2.get('errors_total',0))}"
        )
        lat = stage2.get("latency") or {}
        if lat:

            def _q(v):
                return (
                    f"{v:.4f}"
                    if isinstance(v, (int, float)) and v is not None
                    else "na"
                )

            s2_lines.append(
                f"Latency p50={_q(lat.get('p50'))} "
                f"p90={_q(lat.get('p90'))} "
                f"p99={_q(lat.get('p99'))} "
                f"count={int(lat.get('count') or 0)}"
            )
        reco_dist = stage2.get("recommendation_dist") or {}
        if reco_dist:
            rd = ", ".join(f"{k}:{int(v)}" for k, v in sorted(reco_dist.items()))
            s2_lines.append(f"Reco Dist: {rd}")
        s2_lines.append(
            f"Level updates: {int(stage2.get('level_updates_total',0))} "
            f"skips: {int(stage2.get('level_update_skips_total',0))}"
        )
    else:
        s2_lines.append("No Stage2 metrics")
    parts.append(
        md_section(
            "8. Stage2 Processor",
            s2_lines,
            "Latency (histogram), кількість помилок та дистрибуція рекомендацій",
        )
        if explain
        else md_section("8. Stage2 Processor", s2_lines)
    )

    # 9 Health
    age = health.get("age_seconds")
    if age is not None:
        if age > 48 * 3600:
            age_str = ">48h"
        elif age > 3600:
            age_str = f"{age/3600:.2f}h"
        elif age > 60:
            age_str = f"{age/60:.1f}m"
        else:
            age_str = f"{age:.1f}s"
    else:
        age_str = "n/a"
    status = health.get("status")
    health_lines = [
        f"Last update age: {age_str}  Status: {status}",
        f"Drift Ratio (last): {health.get('drift_ratio')}",
        f"Pressure (last): {health.get('pressure')}",
    ]
    parts.append(
        md_section(
            "9. Health",
            health_lines,
            "Показує коли востаннє Stage3 оновив core (age → статус)",
        )
        if explain
        else md_section("9. Health", health_lines)
    )

    # 10 Observations
    parts.append(
        md_section(
            "10. Observations",
            [f"- {o}" for o in obs],
            "Автоматичні евристики: WARN = потенційний ризик, INFO = контекст",
        )
        if explain
        else md_section("10. Observations", [f"- {o}" for o in obs])
    )

    # 11 Conclusions – зведений actionable summary
    conclusions = sections.get("conclusions", {})
    concl_lines: list[str] = []
    order = [
        "gc",
        "datastore",
        "cache",
        "trade_updater",
        "system_cpu",
        "system_mem",
        "stage1_feed",
        "stage2",
        "health",
    ]
    for key in order:
        block = conclusions.get(key)
        if not block:
            continue
        status = block.get("status")
        summary = block.get("summary")
        rec = block.get("recommendation")
        if rec:
            concl_lines.append(f"[{status}] {key}: {summary} → {rec}")
        else:
            concl_lines.append(f"[{status}] {key}: {summary}")
    if not concl_lines:
        concl_lines.append("[OK] No issues")
    parts.append(
        md_section(
            "11. Conclusions",
            concl_lines,
            "Швидкий стан по доменах: OK – норма, INFO – слідкувати, WARN – дія потрібна",
        )
        if explain
        else md_section("11. Conclusions", concl_lines)
    )

    return "\n".join(parts)


# ── Plain Text Rendering (fallback) ─────────────────────────────────────────


def render_text(sections: dict[str, Any], explain: bool = False) -> str:
    md = render_markdown(sections, explain=explain)
    # Stripping markdown headers minimally for plain mode
    return re.sub(r"^###? ", "", md, flags=re.MULTILINE)


# ── Main Logic ──────────────────────────────────────────────────────────────
async def collect(
    prom_url: str,
    redis_url: str,
    fresh_sec: int,
    late_sec: int,
    stale_sec: int,
    prev_json_path: str | None = None,
) -> dict[str, Any]:
    # Fetch concurrently
    prom_task = asyncio.create_task(fetch_prometheus(prom_url))
    r = redis.from_url(redis_url, decode_responses=True, encoding="utf-8")
    redis_task = asyncio.create_task(fetch_redis_keys(r))
    prom_text, redis_data = await asyncio.gather(prom_task, redis_task)
    metrics = parse_prometheus_text(prom_text)

    sections: dict[str, Any] = {
        "gc": section_gc(metrics),
        "python": section_python(metrics),
        "latency": section_latency(metrics),
        "cache": section_cache(metrics),
        "trade_updater": section_trade_updater(metrics, redis_data),
        "system": section_system(),
        "stage1": section_stage1(metrics),
        "stage2": section_stage2(metrics),
        "health": section_health(redis_data, fresh_sec, late_sec, stale_sec),
    }
    sections["deltas"] = {}
    prev_data: dict[str, Any] = {}
    if prev_json_path:
        try:
            with open(prev_json_path, encoding="utf-8") as fh:
                prev_data = json.load(fh)
        except Exception:
            prev_data = {}
    if prev_data:
        sections["deltas"] = compute_deltas(sections, prev_data)
    sections["observations"] = build_observations(sections)
    sections["conclusions"] = build_conclusions(sections)
    if sections.get("deltas"):
        enrich_conclusions_with_deltas(sections["conclusions"], sections["deltas"])
    return sections


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="One-shot metrics snapshot reporter")
    parser.add_argument(
        "--prom-url", default=os.getenv("PROM_URL", "http://localhost:9109/metrics")
    )
    parser.add_argument(
        "--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0")
    )
    parser.add_argument(
        "--format", choices=["markdown", "text", "json"], default="markdown"
    )
    parser.add_argument(
        "--fresh-sec", type=int, default=120, help="Age seconds for 'fresh' threshold"
    )
    parser.add_argument(
        "--late-sec", type=int, default=600, help="Age seconds for 'late' threshold"
    )
    parser.add_argument(
        "--stale-sec", type=int, default=3600, help="Age seconds for 'stale' threshold"
    )
    parser.add_argument(
        "--explain", action="store_true", help="Додати короткі пояснення до секцій"
    )
    parser.add_argument(
        "--prev-json",
        default=None,
        help="Шлях до попереднього JSON snapshot для порівняння регресій",
    )
    args = parser.parse_args(argv)

    try:
        sections = asyncio.run(
            collect(
                args.prom_url,
                args.redis_url,
                args.fresh_sec,
                args.late_sec,
                args.stale_sec,
                args.prev_json,
            )
        )
    except Exception as e:
        print(f"ERROR collecting metrics: {e}", file=sys.stderr)
        return 1

    if args.format == "markdown":
        out = render_markdown(sections, explain=args.explain)
    elif args.format == "text":
        out = render_text(sections, explain=args.explain)
    else:
        out = json.dumps(sections, indent=2, default=str)
    print(out)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
