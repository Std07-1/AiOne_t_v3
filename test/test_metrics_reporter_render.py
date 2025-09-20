import re

from monitoring import metrics_reporter as mr


def test_render_text_smoke():
    sections = {
        "gc": {
            "collected": {"0": 1, "1": 2, "2": 3},
            "collections": {"0": 10, "1": 20, "2": 30},
            "uncollectable_total": 0,
        },
        "python": {"version": "3.11.0", "implementation": "CPython"},
        "latency": {
            # keep minimal keys; renderer tolerates missing ones
            "get_count": 0,
        },
        "cache": {
            "ram_hit_ratio": 0.9,
            "redis_hit_ratio": 0.0,
            "bytes_in_ram": 1234,
            "bytes_in_ram_h": "1.2 KB",
            "flush_backlog": 0,
            "evictions_total": 0,
            "errors_total": 0,
        },
        "trade_updater": {
            "active": 0,
            "closed": 0,
            "drift_ratio": 1.0,
            "pressure": 0.5,
            "pressure_norm": 0.5,
            "dynamic_interval": 1.0,
            "consecutive_drift_high": 0,
            "consecutive_pressure_high": 0,
        },
        "system": {
            "cpu_percent": 5,
            "rss_bytes": 1024,
            "rss_h": "1.0 KB",
            "uptime_seconds": 60,
        },
        "stage1": {
            "feed_lag_seconds": 0.5,
            "feed_lag_normalized": 0.5,
            "feed_lag_anomalous": False,
            "missing_bars_total": 0,
        },
        "stage2": {
            "processed_total": 0,
            "errors_total": 0,
            "latency": {},
            "recommendation_dist": {},
            "level_updates_total": 0,
            "level_update_skips_total": 0,
        },
        "health": {
            "age_seconds": 1.0,
            "status": "fresh",
            "drift_ratio": 1.0,
            "pressure": 0.5,
            "thresholds": {"fresh": 120, "late": 600, "stale": 3600},
        },
        "observations": ["[OK] No critical anomalies detected."],
        "conclusions": {
            "gc": {
                "status": "OK",
                "summary": "No uncollectable",
                "recommendation": None,
            },
            "datastore": {
                "status": "OK",
                "summary": "Read p99 0.000s",
                "recommendation": None,
            },
            "cache": {
                "status": "OK",
                "summary": "RAM hit 90.0%",
                "recommendation": None,
            },
            "trade_updater": {
                "status": "OK",
                "summary": "Drift 1.00 Pressure 0.50",
                "recommendation": None,
            },
            "system_cpu": {"status": "OK", "summary": "CPU 5%", "recommendation": None},
            "system_mem": {
                "status": "OK",
                "summary": "RSS 0MB",
                "recommendation": None,
            },
            "stage1_feed": {
                "status": "OK",
                "summary": "Lag 0.50s missing=0",
                "recommendation": None,
            },
            "stage2": {"status": "OK", "summary": "p99 0.000s", "recommendation": None},
            "health": {"status": "OK", "summary": "age=1.0s", "recommendation": None},
        },
    }

    out = mr.render_text(sections, explain=True)
    assert isinstance(out, str)
    assert "AiOne Metrics Snapshot" in out
    # Ensure some key sections rendered
    assert re.search(r"\bGarbage Collection\b", out)
    assert re.search(r"\bPython Runtime\b", out)
    assert re.search(r"\bDataStore Latency\b", out)
