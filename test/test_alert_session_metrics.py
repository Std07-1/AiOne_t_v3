import json

import pytest

from app.asset_state_manager import AssetStateManager


def test_finalize_alert_session_records_corridor_metrics(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    manager = AssetStateManager(["btcusdt"])

    manager.start_alert_session(
        "BTCUSDT",
        price=27123.5,
        atr_pct=0.012,
        rsi=61.0,
        side="BUY",
        band_pct=0.18,
        low_gate=0.006,
        near_edge="support",
    )

    manager.update_alert_session(
        "BTCUSDT",
        price=27200.0,
        atr_pct=0.011,
        rsi=62.1,
        htf_ok=True,
        band_pct="0.21",
        low_gate="0.0055",
        near_edge="resistance",
    )

    manager.finalize_alert_session("BTCUSDT", "low_volatility")

    output_path = tmp_path / "alerts_quality.jsonl"
    assert output_path.exists()
    lines = output_path.read_text(encoding="utf-8").strip().splitlines()
    assert lines, "alerts_quality.jsonl має містити хоча б один рядок"
    record = json.loads(lines[-1])

    assert record["symbol"] == "BTCUSDT"
    assert pytest.approx(record["band_pct"]) == 0.21
    assert pytest.approx(record["low_gate"]) == 0.0055
    assert record["near_edge"] == "resistance"
    assert record["downgrade_reason"] == "low_volatility"
    assert record.get("ts_alert")
    assert record.get("ts_end")

    assert "BTCUSDT" not in manager.alert_sessions
