"""Light smoke tests for Stage1/Stage2 integration surfaces.

These tests avoid network / external I-O. They simulate minimal stats payload
flowing through Stage2 QDE engine and ensure output contract keys exist.
"""

from stage2.qde_core import QDEngine


def test_qde_engine_minimal_flow():
    engine = QDEngine()
    sample = {
        "symbol": "TEST",
        "stats": {
            "current_price": 100.0,
            "vwap": 100.1,
            "atr": 1.2,
            "daily_high": 105.0,
            "daily_low": 95.0,
            "rsi": 55.0,
            "volume": 5000.0,
            "volume_z": 1.1,
            "bid_ask_spread": 0.02,
        },
        "trigger_reasons": [],
    }
    out = engine.process(sample)
    assert out["symbol"] == "TEST"
    for k in [
        "market_context",
        "confidence_metrics",
        "anomaly_detection",
        "recommendation",
        "narrative",
        "risk_parameters",
    ]:
        assert k in out
    assert "composite_confidence" in out["confidence_metrics"]
