import pytest

from app.thresholds import Thresholds


@pytest.mark.parametrize(
    "market_state,expected_volz,expected_vwap",
    [
        (None, 2.0, 0.02),
        ("range_bound", 2.0, 0.022),
        ("high_volatility", 2.2, 0.022),
        ("trend_strong", 2.0 + 0.0, 0.02),
    ],
)
def test_effective_thresholds_state_overrides(
    market_state, expected_volz, expected_vwap
):
    cfg = {
        "symbol": "btcusdt",
        "low_gate": 0.006,
        "high_gate": 0.015,
        "atr_target": 0.5,
        "vol_z_threshold": 2.0,
        "rsi_oversold": 30.0,
        "rsi_overbought": 70.0,
        "vwap_deviation": 0.02,
        "signal_thresholds": {
            "volume_spike": {"z_score": 2.0},
            "vwap_deviation": {"threshold": 0.02},
        },
        "state_overrides": {
            "range_bound": {"vwap_deviation": +0.002},
            "high_volatility": {"volume_spike.z_score": +0.2, "vwap_deviation": +0.002},
        },
    }
    thr = Thresholds.from_mapping(cfg)
    eff = thr.effective_thresholds(market_state=market_state)

    assert pytest.approx(eff.get("vol_z_threshold"), rel=1e-6) == expected_volz
    assert pytest.approx(eff.get("vwap_deviation"), rel=1e-6) == expected_vwap


def test_effective_thresholds_gate_consistency():
    cfg = {
        "symbol": "ETHUSDT",
        "low_gate": 0.010,
        "high_gate": 0.012,
        "atr_target": 0.5,
        "vol_z_threshold": 2.0,
        "rsi_oversold": 30.0,
        "rsi_overbought": 70.0,
        "vwap_deviation": 0.02,
        "state_overrides": {"weird": {"low_gate": +0.01, "high_gate": -0.03}},
    }
    thr = Thresholds.from_mapping(cfg)
    eff = thr.effective_thresholds(market_state="weird")
    # після корекції гарантуємо high > low (якщо ні — множимо high)
    assert eff["high_gate"] > eff["low_gate"]
