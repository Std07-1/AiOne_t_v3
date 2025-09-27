import pytest

from app.asset_state_manager import AssetStateManager
from stage3.open_trades import open_trades
from stage3.trade_manager import TradeLifecycleManager


@pytest.mark.asyncio
async def test_open_trades_uses_risk_parameters_fallbacks() -> None:
    trade_manager = TradeLifecycleManager()
    state_manager = AssetStateManager(["abcusdt"])

    signal = {
        "symbol": "ABCUSDT",
        "signal": "ALERT_BUY",
        "confidence": 0.9,
        "stats": {
            "current_price": 100.0,
            "atr": 1.5,
            "rsi": 55.0,
            "volume_mean": 1200.0,
        },
        "risk_parameters": {
            "tp_targets": [105.0, 110.0],
            "sl_level": 98.0,
            "risk_reward_ratio": 2.5,
        },
        "context_metadata": {"low_gate": 0.005, "atr_pct": 0.02, "htf_ok": True},
        "validation_passed": True,
    }

    await open_trades(
        [signal], trade_manager, max_parallel=1, state_manager=state_manager
    )

    active_trades = await trade_manager.get_active_trades()
    assert len(active_trades) == 1
    assert active_trades[0]["symbol"] == "ABCUSDT"
    trade = active_trades[0]

    assert trade["entry_price"] == pytest.approx(100.0)
    assert trade["tp"] == pytest.approx(105.0)
    assert trade["sl"] == pytest.approx(98.0)
    assert trade["indicators"]["atr"] == pytest.approx(1.5)
    assert trade["indicators"]["volume"] == pytest.approx(1200.0)


@pytest.mark.asyncio
async def test_open_trades_low_vol_high_confidence_override() -> None:
    trade_manager = TradeLifecycleManager()
    state_manager = AssetStateManager(["lowvolusdt"])

    signal = {
        "symbol": "LOWVOLUSDT",
        "signal": "ALERT_BUY",
        "confidence": 0.9,
        "stats": {
            "current_price": 10.0,
            "atr": 0.005,
            "rsi": 52.0,
            "volume": 2500.0,
        },
        "risk_parameters": {
            "tp_targets": [10.5],
            "sl_level": 9.8,
        },
        "context_metadata": {
            "low_gate": 0.006,
            "atr_pct": 0.0005,
            "htf_ok": True,
        },
        "validation_passed": True,
    }

    await open_trades(
        [signal], trade_manager, max_parallel=1, state_manager=state_manager
    )

    active_trades = await trade_manager.get_active_trades()
    assert len(active_trades) == 1
