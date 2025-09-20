import asyncio

from app.asset_state_manager import AssetStateManager
from app.screening_producer import process_single_stage2


class _FakeProcessor:
    async def process(self, signal: dict) -> dict:
        # Повертаємо мінімально потрібну структуру Stage2
        return {
            "market_context": {
                "scenario": "TREND_UP",
                "trigger_reasons": ["trend", "rsi"],
            },
            "recommendation": "BUY",
            "trigger_reasons": ["breakout"],
            "risk_parameters": {
                "tp_targets": [signal.get("price", 100.0) * 1.02],
                "sl_level": signal.get("price", 100.0) * 0.98,
            },
            "confidence_metrics": {"composite_confidence": 0.8},
            "narrative": "Test narrative",
        }


def test_process_single_stage2_merges_trigger_reasons() -> None:
    symbol = "ethusdt"
    state = AssetStateManager([symbol])
    # Імітуємо існуючі причини
    state.update_asset(
        symbol, {"trigger_reasons": ["volatility"], "signal": "ALERT_BUY"}
    )

    signal = {"symbol": symbol, "stats": {"current_price": 100.0}, "price": 100.0}

    asyncio.run(process_single_stage2(signal, _FakeProcessor(), state))

    merged = state.state[symbol].get("trigger_reasons")
    assert isinstance(merged, list)
    # Порядок зберігається, дублікати прибираються
    assert merged[0] == "volatility"
    assert set(merged) >= {"volatility", "breakout", "trend", "rsi"}
