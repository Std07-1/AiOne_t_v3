from typing import Any

import pytest

from app.thresholds import Thresholds, load_thresholds


class DummyRedis:
    def __init__(self) -> None:
        self.store: dict[str, dict[str, Any]] = {}

    async def jset(
        self, key: str, subkey: str, value: dict[str, Any], ttl: int
    ) -> None:  # noqa: ARG002
        bucket = self.store.setdefault(key, {})
        bucket[subkey] = value

    async def jget(
        self, key: str, subkey: str, default: Any = None
    ) -> Any:  # noqa: ARG002
        return self.store.get(key, {}).get(subkey, default)


class DummyStore:
    def __init__(self) -> None:
        self.redis = DummyRedis()


@pytest.mark.asyncio
async def test_load_thresholds_cache_hit_returns_saved_values():
    store = DummyStore()
    symbol = "BTCUSDT"

    # Save thresholds
    thr = Thresholds.from_mapping(
        {
            "symbol": symbol,
            "low_gate": 0.009,
            "high_gate": 0.02,
            "atr_target": 0.7,
            "vol_z_threshold": 3.3,
            "rsi_oversold": 25.0,
            "rsi_overbought": 76.0,
            "min_atr_percent": 0.003,
            "vwap_deviation": 0.025,
        }
    )
    # Persist via public API
    from app.thresholds import save_thresholds

    await save_thresholds(symbol, thr, store)

    loaded = await load_thresholds(symbol, store)
    assert isinstance(loaded, Thresholds)
    assert loaded.low_gate == 0.009
    assert loaded.high_gate == 0.02
    assert loaded.atr_target == 0.7
    assert loaded.vol_z_threshold == 3.3
    assert loaded.rsi_oversold == 25.0
    assert loaded.rsi_overbought == 76.0
    assert loaded.min_atr_percent == 0.003
    assert loaded.vwap_deviation == 0.025


@pytest.mark.asyncio
async def test_load_thresholds_top100_fallback_when_cache_miss():
    store = DummyStore()
    symbol = "ethusdt"  # present in TOP100 list

    loaded = await load_thresholds(symbol, store)
    assert isinstance(loaded, Thresholds)
    # From TOP100 defaults mapping in config/TOP100_THRESHOLDS.py
    assert loaded.vol_z_threshold == 2.0
    assert loaded.rsi_overbought == 70.0
    assert loaded.rsi_oversold == 30.0
    # and our base gates injected by adapter
    assert loaded.low_gate == 0.006
    assert loaded.high_gate == 0.015
    assert loaded.atr_target == 0.5


@pytest.mark.asyncio
async def test_load_thresholds_generic_defaults_for_unknown_symbol():
    store = DummyStore()
    symbol = "UNKNOWN123"

    loaded = await load_thresholds(symbol, store)
    assert isinstance(loaded, Thresholds)
    # generic defaults from Thresholds.from_mapping call in load_thresholds fallback
    assert loaded.low_gate == 0.006
    assert loaded.high_gate == 0.015
    assert loaded.atr_target == 0.5
    assert loaded.vol_z_threshold == 1.2
    assert loaded.rsi_oversold == 30.0
    assert loaded.rsi_overbought == 70.0
    assert loaded.min_atr_percent == 0.002
    assert loaded.vwap_deviation == 0.02
