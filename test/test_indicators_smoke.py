"""Smoke tests for Stage1 indicators (VWAP, VolumeZ, ATR).

Focus: basic seed/update/compute flows without external dependencies.
"""

import numpy as np
import pandas as pd

from stage1.indicators import (
    ATRManager,
    VolumeZManager,
    VWAPManager,
    compute_atr,
    compute_volume_z,
    vwap_deviation_trigger,
)


def _sample_df(n=30):
    rng = np.arange(n, dtype=float)
    return pd.DataFrame(
        {
            "close": 100 + np.sin(rng / 3) * 2 + rng * 0.01,
            "volume": 1000 + (rng % 5) * 50,
            "high": 100 + np.sin(rng / 3) * 2 + rng * 0.01 + 0.5,
            "low": 100 + np.sin(rng / 3) * 2 + rng * 0.01 - 0.5,
        }
    )


def test_vwap_manager_basic():
    df = _sample_df()
    mgr = VWAPManager(window=20)
    mgr.ensure_buffer("AAA", df)
    v_before = mgr.compute_vwap("AAA")
    assert np.isfinite(v_before)
    last = mgr.get_last("AAA")
    assert last and "close" in last
    mgr.update("AAA", close=101.23, volume=1111)
    v_after = mgr.compute_vwap("AAA")
    assert np.isfinite(v_after)
    trig = vwap_deviation_trigger(mgr, "AAA", current_price=101.0, threshold=0.00001)
    assert set(trig).issuperset({"trigger", "value", "deviation", "details"})


def test_volume_z_manager_basic():
    df = _sample_df()
    vm = VolumeZManager(window=10)
    vm.ensure_buffer("BBB", df)
    _ = vm.get_last("BBB")
    vm.update("BBB", volume=1500)
    z_new = vm.get_last("BBB")
    # When std>0 new z should be float
    assert isinstance(z_new, float) or z_new is None
    # Vector version
    z_vec = compute_volume_z(df, window=10, symbol="BBB")
    assert isinstance(z_vec, float) or np.isnan(z_vec)


def test_atr_manager_basic():
    df = _sample_df()
    am = ATRManager(period=14)
    am.ensure_state("CCC", df)
    _ = am.get_state("CCC")
    # May still be nan if insufficient rows
    # Update a few synthetic bars
    for i in range(5):
        row = df.iloc[-1]
        am.update(
            "CCC",
            high=row["high"] + 0.1 * i,
            low=row["low"] - 0.1 * i,
            close=row["close"],
        )
    atr_new = am.get_state("CCC")
    # Vector check
    atr_vec = compute_atr(df, window=14, symbol="CCC")
    assert isinstance(atr_new, float) or np.isnan(atr_new)
    assert isinstance(atr_vec, float) or np.isnan(atr_vec)
