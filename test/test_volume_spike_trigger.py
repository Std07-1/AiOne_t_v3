import pandas as pd

from stage1.asset_triggers.volume_spike_trigger import volume_spike_trigger


def _make_df(close_delta: float) -> pd.DataFrame:
    bars = 60
    base_open = 1.0
    closes = [base_open] * (bars - 1)
    closes.append(base_open * (1.0 + close_delta))
    data = {
        "open": [base_open] * bars,
        "high": [base_open * 1.01] * bars,
        "low": [base_open * 0.99] * bars,
        "close": closes,
        "volume": [100.0] * (bars - 1) + [1000.0],
    }
    return pd.DataFrame(data)


def test_volume_spike_flat_bar_within_tolerance_passes() -> None:
    df = _make_df(close_delta=-3e-4)
    flag, meta = volume_spike_trigger(df, upbar_tolerance=5e-4)
    assert flag is True
    assert meta["upbar"] is True


def test_volume_spike_strong_downbar_blocks() -> None:
    df = _make_df(close_delta=-0.01)
    flag, meta = volume_spike_trigger(df, upbar_tolerance=5e-4)
    assert flag is False
    assert meta["upbar"] is False
