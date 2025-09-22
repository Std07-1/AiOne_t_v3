import pandas as pd

from utils.utils import ensure_timestamp_column


def _years(series: pd.Series) -> list[int]:
    return series.dt.tz_convert("UTC").dt.year.tolist()


def test_ensure_timestamp_column_ms_epoch_to_utc():
    # ms since epoch around 2023-2024
    base_ms = 1_700_000_000_000
    df = pd.DataFrame(
        {
            "timestamp": [base_ms + i * 60_000 for i in range(3)],
            "open": [1.0, 1.1, 1.2],
            "high": [1.0, 1.1, 1.2],
            "low": [1.0, 1.1, 1.2],
            "close": [1.0, 1.1, 1.2],
            "volume": [10, 11, 12],
        }
    )
    out = ensure_timestamp_column(df.copy())
    assert not out.empty
    assert pd.api.types.is_datetime64_any_dtype(out["timestamp"])  # dtype is datetime
    assert out["timestamp"].dt.tz is not None  # tz-aware
    ys = _years(out["timestamp"])  # sanity: reasonable years
    assert all(2009 <= y <= 2050 for y in ys)
    # monotonic non-decreasing
    assert out["timestamp"].is_monotonic_increasing


def test_ensure_timestamp_column_seconds_epoch():
    # seconds since epoch
    base_s = 1_700_000_000
    df = pd.DataFrame(
        {
            "timestamp": [base_s + i * 60 for i in range(3)],
        }
    )
    out = ensure_timestamp_column(df.copy())
    assert not out.empty
    assert out["timestamp"].dt.tz is not None
    ys = _years(out["timestamp"])  # reasonable years
    assert all(2009 <= y <= 2050 for y in ys)


def test_ensure_timestamp_column_microseconds_epoch():
    # microseconds since epoch
    base_us = 1_700_000_000_000_000
    df = pd.DataFrame(
        {
            "timestamp": [base_us + i * 60_000_000 for i in range(3)],
        }
    )
    out = ensure_timestamp_column(df.copy())
    assert not out.empty
    assert out["timestamp"].dt.tz is not None
    ys = _years(out["timestamp"])  # reasonable years
    assert all(2009 <= y <= 2050 for y in ys)
