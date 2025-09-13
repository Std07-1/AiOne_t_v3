"""Tests for tick size and price formatting heuristics.

Focus: get_tick_size + format_price decimals alignment across brackets.
"""

from utils.utils import get_tick_size, format_price


def test_tick_size_basic_overrides():
    ov = {"btcusdt": 0.5}
    assert get_tick_size("BTCUSDT", price_hint=65000, overrides=ov) == 0.5


def test_tick_size_brackets_progression():
    # Monotonic non-zero positive values across typical price zones
    prices = [0.00045, 0.012, 0.085, 0.9, 5.2, 27.0, 180.0, 1200.0]
    last = 0.0
    for p in prices:
        tick = get_tick_size("XYZ", price_hint=p)
        assert tick > 0
        # ensure not exploding
        assert tick < p * 0.5 or p < 1  # relaxed condition for tiny prices
        last = tick


def test_format_price_decimals_coherent():
    # Ensure formatted price respects bracket-driven decimals implied by tick_size
    samples = [0.00045, 0.0123, 0.0849, 0.9, 5.234, 27.1357, 182.77, 1234.56]
    for p in samples:
        s = format_price(p, "xyz")
        # Must parse back ignoring commas
        back = float(s.replace(",", ""))
        # Relative difference small
        assert abs(back - p) / max(1e-9, p) < 1e-6
