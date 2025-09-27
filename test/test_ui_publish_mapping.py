"""Тести UI publish mapping: відсутні current_price/RSI не конвертуються у 0."""

import json
from typing import Any

import pytest

from UI.publish_full_state import publish_full_state


class DummyStateMgr:
    def __init__(self, assets: list[dict[str, Any]]):
        self._assets = assets

    def get_all_assets(self) -> list[dict[str, Any]]:
        return self._assets


class DummyRedis:
    def __init__(self) -> None:
        self.published: list[str] = []
        self.snapshot: str | None = None

    async def publish(self, channel: str, message: str) -> int:
        self.published.append(message)
        return 1

    async def set(self, key: str, value: str) -> object:
        self.snapshot = value
        return object()


@pytest.mark.asyncio
async def test_publish_does_not_inject_zero_price_or_rsi():
    # Статистика без current_price та rsi
    assets = [
        {"symbol": "abcusdt", "stats": {"volume_mean": 1000.0}},
        {"symbol": "defusdt", "stats": {"current_price": 0, "rsi": None}},
    ]
    mgr = DummyStateMgr(assets)
    r = DummyRedis()

    await publish_full_state(mgr, object(), r)

    assert r.published, "Очікується хоча б одне повідомлення"
    payload = json.loads(r.published[-1])
    assert "assets" in payload and isinstance(payload["assets"], list)
    ser_assets = payload["assets"]

    # Жодна з двох не повинна мати price_str без валідної ціни
    for a in ser_assets:
        if "price" in a:
            assert a["price"] > 0
        if "price_str" in a:
            assert a.get("price", 0) > 0
        # RSI має зʼявлятися лише якщо дано
        if a.get("symbol").lower() == "abcusdt":
            assert "rsi" not in a


@pytest.mark.asyncio
async def test_publish_sets_tp_sl_string():
    assets = [
        {
            "symbol": "btcusdt",
            "tp": 100.0,
            "sl": 90.0,
            "stats": {"current_price": 95.0},
        },
    ]
    mgr = DummyStateMgr(assets)
    r = DummyRedis()

    await publish_full_state(mgr, object(), r)
    payload = json.loads(r.published[-1])
    first = payload["assets"][0]
    # У PR4 tp_sl формується лише з Stage3 targets; якщо їх немає — очікуємо "-"
    assert isinstance(first.get("tp_sl"), str)
    assert ("TP:" in first["tp_sl"] or "SL:" in first["tp_sl"]) or first["tp_sl"] == "-"


class _DummyCacheWithCore:
    class _Redis:
        async def jget(self, key: str, default: object | None = None) -> object | None:
            # Повертаємо core документ з таргетами для BTCUSDT
            if key == "core":
                return {"trades": {"targets": {"BTCUSDT": {"tp": 120.0, "sl": 80.0}}}}
            return default

    def __init__(self) -> None:
        self.redis = self._Redis()


@pytest.mark.asyncio
async def test_tp_sl_is_dash_when_no_targets():
    assets = [
        {
            "symbol": "btcusdt",
            "stats": {"current_price": 100.0},
        },
    ]
    mgr = DummyStateMgr(assets)
    r = DummyRedis()

    # Без core.targets tp_sl має бути '-'
    await publish_full_state(mgr, object(), r)
    payload = json.loads(r.published[-1])
    first = payload["assets"][0]
    assert first.get("tp_sl") == "-"


@pytest.mark.asyncio
async def test_tp_sl_feature_flag_off_forces_dash(monkeypatch: pytest.MonkeyPatch):
    # Підміняємо флаг у модулі публішера на False
    import UI.publish_full_state as pub

    monkeypatch.setattr(pub, "UI_TP_SL_FROM_STAGE3_ENABLED", False, raising=True)

    assets = [
        {"symbol": "btcusdt", "stats": {"current_price": 100.0}},
    ]
    mgr = DummyStateMgr(assets)
    r = DummyRedis()

    # Навіть при наявності core.targets форматування вимкнено → '-'
    await publish_full_state(mgr, _DummyCacheWithCore(), r)
    payload = json.loads(r.published[-1])
    first = payload["assets"][0]
    assert first.get("tp_sl") == "-"


@pytest.mark.asyncio
async def test_publish_exposes_corridor_analytics_block():
    corridor_meta = {
        "band_pct": 0.02,
        "dist_to_edge_pct": 0.008,
        "dist_to_edge_ratio": 0.4,
        "near_edge": "support",
        "is_near_edge": True,
        "within_corridor": True,
    }
    assets = [
        {
            "symbol": "btcusdt",
            "signal": "ALERT_BUY",
            "stats": {"current_price": 100.0},
            "market_context": {
                "meta": {
                    "low_gate": 0.006,
                    "atr_pct": 0.004,
                    "htf_ok": True,
                    "corridor": corridor_meta,
                }
            },
        },
    ]
    mgr = DummyStateMgr(assets)
    r = DummyRedis()

    await publish_full_state(mgr, object(), r)

    payload = json.loads(r.published[-1])
    asset = payload["assets"][0]
    analytics = asset.get("analytics")
    assert isinstance(analytics, dict)
    assert analytics.get("corridor_band_pct") == pytest.approx(0.02)
    assert analytics.get("corridor_near_edge") == "support"
    assert analytics.get("low_volatility_flag") is True
    summary = payload.get("analytics")
    assert isinstance(summary, dict)
    assert summary.get("near_edge_assets") == 1
    low_vol_summary = summary.get("low_volatility")
    assert isinstance(low_vol_summary, dict)
    assert low_vol_summary.get("alerts") == 1
