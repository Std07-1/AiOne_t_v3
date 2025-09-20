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
    assert isinstance(first.get("tp_sl"), str)
    assert "TP:" in first["tp_sl"] or "SL:" in first["tp_sl"]
