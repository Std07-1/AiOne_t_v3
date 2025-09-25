import json
from typing import Any

import pytest

from UI.publish_full_state import publish_full_state


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


class DummyStateMgr:
    def __init__(self, assets: list[dict[str, Any]]):
        self._assets = assets

    def get_all_assets(self) -> list[dict[str, Any]]:
        return self._assets


@pytest.mark.asyncio
async def test_ui_payload_contains_schema_version():
    mgr = DummyStateMgr(
        [
            {"symbol": "x", "stats": {"current_price": 1.0}},
        ]
    )
    r = DummyRedis()
    await publish_full_state(mgr, object(), r)
    assert r.published, "Очікується публікація"
    payload = json.loads(r.published[-1])
    meta = payload.get("meta", {})
    assert isinstance(meta.get("schema_version"), str)
