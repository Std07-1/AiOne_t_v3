import json
from typing import Any

import pytest

from config.config import REDIS_CHANNEL_ASSET_STATE, REDIS_SNAPSHOT_KEY
from UI.publish_full_state import publish_full_state


class FakeRedis:
    def __init__(self) -> None:
        self.published: list[tuple[str, str]] = []
        self.kv: dict[str, str] = {}

    async def publish(self, channel: str, message: str) -> int:
        self.published.append((channel, message))
        return 1

    async def set(self, key: str, value: str) -> bool:
        self.kv[key] = value
        return True


class FakeStateManager:
    def __init__(self, assets: list[dict[str, Any]]) -> None:
        self._assets = assets

    def get_all_assets(self) -> list[dict[str, Any]]:
        return self._assets


@pytest.mark.asyncio
async def test_publish_full_state_smoke() -> None:
    assets = [
        {
            "symbol": "btcusdt",
            "stats": {"current_price": 50000.0, "volume_mean": 10.0, "rsi": 55},
            "tp": 51000.0,
            "sl": 49000.0,
        }
    ]

    redis = FakeRedis()
    sm = FakeStateManager(assets)

    await publish_full_state(sm, object(), redis)

    # Перевіряємо, що було опубліковано повідомлення і записано снапшот
    assert redis.published, "Очікувався хоча б один publish у Redis"
    ch, msg = redis.published[-1]
    assert ch == REDIS_CHANNEL_ASSET_STATE

    payload = json.loads(msg)
    assert payload["type"] == REDIS_CHANNEL_ASSET_STATE
    assert payload["counters"]["assets"] == 1
    assert len(payload["assets"]) == 1
    assert payload["assets"][0]["symbol"] == "BTCUSDT"  # нормалізація у UPPER
    assert "ts" in payload["meta"] and payload["meta"]["ts"].endswith("Z")

    assert REDIS_SNAPSHOT_KEY in redis.kv
    snap = json.loads(redis.kv[REDIS_SNAPSHOT_KEY])
    assert snap["counters"]["assets"] == 1
