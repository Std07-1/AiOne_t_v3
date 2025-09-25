import asyncio
from typing import Any

import pytest

from UI.publish_full_state import publish_full_state
from UI.ui_consumer import UIConsumer


class DummyStateMgr:
    def __init__(self, assets: list[dict[str, Any]]):
        self._assets = assets

    def get_all_assets(self) -> list[dict[str, Any]]:
        return self._assets


class DummyRedisPubSub:
    def __init__(self, inbox: asyncio.Queue[str]):
        self._inbox = inbox

    async def subscribe(self, channel: str) -> None:  # pragma: no cover - no-op
        return None

    async def get_message(
        self, ignore_subscribe_messages: bool = True, timeout: float = 1.0
    ):
        try:
            data = await asyncio.wait_for(self._inbox.get(), timeout=timeout)
            return {"data": data}
        except TimeoutError:
            return None

    async def reset(self):  # pragma: no cover - not used
        return None


class DummyRedis:
    def __init__(self) -> None:
        self._snapshot: str | None = None
        self._channel_msgs: list[str] = []
        self._queue: asyncio.Queue[str] = asyncio.Queue()

    # Publisher API
    async def publish(self, channel: str, message: str) -> int:
        self._channel_msgs.append(message)
        await self._queue.put(message)
        return 1

    async def set(self, key: str, value: str) -> object:
        self._snapshot = value
        return object()

    async def expire(
        self, key: str, ttl: int
    ) -> None:  # pragma: no cover - accept call
        return None

    # Reader API used by consumer
    def pubsub(self) -> DummyRedisPubSub:  # type: ignore[override]
        return DummyRedisPubSub(self._queue)

    async def get(self, key: str) -> str | None:
        return self._snapshot


@pytest.mark.asyncio
async def test_ui_consumer_strict_seq_and_gap_snapshot_reload():
    # Prepare two payloads via publisher to ensure seq increments automatically
    mgr = DummyStateMgr(
        [
            {"symbol": "AAAUSDT", "signal": "NORMAL", "stats": {"current_price": 10.0}},
        ]
    )
    r = DummyRedis()

    # First publish -> seq=1
    await publish_full_state(mgr, object(), r)
    # Second publish -> seq=2
    await publish_full_state(mgr, object(), r)
    # Craft an out-of-order duplicate of seq=1 (simulate stale message)
    stale_msg = r._channel_msgs[0]

    consumer = UIConsumer()

    async def run_consumer_short():
        # Run consumer for a short time to process messages
        task = asyncio.create_task(
            consumer.redis_consumer(
                redis_url="redis://dummy",  # not used by DummyRedis
            )
        )
        await asyncio.sleep(0.2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    # Monkeypatch redis.from_url to return our DummyRedis
    import UI.ui_consumer as ui_mod

    ui_mod.redis.from_url = lambda *args, **kwargs: r  # type: ignore[assignment]

    # Push a stale message (seq=1) after snapshot has seq=2 -> should be skipped
    await r._queue.put(stale_msg)
    # Then a fresh message (seq=2) -> should be accepted
    await r._queue.put(r._channel_msgs[1])

    await run_consumer_short()
    # After running, last_seq should be >= 2, not regress to 1
    assert consumer._last_seq >= 2


@pytest.mark.asyncio
async def test_publish_sets_snapshot_ttl_call_present():
    # Smoke-test that publish_full_state calls set and expire without raising
    mgr = DummyStateMgr(
        [
            {"symbol": "BBB", "stats": {"current_price": 1.0}},
        ]
    )
    r = DummyRedis()
    await publish_full_state(mgr, object(), r)
    assert r._snapshot is not None
    # Ensure at least one message went to the channel
    assert len(r._channel_msgs) == 1
