import asyncio
import json
from datetime import datetime
from typing import Any

import pytest

from config.config import K_STATS, K_TRIGGER_REASONS
from UI.publish_full_state import publish_full_state
from UI.ui_consumer import UIConsumer


class DummyStateMgr:
    def __init__(self, assets: list[dict[str, Any]]):
        self._assets = assets

    def get_all_assets(self) -> list[dict[str, Any]]:
        return self._assets

    def set_assets(self, assets: list[dict[str, Any]]) -> None:
        self._assets = assets


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
        self._msgs: list[str] = []
        self._queue: asyncio.Queue[str] = asyncio.Queue()

    async def publish(self, channel: str, message: str) -> int:
        self._msgs.append(message)
        await self._queue.put(message)
        return 1

    async def set(self, key: str, value: str) -> object:
        self._snapshot = value
        return object()

    async def expire(self, key: str, ttl: int) -> None:  # pragma: no cover
        return None

    def pubsub(self) -> DummyRedisPubSub:  # type: ignore[override]
        return DummyRedisPubSub(self._queue)

    async def get(self, key: str) -> str | None:
        return self._snapshot


@pytest.mark.asyncio
async def test_ui_streaming_multi_iteration_shows_latest_and_skips_stale():
    # 1) Первинні дані
    mgr = DummyStateMgr(
        [
            {"symbol": "AAA", "signal": "NORMAL", "stats": {"current_price": 1.0}},
        ]
    )
    r = DummyRedis()

    # Публікуємо три оновлення поспіль
    await publish_full_state(mgr, object(), r)  # seq=1

    # 2) Оновлення №2: змінюємо ціну і сигнал
    mgr.set_assets(
        [
            {"symbol": "AAA", "signal": "ALERT_BUY", "stats": {"current_price": 2.0}},
        ]
    )
    await publish_full_state(mgr, object(), r)  # seq=2

    # 3) Оновлення №3: знову змінюємо
    mgr.set_assets(
        [
            {"symbol": "AAA", "signal": "ALERT_SELL", "stats": {"current_price": 3.0}},
        ]
    )
    await publish_full_state(mgr, object(), r)  # seq=3

    # Сконструюємо застаріле повідомлення: перше (seq=1)
    stale_msg = r._msgs[0]

    # Налаштуємо консюмер на наш DummyRedis
    consumer = UIConsumer()
    import UI.ui_consumer as ui_mod

    ui_mod.redis.from_url = lambda *args, **kwargs: r  # type: ignore[assignment]

    async def run_for_short():
        task = asyncio.create_task(consumer.redis_consumer(redis_url="redis://dummy"))
        await asyncio.sleep(0.1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    # Засилаємо спочатку застаріле seq=1 (його слід проігнорувати),
    # а потім актуальне seq=3 (останнє)
    await r._queue.put(stale_msg)
    await r._queue.put(r._msgs[-1])

    await run_for_short()

    # Переконуємося, що останній відображений стан відповідає seq=3 (ALERT_SELL і price≈3.0)
    results = consumer._display_results
    assert results, "UI має показувати останні доступні дані"
    latest = results[0]
    assert latest.get("signal") == "ALERT_SELL"
    assert float(
        latest.get("price", latest.get("stats", {}).get("current_price", 0))
    ) in (3.0,)


def test_signal_table_includes_band_column_with_color() -> None:
    consumer = UIConsumer()
    asset = {
        "symbol": "AAAUSDT",
        "price_str": "1.00",
        "volume_str": "-",
        "atr_pct": 0.01,
        "rsi": 55.0,
        "status": "normal",
        "signal": "ALERT_BUY",
        "confidence": 0.42,
        "recommendation": "BUY_IN_DIPS",
        "tp_sl": "-",
        "analytics": {"corridor_band_pct": 0.012},
        K_STATS: {},
        K_TRIGGER_REASONS: [],
    }

    band_repr = consumer._format_band_pct(asset)
    assert band_repr == "[yellow]1.20%[/]"

    table = consumer._build_signal_table([asset])
    headers = [column.header for column in table.columns]
    assert "Band%" in headers


@pytest.mark.asyncio
async def test_ui_accepts_sequence_reset_after_publisher_restart(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import UI.publish_full_state as publisher_mod
    import UI.ui_consumer as ui_mod

    old_assets = [
        {"symbol": "oldusdt", "signal": "NORMAL", "stats": {"current_price": 1.0}}
    ]
    redis_backend = DummyRedis()
    manager = DummyStateMgr(old_assets)

    publisher_mod._SEQ = 540
    await publish_full_state(manager, object(), redis_backend)

    assert redis_backend._snapshot is not None
    old_payload = json.loads(redis_backend._snapshot)
    old_payload.setdefault("meta", {})["ts"] = "2000-01-01T00:00:00Z"
    redis_backend._snapshot = json.dumps(old_payload)

    publisher_mod._SEQ = 0
    manager.set_assets(
        [{"symbol": "newusdt", "signal": "ALERT_BUY", "stats": {"current_price": 2.0}}]
    )
    await publish_full_state(manager, object(), redis_backend)
    new_message = redis_backend._msgs[-1]

    redis_backend._queue = asyncio.Queue()
    await redis_backend._queue.put(new_message)
    redis_backend._msgs = [new_message]
    redis_backend._snapshot = json.dumps(old_payload)

    monkeypatch.setattr(ui_mod.redis, "from_url", lambda *args, **kwargs: redis_backend)

    consumer = UIConsumer()
    run_task = asyncio.create_task(
        consumer.redis_consumer(
            redis_url="redis://dummy",
            loading_delay=0.0,
            smooth_delay=0.0,
        )
    )
    await asyncio.sleep(0.15)
    run_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run_task

    assert consumer._display_results, "Очікуємо принаймні один актив після reset"
    latest = consumer._display_results[0]
    assert latest.get("symbol") == "NEWUSDT"
    assert consumer._last_seq == 1
    old_ts = datetime.fromisoformat("2000-01-01T00:00:00+00:00").timestamp()
    assert consumer.last_update_time >= old_ts

    publisher_mod._SEQ = 0
