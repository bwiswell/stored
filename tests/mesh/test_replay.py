"""Replay: recorded history republished as ordinary mesh traffic."""
from __future__ import annotations

import asyncio

import pytest
import zeared

import stored
from stored.errors import ConfigError
from stored.mesh import AsyncStore, Binding, Replayer


@zeared.zeared
class Reading(zeared.Message):
    """A replayable contract: the replay scope is a declared template."""

    TOPIC = 'live/reading/{source}'
    EXTRA_TOPICS = ('replay/reading/{source}',)

    source:      str   = zeared.Str(required=True)
    value:       float = zeared.Float(default=0.0)
    observed_at: float = zeared.Float(required=True)


@zeared.zeared
class Plain(zeared.Message):
    """No declared replay scope."""

    TOPIC = 'live/plain'
    value: float = zeared.Float(default=0.0)


async def _settle(seconds: float = 0.25):
    await asyncio.sleep(seconds)


def _filled(count: int = 6, *, step: float = 1.0):
    store = AsyncStore(stored.Store(':memory:', flush_secs=0))
    store.register(Reading, index=('source',), time_field='observed_at')
    for i in range(count):
        store.record(Reading, Reading(source='rtls', value=float(i), observed_at=1000.0 + i * step))
    store.store.flush()
    return store


async def test_replay_reaches_an_ordinary_subscriber(session):
    """The promise of model 3: no special client — the normal subscribe path sees it."""
    zeared.session = session
    store = _filled()
    seen: list[Reading] = []
    sub = Reading.on_message(seen.append, session=session)
    try:
        await _settle()
        sent = await Replayer(store, session=session).replay(Reading)
        await _settle()

        assert sent == 6
        assert [r.value for r in seen] == [float(i) for i in range(6)]
        assert {r.source for r in seen} == {'rtls'}
    finally:
        sub.close()
        await store.close()


async def test_replay_is_not_re_recorded_by_a_live_only_recorder(session):
    """The isolation the scope buys: a historian keeps recording live, and ignores the replay."""
    zeared.session = session
    source_store = _filled(4)
    sink = AsyncStore(stored.Store(':memory:', flush_secs=0))
    sink.register(Reading, index=('source',), time_field='observed_at')
    binding = Binding(sink, session=session)
    try:
        binding.record(Reading, live_only=True)
        await _settle()

        # Live traffic is recorded…
        Reading(source='rtls', value=99.0, observed_at=2000.0).send()
        await _settle()
        assert [r.value for r in await sink.query(Reading)] == [99.0]

        # …and a replay of four rows reaches subscribers without landing in the store.
        await Replayer(source_store, session=session).replay(Reading)
        await _settle()
        assert [r.value for r in await sink.query(Reading)] == [99.0]
    finally:
        binding.close()
        await sink.close()
        await source_store.close()


async def test_replay_without_live_only_is_recorded_again(session):
    """The hazard the flag exists for, pinned so the default's cost is explicit."""
    zeared.session = session
    source_store = _filled(3)
    sink = AsyncStore(stored.Store(':memory:', flush_secs=0))
    sink.register(Reading, index=('source',), time_field='observed_at')
    binding = Binding(sink, session=session)
    try:
        binding.record(Reading)  # records every declared scope
        await _settle()

        await Replayer(source_store, session=session).replay(Reading)
        await _settle()

        assert len(await sink.query(Reading)) == 3
    finally:
        binding.close()
        await sink.close()
        await source_store.close()


async def test_replay_window_and_limit(session):
    zeared.session = session
    store = _filled(8)
    seen: list[Reading] = []
    sub = Reading.on_message(seen.append, session=session)
    try:
        await _settle()
        replayer = Replayer(store, session=session)

        assert await replayer.replay(Reading, limit=2) == 2
        await _settle()
        assert [r.value for r in seen] == [0.0, 1.0]

        seen.clear()
        assert await replayer.replay(Reading, since=1005.0) == 3
        await _settle()
        assert [r.value for r in seen] == [5.0, 6.0, 7.0]
    finally:
        sub.close()
        await store.close()


async def test_paced_replay_follows_the_recorded_gaps(session):
    """speed>0 sleeps the (scaled) gap the recording actually had."""
    zeared.session = session
    store = _filled(4, step=1.0)  # one second between rows
    seen: list[float] = []
    sub = Reading.on_message(lambda _msg: seen.append(asyncio.get_running_loop().time()), session=session)
    try:
        await _settle()
        started = asyncio.get_running_loop().time()
        sent = await Replayer(store, session=session).replay(Reading, speed=20.0)  # 1s gaps → 50ms
        elapsed = asyncio.get_running_loop().time() - started

        assert sent == 4
        assert elapsed >= 0.1, 'a paced replay must not run flat out'
        assert elapsed < 2.0, 'pacing should scale by speed, not ignore it'
    finally:
        sub.close()
        await store.close()


async def test_max_rate_bounds_an_unpaced_replay(session):
    zeared.session = session
    store = _filled(4)
    try:
        started = asyncio.get_running_loop().time()
        sent = await Replayer(store, session=session).replay(Reading, max_rate=40.0)
        elapsed = asyncio.get_running_loop().time() - started

        assert sent == 4
        assert elapsed >= 0.05
    finally:
        await store.close()


async def test_start_returns_a_handle_that_reports_and_stops(session):
    zeared.session = session
    store = _filled(40, step=1.0)
    try:
        handle = Replayer(store, session=session).start(Reading, speed=1.0)  # 1s gaps, real time
        await asyncio.sleep(0.3)
        handle.stop()
        sent = await handle.wait()

        assert handle.done
        assert sent == handle.sent
        assert sent < 40, 'stop() should end the replay early'
    finally:
        await store.close()


async def test_replay_needs_a_destination_it_can_justify(session):
    zeared.session = session
    store = AsyncStore(stored.Store(':memory:', flush_secs=0))
    store.register(Plain)
    store.register(Reading, time_field='observed_at')
    try:
        replayer = Replayer(store, session=session)
        with pytest.raises(ConfigError, match='no replay scope'):
            await replayer.replay(Plain)
        # …but the live topic is available when asked for deliberately.
        assert await replayer.replay(Plain, topic=Plain.TOPIC) == 0
    finally:
        await store.close()


async def test_pacing_requires_an_event_time(session):
    zeared.session = session
    store = AsyncStore(stored.Store(':memory:', flush_secs=0))
    store.register(Reading, index=('source',))  # registered without time_field
    try:
        with pytest.raises(ConfigError, match='pacing needs an event time'):
            await Replayer(store, session=session).replay(Reading, speed=1.0)
    finally:
        await store.close()


async def test_live_only_needs_a_literal_scope(session):
    zeared.session = session
    store = AsyncStore(stored.Store(':memory:', flush_secs=0))
    try:
        store.register(Reading, time_field='observed_at')
        binding = Binding(store, session=session)

        @zeared.zeared
        class Slotted(zeared.Message):
            TOPIC = '{source}/slotted'
            source: str = zeared.Str(required=True)

        store.register(Slotted)
        with pytest.raises(ConfigError, match='no literal scope'):
            binding.record(Slotted, live_only=True)
        binding.close()
    finally:
        await store.close()
