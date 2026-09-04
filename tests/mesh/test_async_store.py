import asyncio

import pytest
import seared as s

import stored
from stored.mesh import AsyncStore


@s.seared
class Msg(s.Seared):
    id:   int = s.Int(required=True)
    name: str = s.Str(default='')


def _astore(tmp_path, **kwargs):
    return AsyncStore(stored.Store(str(tmp_path / 'a.db'), flush_secs=0, **kwargs))


async def test_round_trip(tmp_path):
    store = _astore(tmp_path)
    try:
        store.register(Msg, index=('id',), retention='7d', latest_key=('id',))
        for i in range(3):
            store.record(Msg, Msg(id=i, name=f'n{i}'))
        await store.flush()

        assert [m.id for m in await store.query(Msg)] == [0, 1, 2]
        newest = await store.latest(Msg, id=1)
        assert newest is not None
        assert newest.name == 'n1'
        assert await store.counts(Msg) == (3, 3)
        assert await store.prune() == 0
    finally:
        await store.close()


async def test_iter_streams_every_row(tmp_path):
    store = _astore(tmp_path)
    try:
        store.register(Msg, index=('id',))
        for i in range(20):
            store.record(Msg, Msg(id=i))
        await store.flush()

        assert [m.id async for m in store.iter(Msg, chunk=6)] == list(range(20))
        assert [m.id async for m in store.iter(Msg, chunk=6, limit=4)] == [0, 1, 2, 3]
        assert [m.id async for m in store.iter(Msg, id=7)] == [7]
    finally:
        await store.close()


async def test_iter_yields_the_loop_between_pages(tmp_path):
    """The point of the facade: a long walk must not stall the service around it."""
    store = _astore(tmp_path)
    try:
        store.register(Msg, index=('id',))
        for i in range(50):
            store.record(Msg, Msg(id=i))
        await store.flush()

        ticks = 0

        async def ticker():
            nonlocal ticks
            while True:  # noqa: ASYNC110 — a cooperative-scheduling probe, not a poll
                await asyncio.sleep(0)
                ticks += 1

        task = asyncio.create_task(ticker())
        rows = [m async for m in store.iter(Msg, chunk=5)]
        task.cancel()

        assert len(rows) == 50
        assert ticks > 0, 'the walk never yielded control to the loop'
    finally:
        await store.close()


async def test_iter_can_be_abandoned_midway(tmp_path):
    store = _astore(tmp_path)
    try:
        store.register(Msg, index=('id',))
        for i in range(30):
            store.record(Msg, Msg(id=i))
        await store.flush()

        walk = store.iter(Msg, chunk=4)
        first: list[int] = []
        async for row in walk:
            first.append(row.id)
            if len(first) == 3:
                break
        await walk.aclose()  # closing the async generator closes the sync walk beneath it

        assert first == [0, 1, 2]
    finally:
        await store.close()


async def test_iter_flushes_when_iteration_starts(tmp_path):
    """The documented divergence from ``Store.iter``: an async generator acts on first step."""
    store = _astore(tmp_path)
    try:
        store.register(Msg, index=('id',))
        store.record(Msg, Msg(id=1))
        walk = store.iter(Msg)              # nothing has run yet
        store.record(Msg, Msg(id=2))        # …so this is still ahead of the flush
        assert [m.id async for m in walk] == [1, 2]
    finally:
        await store.close()


async def test_store_property_is_the_escape_hatch(tmp_path):
    store = _astore(tmp_path)
    try:
        store.register(Msg, index=('id',))
        assert isinstance(store.store, stored.Store)
        assert store.store.registry.get(Msg).table == 'stream_msg'
    finally:
        await store.close()


async def test_query_errors_propagate(tmp_path):
    store = _astore(tmp_path)
    try:
        store.register(Msg, index=('id',))
        with pytest.raises(stored.QueryError):
            await store.query(Msg, name='nope')  # not an indexed dimension
    finally:
        await store.close()


async def test_current_state_reads_await_off_the_loop(tmp_path):
    """The console's question, asked from a service: one row per entity, never blocking."""
    store = _astore(tmp_path)
    try:
        store.register(Msg, index=('id',), latest_key=('id',))
        for i in range(12):
            store.record(Msg, Msg(id=i, name=f'n{i}'))
            store.record(Msg, Msg(id=i, name=f'n{i}-newer'))
        await store.flush()

        rows = await store.query_latest(Msg)
        assert len(rows) == 12
        assert all(r.name.endswith('-newer') for r in rows)
        assert [r.id for r in rows] == list(range(12))

        streamed = [r.id async for r in store.iter_latest(Msg, chunk=5)]
        assert streamed == list(range(12))
        assert [r.id async for r in store.iter_latest(Msg, id=7)] == [7]
    finally:
        await store.close()
