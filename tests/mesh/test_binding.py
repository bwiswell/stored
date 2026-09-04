"""The mesh binding: subscribe-and-record, typed range queries, and last-known reads."""
from __future__ import annotations

import asyncio

import pytest
import zeared

import stored
from stored.errors import ConfigError
from stored.mesh import AsyncStore, Binding

# -- contracts (deliberately generic: the binding knows no fleet's vocabulary) --

@zeared.zeared
class HistoryRequest(zeared.Zeared):
    source:  str   = zeared.Str(default='')
    kind:    str   = zeared.Str(default='')
    from_ts: float = zeared.Float(default=0.0)
    to_ts:   float = zeared.Float(default=0.0)
    limit:   int   = zeared.Int(default=100)


@zeared.zeared
class Event(zeared.Message):
    """A row type that doubles as its own query reply."""

    TOPIC = 'test/history/event'
    SCHEMA = '1'
    REQUEST = HistoryRequest

    source:   str   = zeared.Str(required=True)
    kind:     str   = zeared.Str(required=True)
    raised_at: float = zeared.Float(required=True)
    note:     str   = zeared.Str(default='')


@zeared.zeared
class Alarm(zeared.Message):
    """A source contract that normalizes into :class:`Event`."""

    TOPIC = 'test/alarm/{source}'
    source: str   = zeared.Str(required=True)
    at:     float = zeared.Float(required=True)


@zeared.zeared
class Position(zeared.Message):
    TOPIC = 'test/position'
    source:      str   = zeared.Str(required=True)
    epc:         str   = zeared.Str(required=True)
    x:           float = zeared.Float(default=0.0)
    observed_at: float = zeared.Float(required=True)


@zeared.zeared
class LastPositionRequest(zeared.Zeared):
    source: str = zeared.Str(default='')
    epc:    str = zeared.Str(default='')


@zeared.zeared
class LastPosition(zeared.Message):
    """A reply shaped differently from the row it comes from."""

    TOPIC = 'test/history/position'
    SCHEMA = '1'
    REQUEST = LastPositionRequest

    source: str   = zeared.Str(required=True)
    epc:    str   = zeared.Str(required=True)
    found:  bool  = zeared.Bool(default=False)
    x:      float = zeared.Float(default=0.0)


def from_alarm(alarm: Alarm) -> Event:
    """The caller-owned normalizer — domain logic that happens to run on the record path."""
    return Event(source=alarm.source, kind='alarm', raised_at=alarm.at, note='mapped')


def to_last_position(row: Position, request: LastPositionRequest) -> LastPosition:
    return LastPosition(source=request.source, epc=request.epc, found=True, x=row.x)


def no_position(request: LastPositionRequest) -> LastPosition:
    return LastPosition(source=request.source, epc=request.epc, found=False)


# -- helpers -----------------------------------------------------------------

class _CountingBackend:
    """Delegates to a real backend, counting ``select`` calls (page-count proof)."""

    def __init__(self, inner):
        self.inner = inner
        self.selects = 0

    def select(self, sql, params=()):
        self.selects += 1
        return self.inner.select(sql, params)

    def __getattr__(self, name):
        return getattr(self.inner, name)



async def _settle(seconds: float = 0.25):
    await asyncio.sleep(seconds)


def _store():
    store = AsyncStore(stored.Store(':memory:', flush_secs=0))
    store.register(Event, index=('source', 'kind'), time_field='raised_at')
    store.register(
        Position,
        index=('source', 'epc'),
        time_field='observed_at',
        latest_key=('source', 'epc'),
    )
    return store


# -- record ------------------------------------------------------------------

async def test_record_persists_a_subscribed_contract(session):
    zeared.session = session
    store, binding = _store(), None
    try:
        binding = Binding(store, session=session)
        binding.record(Position)
        await _settle()

        Position(source='rtls', epc='E1', x=1.5, observed_at=1000.0).send()
        await _settle()

        rows = await store.query(Position)
        assert [(r.epc, r.x) for r in rows] == [('E1', 1.5)]
    finally:
        if binding:
            binding.close()
        await store.close()


async def test_record_normalizes_one_contract_into_another(session):
    zeared.session = session
    store, binding = _store(), None
    try:
        binding = Binding(store, session=session)
        binding.record(Alarm, store_as=Event, via=from_alarm)
        await _settle()

        Alarm(source='apollo', at=2000.0).send()
        await _settle()

        rows = await store.query(Event)
        assert [(r.source, r.kind, r.raised_at, r.note) for r in rows] == [('apollo', 'alarm', 2000.0, 'mapped')]
    finally:
        if binding:
            binding.close()
        await store.close()


# -- serve_range -------------------------------------------------------------

async def test_serve_range_answers_a_typed_request(session):
    zeared.session = session
    store, binding = _store(), None
    try:
        for i, (source, kind) in enumerate([('rtls', 'a'), ('rtls', 'b'), ('apollo', 'a')]):
            store.record(Event, Event(source=source, kind=kind, raised_at=1000.0 + i))
        await store.flush()

        binding = Binding(store, session=session)
        binding.serve_range(Event, filters=('source', 'kind'), since='from_ts', until='to_ts', limit='limit')
        await _settle()

        everything = await zeared.aquery(Event, request=HistoryRequest(), timeout=5.0)
        assert len(everything) == 3

        one_source = await zeared.aquery(Event, request=HistoryRequest(source='rtls'), timeout=5.0)
        assert {r.kind for r in one_source} == {'a', 'b'}

        narrowed = await zeared.aquery(Event, request=HistoryRequest(source='rtls', kind='b'), timeout=5.0)
        assert [r.raised_at for r in narrowed] == [1001.0]

        windowed = await zeared.aquery(Event, request=HistoryRequest(from_ts=1001.0), timeout=5.0)
        assert {r.raised_at for r in windowed} == {1001.0, 1002.0}

        capped = await zeared.aquery(Event, request=HistoryRequest(limit=1), timeout=5.0)
        assert len(capped) == 1
    finally:
        if binding:
            binding.close()
        await store.close()


async def test_serve_range_sentinel_policy_is_configurable(session):
    """`''` means "unfiltered" by default — but a fleet may spell absence strictly."""
    zeared.session = session
    store, binding = _store(), None
    try:
        store.record(Event, Event(source='', kind='a', raised_at=1000.0))
        store.record(Event, Event(source='rtls', kind='a', raised_at=1001.0))
        await store.flush()

        binding = Binding(store, session=session)
        binding.serve_range(Event, filters=('source',), unset=(None,))  # strict: '' is a real value
        await _settle()

        strict = await zeared.aquery(Event, request=HistoryRequest(source=''), timeout=5.0)
        assert [r.raised_at for r in strict] == [1000.0]
    finally:
        if binding:
            binding.close()
        await store.close()


# -- serve_latest ------------------------------------------------------------

async def test_serve_latest_projects_a_row_into_its_reply(session):
    zeared.session = session
    store, binding = _store(), None
    try:
        store.record(Position, Position(source='rtls', epc='E1', x=1.0, observed_at=1000.0))
        store.record(Position, Position(source='rtls', epc='E1', x=9.0, observed_at=2000.0))
        await store.flush()

        binding = Binding(store, session=session)
        binding.serve_latest(
            LastPosition,
            of=Position,
            key=('source', 'epc'),
            project=to_last_position,
            missing=no_position,
        )
        await _settle()

        found = await zeared.aquery_one(
            LastPosition, request=LastPositionRequest(source='rtls', epc='E1'), timeout=5.0,
        )
        assert found is not None
        assert (found.found, found.x) == (True, 9.0)  # newest wins

        absent = await zeared.aquery_one(
            LastPosition, request=LastPositionRequest(source='rtls', epc='NOPE'), timeout=5.0,
        )
        assert absent is not None
        assert absent.found is False
    finally:
        if binding:
            binding.close()
        await store.close()


async def test_serve_latest_identity_projection_for_a_double_duty_row(session):
    zeared.session = session
    store = AsyncStore(stored.Store(':memory:', flush_secs=0))
    binding = None
    try:
        store.register(Event, index=('source',), time_field='raised_at', latest_key=('source',))
        store.record(Event, Event(source='rtls', kind='a', raised_at=1000.0, note='old'))
        store.record(Event, Event(source='rtls', kind='b', raised_at=2000.0, note='new'))
        await store.flush()

        binding = Binding(store, session=session)
        binding.serve_latest(Event, key={'source': 'source'})  # no `of`, no `project`
        await _settle()

        newest = await zeared.aquery_one(Event, request=HistoryRequest(source='rtls'), timeout=5.0)
        assert newest is not None
        assert newest.note == 'new'
    finally:
        if binding:
            binding.close()
        await store.close()


# -- bind-time validation ----------------------------------------------------

async def test_binding_rejects_incoherent_declarations(session):
    zeared.session = session
    store = _store()
    binding = Binding(store, session=session)
    try:
        with pytest.raises(ConfigError, match='not registered'):
            binding.record(Alarm)  # Alarm itself was never registered
        with pytest.raises(ConfigError, match='needs via'):
            binding.record(Alarm, store_as=Event)
        with pytest.raises(ConfigError, match='no latest projection'):
            binding.serve_latest(Event, key=('source',))  # Event has no latest_key here
        with pytest.raises(ConfigError, match='needs project'):
            binding.serve_latest(LastPosition, of=Position, key=('source', 'epc'))
    finally:
        binding.close()
        await store.close()


async def test_binding_requires_a_request_payload_type(session):
    zeared.session = session
    store = AsyncStore(stored.Store(':memory:', flush_secs=0))
    try:
        store.register(Alarm, index=('source',))
        binding = Binding(store, session=session)
        with pytest.raises(ConfigError, match='no REQUEST'):
            binding.serve_range(Alarm)  # Alarm declares no REQUEST
        binding.close()
    finally:
        await store.close()


async def test_close_releases_every_handle(session):
    zeared.session = session
    store, binding = _store(), None
    try:
        binding = Binding(store, session=session)
        binding.record(Position)
        binding.serve_range(Event, filters=('source',))
        await _settle()

        binding.close()
        binding.close()  # idempotent
        await _settle()

        Position(source='rtls', epc='E9', x=1.0, observed_at=3000.0).send()
        await _settle()
        assert await store.query(Position) == []  # the subscriber is gone
    finally:
        await store.close()


# -- streamed replies --------------------------------------------------------

async def test_streamed_range_matches_the_collected_one(session):
    """Same replies, produced lazily — streaming is not a contract change."""
    zeared.session = session
    store, binding = _store(), None
    try:
        for i in range(12):
            store.record(Event, Event(source='rtls', kind='a', raised_at=1000.0 + i))
        await store.flush()

        binding = Binding(store, session=session)
        binding.serve_range(
            Event, filters=('source',), since='from_ts', limit='limit', stream=True, chunk=4,
        )
        await _settle()

        collected = await zeared.aquery(Event, request=HistoryRequest(source='rtls'), timeout=5.0)
        assert [r.raised_at for r in collected] == [1000.0 + i for i in range(12)]

        windowed = await zeared.aquery(Event, request=HistoryRequest(from_ts=1008.0), timeout=5.0)
        assert [r.raised_at for r in windowed] == [1008.0, 1009.0, 1010.0, 1011.0]
    finally:
        if binding:
            binding.close()
        await store.close()


async def test_streamed_range_reaches_a_streaming_getter(session):
    """The end-to-end shape: a generator handler feeding ``aquery_iter``."""
    zeared.session = session
    store, binding = _store(), None
    try:
        for i in range(9):
            store.record(Event, Event(source='rtls', kind='a', raised_at=1000.0 + i))
        await store.flush()

        binding = Binding(store, session=session)
        binding.serve_range(Event, filters=('source',), stream=True, chunk=3)
        await _settle()

        seen = [row.raised_at async for row in zeared.aquery_iter(Event, request=HistoryRequest(), timeout=5.0)]
        assert sorted(seen) == [1000.0 + i for i in range(9)]
    finally:
        if binding:
            binding.close()
        await store.close()


async def test_streamed_range_reads_page_by_page(session):
    """The point of streaming: the historian never materializes the window."""
    zeared.session = session
    store, binding = _store(), None
    try:
        for i in range(20):
            store.record(Event, Event(source='rtls', kind='a', raised_at=1000.0 + i))
        await store.flush()

        counting = _CountingBackend(store.store._backend)
        store.store._backend = counting
        binding = Binding(store, session=session)
        binding.serve_range(Event, filters=('source',), stream=True, chunk=5)
        await _settle()

        rows = await zeared.aquery(Event, request=HistoryRequest(source='rtls'), timeout=5.0)
        assert len(rows) == 20
        assert counting.selects >= 4, 'a streamed range should read in pages, not one select'
    finally:
        store.store._backend = counting.inner
        if binding:
            binding.close()
        await store.close()


async def test_streamed_range_is_capped_by_default(session):
    """An abandoned getter does not stop the queryable, so the stream carries its own bound."""
    zeared.session = session
    store, binding = _store(), None
    try:
        for i in range(10):
            store.record(Event, Event(source='rtls', kind='a', raised_at=1000.0 + i))
        await store.flush()

        binding = Binding(store, session=session)
        binding.serve_range(Event, filters=('source',), stream=True, chunk=2, default_limit=3)
        await _settle()

        rows = await zeared.aquery(Event, request=HistoryRequest(source='rtls'), timeout=5.0)
        assert len(rows) == 3
    finally:
        if binding:
            binding.close()
        await store.close()
