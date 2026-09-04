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
    source: str = zeared.Str(default='')
    kind: str = zeared.Str(default='')
    from_ts: float = zeared.Float(default=0.0)
    to_ts: float = zeared.Float(default=0.0)
    limit: int = zeared.Int(default=100)


@zeared.zeared
class Event(zeared.Message):
    """A row type that doubles as its own query reply."""

    TOPIC = 'test/history/event'
    SCHEMA = '1'
    REQUEST = HistoryRequest

    source: str = zeared.Str(required=True)
    kind: str = zeared.Str(required=True)
    raised_at: float = zeared.Float(required=True)
    note: str = zeared.Str(default='')


@zeared.zeared
class Alarm(zeared.Message):
    """A source contract that normalizes into :class:`Event`."""

    TOPIC = 'test/alarm/{source}'
    source: str = zeared.Str(required=True)
    at: float = zeared.Float(required=True)


@zeared.zeared
class Position(zeared.Message):
    TOPIC = 'test/position'
    source: str = zeared.Str(required=True)
    epc: str = zeared.Str(required=True)
    x: float = zeared.Float(default=0.0)
    observed_at: float = zeared.Float(required=True)


@zeared.zeared
class LastPositionRequest(zeared.Zeared):
    source: str = zeared.Str(default='')
    epc: str = zeared.Str(default='')


@zeared.zeared
class LastPosition(zeared.Message):
    """A reply shaped differently from the row it comes from."""

    TOPIC = 'test/history/position'
    SCHEMA = '1'
    REQUEST = LastPositionRequest

    source: str = zeared.Str(required=True)
    epc: str = zeared.Str(required=True)
    found: bool = zeared.Bool(default=False)
    x: float = zeared.Float(default=0.0)


@zeared.zeared
class ZoneRequest(zeared.Zeared):
    zone: int = zeared.Int(default=0)
    layer: str = zeared.Str(default='department')
    source: str = zeared.Str(default='')
    limit: int = zeared.Int(default=100)


@zeared.zeared
class Placed(zeared.Message):
    """A row that doubles as its own reply, with open-ended zone layers."""

    TOPIC = 'test/placed'
    SCHEMA = '1'
    REQUEST = ZoneRequest

    source: str = zeared.Str(required=True)
    epc: str = zeared.Str(required=True)
    zones: dict[str, int] = zeared.Dict(default_factory=dict)
    observed_at: float = zeared.Float(required=True)


@zeared.zeared
class ZoneOccupant(zeared.Message):
    """A reply shaped differently from the row it comes from."""

    TOPIC = 'test/history/occupant'
    SCHEMA = '1'
    REQUEST = ZoneRequest

    epc: str = zeared.Str(required=True)
    zone: int = zeared.Int(default=0)
    seen_at: float = zeared.Float(default=0.0)


def to_occupant(row: Placed, request: ZoneRequest) -> ZoneOccupant:
    return ZoneOccupant(epc=row.epc, zone=request.zone, seen_at=row.observed_at)


def _zoned_store():
    store = AsyncStore(stored.Store(':memory:', flush_secs=0))
    store.register(
        Placed,
        index=('source',),
        time_field='observed_at',
        latest_key=('source', 'epc'),
        json_index=('zones.department',),
    )
    for epc, dept, at in [('E1', 5, 1000.0), ('E2', 6, 1001.0), ('E1', 5, 2000.0), ('E3', 5, 2001.0)]:
        store.record(Placed, Placed(source='rtls', epc=epc, zones={'department': dept}, observed_at=at))
    store.store.flush()
    return store


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
            LastPosition,
            request=LastPositionRequest(source='rtls', epc='E1'),
            timeout=5.0,
        )
        assert found is not None
        assert (found.found, found.x) == (True, 9.0)  # newest wins

        absent = await zeared.aquery_one(
            LastPosition,
            request=LastPositionRequest(source='rtls', epc='NOPE'),
            timeout=5.0,
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
            Event,
            filters=('source',),
            since='from_ts',
            limit='limit',
            stream=True,
            chunk=4,
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


# -- current state over the mesh --------------------------------------------


async def test_serve_snapshot_answers_one_reply_per_entity(session):
    """The console's question: everything currently in department 5."""
    zeared.session = session
    store, binding = _zoned_store(), None
    try:
        binding = Binding(store, session=session)
        binding.serve_snapshot(Placed, filters={'zone': 'zones.department', 'source': 'source'}, limit='limit')
        await _settle()

        in_five = await zeared.aquery(Placed, request=ZoneRequest(zone=5), timeout=5.0)
        assert sorted(r.epc for r in in_five) == ['E1', 'E3']  # one per entity, not per observation
        assert {r.observed_at for r in in_five} == {2000.0, 2001.0}  # …and the newest of each

        everything = await zeared.aquery(Placed, request=ZoneRequest(), timeout=5.0)
        assert sorted(r.epc for r in everything) == ['E1', 'E2', 'E3']
    finally:
        if binding:
            binding.close()
        await store.close()


async def test_serve_snapshot_projects_when_the_reply_differs(session):
    zeared.session = session
    store, binding = _zoned_store(), None
    try:
        binding = Binding(store, session=session)
        binding.serve_snapshot(ZoneOccupant, of=Placed, filters={'zone': 'zones.department'}, project=to_occupant)
        await _settle()

        occupants = await zeared.aquery(ZoneOccupant, request=ZoneRequest(zone=5), timeout=5.0)
        assert sorted(o.epc for o in occupants) == ['E1', 'E3']
        assert {o.zone for o in occupants} == {5}
    finally:
        if binding:
            binding.close()
        await store.close()


async def test_serve_snapshot_streams(session):
    zeared.session = session
    store, binding = _zoned_store(), None
    try:
        binding = Binding(store, session=session)
        binding.serve_snapshot(Placed, filters={'zone': 'zones.department'}, stream=True, chunk=1)
        await _settle()

        seen = [r.epc async for r in zeared.aquery_iter(Placed, request=ZoneRequest(zone=5), timeout=5.0)]
        assert sorted(seen) == ['E1', 'E3']
    finally:
        if binding:
            binding.close()
        await store.close()


# -- path filters need no new binding API ------------------------------------


async def test_serve_range_filters_on_a_path_with_no_new_api(session):
    """A declared path is just another filter target — the mesh layer does not change."""
    zeared.session = session
    store, binding = _zoned_store(), None
    try:
        binding = Binding(store, session=session)
        binding.serve_range(Placed, filters={'zone': 'zones.department'}, limit='limit')
        await _settle()

        history = await zeared.aquery(Placed, request=ZoneRequest(zone=5), timeout=5.0)
        assert sorted(r.epc for r in history) == ['E1', 'E1', 'E3']  # every observation in that zone
    finally:
        if binding:
            binding.close()
        await store.close()


async def test_filters_resolve_against_the_declaration(session):
    """A target is a column or a path because the stream says so — and neither is a bind-time error."""
    zeared.session = session
    store = _zoned_store()
    binding = Binding(store, session=session)
    try:
        with pytest.raises(ConfigError, match='neither an indexed dimension'):
            binding.serve_range(Placed, filters={'zone': 'zones.aisle'})  # path never declared
        with pytest.raises(ConfigError, match='neither an indexed dimension'):
            binding.serve_snapshot(Placed, filters={'epc': 'epc'})  # column never indexed
    finally:
        binding.close()
        await store.close()


async def test_serve_snapshot_needs_a_projection_it_can_justify(session):
    zeared.session = session
    store = _zoned_store()
    binding = Binding(store, session=session)
    try:
        with pytest.raises(ConfigError, match='needs project'):
            binding.serve_snapshot(ZoneOccupant, of=Placed, filters={'zone': 'zones.department'})
    finally:
        binding.close()
        await store.close()


async def test_serve_snapshot_needs_a_latest_projection(session):
    zeared.session = session
    store = AsyncStore(stored.Store(':memory:', flush_secs=0))
    binding = Binding(store, session=session)
    try:
        store.register(Placed, index=('source',), time_field='observed_at')  # no latest_key
        with pytest.raises(ConfigError, match='no latest projection'):
            binding.serve_snapshot(Placed)
    finally:
        binding.close()
        await store.close()


# -- a target the caller names ----------------------------------------------


async def test_a_computed_target_picks_the_layer_per_request(session):
    """Zone layers are open-ended, so the request names one and the target follows."""
    zeared.session = session
    store = AsyncStore(stored.Store(':memory:', flush_secs=0))
    binding = None
    try:
        store.register(
            Placed,
            index=('source',),
            time_field='observed_at',
            latest_key=('source', 'epc'),
            json_index=('zones.department', 'zones.front_back'),
        )
        store.record(
            Placed, Placed(source='rtls', epc='E1', zones={'department': 5, 'front_back': 1}, observed_at=1000.0)
        )
        store.record(
            Placed, Placed(source='rtls', epc='E2', zones={'department': 6, 'front_back': 1}, observed_at=1001.0)
        )
        store.store.flush()

        binding = Binding(store, session=session)
        binding.serve_snapshot(Placed, filters={'zone': lambda request: f'zones.{request.layer}'})
        await _settle()

        by_department = await zeared.aquery(
            Placed,
            request=ZoneRequest(zone=5, layer='department'),
            timeout=5.0,
        )
        assert [r.epc for r in by_department] == ['E1']

        by_aisle_side = await zeared.aquery(
            Placed,
            request=ZoneRequest(zone=1, layer='front_back'),
            timeout=5.0,
        )
        assert sorted(r.epc for r in by_aisle_side) == ['E1', 'E2']
    finally:
        if binding:
            binding.close()
        await store.close()


async def test_an_undeclared_layer_errors_the_query_rather_than_answering_unfiltered(session, caplog):
    """The trade for a computed target — and its sharp edge, stated exactly.

    An unknown layer raises server-side, so the historian never answers as though the
    filter had been applied. But zeared drops error replies from ``aquery``'s result,
    so a caller that passes no ``on_error`` sees an empty list and cannot tell "nobody
    is there" from "I do not index that layer". Callers that need the difference must
    pass ``on_error``.
    """
    zeared.session = session
    store, binding = _zoned_store(), None
    try:
        binding = Binding(store, session=session)
        binding.serve_snapshot(Placed, filters={'zone': lambda request: f'zones.{request.layer}'})
        await _settle()

        errors: list[Exception] = []
        with caplog.at_level('WARNING'):
            answered = await zeared.aquery(
                Placed,
                request=ZoneRequest(zone=5, layer='aisle'),
                timeout=5.0,
                on_error=lambda exc, _raw: errors.append(exc),
            )

        assert answered == []  # …indistinguishable from "nobody there" without on_error
        assert errors or 'aisle' in caplog.text, 'the failure must reach the caller or the log'
    finally:
        if binding:
            binding.close()
        await store.close()


# -- serve_range projects, like its siblings ---------------------------------


async def test_serve_range_projects_a_row_into_a_different_reply(session):
    zeared.session = session
    store, binding = _zoned_store(), None
    try:
        binding = Binding(store, session=session)
        binding.serve_range(ZoneOccupant, of=Placed, filters={'zone': 'zones.department'}, project=to_occupant)
        await _settle()

        visits = await zeared.aquery(ZoneOccupant, request=ZoneRequest(zone=5), timeout=5.0)
        assert sorted(v.epc for v in visits) == ['E1', 'E1', 'E3']  # every observation, projected
        assert {v.zone for v in visits} == {5}
    finally:
        if binding:
            binding.close()
        await store.close()


async def test_serve_range_needs_a_projection_it_can_justify(session):
    zeared.session = session
    store = _zoned_store()
    binding = Binding(store, session=session)
    try:
        with pytest.raises(ConfigError, match='needs project'):
            binding.serve_range(ZoneOccupant, of=Placed, filters={'zone': 'zones.department'})
    finally:
        binding.close()
        await store.close()


# -- the service's ceiling ---------------------------------------------------


async def test_max_limit_clamps_what_the_caller_asks_for(session):
    """`default_limit` fills an omission; `max_limit` overrules an explicit request."""
    zeared.session = session
    store, binding = _store(), None
    try:
        for i in range(20):
            store.record(Event, Event(source='rtls', kind='a', raised_at=1000.0 + i))
        await store.flush()

        binding = Binding(store, session=session)
        binding.serve_range(Event, filters=('source',), limit='limit', max_limit=5)
        await _settle()

        greedy = await zeared.aquery(Event, request=HistoryRequest(limit=1000), timeout=5.0)
        assert len(greedy) == 5, 'an explicit limit must not talk past the service ceiling'

        modest = await zeared.aquery(Event, request=HistoryRequest(limit=3), timeout=5.0)
        assert len(modest) == 3, 'a request under the ceiling is honoured as asked'
    finally:
        if binding:
            binding.close()
        await store.close()


async def test_max_limit_bounds_a_snapshot_too(session):
    zeared.session = session
    store, binding = _zoned_store(), None
    try:
        binding = Binding(store, session=session)
        binding.serve_snapshot(Placed, filters={'zone': 'zones.department'}, limit='limit', max_limit=1)
        await _settle()

        capped = await zeared.aquery(Placed, request=ZoneRequest(zone=5, limit=100), timeout=5.0)
        assert len(capped) == 1
    finally:
        if binding:
            binding.close()
        await store.close()


async def test_max_limit_bounds_an_omitted_limit_when_streaming(session):
    """Streaming needs a fallback (None means unbounded), and the ceiling is the honest one."""
    zeared.session = session
    store, binding = _store(), None
    try:
        for i in range(20):
            store.record(Event, Event(source='rtls', kind='a', raised_at=1000.0 + i))
        await store.flush()

        binding = Binding(store, session=session)
        binding.serve_range(Event, filters=('source',), stream=True, chunk=2, max_limit=4)
        await _settle()

        streamed = await zeared.aquery(Event, request=HistoryRequest(), timeout=5.0)
        assert len(streamed) == 4
    finally:
        if binding:
            binding.close()
        await store.close()
