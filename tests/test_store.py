import datetime

import pytest
import seared as s

from stored import Store
from stored.errors import ConfigError, QueryError


@s.seared
class Msg(s.Seared):
    id: int = s.Int(required=True)
    name: str = s.Str(default='')


@s.seared
class Obs(s.Seared):
    id: int = s.Int(required=True)
    observed_at: float = s.Float(required=True)
    label: str = s.Str(default='')


@s.seared
class Nullable(s.Seared):
    id: int = s.Int(required=True)
    observed_at: float | None = s.Float(default=None)


@s.seared
class Zoned(s.Seared):
    id: int = s.Int(required=True)
    zones: dict = s.Dict(data_key='zn', default_factory=dict)  # aliased on the wire
    observed_at: float = s.Float(required=True)


class FixedMeta:
    """Meta with a fixed (key_expr, timestamp) — drives dedup on the PK."""

    key_expr = 'k'
    timestamp = 't1'
    issued_at = None
    source_info = None
    schema = None


def test_register_creates_stream(tmp_path):
    store = Store(str(tmp_path / 'c.duckdb'))
    try:
        stream = store.register(Msg, retention='1d', index=('id',))
        assert stream.table == 'stream_msg'
        assert store.registry.get(Msg).retention == '1d'
    finally:
        store.close()


def test_register_accepts_seconds_and_timedeltas(tmp_path):
    store = Store(str(tmp_path / 'c.duckdb'))
    try:
        stream = store.register(
            Obs,
            retention=datetime.timedelta(hours=36),
            time_field='observed_at',
            latest_key=('id',),
            latest_retention=90 * 86400,
        )
        assert stream.retention == '129600s'
        assert stream.latest_retention == '7776000s'
    finally:
        store.close()


def test_register_rejects_bad_horizon_units(tmp_path):
    store = Store(str(tmp_path / 'c.duckdb'), flush_secs=0)
    try:
        with pytest.raises(ConfigError):
            store.register(Msg, retention=-5)
    finally:
        store.close()


def test_reads_return_the_queried_class(tmp_path):
    """``query``/``latest`` answer with ``cls`` itself — the contract ``ty`` is told about."""
    store = Store(str(tmp_path / 'c.duckdb'))
    try:
        store.register(Obs, time_field='observed_at', latest_key=('id',))
        store.record(Obs, Obs(id=1, observed_at=1000.0))

        rows = store.query(Obs)
        assert [type(row) for row in rows] == [Obs]
        newest = store.latest(Obs, id=1)
        assert type(newest) is Obs
        assert newest.observed_at == 1000.0
    finally:
        store.close()


def test_record_and_query_round_trip(tmp_path):
    store = Store(str(tmp_path / 'c.duckdb'))
    try:
        store.register(Msg, index=('id',))
        store.record(Msg, Msg(id=1, name='a'))
        store.record(Msg, Msg(id=2, name='b'))

        rows = store.query(Msg)
        assert [r.id for r in rows] == [1, 2]
        assert {r.name for r in rows} == {'a', 'b'}
    finally:
        store.close()


def test_query_field_filter(tmp_path):
    store = Store(str(tmp_path / 'c.duckdb'))
    try:
        store.register(Msg, index=('id',))
        store.record(Msg, Msg(id=1, name='a'))
        store.record(Msg, Msg(id=2, name='b'))

        only = store.query(Msg, id=2)
        assert len(only) == 1
        assert only[0].id == 2
    finally:
        store.close()


def test_query_limit_and_order(tmp_path):
    store = Store(str(tmp_path / 'c.duckdb'))
    try:
        store.register(Msg)
        for i in range(5):
            store.record(Msg, Msg(id=i))
        latest = store.query(Msg, order='desc', limit=2)
        assert [r.id for r in latest] == [4, 3]
    finally:
        store.close()


def test_query_range_keys_on_event_time(tmp_path):
    # Both rows are recorded *now*; only observed_at differs. A '-1h' window keyed
    # on event time returns just the recent-by-event-time row (delivery time would
    # match both).
    store = Store(str(tmp_path / 'c.db'))
    try:
        store.register(Obs, index=('id',), time_field='observed_at')
        now = datetime.datetime.now(datetime.UTC).timestamp()
        store.record(Obs, Obs(id=1, observed_at=now - 7200))  # 2h ago
        store.record(Obs, Obs(id=2, observed_at=now - 60))  # 1m ago

        recent = store.query(Obs, since='-1h')
        assert [r.id for r in recent] == [2]
    finally:
        store.close()


def test_latest_returns_newest_per_key(tmp_path):
    store = Store(str(tmp_path / 'c.db'))
    try:
        store.register(Obs, index=('id',), time_field='observed_at', latest_key=('id',))
        now = datetime.datetime.now(datetime.UTC).timestamp()
        store.record(Obs, Obs(id=1, observed_at=now - 100))
        store.record(Obs, Obs(id=1, observed_at=now))  # newest for id=1
        store.record(Obs, Obs(id=1, observed_at=now - 50))  # out of order, older -> ignored
        store.record(Obs, Obs(id=2, observed_at=now - 10))

        assert store.latest(Obs, id=1).observed_at == now
        assert store.latest(Obs, id=2).observed_at == now - 10
        assert store.latest(Obs, id=999) is None
    finally:
        store.close()


def test_counts_reports_history_and_latest(tmp_path):
    store = Store(str(tmp_path / 'c.db'))
    try:
        store.register(Obs, index=('id',), time_field='observed_at', latest_key=('id',))
        now = datetime.datetime.now(datetime.UTC).timestamp()
        store.record(Obs, Obs(id=1, observed_at=now - 10))
        store.record(Obs, Obs(id=1, observed_at=now))  # 2 history rows, 1 latest entity
        store.record(Obs, Obs(id=2, observed_at=now))
        history, latest = store.counts(Obs)
        assert history == 3
        assert latest == 2  # distinct ids
    finally:
        store.close()


def test_counts_zero_latest_without_projection(tmp_path):
    store = Store(str(tmp_path / 'c.db'))
    try:
        store.register(Msg, index=('id',))
        store.record(Msg, Msg(id=1))
        assert store.counts(Msg) == (1, 0)
    finally:
        store.close()


def test_prune_flushes_pending_writes_first(tmp_path):
    # flush_secs=0 disables the timer, so the row stays buffered until prune flushes it.
    store = Store(str(tmp_path / 'c.db'), flush_secs=0)
    try:
        store.register(Obs, time_field='observed_at', retention='1d')
        old = datetime.datetime.now(datetime.UTC).timestamp() - 10 * 365 * 86400
        store.record(Obs, Obs(id=1, observed_at=old))  # buffered, not yet flushed
        assert store.prune() == 1  # prune flushes first, then sweeps the stale row
    finally:
        store.close()


def test_latest_without_projection_raises(tmp_path):
    store = Store(str(tmp_path / 'c.db'))
    try:
        store.register(Obs)
        with pytest.raises(ConfigError):
            store.latest(Obs, id=1)
    finally:
        store.close()


def test_latest_wrong_key_raises(tmp_path):
    store = Store(str(tmp_path / 'c.db'))
    try:
        store.register(Obs, latest_key=('id',))
        with pytest.raises(QueryError):
            store.latest(Obs, observed_at=1.0)
    finally:
        store.close()


def test_record_is_idempotent_on_primary_key(tmp_path):
    store = Store(str(tmp_path / 'c.duckdb'))
    try:
        store.register(Msg)
        store.record(Msg, Msg(id=1), meta=FixedMeta())
        store.record(Msg, Msg(id=1), meta=FixedMeta())
        assert len(store.query(Msg)) == 1
    finally:
        store.close()


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


def _filled(tmp_path, count, **register):
    """A store holding ``count`` Msg rows, writer already drained."""
    store = Store(str(tmp_path / 'c.duckdb'), flush_secs=0)
    store.register(Msg, index=('id',), **register)
    for i in range(count):
        store.record(Msg, Msg(id=i, name=f'n{i}'))
    store.flush()
    return store


def test_iter_walks_every_row_in_order(tmp_path):
    store = _filled(tmp_path, 25)
    try:
        assert [m.id for m in store.iter(Msg, chunk=4)] == list(range(25))
        assert [m.id for m in store.iter(Msg, chunk=4, order='desc')] == list(reversed(range(25)))
    finally:
        store.close()


def test_iter_pages_instead_of_materializing(tmp_path):
    """The point of ``iter``: one page is fetched before the first row is yielded."""
    store = _filled(tmp_path, 50)
    counting = _CountingBackend(store._backend)
    store._backend = counting
    try:
        walk = store.iter(Msg, chunk=10)
        assert counting.selects == 0  # nothing read until the first row is pulled
        assert next(walk).id == 0
        assert counting.selects == 1  # …and then exactly one page, not fifty rows
        assert [m.id for m in walk] == list(range(1, 50))
        assert counting.selects == 6  # 5 full pages + the short one that ends the walk
    finally:
        store._backend = counting.inner
        store.close()


def test_iter_respects_limit_bounds_and_filters(tmp_path):
    store = _filled(tmp_path, 20)
    try:
        assert [m.id for m in store.iter(Msg, chunk=3, limit=7)] == list(range(7))
        assert [m.id for m in store.iter(Msg, chunk=3, id=11)] == [11]
        assert list(store.iter(Msg, until='-1h')) == []
    finally:
        store.close()


def test_iter_agrees_with_query(tmp_path):
    store = _filled(tmp_path, 30)
    try:
        assert [m.id for m in store.iter(Msg, chunk=7)] == [m.id for m in store.query(Msg, limit=100)]
    finally:
        store.close()


def test_iter_flushes_at_open(tmp_path):
    """Pending writes are visible without an explicit flush — and flushed on the call."""
    store = Store(str(tmp_path / 'c.duckdb'), flush_secs=0)
    try:
        store.register(Msg, index=('id',))
        store.record(Msg, Msg(id=1, name='buffered'))
        walk = store.iter(Msg)  # flush happens here, not on first next()
        store.record(Msg, Msg(id=2, name='after open'))
        assert [m.id for m in walk] == [1]
    finally:
        store.close()


def test_iter_skips_rows_with_no_event_time(tmp_path):
    """A nullable ``time_field`` left unset has no place on the axis the walk resumes along."""
    store = Store(str(tmp_path / 'c.duckdb'), flush_secs=0)
    try:
        store.register(Nullable, time_field='observed_at', index=('id',))
        store.record(Nullable, Nullable(id=1, observed_at=1000.0))
        store.record(Nullable, Nullable(id=2))  # no event time
        store.flush()

        assert [n.id for n in store.iter(Nullable, chunk=1)] == [1]
        assert {n.id for n in store.query(Nullable)} == {1, 2}  # query still returns it
    finally:
        store.close()


def test_iter_rejects_bad_paging_arguments(tmp_path):
    store = _filled(tmp_path, 1)
    try:
        with pytest.raises(QueryError):
            store.iter(Msg, chunk=0)
        with pytest.raises(QueryError):
            store.iter(Msg, limit=-1)
    finally:
        store.close()


def test_register_creates_secondary_indexes(tmp_path):
    store = Store(str(tmp_path / 'c.duckdb'), flush_secs=0)
    try:
        store.register(Obs, time_field='observed_at', index=('id',))
        names = {
            row['name']
            for row in store._backend.select(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'stream_obs'",
            )
        }
        assert {'idx_stream_obs_time', 'idx_stream_obs_id'} <= names
    finally:
        store.close()


def test_iter_and_indexes_on_the_duckdb_backend(tmp_path):
    """Paging binds a datetime inside a row-value comparison — worth proving on both engines."""
    pytest.importorskip('duckdb')
    store = Store(str(tmp_path / 'c.duckdb'), backend='duckdb', flush_secs=0)
    try:
        store.register(Msg, index=('id',))
        for i in range(5):
            store.record(Msg, Msg(id=i))
        store.flush()

        assert [m.id for m in store.iter(Msg, chunk=2)] == list(range(5))
        names = {
            row['index_name']
            for row in store._backend.select("SELECT index_name FROM duckdb_indexes() WHERE table_name = 'stream_msg'")
        }
        assert {'idx_stream_msg_time', 'idx_stream_msg_id'} <= names
    finally:
        store.close()


# -- current state (the projection read) -------------------------------------


def _population(tmp_path, count=6):
    """A store whose latest projection holds one row per entity."""
    store = Store(str(tmp_path / 'c.duckdb'), flush_secs=0)
    store.register(Obs, index=('id',), time_field='observed_at', latest_key=('id',))
    for i in range(count):
        store.record(Obs, Obs(id=i, observed_at=1000.0 + i))
        store.record(Obs, Obs(id=i, observed_at=2000.0 + i))  # a newer observation per entity
    store.flush()
    return store


def test_query_latest_answers_one_row_per_entity(tmp_path):
    """The population question: everything's current state, not every observation."""
    store = _population(tmp_path)
    try:
        rows = store.query_latest(Obs)
        assert [r.id for r in rows] == list(range(6))
        assert all(r.observed_at >= 2000.0 for r in rows)  # the newest per entity
        assert len(store.query(Obs, limit=100)) == 12  # …where history kept both
    finally:
        store.close()


def test_query_latest_filters_and_caps(tmp_path):
    store = _population(tmp_path)
    try:
        assert [r.id for r in store.query_latest(Obs, id=3)] == [3]
        assert len(store.query_latest(Obs, limit=2)) == 2
        assert [r.id for r in store.query_latest(Obs, order='desc')] == list(reversed(range(6)))
    finally:
        store.close()


def test_query_latest_window_means_last_seen(tmp_path):
    """``since`` on the projection asks when the entity was last seen, not when it was recorded."""
    store = Store(str(tmp_path / 'c.duckdb'), flush_secs=0)
    try:
        store.register(Obs, index=('id',), time_field='observed_at', latest_key=('id',))
        store.record(Obs, Obs(id=1, observed_at=1000.0))  # stale entity
        store.record(Obs, Obs(id=2, observed_at=9000.0))  # recently seen
        store.flush()

        assert [r.id for r in store.query_latest(Obs, since=5000.0)] == [2]
        assert [r.id for r in store.query_latest(Obs, until=5000.0)] == [1]
    finally:
        store.close()


def test_iter_latest_pages_through_the_population(tmp_path):
    store = _population(tmp_path, count=20)
    counting = _CountingBackend(store._backend)
    store._backend = counting
    try:
        walk = store.iter_latest(Obs, chunk=5)
        assert next(walk).id == 0
        assert counting.selects == 1  # a page, not the population
        assert [row.id for row in walk] == list(range(1, 20))
    finally:
        store._backend = counting.inner
        store.close()


def test_projection_reads_need_a_projection(tmp_path):
    store = Store(str(tmp_path / 'c.duckdb'), flush_secs=0)
    try:
        store.register(Msg, index=('id',))  # no latest_key
        with pytest.raises(ConfigError, match='needs a latest projection'):
            store.query_latest(Msg)
        with pytest.raises(ConfigError, match='needs a latest projection'):
            store.iter_latest(Msg)
    finally:
        store.close()


def test_projection_is_indexed_for_the_queries_that_read_it(tmp_path):
    """A filtered snapshot should not scan — and a dimension the entity key leads needs no index."""
    store = Store(str(tmp_path / 'c.duckdb'), flush_secs=0)
    try:
        store.register(Obs, index=('id', 'label'), time_field='observed_at', latest_key=('id',))
        names = {
            row['name']
            for row in store._backend.select(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'latest_obs'",
            )
        }
        assert 'idx_latest_obs_time' in names
        assert 'idx_latest_obs_label' in names
        assert 'idx_latest_obs_id' not in names  # `id` leads the latest key's own index
    finally:
        store.close()


# -- path filters (into a Dict field) ----------------------------------------


def _zoned(tmp_path):
    """Four entities across three zones, recorded twice each."""
    store = Store(str(tmp_path / 'c.duckdb'), flush_secs=0)
    store.register(
        Zoned,
        index=('id',),
        time_field='observed_at',
        latest_key=('id',),
        json_index=('zones.department',),
    )
    for i, dept in enumerate([5, 6, 5, 7]):
        store.record(Zoned, Zoned(id=i, zones={'department': dept}, observed_at=1000.0 + i))
        store.record(Zoned, Zoned(id=i, zones={'department': dept}, observed_at=2000.0 + i))
    store.flush()
    return store


def test_path_filter_narrows_history(tmp_path):
    store = _zoned(tmp_path)
    try:
        rows = store.query(Zoned, where={'zones.department': 5})
        assert sorted({r.id for r in rows}) == [0, 2]
        assert len(rows) == 4  # both observations of each
    finally:
        store.close()


def test_path_filter_narrows_current_state(tmp_path):
    """The console's question: who is in department 5 *now*."""
    store = _zoned(tmp_path)
    try:
        rows = store.query_latest(Zoned, where={'zones.department': 5})
        assert [r.id for r in rows] == [0, 2]  # one row per entity
    finally:
        store.close()


def test_path_filter_survives_a_wire_alias(tmp_path):
    """The payload says `zn`; the caller says `zones`. Getting this wrong matches nothing."""
    store = _zoned(tmp_path)
    try:
        assert store.registry.get(Zoned).json_paths == {'zones.department': 'zn.department'}
        assert store.query(Zoned, where={'zones.department': 7})
    finally:
        store.close()


def test_path_filter_works_while_paging(tmp_path):
    store = _zoned(tmp_path)
    try:
        walked = [r.id for r in store.iter(Zoned, where={'zones.department': 5}, chunk=1)]
        assert walked == [0, 2, 0, 2]  # two entities, two observations each, time-ordered
        current = [r.id for r in store.iter_latest(Zoned, where={'zones.department': 5}, chunk=1)]
        assert current == [0, 2]
    finally:
        store.close()


def test_path_filter_composes_with_column_filters_and_windows(tmp_path):
    store = _zoned(tmp_path)
    try:
        assert [r.id for r in store.query(Zoned, id=2, where={'zones.department': 5}, since=1500.0)] == [2]
        assert store.query(Zoned, id=1, where={'zones.department': 5}) == []
    finally:
        store.close()


def test_path_filter_rejects_an_undeclared_path(tmp_path):
    store = _zoned(tmp_path)
    try:
        with pytest.raises(QueryError, match='not a declared json_index'):
            store.query(Zoned, where={'zones.aisle': 1})
    finally:
        store.close()


def test_missing_key_simply_does_not_match(tmp_path):
    """A row whose dict lacks the key is absent from the result, not an error."""
    store = Store(str(tmp_path / 'c.duckdb'), flush_secs=0)
    try:
        store.register(Zoned, index=('id',), time_field='observed_at', json_index=('zones.department',))
        store.record(Zoned, Zoned(id=1, zones={'aisle': 3}, observed_at=1000.0))  # no department
        store.record(Zoned, Zoned(id=2, zones={'department': 5}, observed_at=1001.0))
        store.flush()

        assert [r.id for r in store.query(Zoned, where={'zones.department': 5})] == [2]
    finally:
        store.close()


def _query_plan(store, stream, filters=None, *, table=None, **kwargs):
    """How SQLite says it will answer a planned read — the only proof an index is used."""
    from stored.query import parse_window, plan

    sql, params = plan(
        stream,
        '',
        parse_window(),
        filters,
        table=table,
        dialect=store._backend.dialect,
        **kwargs,
    )
    return [row['detail'] for row in store._backend.select('EXPLAIN QUERY PLAN ' + sql, params)]


def test_declared_paths_are_indexed_on_both_tables(tmp_path):
    store = _zoned(tmp_path)
    try:
        names = {
            row['name']
            for row in store._backend.select(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND name LIKE '%json%'",
            )
        }
        assert names == {'idx_stream_zoned_json_zones_department', 'idx_latest_zoned_json_zones_department'}
    finally:
        store.close()


def test_a_path_filter_actually_uses_its_index(tmp_path):
    """The whole point of Stage 4: declared, emitted — and *chosen* by the planner."""
    store = _zoned(tmp_path)
    stream = store.registry.get(Zoned)
    try:
        history = _query_plan(store, stream, where={'zones.department': 5})
        assert any('USING INDEX idx_stream_zoned_json_zones_department' in step for step in history)

        current = _query_plan(store, stream, table=stream.latest_table, where={'zones.department': 5})
        assert any('USING INDEX idx_latest_zoned_json_zones_department' in step for step in current)
    finally:
        store.close()


def test_the_path_index_serves_the_ordering_too(tmp_path):
    """The sort key rides in the index, so a filtered read does not also sort."""
    store = _zoned(tmp_path)
    stream = store.registry.get(Zoned)
    try:
        steps = _query_plan(store, stream, where={'zones.department': 5})
        assert not any('TEMP B-TREE' in step for step in steps), steps
    finally:
        store.close()


def test_a_column_filter_still_uses_its_own_index(tmp_path):
    """The path index is an addition, not a replacement: dimension filters are unaffected."""
    store = _zoned(tmp_path)
    stream = store.registry.get(Zoned)
    try:
        steps = _query_plan(store, stream, {'id': 3})
        assert any('idx_stream_zoned_id' in step for step in steps), steps
    finally:
        store.close()
