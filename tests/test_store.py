import datetime

import pytest
import seared as s

from stored import Store
from stored.errors import ConfigError, QueryError


@s.seared
class Msg(s.Seared):
    id:   int = s.Int(required=True)
    name: str = s.Str(default='')


@s.seared
class Obs(s.Seared):
    id:          int   = s.Int(required=True)
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
        store.record(Obs, Obs(id=2, observed_at=now - 60))    # 1m ago

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
        store.record(Obs, Obs(id=1, observed_at=now))        # newest for id=1
        store.record(Obs, Obs(id=1, observed_at=now - 50))   # out of order, older -> ignored
        store.record(Obs, Obs(id=2, observed_at=now - 10))

        assert store.latest(Obs, id=1).observed_at == now
        assert store.latest(Obs, id=2).observed_at == now - 10
        assert store.latest(Obs, id=999) is None
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
