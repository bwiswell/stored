import datetime as dt

import pytest
import seared as s

from stored import Store
from stored.errors import ConfigError


@s.seared
class Msg(s.Seared):
    id: int = s.Int(required=True)


@s.seared
class Obs(s.Seared):
    id: int = s.Int(required=True)
    observed_at: float = s.Float(required=True)


@s.seared
class Sighting(s.Seared):
    """An optional event time: a row may carry none."""

    id: int = s.Int(required=True)
    observed_at: float | None = s.Float(default=None)


class Meta:
    def __init__(self, key, ts, issued_at):
        self.key_expr = key
        self.timestamp = ts
        self.issued_at = issued_at
        self.source_info = None
        self.schema = None


def test_prune_removes_rows_past_retention(tmp_path):
    store = Store(str(tmp_path / 'c.db'), flush_secs=0)
    try:
        store.register(Msg, retention='7d')
        now = dt.datetime.now(dt.UTC)
        old = now - dt.timedelta(days=8)
        store.record(Msg, Msg(id=1), meta=Meta('k1', 't-old', old))
        store.record(Msg, Msg(id=2), meta=Meta('k2', 't-new', now))
        store.flush()

        assert store.prune() == 1
        assert [r.id for r in store.query(Msg)] == [2]
    finally:
        store.close()


def test_prune_noop_without_retention(tmp_path):
    store = Store(str(tmp_path / 'c.db'), flush_secs=0)
    try:
        store.register(Msg)
        store.record(Msg, Msg(id=1))
        store.flush()
        assert store.prune() == 0
        assert len(store.query(Msg)) == 1
    finally:
        store.close()


def test_prune_keys_on_event_time_not_delivery_time(tmp_path):
    # Both rows are *delivered* now (recent issued_at); only their domain event
    # time (observed_at) differs — retention must key on the event time.
    store = Store(str(tmp_path / 'c.db'), flush_secs=0)
    try:
        store.register(Obs, retention='7d', time_field='observed_at')
        now = dt.datetime.now(dt.UTC)
        old_event = (now - dt.timedelta(days=8)).timestamp()
        new_event = now.timestamp()
        store.record(Obs, Obs(id=1, observed_at=old_event), meta=Meta('k1', 't1', now))
        store.record(Obs, Obs(id=2, observed_at=new_event), meta=Meta('k2', 't2', now))
        store.flush()

        assert store.prune() == 1  # the row old by event time, though issued recently
        assert [r.id for r in store.query(Obs)] == [2]
    finally:
        store.close()


@pytest.mark.parametrize('backend', ['sqlite', 'duckdb'])
def test_a_row_with_no_event_time_expires_on_its_issue_time(tmp_path, backend):
    # Until 0.4.0 it never expired: retention compared `_event_at`, and NULL is older
    # than no cutoff. It has no event time, so it expires as on a stream without one.
    if backend == 'duckdb':
        pytest.importorskip('duckdb')
    store = Store(str(tmp_path / f'c.{backend}'), backend=backend, flush_secs=0)
    try:
        store.register(Sighting, retention='7d', time_field='observed_at')
        now = dt.datetime.now(dt.UTC)
        old = now - dt.timedelta(days=8)
        store.record(Sighting, Sighting(id=1), meta=Meta('k1', 't1', old))  # issued long ago
        store.record(Sighting, Sighting(id=2), meta=Meta('k2', 't2', now))  # issued now
        store.record(Sighting, Sighting(id=3, observed_at=now.timestamp()), meta=Meta('k3', 't3', old))
        store.flush()

        assert store.prune() == 1
        assert sorted(r.id for r in store.query(Sighting)) == [2, 3]  # the third keys on its event time
    finally:
        store.close()


def test_latest_survives_history_expiry_on_longer_horizon(tmp_path):
    # The whole point of the latest projection: a tag's last-known position outlives
    # its history rows. An observation 8 days old is pruned from history (1d) but kept
    # in the latest index (30d).
    store = Store(str(tmp_path / 'c.db'), flush_secs=0)
    try:
        store.register(
            Obs,
            retention='1d',
            time_field='observed_at',
            latest_key=('id',),
            latest_retention='30d',
        )
        now = dt.datetime.now(dt.UTC)
        event = (now - dt.timedelta(days=8)).timestamp()
        store.record(Obs, Obs(id=1, observed_at=event), meta=Meta('k1', 't1', now))
        store.flush()

        assert store.prune() == 1  # the history row (older than 1d) is pruned...
        assert store.query(Obs) == []
        assert store.latest(Obs, id=1) is not None  # ...but last-known survives (within 30d)
    finally:
        store.close()


def test_invalid_retention_raises(tmp_path):
    store = Store(str(tmp_path / 'c.db'), flush_secs=0)
    try:
        with pytest.raises(ConfigError):
            store.register(Msg, retention='nonsense')
    finally:
        store.close()
