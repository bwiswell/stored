import datetime

import pytest
import seared as s

from stored import Store
from stored.errors import ConfigError


@s.seared
class Msg(s.Seared):
    id: int = s.Int(required=True)


class Meta:
    def __init__(self, key, ts, issued_at):
        self.key_expr = key
        self.timestamp = ts
        self.issued_at = issued_at
        self.source_info = None
        self.schema = None


def test_prune_removes_rows_past_retention(tmp_path):
    store = Store(str(tmp_path / 'c.duckdb'), flush_secs=0)
    try:
        store.register(Msg, retention='7d')
        now = datetime.datetime.now(datetime.UTC)
        old = now - datetime.timedelta(days=8)
        store.record(Msg, Msg(id=1), meta=Meta('k1', 't-old', old))
        store.record(Msg, Msg(id=2), meta=Meta('k2', 't-new', now))
        store.flush()

        assert store.prune() == 1
        assert [r.id for r in store.query(Msg)] == [2]
    finally:
        store.close()


def test_prune_noop_without_retention(tmp_path):
    store = Store(str(tmp_path / 'c.duckdb'), flush_secs=0)
    try:
        store.register(Msg)
        store.record(Msg, Msg(id=1))
        store.flush()
        assert store.prune() == 0
        assert len(store.query(Msg)) == 1
    finally:
        store.close()


def test_invalid_retention_raises(tmp_path):
    store = Store(str(tmp_path / 'c.duckdb'), flush_secs=0)
    try:
        with pytest.raises(ConfigError):
            store.register(Msg, retention='nonsense')
    finally:
        store.close()
