import seared as s

from stored import Store


@s.seared
class Msg(s.Seared):
    id:   int = s.Int(required=True)
    name: str = s.Str(default='')


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


def test_record_is_idempotent_on_primary_key(tmp_path):
    store = Store(str(tmp_path / 'c.duckdb'))
    try:
        store.register(Msg)
        store.record(Msg, Msg(id=1), meta=FixedMeta())
        store.record(Msg, Msg(id=1), meta=FixedMeta())
        assert len(store.query(Msg)) == 1
    finally:
        store.close()
