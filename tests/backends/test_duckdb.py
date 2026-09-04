import datetime

import pytest

pytest.importorskip('duckdb')  # the DuckDB backend is the optional ``stored[duckdb]`` extra

from stored.backends.duckdb_ import DuckDBBackend  # noqa: E402
from stored.dialect import Dialect  # noqa: E402
from stored.errors import BackendError  # noqa: E402


def test_open_memory_and_close():
    backend = DuckDBBackend(':memory:')
    assert backend.path == ':memory:'
    backend.close()


def test_open_file_and_close(tmp_path):
    path = str(tmp_path / 'c.duckdb')
    backend = DuckDBBackend(path)
    assert backend.path == path
    backend.close()


def test_ensure_append_select_round_trip():
    backend = DuckDBBackend(':memory:')
    try:
        columns = {'_key_expr': 'VARCHAR', '_ts_hlc': 'VARCHAR', 'id': 'BIGINT'}
        backend.ensure_table('t', columns, ('_key_expr', '_ts_hlc'))
        backend.append_batch('t', [
            {'_key_expr': 'k', '_ts_hlc': 'a', 'id': 1},
            {'_key_expr': 'k', '_ts_hlc': 'b', 'id': 2},
        ])
        rows = backend.select('SELECT * FROM "t" ORDER BY "_ts_hlc"')
        assert [r['id'] for r in rows] == [1, 2]
        assert rows[0]['_key_expr'] == 'k'
    finally:
        backend.close()


def test_append_ignores_primary_key_conflict():
    backend = DuckDBBackend(':memory:')
    try:
        backend.ensure_table('t', {'_key_expr': 'VARCHAR', '_ts_hlc': 'VARCHAR'}, ('_key_expr', '_ts_hlc'))
        backend.append_batch('t', [{'_key_expr': 'k', '_ts_hlc': 'a'}])
        backend.append_batch('t', [{'_key_expr': 'k', '_ts_hlc': 'a'}])
        assert len(backend.select('SELECT * FROM "t"')) == 1
    finally:
        backend.close()


def test_ensure_table_adds_missing_column():
    backend = DuckDBBackend(':memory:')
    try:
        pk = ('_key_expr', '_ts_hlc')
        base = {'_key_expr': 'VARCHAR', '_ts_hlc': 'VARCHAR'}
        backend.ensure_table('t', base, pk)
        backend.ensure_table('t', {**base, 'extra': 'BIGINT'}, pk)
        backend.append_batch('t', [{'_key_expr': 'k', '_ts_hlc': 'a', 'extra': 9}])
        assert backend.select('SELECT * FROM "t"')[0]['extra'] == 9
    finally:
        backend.close()


def test_upsert_latest_keeps_newest():
    backend = DuckDBBackend(':memory:')
    try:
        cols = {'source': 'VARCHAR', 'epc': 'VARCHAR', 'x': 'DOUBLE', '_event_at': 'TIMESTAMP'}
        backend.ensure_table('latest_t', cols, ('source', 'epc'))
        key, cmp = ('source', 'epc'), '_event_at'
        backend.upsert_latest('latest_t', [
            {'source': 'rtls', 'epc': 'A', 'x': 1.0, '_event_at': datetime.datetime(2026, 1, 1)},
            {'source': 'rtls', 'epc': 'A', 'x': 3.0, '_event_at': datetime.datetime(2026, 3, 1)},  # newest
            {'source': 'rtls', 'epc': 'A', 'x': 2.0, '_event_at': datetime.datetime(2026, 2, 1)},  # out of order
        ], key, cmp)
        rows = backend.select('SELECT * FROM "latest_t"')
        assert len(rows) == 1
        assert rows[0]['x'] == 3.0

        def put(x, month):
            row = {'source': 'rtls', 'epc': 'A', 'x': x, '_event_at': datetime.datetime(2026, month, 1)}
            backend.upsert_latest('latest_t', [row], key, cmp)

        put(9.0, 2)  # older observation in a later call is ignored
        assert backend.select('SELECT x FROM "latest_t"')[0]['x'] == 3.0
        put(7.0, 4)  # newer one wins
        assert backend.select('SELECT x FROM "latest_t"')[0]['x'] == 7.0
    finally:
        backend.close()


def test_delete_before_removes_old_rows():
    backend = DuckDBBackend(':memory:')
    try:
        cols = {'_key_expr': 'VARCHAR', '_ts_hlc': 'VARCHAR', '_issued_at': 'TIMESTAMP'}
        backend.ensure_table('t', cols, ('_key_expr', '_ts_hlc'))
        backend.append_batch('t', [
            {'_key_expr': 'k', '_ts_hlc': 'a', '_issued_at': datetime.datetime(2026, 1, 1)},
            {'_key_expr': 'k', '_ts_hlc': 'b', '_issued_at': datetime.datetime(2026, 8, 1)},
        ])
        removed = backend.delete_before('t', '_issued_at', datetime.datetime(2026, 6, 1))
        assert removed == 1
        rows = backend.select('SELECT * FROM "t"')
        assert len(rows) == 1
        assert rows[0]['_ts_hlc'] == 'b'
    finally:
        backend.close()


def test_ensure_index_creates_and_is_idempotent():
    backend = DuckDBBackend(':memory:')
    try:
        columns = {'_key_expr': 'VARCHAR', '_ts_hlc': 'VARCHAR', 'id': 'BIGINT'}
        backend.ensure_table('t', columns, ('_key_expr', '_ts_hlc'))
        backend.ensure_index('idx_t_id', 't', ('id',))
        backend.ensure_index('idx_t_id', 't', ('id',))  # re-registration must be a no-op
        rows = backend.select("SELECT index_name FROM duckdb_indexes() WHERE table_name = 't'")
        assert 'idx_t_id' in {r['index_name'] for r in rows}
    finally:
        backend.close()


def test_dialect_json_value_executes_on_this_engine():
    """DuckDB's json_extract yields JSON: a text compare needs the string extractor."""
    backend = DuckDBBackend(':memory:')
    try:
        backend.ensure_table('t', {'_key_expr': 'VARCHAR', '_ts_hlc': 'VARCHAR', '_payload': 'VARCHAR'},
                             ('_key_expr', '_ts_hlc'))
        backend.append_batch('t', [{
            '_key_expr': 'k', '_ts_hlc': 'a',
            '_payload': '{"zones": {"department": 5}, "conf": {"d": 0.75}, "tag": {"k": "abc"}}',
        }])
        dialect = backend.dialect

        numeric = dialect.json_value('_payload', 'zones.department', text=False)
        assert backend.select(f'SELECT 1 AS hit FROM "t" WHERE {numeric} = ?', [5])  # noqa: S608 — rendered fragment
        floating = dialect.json_value('_payload', 'conf.d', text=False)
        assert backend.select(f'SELECT 1 AS hit FROM "t" WHERE {floating} = ?', [0.75])  # noqa: S608
        textual = dialect.json_value('_payload', 'tag.k', text=True)
        assert backend.select(f'SELECT 1 AS hit FROM "t" WHERE {textual} = ?', ['abc'])  # noqa: S608

        # …and the baseline spelling does not quietly mismatch here — it refuses:
        # DuckDB tries to parse the bound string as JSON to compare against JSON.
        baseline = Dialect().json_value('_payload', 'tag.k', text=True)
        with pytest.raises(BackendError, match='Malformed JSON'):
            backend.select(f'SELECT 1 AS hit FROM "t" WHERE {baseline} = ?', ['abc'])  # noqa: S608
    finally:
        backend.close()
