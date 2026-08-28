import datetime

import pytest

pytest.importorskip('duckdb')  # the DuckDB backend is the optional ``stored[duckdb]`` extra

from stored.backends.duckdb_ import DuckDBBackend  # noqa: E402


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
