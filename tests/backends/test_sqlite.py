import datetime
import decimal

from stored.backends.sqlite_ import SQLiteBackend


def test_open_memory_and_close():
    backend = SQLiteBackend(':memory:')
    assert backend.path == ':memory:'
    backend.close()


def test_open_file_and_close(tmp_path):
    path = str(tmp_path / 'c.db')
    backend = SQLiteBackend(path)
    assert backend.path == path
    backend.close()


def test_ensure_append_select_round_trip():
    backend = SQLiteBackend(':memory:')
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
    backend = SQLiteBackend(':memory:')
    try:
        backend.ensure_table('t', {'_key_expr': 'VARCHAR', '_ts_hlc': 'VARCHAR'}, ('_key_expr', '_ts_hlc'))
        backend.append_batch('t', [{'_key_expr': 'k', '_ts_hlc': 'a'}])
        backend.append_batch('t', [{'_key_expr': 'k', '_ts_hlc': 'a'}])
        assert len(backend.select('SELECT * FROM "t"')) == 1
    finally:
        backend.close()


def test_ensure_table_adds_missing_column():
    backend = SQLiteBackend(':memory:')
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
    backend = SQLiteBackend(':memory:')
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


def test_timestamp_round_trips_as_datetime():
    """TIMESTAMP columns adapt on write and convert back to native ``datetime`` on read."""
    backend = SQLiteBackend(':memory:')
    try:
        cols = {'_key_expr': 'VARCHAR', '_ts_hlc': 'VARCHAR', '_issued_at': 'TIMESTAMP'}
        backend.ensure_table('t', cols, ('_key_expr', '_ts_hlc'))
        stamp = datetime.datetime(2026, 8, 27, 13, 45, 6, 123456)
        backend.append_batch('t', [{'_key_expr': 'k', '_ts_hlc': 'a', '_issued_at': stamp}])
        got = backend.select('SELECT * FROM "t"')[0]['_issued_at']
        assert got == stamp
    finally:
        backend.close()


def test_time_ordering_matches_chronology_across_fractional_seconds():
    """ISO-text storage keeps range/order comparisons chronological with mixed microseconds."""
    backend = SQLiteBackend(':memory:')
    try:
        cols = {'_key_expr': 'VARCHAR', '_ts_hlc': 'VARCHAR', '_issued_at': 'TIMESTAMP'}
        backend.ensure_table('t', cols, ('_key_expr', '_ts_hlc'))
        whole = datetime.datetime(2026, 8, 27, 12, 0, 0)  # no fractional part
        frac = datetime.datetime(2026, 8, 27, 12, 0, 0, 1)  # 1 microsecond later
        backend.append_batch('t', [
            {'_key_expr': 'k', '_ts_hlc': 'b', '_issued_at': frac},
            {'_key_expr': 'k', '_ts_hlc': 'a', '_issued_at': whole},
        ])
        rows = backend.select('SELECT * FROM "t" ORDER BY "_issued_at"')
        assert [r['_ts_hlc'] for r in rows] == ['a', 'b']
        cutoff = backend.delete_before('t', '_issued_at', frac)
        assert cutoff == 1  # only the earlier whole-second row is older than frac
    finally:
        backend.close()


def test_decimal_and_blob_round_trip():
    """DECIMAL adapts/reads back as ``Decimal``; BLOB binds native ``bytes``."""
    backend = SQLiteBackend(':memory:')
    try:
        cols = {'_key_expr': 'VARCHAR', '_ts_hlc': 'VARCHAR', 'amount': 'DECIMAL(38, 9)', 'blob': 'BLOB'}
        backend.ensure_table('t', cols, ('_key_expr', '_ts_hlc'))
        backend.append_batch('t', [
            {'_key_expr': 'k', '_ts_hlc': 'a', 'amount': decimal.Decimal('1.500000000'), 'blob': b'\x00\x01'},
        ])
        row = backend.select('SELECT * FROM "t"')[0]
        assert row['amount'] == decimal.Decimal('1.500000000')
        assert row['blob'] == b'\x00\x01'
    finally:
        backend.close()
