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
        backend.append_batch(
            't',
            [
                {'_key_expr': 'k', '_ts_hlc': 'a', 'id': 1},
                {'_key_expr': 'k', '_ts_hlc': 'b', 'id': 2},
            ],
        )
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
        backend.append_batch(
            't',
            [
                {'_key_expr': 'k', '_ts_hlc': 'a', '_issued_at': datetime.datetime(2026, 1, 1)},
                {'_key_expr': 'k', '_ts_hlc': 'b', '_issued_at': datetime.datetime(2026, 8, 1)},
            ],
        )
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
        backend.append_batch(
            't',
            [
                {'_key_expr': 'k', '_ts_hlc': 'b', '_issued_at': frac},
                {'_key_expr': 'k', '_ts_hlc': 'a', '_issued_at': whole},
            ],
        )
        rows = backend.select('SELECT * FROM "t" ORDER BY "_issued_at"')
        assert [r['_ts_hlc'] for r in rows] == ['a', 'b']
        cutoff = backend.delete_before('t', '_issued_at', frac)
        assert cutoff == 1  # only the earlier whole-second row is older than frac
    finally:
        backend.close()


def _latest_table(backend):
    cols = {'source': 'VARCHAR', 'epc': 'VARCHAR', 'x': 'DOUBLE', '_event_at': 'TIMESTAMP'}
    backend.ensure_table('latest_t', cols, ('source', 'epc'))


def test_upsert_latest_keeps_newest_within_a_batch():
    backend = SQLiteBackend(':memory:')
    try:
        _latest_table(backend)
        backend.upsert_latest(
            'latest_t',
            [
                {'source': 'rtls', 'epc': 'A', 'x': 1.0, '_event_at': datetime.datetime(2026, 1, 1)},
                {'source': 'rtls', 'epc': 'A', 'x': 3.0, '_event_at': datetime.datetime(2026, 3, 1)},  # newest
                {'source': 'rtls', 'epc': 'A', 'x': 2.0, '_event_at': datetime.datetime(2026, 2, 1)},  # out of order
            ],
            ('source', 'epc'),
            '_event_at',
        )
        rows = backend.select('SELECT * FROM "latest_t"')
        assert len(rows) == 1
        assert rows[0]['x'] == 3.0
        assert rows[0]['_event_at'] == datetime.datetime(2026, 3, 1)
    finally:
        backend.close()


def test_upsert_latest_ignores_older_across_calls():
    backend = SQLiteBackend(':memory:')
    try:
        _latest_table(backend)
        key, cmp = ('source', 'epc'), '_event_at'

        def put(x, month):
            row = {'source': 's', 'epc': 'A', 'x': x, '_event_at': datetime.datetime(2026, month, 1)}
            backend.upsert_latest('latest_t', [row], key, cmp)

        put(3.0, 3)
        put(9.0, 2)  # older -> ignored
        assert backend.select('SELECT x FROM "latest_t"')[0]['x'] == 3.0
        put(7.0, 4)  # newer -> wins
        assert backend.select('SELECT x FROM "latest_t"')[0]['x'] == 7.0
    finally:
        backend.close()


def test_decimal_and_blob_round_trip():
    """DECIMAL adapts/reads back as ``Decimal``; BLOB binds native ``bytes``."""
    backend = SQLiteBackend(':memory:')
    try:
        cols = {'_key_expr': 'VARCHAR', '_ts_hlc': 'VARCHAR', 'amount': 'DECIMAL(38, 9)', 'blob': 'BLOB'}
        backend.ensure_table('t', cols, ('_key_expr', '_ts_hlc'))
        backend.append_batch(
            't',
            [
                {'_key_expr': 'k', '_ts_hlc': 'a', 'amount': decimal.Decimal('1.500000000'), 'blob': b'\x00\x01'},
            ],
        )
        row = backend.select('SELECT * FROM "t"')[0]
        assert row['amount'] == decimal.Decimal('1.500000000')
        assert row['blob'] == b'\x00\x01'
    finally:
        backend.close()


def test_ensure_index_creates_and_is_idempotent():
    backend = SQLiteBackend(':memory:')
    try:
        columns = {'_key_expr': 'VARCHAR', '_ts_hlc': 'VARCHAR', 'id': 'BIGINT'}
        backend.ensure_table('t', columns, ('_key_expr', '_ts_hlc'))
        backend.ensure_index('idx_t_id', 't', ('id',))
        backend.ensure_index('idx_t_id', 't', ('id',))  # re-registration must be a no-op
        rows = backend.select("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 't'")
        assert 'idx_t_id' in {r['name'] for r in rows}
    finally:
        backend.close()


def test_ensure_index_composite_columns():
    backend = SQLiteBackend(':memory:')
    try:
        columns = {'_key_expr': 'VARCHAR', '_ts_hlc': 'VARCHAR', '_issued_at': 'TIMESTAMP'}
        backend.ensure_table('t', columns, ('_key_expr', '_ts_hlc'))
        backend.ensure_index('idx_t_time', 't', ('_issued_at', '_ts_hlc', '_key_expr'))
        rows = backend.select('PRAGMA index_info("idx_t_time")')
        assert [r['name'] for r in rows] == ['_issued_at', '_ts_hlc', '_key_expr']
    finally:
        backend.close()


def test_dialect_json_value_executes_on_this_engine():
    """The fragment is only right if the engine agrees — so run it, don't just spell it."""
    backend = SQLiteBackend(':memory:')
    try:
        backend.ensure_table(
            't', {'_key_expr': 'VARCHAR', '_ts_hlc': 'VARCHAR', '_payload': 'VARCHAR'}, ('_key_expr', '_ts_hlc')
        )
        backend.append_batch(
            't',
            [
                {
                    '_key_expr': 'k',
                    '_ts_hlc': 'a',
                    '_payload': '{"zones": {"department": 5}, "conf": {"d": 0.75}, "tag": {"k": "abc"}}',
                }
            ],
        )
        dialect = backend.dialect

        numeric = dialect.json_value('_payload', 'zones.department', text=False)
        assert backend.select(f'SELECT 1 AS hit FROM "t" WHERE {numeric} = ?', [5])  # noqa: S608 — rendered fragment
        floating = dialect.json_value('_payload', 'conf.d', text=False)
        assert backend.select(f'SELECT 1 AS hit FROM "t" WHERE {floating} = ?', [0.75])  # noqa: S608
        textual = dialect.json_value('_payload', 'tag.k', text=True)
        assert backend.select(f'SELECT 1 AS hit FROM "t" WHERE {textual} = ?', ['abc'])  # noqa: S608

        missing = dialect.json_value('_payload', 'zones.nope', text=False)
        assert backend.select(f'SELECT 1 AS hit FROM "t" WHERE {missing} = ?', [5]) == []  # noqa: S608
    finally:
        backend.close()


def _json_table(backend):
    columns = {'_key_expr': 'VARCHAR', '_ts_hlc': 'VARCHAR', '_issued_at': 'TIMESTAMP', '_payload': 'VARCHAR'}
    backend.ensure_table('t', columns, ('_key_expr', '_ts_hlc'))


def test_ensure_json_index_creates_and_is_idempotent():
    backend = SQLiteBackend(':memory:')
    try:
        _json_table(backend)
        backend.ensure_json_index('idx_t_json_zn_dept', 't', 'zn.department')
        backend.ensure_json_index('idx_t_json_zn_dept', 't', 'zn.department')  # re-registration is a no-op
        names = {
            r['name'] for r in backend.select("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 't'")
        }
        assert 'idx_t_json_zn_dept' in names
    finally:
        backend.close()


def test_ensure_json_index_appends_the_sort_key():
    """One index serves the search *and* the ORDER BY — the sort columns follow the expression."""
    backend = SQLiteBackend(':memory:')
    try:
        _json_table(backend)
        backend.ensure_json_index('ix', 't', 'zn.department', ('_issued_at', '_ts_hlc', '_key_expr'))
        (row,) = backend.select("SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'ix'")
        assert 'json_extract' in row['sql']
        assert '"_issued_at", "_ts_hlc", "_key_expr"' in row['sql']
    finally:
        backend.close()
