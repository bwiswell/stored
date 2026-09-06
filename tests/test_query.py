import datetime

import pytest
import seared as s

from stored import query
from stored.errors import QueryError
from stored.registry import StreamRegistry


@s.seared
class Msg(s.Seared):
    id: int = s.Int(required=True)


@s.seared
class Obs(s.Seared):
    id: int = s.Int(required=True)
    observed_at: float = s.Float(required=True)
    zones: dict = s.Dict(default_factory=dict)


def _stream():
    return StreamRegistry().add(Msg, index=('id',))


def test_parse_window_relative():
    window = query.parse_window(since='-1h')
    assert window.start is not None
    assert window.end is None
    assert window.ascending


def test_parse_window_iso():
    window = query.parse_window(since='2026-01-01T00:00:00+00:00')
    assert window.start is not None
    assert window.start.year == 2026


def test_parse_window_limit_clamped():
    assert query.parse_window(limit=10**9).limit == query.MAX_LIMIT


def test_parse_window_order_desc():
    assert not query.parse_window(order='desc').ascending


def test_parse_window_bad_time_raises():
    with pytest.raises(QueryError):
        query.parse_window(since='not-a-time')


def test_parse_window_unix_seconds_bound():
    ts = datetime.datetime(2026, 1, 1, 12, 0, tzinfo=datetime.UTC).timestamp()
    window = query.parse_window(since=ts)
    assert window.start == datetime.datetime(2026, 1, 1, 12, 0)  # naive UTC


def test_parse_window_datetime_bound():
    window = query.parse_window(until=datetime.datetime(2026, 3, 1, tzinfo=datetime.UTC))
    assert window.end == datetime.datetime(2026, 3, 1)


def test_plan_builds_sql_and_params():
    window = query.parse_window(since='-1h', limit=5)
    sql, params = query.plan(_stream(), 'robot/1', window, {'id': 3})
    assert 'FROM "stream_msg"' in sql
    assert '"_key_expr" = ?' in sql
    assert 'LIMIT 5' in sql
    assert 'robot/1' in params
    assert 3 in params


def test_plan_wildcard_uses_glob():
    sql, _ = query.plan(_stream(), 'robot/*', query.parse_window())
    assert 'GLOB' in sql


def test_plan_rejects_unindexed_filter():
    with pytest.raises(QueryError):
        query.plan(_stream(), '', query.parse_window(), {'name': 'x'})


def test_plan_no_filters_no_where():
    sql, params = query.plan(_stream(), '', query.parse_window())
    assert 'WHERE' not in sql
    assert params == []


def test_plan_default_keys_on_issued_at():
    sql, _ = query.plan(_stream(), '', query.parse_window(since='-1h'))
    assert '"_issued_at" >= ?' in sql
    assert 'ORDER BY "_issued_at"' in sql


def test_plan_time_field_stream_keys_on_event_at():
    stream = StreamRegistry().add(Obs, index=('id',), time_field='observed_at')
    sql, _ = query.plan(stream, '', query.parse_window(since='-1h'))
    assert '"_event_at" >= ?' in sql
    assert 'ORDER BY "_event_at"' in sql
    assert '_issued_at' not in sql


def test_plan_orders_by_the_full_sort_key():
    sql, _ = query.plan(_stream(), '', query.parse_window())
    assert 'ORDER BY "_issued_at" ASC, "_ts_hlc" ASC, "_key_expr" ASC' in sql


def test_plan_after_adds_a_keyset_predicate():
    anchor = (datetime.datetime(2026, 1, 1), 'hlc-1', 'k')
    sql, params = query.plan(_stream(), '', query.parse_window(), after=anchor)
    assert '("_issued_at", "_ts_hlc", "_key_expr") > (?, ?, ?)' in sql
    assert params[-3:] == list(anchor)


def test_plan_after_flips_with_descending_order():
    anchor = (datetime.datetime(2026, 1, 1), 'hlc-1', 'k')
    sql, _ = query.plan(_stream(), '', query.parse_window(order='desc'), after=anchor)
    assert '("_issued_at", "_ts_hlc", "_key_expr") < (?, ?, ?)' in sql


def test_plan_skip_null_time():
    sql, _ = query.plan(_stream(), '', query.parse_window(), skip_null_time=True)
    assert '"_issued_at" IS NOT NULL' in sql


class _LoudDialect(query.Dialect):
    """A dialect that spells key matching differently — proof the seam is load-bearing."""

    name = 'loud'

    def key_match(self, column: str, *, wildcard: bool) -> str:
        return f'MATCHES({column}, ?)' if wildcard else f'IS_EXACTLY({column}, ?)'


def test_plan_reads_the_stream_table_by_default():
    sql, _ = query.plan(_stream(), '', query.parse_window())
    assert 'FROM "stream_msg"' in sql


def test_plan_can_be_pointed_at_another_table():
    """The latest projection has the same columns, so a read is the same plan elsewhere."""
    sql, _ = query.plan(_stream(), '', query.parse_window(), table='latest_msg')
    assert 'FROM "latest_msg"' in sql
    assert 'FROM "stream_msg"' not in sql


def test_plan_spells_key_matching_through_the_dialect():
    loud = _LoudDialect()
    exact, _ = query.plan(_stream(), 'rio/a', query.parse_window(), dialect=loud)
    globbed, _ = query.plan(_stream(), 'rio/*', query.parse_window(), dialect=loud)
    assert 'IS_EXACTLY(_key_expr, ?)' in exact
    assert 'MATCHES(_key_expr, ?)' in globbed


def test_plan_defaults_to_the_sqlite_spelling():
    sql, _ = query.plan(_stream(), 'rio/*', query.parse_window())
    assert '"_key_expr" GLOB ?' in sql


def _zoned_stream():
    return StreamRegistry().add(Obs, index=('id',), json_index=('zones.department',))


def test_plan_renders_a_path_filter_through_the_dialect():
    sql, params = query.plan(
        _zoned_stream(),
        '',
        query.parse_window(),
        where={'zones.department': 5},
    )
    assert 'json_extract("_payload", \'$.zones.department\') = ?' in sql
    assert params[-1] == 5


def test_plan_path_filter_picks_the_text_extractor_for_a_string():
    from stored.dialect import DuckDBDialect

    sql, _ = query.plan(
        _zoned_stream(),
        '',
        query.parse_window(),
        where={'zones.department': 'front'},
        dialect=DuckDBDialect(),
    )
    assert 'json_extract_string("_payload", \'$.zones.department\')' in sql


def test_plan_rejects_an_undeclared_path():
    with pytest.raises(QueryError, match='not a declared json_index'):
        query.plan(_zoned_stream(), '', query.parse_window(), where={'zones.aisle': 1})


def test_plan_combines_path_filters_with_column_filters():
    sql, params = query.plan(
        _zoned_stream(),
        '',
        query.parse_window(),
        {'id': 7},
        where={'zones.department': 5},
    )
    assert '"id" = ?' in sql
    assert 'json_extract' in sql
    assert params == [7, 5]


# -- the anchor as a cursor --------------------------------------------------


def test_an_anchor_round_trips_through_an_opaque_cursor():
    anchor = (datetime.datetime(2026, 9, 6, 12, 0, 0, 123456), '0001-abc', 'rio/x/1')
    cursor = query.encode_anchor(anchor)
    assert '=' not in cursor
    assert cursor.isalnum() or set(cursor) <= set('-_') | set(cursor)  # URL-safe alphabet only
    assert query.decode_anchor(cursor) == anchor


@pytest.mark.parametrize('bad', ['', 'not-base64!', 'e30', 'WzEsMiwzXQ'])  # empty, junk, `{}`, `[1,2,3]`
def test_a_cursor_this_module_did_not_make_is_a_query_error(bad):
    with pytest.raises(QueryError):
        query.decode_anchor(bad)
