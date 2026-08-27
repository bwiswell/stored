import pytest
import seared as s

from stored import query
from stored.errors import QueryError
from stored.registry import StreamRegistry


@s.seared
class Msg(s.Seared):
    id: int = s.Int(required=True)


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
