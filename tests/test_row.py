import datetime

import seared as s

from stored.registry import StreamRegistry
from stored.row import build_row, rehydrate


@s.seared
class Msg(s.Seared):
    id:   int = s.Int(required=True)
    name: str = s.Str(default='')


class FakeMeta:
    key_expr = 'robot/1/telemetry'
    timestamp = 'abcd1234abcd1234/01'
    issued_at = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
    source_info = 'peerA'
    schema = '1'


def _stream():
    return StreamRegistry().add(Msg, index=('id',))


def test_build_row_meta_and_field_columns():
    row = build_row(_stream(), Msg(id=7, name='a'), None, key='k/1')
    assert row['_key_expr'] == 'k/1'
    assert row['id'] == 7
    assert row['name'] == 'a'
    assert row['_ts_source'] == 'recv'
    assert isinstance(row['_issued_at'], datetime.datetime)
    assert row['_payload']


def test_build_row_uses_meta():
    row = build_row(_stream(), Msg(id=1), FakeMeta())
    assert row['_ts_hlc'] == 'abcd1234abcd1234/01'
    assert row['_key_expr'] == 'robot/1/telemetry'
    assert row['_source'] == 'peerA'
    assert row['_schema'] == '1'
    assert row['_ts_source'] == 'hlc'


def test_synth_ts_is_unique():
    stream = _stream()
    first = build_row(stream, Msg(id=1), None)
    second = build_row(stream, Msg(id=2), None)
    assert first['_ts_hlc'] != second['_ts_hlc']
    assert first['_ts_hlc'] < second['_ts_hlc']


def test_rehydrate_round_trips():
    stream = _stream()
    back = rehydrate(stream, build_row(stream, Msg(id=42, name='z'), None))
    assert back.id == 42
    assert back.name == 'z'
