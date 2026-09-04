"""The core-local ``stored.row.Meta`` Protocol must track zeared's real ``ZenohMeta``.

The core mirrors ``ZenohMeta`` structurally (plan 02 Stage 3) without importing
it, so ``build_row`` reads its attributes directly. This exercises a *real*
``ZenohMeta`` through that path, so an upstream field rename surfaces here.
"""

from __future__ import annotations

import datetime

import seared as s
from zeared import ZenohMeta

from stored.registry import StreamRegistry
from stored.row import build_row


@s.seared
class _Msg(s.Seared):
    id: int = s.Int(required=True)


def test_real_zenoh_meta_flows_through_build_row():
    meta = ZenohMeta(
        key_expr='robot/1/telemetry',
        timestamp='abcd1234abcd1234/01',
        issued_at=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        source_info='peerA',
        schema='1',
    )
    stream = StreamRegistry().add(_Msg, index=('id',))
    row = build_row(stream, _Msg(id=1), meta)
    assert row['_key_expr'] == 'robot/1/telemetry'
    assert row['_ts_hlc'] == 'abcd1234abcd1234/01'
    assert row['_source'] == 'peerA'
    assert row['_schema'] == '1'
    assert row['_ts_source'] == 'hlc'


def test_none_meta_falls_back_to_recv():
    stream = StreamRegistry().add(_Msg, index=('id',))
    row = build_row(stream, _Msg(id=1), None, key='k/1')
    assert row['_key_expr'] == 'k/1'
    assert row['_source'] is None
    assert row['_schema'] is None
    assert row['_ts_source'] == 'recv'
