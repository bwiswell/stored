"""Tests for ``stored/latest.py``: the projection's upsert statement, and the meta a row carries.

The statements run on both engines in ``tests/backends``; these pin their shape.
"""

from __future__ import annotations

import datetime as dt

from stored import schema
from stored.dialect import DEFAULT_DIALECT
from stored.latest import LatestMeta, meta_of, upsert_params, upsert_sql

AT = dt.datetime(2026, 3, 1)
COLS = ['id', 'x', '_event_at']


class TestMetaOf:
    def test_a_stream_keeping_runs_reads_the_run_start(self):
        assert meta_of({schema.RUN_START: AT}) == LatestMeta(run_start=AT)

    def test_a_row_from_before_runs_were_kept_reads_none(self):
        assert meta_of({'id': 1}) == LatestMeta()

    def test_a_stream_without_runs_reads_none_whatever_the_row_holds(self):
        assert meta_of({schema.RUN_START: AT}, keeps_runs=False) == LatestMeta()


class TestUpsertParams:
    def test_the_row_in_column_order(self):
        assert upsert_params({'_event_at': AT, 'x': 2.0, 'id': 1}, COLS, '_event_at') == [1, 2.0, AT]

    def test_with_runs_a_new_entity_starts_its_run_at_its_record(self):
        params = upsert_params({'id': 1, 'x': 2.0, '_event_at': AT}, COLS, '_event_at', run_gap_s=60.0)
        assert params == [1, 2.0, AT, AT]


class TestUpsertSql:
    def test_newest_wins_and_a_stored_row_with_no_time_yields(self):
        sql = upsert_sql('latest_t', COLS, ['id'], '_event_at', dialect=DEFAULT_DIALECT)
        assert sql.startswith('INSERT INTO "latest_t" ("id", "x", "_event_at") VALUES (?, ?, ?) ON CONFLICT ("id")')
        assert 'DO UPDATE SET "x" = excluded."x", "_event_at" = excluded."_event_at" WHERE' in sql
        assert sql.endswith('WHERE (excluded."_event_at" >= "latest_t"."_event_at" OR "latest_t"."_event_at" IS NULL)')

    def test_with_runs_every_column_decides_and_the_run_start_is_one_more(self):
        sql = upsert_sql('latest_t', COLS, ['id'], '_event_at', dialect=DEFAULT_DIALECT, run_gap_s=3600.0)
        head, update = sql.split(' DO UPDATE SET ')
        assert head.endswith('("id", "x", "_event_at", "_run_start") VALUES (?, ?, ?, ?) ON CONFLICT ("id")')
        assert '"x" = CASE WHEN' in update
        assert '"_run_start" = CASE' in update
        assert ' WHERE ' not in update  # an older record may still extend the run
        assert '"id" =' not in update  # the key is never updated
