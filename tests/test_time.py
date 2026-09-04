import datetime

import pytest

from stored._time import duration_text, parse_duration, to_naive_utc, utcnow


def test_parse_duration_units():
    assert parse_duration('7d') == datetime.timedelta(days=7)
    assert parse_duration('48h') == datetime.timedelta(hours=48)
    assert parse_duration('30m') == datetime.timedelta(minutes=30)
    assert parse_duration('10s') == datetime.timedelta(seconds=10)
    assert parse_duration('2w') == datetime.timedelta(weeks=2)


def test_parse_duration_accepts_decimals():
    assert parse_duration('1.5h') == datetime.timedelta(hours=1.5)
    assert parse_duration('0.5s') == datetime.timedelta(seconds=0.5)


def test_parse_duration_invalid():
    with pytest.raises(ValueError):
        parse_duration('nonsense')


def test_duration_text_passes_strings_through():
    assert duration_text('7d') == '7d'
    assert duration_text('48h') == '48h'


def test_duration_text_renders_seconds_and_timedeltas():
    assert duration_text(3600) == '3600s'
    assert duration_text(0.5) == '0.5s'
    assert duration_text(datetime.timedelta(days=7)) == '604800s'
    assert duration_text(datetime.timedelta(days=1.5)) == '129600s'


def test_duration_text_round_trips_through_parse():
    for value in (90, 0.25, datetime.timedelta(hours=36)):
        expected = value if isinstance(value, datetime.timedelta) else datetime.timedelta(seconds=value)
        assert parse_duration(duration_text(value)) == expected


def test_duration_text_rejects_bad_values():
    for bad in ('nonsense', -1, float('inf'), datetime.timedelta(seconds=-1), object()):
        with pytest.raises(ValueError):
            duration_text(bad)


def test_to_naive_utc_strips_tz():
    aware = datetime.datetime(2026, 1, 1, 12, tzinfo=datetime.UTC)
    naive = to_naive_utc(aware)
    assert naive.tzinfo is None
    assert naive.hour == 12


def test_utcnow_is_naive():
    assert utcnow().tzinfo is None
