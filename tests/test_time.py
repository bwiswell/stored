import datetime

import pytest

from stored._time import parse_duration, to_naive_utc, utcnow


def test_parse_duration_units():
    assert parse_duration('7d') == datetime.timedelta(days=7)
    assert parse_duration('48h') == datetime.timedelta(hours=48)
    assert parse_duration('30m') == datetime.timedelta(minutes=30)
    assert parse_duration('10s') == datetime.timedelta(seconds=10)
    assert parse_duration('2w') == datetime.timedelta(weeks=2)


def test_parse_duration_invalid():
    with pytest.raises(ValueError):
        parse_duration('nonsense')


def test_to_naive_utc_strips_tz():
    aware = datetime.datetime(2026, 1, 1, 12, tzinfo=datetime.UTC)
    naive = to_naive_utc(aware)
    assert naive.tzinfo is None
    assert naive.hour == 12


def test_utcnow_is_naive():
    assert utcnow().tzinfo is None
