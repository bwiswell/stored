"""Time canonicalization: everything stored and queried is naive UTC.

DuckDB's Python client needs ``pytz`` to round-trip ``TIMESTAMPTZ``. We sidestep
that dependency by canonicalizing every datetime to naive UTC at the boundary
and storing ``TIMESTAMP`` columns — all stored times are UTC by construction.
"""
from __future__ import annotations

import datetime


def to_naive_utc(value: datetime.datetime) -> datetime.datetime:
    """Return ``value`` as a naive (tz-free) UTC datetime."""
    if value.tzinfo is not None:
        value = value.astimezone(datetime.UTC).replace(tzinfo=None)
    return value


def utcnow() -> datetime.datetime:
    """Return the current time as a naive UTC datetime."""
    return datetime.datetime.now(datetime.UTC).replace(tzinfo=None)


__all__ = ['to_naive_utc', 'utcnow']
