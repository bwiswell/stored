"""Time canonicalization: everything stored and queried is naive UTC.

DuckDB's Python client needs ``pytz`` to round-trip ``TIMESTAMPTZ``. We sidestep
that dependency by canonicalizing every datetime to naive UTC at the boundary
and storing ``TIMESTAMP`` columns — all stored times are UTC by construction.
"""
from __future__ import annotations

import datetime
import re

_DURATION = re.compile(r'^(\d+)\s*([smhdw])$')
_DURATION_UNIT = {
    's': 'seconds',
    'm': 'minutes',
    'h': 'hours',
    'd': 'days',
    'w': 'weeks',
}


def parse_duration(text: str) -> datetime.timedelta:
    """Parse a retention horizon like ``'7d'`` / ``'48h'`` into a timedelta.

    Units: ``s`` seconds, ``m`` minutes, ``h`` hours, ``d`` days, ``w`` weeks.

    Args:
        text: The horizon string.

    Returns:
        The corresponding :class:`datetime.timedelta`.

    Raises:
        ValueError: If ``text`` is not a recognized duration.
    """
    match = _DURATION.match(text.strip().lower())
    if match is None:
        raise ValueError(f'invalid duration {text!r} (expected e.g. 7d, 48h, 30m)')
    return datetime.timedelta(**{_DURATION_UNIT[match.group(2)]: int(match.group(1))})


def to_naive_utc(value: datetime.datetime) -> datetime.datetime:
    """Return ``value`` as a naive (tz-free) UTC datetime."""
    if value.tzinfo is not None:
        value = value.astimezone(datetime.UTC).replace(tzinfo=None)
    return value


def utcnow() -> datetime.datetime:
    """Return the current time as a naive UTC datetime."""
    return datetime.datetime.now(datetime.UTC).replace(tzinfo=None)


__all__ = ['parse_duration', 'to_naive_utc', 'utcnow']
