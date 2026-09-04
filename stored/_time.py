"""Time canonicalization: everything stored and queried is naive UTC.

Canonicalizing every datetime to naive UTC at the boundary keeps storage
dependency-free across backends — DuckDB's client would otherwise need ``pytz``
to round-trip ``TIMESTAMPTZ``, and the SQLite backend stores ISO-8601 text whose
lexical order is chronological. All stored times are UTC by construction.
"""
from __future__ import annotations

import datetime
import math
import re

_DURATION = re.compile(r'^(\d+(?:\.\d+)?)\s*([smhdw])$')
_DURATION_UNIT = {
    's': 'seconds',
    'm': 'minutes',
    'h': 'hours',
    'd': 'days',
    'w': 'weeks',
}


#: A retention horizon as a caller may express it: the string grammar
#: (``'7d'``), a number of **seconds** (``3600``, ``0.5``), or a timedelta.
#: :func:`duration_text` canonicalizes any of these to the string form.
Duration = str | int | float | datetime.timedelta


def parse_duration(text: str) -> datetime.timedelta:
    """Parse a retention horizon like ``'7d'`` / ``'48h'`` into a timedelta.

    Units: ``s`` seconds, ``m`` minutes, ``h`` hours, ``d`` days, ``w`` weeks.
    The count may carry a decimal part (``'1.5h'``, ``'0.5s'``).

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
    return datetime.timedelta(**{_DURATION_UNIT[match.group(2)]: float(match.group(1))})


def duration_text(value: Duration) -> str:
    """Canonicalize a retention horizon to its string form.

    Callers usually hold a *computed* horizon — a settings field in days, a
    ``timedelta`` — and had to format it themselves before handing it over.
    Numbers and timedeltas are read as **seconds** and rendered as ``'<n>s'``;
    a string is validated and returned unchanged (it is already canonical).

    Args:
        value: The horizon as a duration string, a number of seconds, or a
            :class:`datetime.timedelta`.

    Returns:
        The canonical horizon string.

    Raises:
        ValueError: If ``value`` is not a recognized duration, is negative, or
            is not finite.
    """
    if isinstance(value, str):
        parse_duration(value)  # validate; the string grammar is already canonical
        return value
    if isinstance(value, datetime.timedelta):
        seconds = value.total_seconds()
    elif isinstance(value, (int, float)):
        seconds = float(value)
    else:
        raise ValueError(f'invalid duration {value!r} (expected a string, seconds, or a timedelta)')
    if not math.isfinite(seconds):
        raise ValueError(f'invalid duration {value!r} (not a finite number of seconds)')
    if seconds < 0:
        raise ValueError(f'invalid duration {value!r} (negative)')
    # Six decimals is well past any sane retention horizon and keeps the
    # rendering inside the string grammar (no exponent notation).
    text = f'{seconds:.6f}'.rstrip('0').rstrip('.') or '0'
    return f'{text}s'


def to_naive_utc(value: datetime.datetime) -> datetime.datetime:
    """Return ``value`` as a naive (tz-free) UTC datetime."""
    if value.tzinfo is not None:
        value = value.astimezone(datetime.UTC).replace(tzinfo=None)
    return value


def utcnow() -> datetime.datetime:
    """Return the current time as a naive UTC datetime."""
    return datetime.datetime.now(datetime.UTC).replace(tzinfo=None)


__all__ = ['Duration', 'duration_text', 'parse_duration', 'to_naive_utc', 'utcnow']
