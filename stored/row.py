"""Convert between message instances and table rows.

:func:`build_row` turns a decoded message plus its Zenoh metadata into a
column-keyed dict; :func:`rehydrate` reverses that into a typed instance for
query replies. ``_payload`` (a seared JSON string) is the lossless source for
rehydration; scalar fields are additionally projected into typed columns.
"""
from __future__ import annotations

import datetime
import enum
import itertools
import pathlib
import uuid
from typing import Any

import seared as s

from . import schema
from ._time import to_naive_utc, utcnow
from .registry import Stream

# Monotonic tiebreaker so records lacking an HLC stamp still get a unique,
# sortable ``_ts_hlc``. ``itertools.count().__next__`` is atomic under CPython.
_synth_counter = itertools.count()


def _synth_ts(now: datetime.datetime) -> str:
    """Return a unique, lexicographically sortable ``_ts_hlc`` token for ``now``.

    ``now`` is treated as UTC (naive or aware) when deriving the epoch prefix.
    """
    micros = int(now.replace(tzinfo=datetime.UTC).timestamp() * 1_000_000)
    return f'{micros:020d}-{next(_synth_counter):08d}'


def _column_value(value: Any) -> Any:
    """Coerce a native field value into a backend-bindable scalar."""
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return to_naive_utc(value)
    if isinstance(value, enum.Enum):
        return value.name
    if isinstance(value, (uuid.UUID, pathlib.PurePath)):
        return str(value)
    return value


def _event_time(value: Any) -> datetime.datetime | None:
    """Normalize a ``time_field`` value to a naive-UTC datetime for ``_event_at``.

    A numeric value is read as **unix epoch seconds** (UTC); a ``datetime`` is
    canonicalized; a bare ``date`` becomes midnight UTC. ``None`` stays ``None``
    (the row is stored but excluded from event-time range/retention). The register-time
    check restricts ``time_field`` to these kinds, so no other type reaches here.
    """
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return to_naive_utc(value)
    if isinstance(value, datetime.date):
        return datetime.datetime(value.year, value.month, value.day)
    return datetime.datetime.fromtimestamp(value, datetime.UTC).replace(tzinfo=None)


def build_row(
    stream: Stream,
    msg: s.Seared,
    meta: Any = None,
    *,
    key: str | None = None,
    recv_at: datetime.datetime | None = None,
) -> dict[str, Any]:
    """Build a table row from ``msg`` and its ``meta``.

    Args:
        stream: The registered stream ``msg`` belongs to.
        msg: A decoded message instance.
        meta: Zenoh metadata (``ZenohMeta``-shaped: ``timestamp``, ``issued_at``,
            ``key_expr``, ``source_info``, ``schema``), or ``None`` for a
            non-mesh record.
        key: Explicit ``_key_expr`` when there is no ``meta`` (non-mesh record).
        recv_at: Override the local receive time (mainly for tests).

    Returns:
        A column-keyed row dict (meta columns + scalar fields + ``_payload``).
    """
    cls = stream.cls
    now = to_naive_utc(recv_at) if recv_at is not None else utcnow()
    issued_at = getattr(meta, 'issued_at', None)
    ts_hlc = getattr(meta, 'timestamp', None)
    key_expr = getattr(meta, 'key_expr', None) or key or ''

    if ts_hlc is None:
        ts_hlc = _synth_ts(now)
        ts_source = 'recv'
    else:
        ts_source = 'hlc' if issued_at is not None else 'recv'
    issued_at = to_naive_utc(issued_at) if issued_at is not None else now

    event_at = _event_time(getattr(msg, stream.time_field)) if stream.time_field is not None else None

    row: dict[str, Any] = {
        '_key_expr': key_expr,
        '_ts_hlc': ts_hlc,
        '_issued_at': issued_at,
        '_event_at': event_at,
        '_source': getattr(meta, 'source_info', None),
        '_schema': getattr(meta, 'schema', None),
        '_recv_at': now,
        '_ts_source': ts_source,
        '_payload': cls.dumps(msg),
    }
    for attr, _wire, field in cls.__seared_fields__:
        if schema.column_type(field) is None:
            continue  # complex/collection field — lives in _payload only
        row[attr] = _column_value(getattr(msg, attr, None))
    return row


def rehydrate(stream: Stream, columns: dict[str, Any]) -> s.Seared:
    """Reconstruct a typed message instance from a stored ``columns`` row.

    Args:
        stream: The registered stream to rebuild an instance for.
        columns: A column-keyed row dict as returned by the backend.

    Returns:
        An instance of ``stream.cls``.
    """
    return stream.cls.loads(columns['_payload'])


__all__ = ['build_row', 'rehydrate']
