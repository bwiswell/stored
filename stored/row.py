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
from typing import Any, Protocol

import seared as s

from . import schema
from ._time import to_naive_utc, utcnow
from .registry import Stream


class Meta(Protocol):
    """Structural shape of the Zenoh metadata :func:`build_row` consumes.

    Mirrors zeared's ``ZenohMeta`` **without importing it**, so the seared-only
    core stays transport-free (plan 02) while ``ty`` gets a real shape for the
    ``meta`` surface in place of ``Any``. Any object exposing these attributes —
    the real ``ZenohMeta``, or a test stub — satisfies it.

    Attributes:
        key_expr: The Zenoh key expression the sample arrived on.
        timestamp: The raw HLC string (dedup/ordering tiebreaker), or ``None``.
        issued_at: The HLC delivery time parsed to UTC, or ``None``.
        source_info: The publisher's source id, or ``None``.
        schema: The publisher's wire ``SCHEMA`` tag, or ``None``.
    """

    key_expr: str
    timestamp: str | None
    issued_at: datetime.datetime | None
    source_info: str | None
    schema: str | None


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
    meta: Meta | None = None,
    *,
    key: str | None = None,
    recv_at: datetime.datetime | None = None,
) -> dict[str, Any]:
    """Build a table row from ``msg`` and its ``meta``.

    Args:
        stream: The registered stream ``msg`` belongs to.
        msg: A decoded message instance.
        meta: Zenoh metadata (see :class:`Meta`), or ``None`` for a non-mesh
            record.
        key: Explicit ``_key_expr`` when there is no ``meta`` (non-mesh record).
        recv_at: Override the local receive time (mainly for tests).

    Returns:
        A column-keyed row dict (meta columns + scalar fields + ``_payload``).
    """
    cls = stream.cls
    now = to_naive_utc(recv_at) if recv_at is not None else utcnow()
    if meta is not None:
        issued_at = meta.issued_at
        ts_hlc = meta.timestamp
        key_expr = meta.key_expr or key or ''
        source_info = meta.source_info
        msg_schema = meta.schema
    else:
        issued_at = ts_hlc = source_info = msg_schema = None
        key_expr = key or ''

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
        '_source': source_info,
        '_schema': msg_schema,
        '_recv_at': now,
        '_ts_source': ts_source,
        '_payload': cls.dumps(msg),
    }
    for attr, _wire, field in cls.__seared_fields__:
        if schema.column_type(field) is None:
            continue  # complex/collection field — lives in _payload only
        row[attr] = _column_value(getattr(msg, attr, None))
    return row


def rehydrate[M: s.Seared](cls: type[M], columns: dict[str, Any]) -> M:
    """Reconstruct a typed ``cls`` instance from a stored ``columns`` row.

    Takes the class rather than the :class:`~stored.registry.Stream` so the
    reconstructed type is the *caller's*: ``Store.query(cls)`` answers with
    ``cls``, and ``ty`` can see it.

    Args:
        cls: The class to rebuild — the class the stream is stored as.
        columns: A column-keyed row dict as returned by the backend.

    Returns:
        An instance of ``cls``.
    """
    return cls.loads(columns['_payload'])


__all__ = ['Meta', 'build_row', 'rehydrate']
