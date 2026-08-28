"""The history query planner.

Turns a time window + topic key + optional field filters into a parameterized
SELECT against a stream table, ordered by the stream's temporal axis
(``stream.time_column`` — ``_event_at`` when a ``time_field`` is set, else
``_issued_at``; then ``_ts_hlc`` as a stable tiebreaker). Used by both the Python
``Store.query`` surface and the mesh ``on_query`` serve path.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from ._time import to_naive_utc, utcnow
from .errors import QueryError
from .registry import Stream

#: What a query time bound may be: a relative/ISO string, unix epoch seconds, a
#: ``datetime``, or ``None`` (open bound). Numeric/datetime bounds let a service map
#: a request's timestamps straight through — the same unix-seconds axis as
#: ``time_field`` (event time), no string round-trip.
TimeBound = str | int | float | datetime | None

DEFAULT_LIMIT = 1000
MAX_LIMIT = 100_000

_RELATIVE = re.compile(r'^-(\d+)([smhd])$')
_UNIT_KW = {'s': 'seconds', 'm': 'minutes', 'h': 'hours', 'd': 'days'}


@dataclass(frozen=True, slots=True)
class Window:
    """A resolved query window.

    Attributes:
        start: Inclusive lower bound, or ``None`` for open-start.
        end: Inclusive upper bound, or ``None`` for open-end.
        limit: Maximum rows to return (clamped to :data:`MAX_LIMIT`).
        ascending: Sort direction on ``_issued_at`` / ``_ts_hlc``.
    """

    start: datetime | None
    end: datetime | None
    limit: int
    ascending: bool = True


def _resolve_time(value: TimeBound, now: datetime) -> datetime | None:
    """Resolve one bound to naive UTC.

    Accepts ``None`` (open), a ``datetime``, unix epoch **seconds** (int/float), a
    ``-<n>{s,m,h,d}`` relative offset, or an ISO-8601 string.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return to_naive_utc(value)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, UTC).replace(tzinfo=None)
    match = _RELATIVE.match(value)
    if match is not None:
        return now - timedelta(**{_UNIT_KW[match.group(2)]: int(match.group(1))})
    parsed = datetime.fromisoformat(value)
    return to_naive_utc(parsed) if parsed.tzinfo is not None else parsed


def parse_window(
    *,
    since: TimeBound = None,
    until: TimeBound = None,
    limit: int | None = None,
    order: str = 'asc',
) -> Window:
    """Resolve a :class:`Window` from its bounds.

    Args:
        since: Lower bound — ISO-8601, relative (``'-1h'``), unix seconds, a
            ``datetime``, or ``None``.
        until: Upper bound (same forms), or ``None``.
        limit: Row cap; defaults to :data:`DEFAULT_LIMIT`, clamped to
            :data:`MAX_LIMIT`.
        order: ``'asc'`` or ``'desc'``.

    Returns:
        A resolved :class:`Window`.

    Raises:
        QueryError: If a bound is unparseable or ``limit`` is negative.
    """
    now = utcnow()
    try:
        start = _resolve_time(since, now)
        end = _resolve_time(until, now)
    except ValueError as exc:
        raise QueryError(f'invalid time bound: {exc}') from exc
    resolved = DEFAULT_LIMIT if limit is None else min(int(limit), MAX_LIMIT)
    if resolved < 0:
        raise QueryError(f'limit must be non-negative, got {resolved}')
    return Window(start=start, end=end, limit=resolved, ascending=order.lower() != 'desc')


def plan(
    stream: Stream,
    key_expr: str,
    window: Window,
    filters: dict[str, Any] | None = None,
) -> tuple[str, list[Any]]:
    """Plan a parameterized SELECT for ``stream`` over ``key_expr`` + ``window``.

    Args:
        stream: The stream to query.
        key_expr: A concrete or wildcard topic to match against ``_key_expr``
            (``''`` matches everything). A ``*`` triggers a ``GLOB`` match.
        window: The resolved time window.
        filters: Allow-listed field equality filters (must be in
            ``stream.index``).

    Returns:
        ``(sql, params)`` ready for the backend.

    Raises:
        QueryError: If a filter names a non-indexed field.
    """
    where: list[str] = []
    params: list[Any] = []
    time_col = stream.time_column

    if key_expr:
        where.append('"_key_expr" GLOB ?' if '*' in key_expr else '"_key_expr" = ?')
        params.append(key_expr)
    if window.start is not None:
        where.append(f'"{time_col}" >= ?')
        params.append(window.start)
    if window.end is not None:
        where.append(f'"{time_col}" <= ?')
        params.append(window.end)
    if filters:
        allowed = set(stream.index)
        for name, value in filters.items():
            if name not in allowed:
                raise QueryError(
                    f'filter {name!r} is not an indexed dimension of '
                    f'{stream.cls.__name__} (indexed: {sorted(allowed)})',
                )
            where.append(f'"{name}" = ?')
            params.append(value)

    clause = f' WHERE {" AND ".join(where)}' if where else ''
    direction = 'ASC' if window.ascending else 'DESC'
    sql = (
        f'SELECT * FROM "{stream.table}"{clause} '
        f'ORDER BY "{time_col}" {direction}, "_ts_hlc" {direction} '
        f'LIMIT {int(window.limit)}'
    )
    return sql, params


__all__ = ['Window', 'TimeBound', 'parse_window', 'plan', 'DEFAULT_LIMIT', 'MAX_LIMIT']
