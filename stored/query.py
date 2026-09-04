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
from typing import TYPE_CHECKING, Any

from ._time import to_naive_utc, utcnow
from .dialect import DEFAULT_DIALECT, Dialect
from .errors import QueryError

if TYPE_CHECKING:
    from .registry import Stream

#: What a query time bound may be: a relative/ISO string, unix epoch seconds, a
#: ``datetime``, or ``None`` (open bound). Numeric/datetime bounds let a service map
#: a request's timestamps straight through — the same unix-seconds axis as
#: ``time_field`` (event time), no string round-trip.
TimeBound = str | int | float | datetime | None

#: A pagination anchor: the ``(time_column, _ts_hlc, _key_expr)`` values of the last
#: row already yielded. Those three are a **total** order — the first two order the
#: stream, and ``(_key_expr, _ts_hlc)`` is the primary key, so the triple is unique —
#: which is what lets a walk resume exactly where it stopped.
Anchor = tuple[Any, str, str]

DEFAULT_LIMIT = 1000
MAX_LIMIT = 100_000

#: Rows fetched per page by ``Store.iter``. Bounds peak memory to one page.
DEFAULT_CHUNK = 1000

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
        msg = f'invalid time bound: {exc}'
        raise QueryError(msg) from exc
    resolved = DEFAULT_LIMIT if limit is None else min(int(limit), MAX_LIMIT)
    if resolved < 0:
        msg = f'limit must be non-negative, got {resolved}'
        raise QueryError(msg)
    return Window(start=start, end=end, limit=resolved, ascending=order.lower() != 'desc')


def _equality_clauses(
    stream: Stream,
    filters: dict[str, Any] | None,
    where: dict[str, Any] | None,
    dialect: Dialect,
) -> list[tuple[str, Any]]:
    """Build the equality predicates: indexed columns, then declared JSON paths.

    Both are allow-listed against what the stream declared, so a name that reaches here
    unrecognized is a caller error rather than an unfiltered read.

    Args:
        stream: The stream being queried.
        filters: Column equality filters, or ``None``.
        where: Path equality filters, or ``None``.
        dialect: How this engine spells a JSON extraction.

    Returns:
        ``(sql fragment, bound value)`` pairs, in the order they should be applied.

    Raises:
        QueryError: If a filter names a non-indexed field, or a path is undeclared.
    """
    built: list[tuple[str, Any]] = []
    for name, value in (filters or {}).items():
        if name not in set(stream.index):
            msg = (
                f'filter {name!r} is not an indexed dimension of '
                f'{stream.cls.__name__} (indexed: {sorted(stream.index)})'
            )
            raise QueryError(msg)
        built.append((f'"{name}" = ?', value))
    for path, value in (where or {}).items():
        wire = stream.json_paths.get(path)
        if wire is None:
            msg = (
                f'path {path!r} is not a declared json_index of '
                f'{stream.cls.__name__} (declared: {sorted(stream.json_paths)})'
            )
            raise QueryError(msg)
        # The extractor depends on the value's type: DuckDB's json_extract returns
        # JSON, which will not compare against a bound string.
        built.append((f'{dialect.json_value("_payload", wire, text=isinstance(value, str))} = ?', value))
    return built


def plan(
    stream: Stream,
    key_expr: str,
    window: Window,
    filters: dict[str, Any] | None = None,
    *,
    where: dict[str, Any] | None = None,
    after: Anchor | None = None,
    skip_null_time: bool = False,
    table: str | None = None,
    dialect: Dialect = DEFAULT_DIALECT,
) -> tuple[str, list[Any]]:
    """Plan a parameterized SELECT for ``stream`` over ``key_expr`` + ``window``.

    Args:
        stream: The stream to query.
        key_expr: A concrete or wildcard topic to match against ``_key_expr``
            (``''`` matches everything). A ``*`` triggers a ``GLOB`` match.
        window: The resolved time window.
        filters: Allow-listed field equality filters (must be in
            ``stream.index``).
        where: Allow-listed **path** equality filters (must be in
            ``stream.json_paths``) — equality on a key inside a ``Dict`` field,
            read out of ``_payload``.
        after: Resume strictly after this :data:`Anchor` — the keyset predicate
            behind ``Store.iter``'s paging. ``None`` starts at the beginning.
        skip_null_time: Exclude rows whose temporal axis is ``NULL`` (a nullable
            ``time_field`` that arrived unset). Paging requires it: a ``NULL``
            compares as unknown against any anchor, so such a row could otherwise
            be yielded once and then silently strand the walk.
        table: The table to read. Defaults to the stream's history table; the
            latest projection carries the same columns and sort key, so pointing
            this at it is the whole difference between a history read and a
            current-state one.
        dialect: How to spell the non-portable fragments (see
            :mod:`stored.dialect`). Defaults to the SQLite baseline.

    Returns:
        ``(sql, params)`` ready for the backend.

    Raises:
        QueryError: If a filter names a non-indexed field, or a ``where`` key names
            an undeclared path.
    """
    clauses: list[str] = []
    params: list[Any] = []
    time_col = stream.time_column

    if key_expr:
        clauses.append(dialect.key_match('_key_expr', wildcard='*' in key_expr))
        params.append(key_expr)
    if window.start is not None:
        clauses.append(f'"{time_col}" >= ?')
        params.append(window.start)
    if window.end is not None:
        clauses.append(f'"{time_col}" <= ?')
        params.append(window.end)
    if skip_null_time:
        clauses.append(f'"{time_col}" IS NOT NULL')
    if after is not None:
        # Row-value comparison (SQLite >= 3.15, DuckDB) over the full sort key.
        comparison = '>' if window.ascending else '<'
        clauses.append(f'("{time_col}", "_ts_hlc", "_key_expr") {comparison} (?, ?, ?)')
        params.extend(after)
    for clause_sql, value in _equality_clauses(stream, filters, where, dialect):
        clauses.append(clause_sql)
        params.append(value)

    clause = f' WHERE {" AND ".join(clauses)}' if clauses else ''
    direction = 'ASC' if window.ascending else 'DESC'
    sql = (
        f'SELECT * FROM "{table or stream.table}"{clause} '  # noqa: S608 (identifiers are quoted; values are bound)
        f'ORDER BY "{time_col}" {direction}, "_ts_hlc" {direction}, "_key_expr" {direction} '
        f'LIMIT {int(window.limit)}'
    )
    return sql, params


__all__ = ['DEFAULT_CHUNK', 'DEFAULT_LIMIT', 'MAX_LIMIT', 'Anchor', 'TimeBound', 'Window', 'parse_window', 'plan']
