"""The history query planner.

Turns a time window + topic key + optional field filters into a parameterized
SELECT against a stream table, ordered by ``_ts_hlc``. Used by both the Python
``Store.query`` surface and the mesh ``on_query`` serve path.

.. note::
   M0 scaffold: window parsing and SQL planning land in M1/M3.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .registry import Stream

DEFAULT_LIMIT = 1000
MAX_LIMIT = 100_000


@dataclass(frozen=True, slots=True)
class Window:
    """A resolved query window.

    Attributes:
        start: Inclusive lower bound, or ``None`` for open-start.
        end: Inclusive upper bound, or ``None`` for open-end.
        limit: Maximum rows to return (clamped to :data:`MAX_LIMIT`).
        ascending: Sort direction on ``_ts_hlc``.
    """

    start: datetime | None
    end: datetime | None
    limit: int
    ascending: bool = True


def parse_window(
    *,
    since: str | None = None,
    until: str | None = None,
    limit: int | None = None,
    order: str = 'asc',
) -> Window:
    """Resolve a :class:`Window` from ISO-8601 or relative (``'-1h'``) bounds.

    Args:
        since: Lower bound (ISO-8601 or relative), or ``None``.
        until: Upper bound (ISO-8601 or relative), or ``None``.
        limit: Row cap; defaults to :data:`DEFAULT_LIMIT`, clamped to
            :data:`MAX_LIMIT`.
        order: ``'asc'`` or ``'desc'``.

    Returns:
        A resolved :class:`Window`.
    """
    raise NotImplementedError('query.parse_window lands in M1')


def plan(
    stream: Stream,
    key_expr: str,
    window: Window,
    filters: dict[str, Any] | None = None,
) -> tuple[str, list[Any]]:
    """Plan a parameterized SELECT for ``stream`` over ``key_expr`` + ``window``.

    Args:
        stream: The stream to query.
        key_expr: A concrete or wildcard topic to match against ``_key_expr``.
        window: The resolved time window.
        filters: Allow-listed field equality filters.

    Returns:
        ``(sql, params)`` ready for the backend.
    """
    raise NotImplementedError('query.plan lands in M1')


__all__ = ['Window', 'parse_window', 'plan', 'DEFAULT_LIMIT', 'MAX_LIMIT']
