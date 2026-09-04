"""Serve stored history over zeared queryables.

Builds an ``on_query`` handler that reads the time window from the query's
selector params (``from`` / ``to`` / ``limit`` / ``order``), queries the store
over the queried key expression, and streams each historical row back with
``ctx.reply(inst)``.

Only **non-RETAINED** classes can be served this way — zeared forbids
``on_query`` on a RETAINED class (retention already owns a queryable there).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..errors import QueryError

if TYPE_CHECKING:
    from collections.abc import Callable

    from ..registry import Stream
    from ..store import Store


def make_query_handler(store: Store, stream: Stream) -> Callable[[Any], None]:
    """Build an ``on_query`` handler serving ``stream``'s history from ``store``.

    Args:
        store: The store holding the recorded history.
        stream: The stream to serve.

    Returns:
        A handler ``(ctx) -> None`` suitable for ``Cls.on_query``.
    """
    cls = stream.cls

    def handler(ctx: Any) -> None:
        """Answer one query with matching stored history (streamed replies)."""
        params = ctx.params
        try:
            limit = int(params['limit']) if 'limit' in params else None
            results = store.query(
                cls,
                key=ctx.key_expr,
                since=params.get('from'),
                until=params.get('to'),
                limit=limit,
                order=params.get('order', 'asc'),
            )
        except (QueryError, ValueError) as exc:
            ctx.reply_err(str(exc))
            return
        for instance in results:
            ctx.reply(instance)

    return handler


__all__ = ['make_query_handler']
