"""Serve stored history over zeared queryables.

Builds an ``on_query`` handler that reads the time window + filters from the
query's selector params, plans a query against the store, and streams each
historical row back with ``ctx.reply(inst)``.

.. note::
   M0 scaffold: handler construction lands in M3. Only **non-RETAINED** classes
   can be served this way — zeared forbids ``on_query`` on a RETAINED class.
"""
from __future__ import annotations

from collections.abc import Callable
from typing import Any

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
    raise NotImplementedError('zenoh.serve.make_query_handler lands in M3')


__all__ = ['make_query_handler']
