"""``stored.mesh`` — the contract-shaped surface over the transport.

Two namespaces, one dependency. :mod:`stored.zenoh` is named for the *transport*:
a session, the ``Chronicler``, the daemon, the selector-param query handler.
``stored.mesh`` is the layer *over* it — the shapes a service actually binds:
an async view of a blocking :class:`~stored.store.Store`, and (as they land) a
typed-request query binding, a ``latest`` queryable, and a replayer.

:class:`AsyncStore` needs **no transport at all** — it is asyncio over the core —
so it is imported eagerly here and stays importable without ``zeared``. Names that
do need the transport resolve lazily (PEP 562), the same pattern the top-level
``stored.Chronicler`` re-export uses, so ``from stored.mesh import AsyncStore``
never pays for Zenoh.
"""
from __future__ import annotations

from .async_store import AsyncStore

__all__ = ['AsyncStore']
