"""``stored.mesh`` — the contract-shaped surface over the transport.

Two namespaces, one dependency. :mod:`stored.zenoh` is named for the *transport*:
a session, the ``Chronicler``, the daemon, the selector-param query handler.
``stored.mesh`` is the layer *over* it — the shapes a service actually binds: an
async view of a blocking :class:`~stored.store.Store`, a typed-request query
binding with a ``latest`` queryable, and a replayer that publishes recorded
history back onto the mesh.

:class:`AsyncStore` needs **no transport at all** — it is asyncio over the core —
so it is imported eagerly here and stays importable without ``zeared``. Names that
do need the transport resolve lazily (PEP 562), the same pattern the top-level
``stored.Chronicler`` re-export uses, so ``from stored.mesh import AsyncStore``
never pays for Zenoh.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from .async_store import AsyncStore

if TYPE_CHECKING:
    from .binding import UNSET_FALSY, Binding
    from .replay import Replayer, ReplayHandle

__all__ = ['UNSET_FALSY', 'AsyncStore', 'Binding', 'ReplayHandle', 'Replayer']


def __getattr__(name: str) -> object:
    """Resolve the transport-bound names lazily (PEP 562).

    :class:`Binding` imports ``zeared``; :class:`AsyncStore` must not have to. Keeping
    the binding behind this hook is what lets ``from stored.mesh import AsyncStore``
    stay Zenoh-free — an invariant with a test, not a convention.
    """
    if name in ('Binding', 'UNSET_FALSY'):
        from . import binding

        return getattr(binding, name)
    if name in ('Replayer', 'ReplayHandle'):
        from . import replay

        return getattr(replay, name)
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
