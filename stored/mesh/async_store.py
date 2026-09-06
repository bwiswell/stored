"""An async view of a blocking :class:`~stored.store.Store`.

Every ``stored`` read touches SQLite, which blocks. A service on an event loop
must therefore hop to a thread for each one — which is why all three consumers
hand-rolled the same ``_db_call = asyncio.to_thread(...)`` shim. :class:`AsyncStore`
is that shim, once: the core stays sync, this wraps it.

Deliberately **not** async: ``register`` (one-time DDL on the open path, before the
service starts serving) and ``record`` (a buffered enqueue — a thread hop per
message would cost more than the work it defers). Everything that reaches the
backend is awaited.
"""

from __future__ import annotations

import asyncio
import itertools
from typing import TYPE_CHECKING, Any

import seared as s

from ..query import DEFAULT_CHUNK, Anchor, TimeBound

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from ..registry import Stream
    from ..row import Meta
    from ..store import Store


class AsyncStore:
    """An awaitable facade over a :class:`~stored.store.Store`.

    Wraps an already-constructed store rather than opening one, so the caller keeps
    control of backend selection and writer tuning:

    ```python
    store = AsyncStore(stored.Store('history.db', flush_rows=5000))
    store.register(Location, retention=timedelta(days=3), latest_key=('source', 'epc'))
    ...
    newest = await store.latest(Location, source='rtls', epc=epc)
    ```

    Args:
        store: The blocking store to wrap. ``AsyncStore`` takes ownership of its
            lifecycle only insofar as :meth:`close` closes it.
    """

    __slots__ = ('_store',)

    def __init__(self, store: Store) -> None:
        self._store = store

    @property
    def store(self) -> Store:
        """The wrapped blocking store — the escape hatch for anything not mirrored here."""
        return self._store

    # -- open path (sync by design) ---------------------------------------

    def register(self, cls: type[s.Seared], **spec: Any) -> Stream:
        """Register a stream. Sync: one-time DDL on the open path, before serving starts.

        Args:
            cls: A ``@s.seared`` / ``@z.zeared`` message class.
            **spec: As :meth:`stored.Store.register` — ``retention``, ``index``,
                ``time_field``, ``latest_key``, ``latest_retention``, ``archive``.

        Returns:
            The registered :class:`~stored.registry.Stream`.
        """
        return self._store.register(cls, **spec)

    def record(self, cls: type[s.Seared], msg: s.Seared, *, meta: Meta | None = None, key: str | None = None) -> None:
        """Buffer one message. Sync: the enqueue is cheaper than the thread hop would be.

        Args:
            cls: The registered message class.
            msg: The instance to record.
            meta: Zenoh metadata on the mesh path, or ``None``.
            key: Explicit ``_key_expr`` when there is no ``meta``.
        """
        self._store.record(cls, msg, meta=meta, key=key)

    # -- reads -------------------------------------------------------------

    async def query[M: s.Seared](
        self,
        cls: type[M],
        *,
        key: str | None = None,
        since: TimeBound = None,
        until: TimeBound = None,
        limit: int | None = None,
        order: str = 'asc',
        where: dict[str, Any] | None = None,
        **filters: Any,
    ) -> list[M]:
        """Await :meth:`stored.Store.query` on a worker thread. Returns ``cls`` instances."""
        return await asyncio.to_thread(
            lambda: self._store.query(
                cls,
                key=key,
                since=since,
                until=until,
                limit=limit,
                order=order,
                where=where,
                **filters,
            ),
        )

    async def query_page[M: s.Seared](
        self,
        cls: type[M],
        *,
        key: str | None = None,
        since: TimeBound = None,
        until: TimeBound = None,
        limit: int | None = None,
        order: str = 'asc',
        where: dict[str, Any] | None = None,
        after: Anchor | None = None,
        **filters: Any,
    ) -> tuple[list[M], Anchor | None]:
        """Await :meth:`stored.Store.query_page` — one resumable page of history."""
        return await asyncio.to_thread(
            lambda: self._store.query_page(
                cls,
                key=key,
                since=since,
                until=until,
                limit=limit,
                order=order,
                where=where,
                after=after,
                **filters,
            ),
        )

    async def query_latest_page[M: s.Seared](
        self,
        cls: type[M],
        *,
        key: str | None = None,
        since: TimeBound = None,
        until: TimeBound = None,
        limit: int | None = None,
        order: str = 'asc',
        where: dict[str, Any] | None = None,
        after: Anchor | None = None,
        **filters: Any,
    ) -> tuple[list[M], Anchor | None]:
        """Await :meth:`stored.Store.query_latest_page` — one resumable page of current state."""
        return await asyncio.to_thread(
            lambda: self._store.query_latest_page(
                cls,
                key=key,
                since=since,
                until=until,
                limit=limit,
                order=order,
                where=where,
                after=after,
                **filters,
            ),
        )

    async def latest[M: s.Seared](self, cls: type[M], **key: Any) -> M | None:
        """Await :meth:`stored.Store.latest` on a worker thread."""
        return await asyncio.to_thread(lambda: self._store.latest(cls, **key))

    async def query_latest[M: s.Seared](
        self,
        cls: type[M],
        *,
        key: str | None = None,
        since: TimeBound = None,
        until: TimeBound = None,
        limit: int | None = None,
        order: str = 'asc',
        where: dict[str, Any] | None = None,
        **filters: Any,
    ) -> list[M]:
        """Await :meth:`stored.Store.query_latest` — current state for every matching entity."""
        return await asyncio.to_thread(
            lambda: self._store.query_latest(
                cls,
                key=key,
                since=since,
                until=until,
                limit=limit,
                order=order,
                where=where,
                **filters,
            ),
        )

    async def iter_latest[M: s.Seared](
        self,
        cls: type[M],
        *,
        key: str | None = None,
        since: TimeBound = None,
        until: TimeBound = None,
        limit: int | None = None,
        order: str = 'asc',
        chunk: int = DEFAULT_CHUNK,
        where: dict[str, Any] | None = None,
        **filters: Any,
    ) -> AsyncIterator[M]:
        """Stream current state without blocking the loop — :meth:`iter`'s projection sibling.

        Same one-hop-per-page pump and the same flush-on-first-step divergence from the
        sync call; see :meth:`iter`.
        """
        walk = await asyncio.to_thread(
            lambda: self._store.iter_latest(
                cls,
                key=key,
                since=since,
                until=until,
                limit=limit,
                order=order,
                chunk=chunk,
                where=where,
                **filters,
            ),
        )
        try:
            while True:
                page = await asyncio.to_thread(lambda: list(itertools.islice(walk, chunk)))
                for row in page:
                    yield row
                if len(page) < chunk:
                    return
        finally:
            walk.close()

    async def iter[M: s.Seared](
        self,
        cls: type[M],
        *,
        key: str | None = None,
        since: TimeBound = None,
        until: TimeBound = None,
        limit: int | None = None,
        order: str = 'asc',
        chunk: int = DEFAULT_CHUNK,
        where: dict[str, Any] | None = None,
        **filters: Any,
    ) -> AsyncIterator[M]:
        """Stream stored history without blocking the event loop.

        One thread hop **per page**, not per row: each hop drains ``chunk`` rows,
        which is exactly the page the underlying walk fetches per ``SELECT``. The
        loop is free between hops, so a long walk never stalls the service around it.

        Unlike :meth:`stored.Store.iter` — which flushes when *called* — the flush
        happens when iteration **starts**, since an async generator has no chance to
        act before its first ``await``. Everything else (keyset resumption, the
        not-a-snapshot property, skipping rows with no event time) is the sync walk's
        behaviour, unchanged.

        Args:
            cls: The registered message class.
            key: Topic key to match (``None`` matches all; ``*`` globs).
            since: Lower time bound.
            until: Upper time bound.
            limit: Maximum rows in total, or ``None`` for the whole window.
            order: ``'asc'`` or ``'desc'`` by time.
            chunk: Rows per page — the memory bound *and* the thread-hop size.
            where: Equality filters on declared ``json_index`` paths.
            **filters: Equality filters on indexed field dimensions.

        Yields:
            Decoded instances of ``cls``, time-ordered.
        """
        walk = await asyncio.to_thread(
            lambda: self._store.iter(
                cls,
                key=key,
                since=since,
                until=until,
                limit=limit,
                order=order,
                chunk=chunk,
                where=where,
                **filters,
            ),
        )
        try:
            while True:
                # Inline, not a generic helper: the pull must stay bound to *this*
                # call's element type for the yield below to check.
                page = await asyncio.to_thread(lambda: list(itertools.islice(walk, chunk)))
                for row in page:
                    yield row
                if len(page) < chunk:
                    return
        finally:
            walk.close()

    async def counts(self, cls: type[s.Seared]) -> tuple[int, int]:
        """Await :meth:`stored.Store.counts` — ``(history_rows, latest_rows)``."""
        return await asyncio.to_thread(lambda: self._store.counts(cls))

    # -- maintenance -------------------------------------------------------

    async def prune(self) -> int:
        """Await :meth:`stored.Store.prune` — rows removed across every stream."""
        return await asyncio.to_thread(self._store.prune)

    async def flush(self) -> None:
        """Await :meth:`stored.Store.flush` — push buffered rows to the backend."""
        await asyncio.to_thread(self._store.flush)

    async def close(self) -> None:
        """Flush and close the wrapped store, off the event loop."""
        await asyncio.to_thread(self._store.close)


__all__ = ['AsyncStore']
