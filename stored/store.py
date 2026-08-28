"""The :class:`Store` facade — the seared-only core surface.

Persists and queries ``seared`` objects with no mesh involved. The Zenoh
chronicler (``stored.zenoh``) wires a ``Store`` to a ``zeared`` session, but the
``Store`` itself never imports ``zeared``.
"""
from __future__ import annotations

import threading
from typing import Any

import seared as s

from . import schema
from ._time import parse_duration
from .backends.base import StorageBackend
from .errors import ConfigError
from .log import get_logger
from .query import parse_window, plan
from .registry import Stream, StreamRegistry
from .row import build_row, rehydrate
from .ttl import Reaper
from .writer import Writer

_log = get_logger('store')


def _make_backend(backend: str, path: str) -> StorageBackend:
    """Construct the named storage backend (imported lazily, so the core stays dep-light).

    The default ``sqlite`` backend is stdlib-only; ``duckdb`` needs the ``stored[duckdb]``
    extra and is imported only when requested.
    """
    if backend == 'sqlite':
        from .backends.sqlite_ import SQLiteBackend

        return SQLiteBackend(path)
    if backend == 'duckdb':
        from .backends.duckdb_ import DuckDBBackend

        return DuckDBBackend(path)
    raise ConfigError(f'unknown backend {backend!r} (expected one of: sqlite, duckdb)')


class Store:
    """A persistence store over a pluggable :class:`StorageBackend`.

    Records are buffered by a batched :class:`~stored.writer.Writer` and flushed
    by count or interval; ``query`` flushes first, so reads always see prior
    writes (read-your-writes).

    Args:
        path: Path to the backing database file.
        backend: Backend name (``'sqlite'`` default; ``'duckdb'`` via the extra;
            ``'postgres'`` later).
        flush_rows: Writer flush threshold in buffered rows.
        flush_secs: Writer flush interval in seconds (``0`` disables the timer).
    """

    __slots__ = ('_backend', '_registry', '_lock', '_writer', '_reaper')

    def __init__(
        self,
        path: str = 'chronicle.db',
        *,
        backend: str = 'sqlite',
        flush_rows: int = 1000,
        flush_secs: float = 1.0,
    ) -> None:
        self._lock = threading.RLock()
        self._backend = _make_backend(backend, path)
        self._registry = StreamRegistry()
        self._writer = Writer(
            self._backend,
            flush_rows=flush_rows,
            flush_secs=flush_secs,
            backend_lock=self._lock,
        )
        self._writer.start()
        self._reaper = Reaper(self._backend, self._registry)

    @property
    def registry(self) -> StreamRegistry:
        """The store's stream registry."""
        return self._registry

    @property
    def backend(self) -> StorageBackend:
        """The underlying storage backend."""
        return self._backend

    def register(
        self,
        cls: type[s.Seared],
        *,
        retention: str | None = None,
        archive: str | None = None,
        index: tuple[str, ...] = (),
        time_field: str | None = None,
    ) -> Stream:
        """Register a message class as a recorded stream and create its table.

        Args:
            cls: A ``@s.seared`` / ``@z.zeared`` message class.
            retention: Retention horizon (e.g. ``'7d'``), or ``None``.
            archive: Cold-archival horizon (roadmap), or ``None``.
            index: Extra field names to index as queryable dimensions.
            time_field: A payload field naming the **domain event time** — retention
                and range queries key off it instead of the mesh delivery time.

        Returns:
            The registered :class:`Stream`.

        Raises:
            ConfigError: If ``retention`` is not a valid duration.
            RegistrationError: If ``time_field`` names no temporal field of ``cls``.
        """
        if retention is not None:
            try:
                parse_duration(retention)
            except ValueError as exc:
                raise ConfigError(f'invalid retention {retention!r}: {exc}') from exc
        stream = self._registry.add(
            cls, retention=retention, archive=archive, index=index, time_field=time_field,
        )
        with self._lock:
            self._backend.ensure_table(stream.table, schema.derive_columns(cls), schema.PRIMARY_KEY)
        return stream

    def record(
        self,
        cls: type[s.Seared],
        msg: s.Seared,
        *,
        meta: Any = None,
        key: str | None = None,
    ) -> None:
        """Buffer one message for persistence (flushed by the writer).

        Args:
            cls: The registered message class.
            msg: The instance to record.
            meta: Zenoh metadata (mesh path), or ``None`` for a non-mesh record.
            key: Explicit ``_key_expr`` when there is no ``meta``.
        """
        stream = self._registry.get(cls)
        self._writer.enqueue(stream.table, build_row(stream, msg, meta, key=key))

    def query(
        self,
        cls: type[s.Seared],
        *,
        key: str | None = None,
        since: str | None = None,
        until: str | None = None,
        limit: int | None = None,
        order: str = 'asc',
        **filters: Any,
    ) -> list[s.Seared]:
        """Query stored history for ``cls`` (flushes pending writes first).

        Args:
            cls: The registered message class.
            key: Topic key to match (``None`` matches all; ``*`` globs).
            since: Lower time bound (ISO-8601 or relative, e.g. ``'-1h'``).
            until: Upper time bound (ISO-8601 or relative).
            limit: Maximum rows (defaults + clamps per the query planner).
            order: ``'asc'`` or ``'desc'`` by time.
            **filters: Equality filters on indexed field dimensions.

        Returns:
            Decoded instances of ``cls``, time-ordered.
        """
        stream = self._registry.get(cls)
        window = parse_window(since=since, until=until, limit=limit, order=order)
        sql, params = plan(stream, key or '', window, filters or None)
        self._writer.flush()
        with self._lock:
            rows = self._backend.select(sql, params)
        return [rehydrate(stream, columns) for columns in rows]

    def flush(self) -> None:
        """Flush buffered writes to the backend."""
        self._writer.flush()

    def prune(self) -> int:
        """Force a TTL sweep across all streams; return the row count removed."""
        with self._lock:
            return self._reaper.sweep()

    def close(self) -> None:
        """Flush, stop the writer, and close the backend."""
        self._writer.close()
        with self._lock:
            self._backend.close()


__all__ = ['Store']
