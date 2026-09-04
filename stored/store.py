"""The :class:`Store` facade — the seared-only core surface.

Persists and queries ``seared`` objects with no mesh involved. The Zenoh
chronicler (``stored.zenoh``) wires a ``Store`` to a ``zeared`` session, but the
``Store`` itself never imports ``zeared``.
"""
from __future__ import annotations

import threading
from collections.abc import Iterator
from dataclasses import replace
from typing import Any

import seared as s

from . import schema
from ._time import Duration, duration_text
from .backends.base import StorageBackend
from .errors import ConfigError, QueryError
from .log import get_logger
from .query import DEFAULT_CHUNK, Anchor, TimeBound, Window, parse_window, plan
from .registry import Stream, StreamRegistry
from .row import Meta, build_row, rehydrate
from .ttl import Reaper
from .writer import Writer

_log = get_logger('store')


def _horizon(value: Duration | None) -> str | None:
    """Canonicalize a retention horizon for registration, as a ``ConfigError`` on bad input."""
    if value is None:
        return None
    try:
        return duration_text(value)
    except ValueError as exc:
        raise ConfigError(f'invalid retention {value!r}: {exc}') from exc


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
        retention: Duration | None = None,
        archive: Duration | None = None,
        index: tuple[str, ...] = (),
        time_field: str | None = None,
        latest_key: tuple[str, ...] = (),
        latest_retention: Duration | None = None,
    ) -> Stream:
        """Register a message class as a recorded stream and create its table(s).

        Args:
            cls: A ``@s.seared`` / ``@z.zeared`` message class.
            retention: Retention horizon — a duration string (``'7d'``), a number
                of **seconds** (``3600``), or a :class:`datetime.timedelta`;
                ``None`` keeps forever.
            archive: Cold-archival horizon (roadmap), same forms, or ``None``.
            index: Extra field names to index as queryable dimensions.
            time_field: A payload field naming the **domain event time** — retention
                and range queries key off it instead of the mesh delivery time.
            latest_key: Field names forming a **latest-per-key** projection's logical
                key (e.g. ``('source', 'epc')``). When set, a ``latest_<name>`` table
                keeps one newest-wins row per key, read via :meth:`latest`.
            latest_retention: Retention horizon for the latest projection (usually
                longer than ``retention``, same forms), or ``None`` to keep forever.

        Returns:
            The registered :class:`Stream`.

        Raises:
            ConfigError: If ``retention`` / ``latest_retention`` is not a valid duration.
            RegistrationError: If ``time_field`` / ``latest_key`` name unsuitable fields.
        """
        retention = _horizon(retention)
        archive = _horizon(archive)
        latest_retention = _horizon(latest_retention)
        stream = self._registry.add(
            cls, retention=retention, archive=archive, index=index, time_field=time_field,
            latest_key=latest_key, latest_retention=latest_retention,
        )
        columns = schema.derive_columns(cls)
        with self._lock:
            self._backend.ensure_table(stream.table, columns, schema.PRIMARY_KEY)
            for index_name, index_columns in schema.index_specs(stream.table, stream.time_column, stream.index):
                self._backend.ensure_index(index_name, stream.table, index_columns)
            if stream.has_latest:
                self._backend.ensure_table(stream.latest_table, columns, stream.latest_key)
        if stream.has_latest:
            self._writer.register_latest(
                stream.table, stream.latest_table, stream.latest_key, stream.time_column,
            )
        return stream

    def record(
        self,
        cls: type[s.Seared],
        msg: s.Seared,
        *,
        meta: Meta | None = None,
        key: str | None = None,
    ) -> None:
        """Buffer one message for persistence (flushed by the writer).

        Args:
            cls: The registered message class.
            msg: The instance to record.
            meta: Zenoh metadata (:class:`~stored.row.Meta`) on the mesh path, or
                ``None`` for a non-mesh record.
            key: Explicit ``_key_expr`` when there is no ``meta``.
        """
        stream = self._registry.get(cls)
        self._writer.enqueue(stream.table, build_row(stream, msg, meta, key=key))

    def query[M: s.Seared](
        self,
        cls: type[M],
        *,
        key: str | None = None,
        since: TimeBound = None,
        until: TimeBound = None,
        limit: int | None = None,
        order: str = 'asc',
        **filters: Any,
    ) -> list[M]:
        """Query stored history for ``cls`` (flushes pending writes first).

        Args:
            cls: The registered message class.
            key: Topic key to match (``None`` matches all; ``*`` globs).
            since: Lower time bound — ISO-8601, relative (``'-1h'``), unix seconds,
                a ``datetime``, or ``None``.
            until: Upper time bound (same forms).
            limit: Maximum rows (defaults + clamps per the query planner).
            order: ``'asc'`` or ``'desc'`` by time.
            **filters: Equality filters on indexed field dimensions.

        Returns:
            Decoded instances of ``cls``, time-ordered. A stream is always queried
            by — and answers with — the class it is *stored as*.
        """
        stream = self._registry.get(cls)
        window = parse_window(since=since, until=until, limit=limit, order=order)
        sql, params = plan(stream, key or '', window, filters or None)
        self._writer.flush()
        with self._lock:
            rows = self._backend.select(sql, params)
        return [rehydrate(cls, columns) for columns in rows]

    def iter[M: s.Seared](  # noqa: A003 — the streaming sibling of ``query``
        self,
        cls: type[M],
        *,
        key: str | None = None,
        since: TimeBound = None,
        until: TimeBound = None,
        limit: int | None = None,
        order: str = 'asc',
        chunk: int = DEFAULT_CHUNK,
        **filters: Any,
    ) -> Iterator[M]:
        """Stream stored history for ``cls`` in bounded memory.

        The streaming sibling of :meth:`query`: same window, key and filters, but
        rows arrive a page at a time instead of as one list, so a window far larger
        than memory can be walked. Three properties worth knowing:

        - **Flush-at-open, once.** Pending writes are flushed when ``iter`` is
          *called* (not on first ``next``), so the walk sees everything recorded
          before it started. Unlike :meth:`query`, it does **not** re-flush per page.
        - **Not a snapshot.** The store lock is released between pages so the writer
          keeps draining. Rows recorded mid-walk that sort *after* the current
          position will be yielded; the keyset anchor guarantees no row is yielded
          twice and none is skipped.
        - **Rows with no event time are skipped.** A nullable ``time_field`` that
          arrived unset has no place on the temporal axis the walk resumes along.
          :meth:`query` still returns them within an unbounded window.

        Args:
            cls: The registered message class.
            key: Topic key to match (``None`` matches all; ``*`` globs).
            since: Lower time bound — ISO-8601, relative (``'-1h'``), unix seconds,
                a ``datetime``, or ``None``.
            until: Upper time bound (same forms).
            limit: Maximum rows in total, or ``None`` for the whole window. Unlike
                :meth:`query` there is no implicit cap — streaming is the point.
            order: ``'asc'`` or ``'desc'`` by time.
            chunk: Rows per page — the memory bound, not a row cap.
            **filters: Equality filters on indexed field dimensions.

        Yields:
            Decoded instances of ``cls``, time-ordered.

        Raises:
            QueryError: If ``chunk``/``limit`` is invalid, a bound is unparseable,
                or a filter names a non-indexed field.
        """
        if chunk < 1:
            raise QueryError(f'chunk must be positive, got {chunk}')
        if limit is not None and limit < 0:
            raise QueryError(f'limit must be non-negative, got {limit}')
        stream = self._registry.get(cls)
        # Resolve the window once: a relative bound ('-1h') must not drift per page.
        window = parse_window(since=since, until=until, limit=chunk, order=order)
        self._writer.flush()
        return self._pages(cls, stream, window, key or '', filters or None, chunk, limit)

    def _pages[M: s.Seared](
        self,
        cls: type[M],
        stream: Stream,
        window: Window,
        key_expr: str,
        filters: dict[str, Any] | None,
        chunk: int,
        limit: int | None,
    ) -> Iterator[M]:
        """Walk ``stream`` page by page, resuming each from the previous page's last row."""
        remaining = limit
        anchor: Anchor | None = None
        time_col = stream.time_column
        while remaining is None or remaining > 0:
            size = chunk if remaining is None else min(chunk, remaining)
            sql, params = plan(
                stream,
                key_expr,
                replace(window, limit=size),
                filters,
                after=anchor,
                skip_null_time=True,
            )
            with self._lock:
                rows = self._backend.select(sql, params)
            if not rows:
                return
            for columns in rows:
                yield rehydrate(cls, columns)
            if remaining is not None:
                remaining -= len(rows)
            if len(rows) < size:
                return  # short page — the window is exhausted
            last = rows[-1]
            anchor = (last[time_col], last['_ts_hlc'], last['_key_expr'])

    def latest[M: s.Seared](self, cls: type[M], **key: Any) -> M | None:
        """Return the newest-recorded instance for one logical-entity ``key``.

        Reads the stream's latest-per-key projection (see ``latest_key`` on
        :meth:`register`): the last value for ``key`` **however old**, surviving
        history expiry — a tag's last-known position, a device's last state.
        Flushes pending writes first (read-your-writes).

        Args:
            cls: A registered class with a latest projection.
            **key: The full logical key (every ``latest_key`` field, exactly).

        Returns:
            The decoded ``cls`` instance, or ``None`` when the key has no recorded value.

        Raises:
            ConfigError: If ``cls`` has no latest projection.
            QueryError: If ``key`` does not name exactly the projection's key fields.
        """
        stream = self._registry.get(cls)
        if not stream.has_latest:
            raise ConfigError(f'{cls.__name__} has no latest projection (register with latest_key=…)')
        if set(key) != set(stream.latest_key):
            raise QueryError(
                f'latest({cls.__name__}) needs exactly {list(stream.latest_key)}, got {sorted(key)}',
            )
        where = ' AND '.join(f'"{col}" = ?' for col in stream.latest_key)
        sql = f'SELECT * FROM "{stream.latest_table}" WHERE {where} LIMIT 1'  # noqa: S608 (quoted identifiers)
        params = [key[col] for col in stream.latest_key]
        self._writer.flush()
        with self._lock:
            rows = self._backend.select(sql, params)
        return rehydrate(cls, rows[0]) if rows else None

    def counts(self, cls: type[s.Seared]) -> tuple[int, int]:
        """Row counts ``(history, latest)`` for ``cls`` — for observability/status.

        Flushes pending writes first (so the counts reflect everything recorded).
        ``latest`` is ``0`` when the stream has no latest projection.

        Args:
            cls: A registered message class.

        Returns:
            ``(history_rows, latest_rows)``.
        """
        stream = self._registry.get(cls)
        self._writer.flush()
        with self._lock:
            history = self._backend.select(f'SELECT COUNT(*) AS n FROM "{stream.table}"')[0]['n']  # noqa: S608 (quoted identifier)
            latest = 0
            if stream.has_latest:
                latest = self._backend.select(f'SELECT COUNT(*) AS n FROM "{stream.latest_table}"')[0]['n']  # noqa: S608 (quoted identifier)
        return int(history), int(latest)

    def flush(self) -> None:
        """Flush buffered writes to the backend."""
        self._writer.flush()

    def prune(self) -> int:
        """Force a TTL sweep across all streams; return the row count removed.

        Flushes pending writes first, so buffered rows are subject to the same sweep
        (consistent with :meth:`query` / :meth:`latest` read-your-writes).
        """
        self._writer.flush()
        with self._lock:
            return self._reaper.sweep()

    def close(self) -> None:
        """Flush, stop the writer, and close the backend."""
        self._writer.close()
        with self._lock:
            self._backend.close()


__all__ = ['Store']
