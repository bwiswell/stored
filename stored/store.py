"""The :class:`Store` facade — the seared-only core surface.

Persists and queries ``seared`` objects with no mesh involved. The Zenoh
chronicler (``stored.zenoh``) wires a ``Store`` to a ``zeared`` session, but the
``Store`` itself never imports ``zeared``.
"""

from __future__ import annotations

import threading
from dataclasses import replace
from typing import TYPE_CHECKING, Any

import seared as s

from . import schema
from ._time import Duration, duration_text
from .errors import ConfigError, QueryError
from .log import get_logger
from .query import DEFAULT_CHUNK, Anchor, TimeBound, Window, parse_window, plan
from .registry import Stream, StreamRegistry
from .row import Meta, build_row, rehydrate
from .ttl import Reaper
from .writer import Writer

if TYPE_CHECKING:
    from collections.abc import Generator

    from .backends.base import StorageBackend

_log = get_logger('store')


def _horizon(value: Duration | None) -> str | None:
    """Canonicalize a retention horizon for registration, as a ``ConfigError`` on bad input."""
    if value is None:
        return None
    try:
        return duration_text(value)
    except ValueError as exc:
        msg = f'invalid retention {value!r}: {exc}'
        raise ConfigError(msg) from exc


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
    msg = f'unknown backend {backend!r} (expected one of: sqlite, duckdb)'
    raise ConfigError(msg)


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

    __slots__ = ('_backend', '_lock', '_reaper', '_registry', '_writer')

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
        json_index: tuple[str, ...] = (),
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
            json_index: Dotted paths into ``Dict`` fields to make filterable via
                ``where=`` — for keys that are open-ended by design (zone layers,
                say) and so can never be columns.

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
            cls,
            retention=retention,
            archive=archive,
            index=index,
            time_field=time_field,
            latest_key=latest_key,
            latest_retention=latest_retention,
            json_index=json_index,
        )
        columns = schema.derive_columns(cls)
        with self._lock:
            self._backend.ensure_table(stream.table, columns, schema.PRIMARY_KEY)
            for index_name, index_columns in schema.index_specs(
                stream.table,
                stream.time_column,
                stream.index,
                served_by_pk=schema.PRIMARY_KEY[:1],
            ):
                self._backend.ensure_index(index_name, stream.table, index_columns)
            sort_key = (stream.time_column, '_ts_hlc', '_key_expr')
            for index_name, wire in schema.json_index_specs(stream.table, stream.json_paths):
                self._backend.ensure_json_index(index_name, stream.table, wire, sort_key)
            if stream.has_latest:
                self._backend.ensure_table(stream.latest_table, columns, stream.latest_key)
                # ``query_latest`` reads this table, so it wants the same indexes —
                # minus whatever the entity key already leads.
                for index_name, index_columns in schema.index_specs(
                    stream.latest_table,
                    stream.time_column,
                    stream.index,
                    served_by_pk=stream.latest_key[:1],
                ):
                    self._backend.ensure_index(index_name, stream.latest_table, index_columns)
                for index_name, wire in schema.json_index_specs(stream.latest_table, stream.json_paths):
                    self._backend.ensure_json_index(index_name, stream.latest_table, wire, sort_key)
        if stream.has_latest:
            self._writer.register_latest(
                stream.table,
                stream.latest_table,
                stream.latest_key,
                stream.time_column,
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
        where: dict[str, Any] | None = None,
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
            where: Equality filters on declared ``json_index`` paths, e.g.
                ``{'zones.department': 5}``.
            **filters: Equality filters on indexed field dimensions.

        Returns:
            Decoded instances of ``cls``, time-ordered. A stream is always queried
            by — and answers with — the class it is *stored as*.
        """
        stream = self._registry.get(cls)
        window = parse_window(since=since, until=until, limit=limit, order=order)
        return self._select(cls, stream, stream.table, window, key or '', filters or None, where)

    def query_page[M: s.Seared](
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
        """One resumable page of :meth:`query`: the rows after ``after``, and where they stop.

        :meth:`iter` is the keyset walk run in-process; this is the same walk cut into
        request-sized steps for a caller on the far side of a query boundary — a historian
        answers one page and hands back the anchor (see :func:`~stored.query.encode_anchor`)
        the caller passes in to continue. The step semantics are :meth:`iter`'s: a full
        page always returns an anchor (the next page may then turn out empty), a short one
        ends the walk with ``None``, and rows with no event time are skipped, since they
        have no place on the axis the walk resumes along.

        Args:
            cls: The registered message class.
            key: Topic key to match (``None`` matches all; ``*`` globs).
            since: Lower time bound — ISO-8601, relative (``'-1h'``), unix seconds,
                a ``datetime``, or ``None``.
            until: Upper time bound (same forms).
            limit: Rows per page (defaults + clamps per the query planner).
            order: ``'asc'`` or ``'desc'`` by time. Resume with the same order.
            where: Equality filters on declared ``json_index`` paths.
            after: Resume strictly after this anchor; ``None`` is the first page.
            **filters: Equality filters on indexed field dimensions.

        Returns:
            ``(rows, anchor)`` — decoded ``cls`` instances in order, and the anchor to
            continue from, or ``None`` when this was the last page.
        """
        stream = self._registry.get(cls)
        window = parse_window(since=since, until=until, limit=limit, order=order)
        return self._select_page(cls, stream, stream.table, window, key or '', filters or None, where, after)

    def iter[M: s.Seared](
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
    ) -> Generator[M]:
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
            where: Equality filters on declared ``json_index`` paths.
            **filters: Equality filters on indexed field dimensions.

        Yields:
            Decoded instances of ``cls``, time-ordered. The walk is a generator, so
            an abandoned one can be released early with ``close()``.

        Raises:
            QueryError: If ``chunk``/``limit`` is invalid, a bound is unparseable,
                or a filter names a non-indexed field.
        """
        if chunk < 1:
            msg = f'chunk must be positive, got {chunk}'
            raise QueryError(msg)
        if limit is not None and limit < 0:
            msg = f'limit must be non-negative, got {limit}'
            raise QueryError(msg)
        stream = self._registry.get(cls)
        # Resolve the window once: a relative bound ('-1h') must not drift per page.
        window = parse_window(since=since, until=until, limit=chunk, order=order)
        self._writer.flush()
        return self._pages(cls, stream, stream.table, window, key or '', filters or None, chunk, limit, where)

    def _select[M: s.Seared](
        self,
        cls: type[M],
        stream: Stream,
        table: str,
        window: Window,
        key_expr: str,
        filters: dict[str, Any] | None,
        where: dict[str, Any] | None = None,
    ) -> list[M]:
        """Plan and run one read against ``table`` (flushes first — read-your-writes)."""
        sql, params = plan(
            stream,
            key_expr,
            window,
            filters,
            where=where,
            table=table,
            dialect=self._backend.dialect,
        )
        self._writer.flush()
        with self._lock:
            rows = self._backend.select(sql, params)
        return [rehydrate(cls, columns) for columns in rows]

    def _select_page[M: s.Seared](
        self,
        cls: type[M],
        stream: Stream,
        table: str,
        window: Window,
        key_expr: str,
        filters: dict[str, Any] | None,
        where: dict[str, Any] | None,
        after: Anchor | None,
    ) -> tuple[list[M], Anchor | None]:
        """One keyset step of :meth:`_pages`, answered with the anchor it stopped at."""
        sql, params = plan(
            stream,
            key_expr,
            window,
            filters,
            where=where,
            after=after,
            skip_null_time=True,
            table=table,
            dialect=self._backend.dialect,
        )
        self._writer.flush()
        with self._lock:
            rows = self._backend.select(sql, params)
        items = [rehydrate(cls, columns) for columns in rows]
        if not rows or len(rows) < window.limit:
            return items, None  # a short page ends the walk
        last = rows[-1]
        return items, (last[stream.time_column], last['_ts_hlc'], last['_key_expr'])

    def _pages[M: s.Seared](
        self,
        cls: type[M],
        stream: Stream,
        table: str,
        window: Window,
        key_expr: str,
        filters: dict[str, Any] | None,
        chunk: int,
        limit: int | None,
        where: dict[str, Any] | None = None,
    ) -> Generator[M]:
        """Walk ``table`` page by page, resuming each from the previous page's last row."""
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
                where=where,
                after=anchor,
                skip_null_time=True,
                table=table,
                dialect=self._backend.dialect,
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

    def query_latest[M: s.Seared](
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
        """Query **current state**: the newest instance of every entity, filtered.

        Where :meth:`latest` answers "where is this one tag", this answers "where is
        everything" — the population question an operator console asks. It reads the
        latest-per-key projection, which carries the same columns and the same sort
        key as the history table, so filters, ordering and paging behave identically;
        only the rows differ (one per entity, rather than one per observation).

        ``since``/``until`` therefore mean **last seen in this window** — a different
        question from "recorded in this window", and a useful one ("who has been seen
        in department 5 in the last hour"). Omit them for a straight snapshot.

        Args:
            cls: A registered class with a latest projection.
            key: Topic key to match (``None`` matches all; ``*`` globs).
            since: Lower bound on when the entity was last seen.
            until: Upper bound on the same.
            limit: Maximum rows (defaults + clamps per the query planner).
            order: ``'asc'`` or ``'desc'`` by last-seen time.
            where: Equality filters on declared ``json_index`` paths, e.g.
                ``{'zones.department': 5}``.
            **filters: Equality filters on indexed field dimensions.

        Returns:
            One decoded ``cls`` instance per matching entity, ordered by last-seen.

        Raises:
            ConfigError: If ``cls`` has no latest projection.
        """
        stream = self._require_latest(cls, 'query_latest')
        window = parse_window(since=since, until=until, limit=limit, order=order)
        return self._select(cls, stream, stream.latest_table, window, key or '', filters or None, where)

    def query_latest_page[M: s.Seared](
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
        """One resumable page of :meth:`query_latest` — :meth:`query_page` over current state.

        Same step semantics as :meth:`query_page`; the rows are one per entity, ordered by
        last-seen, which is the axis the anchor resumes along.

        Args:
            cls: A registered class with a latest projection.
            key: Topic key to match (``None`` matches all; ``*`` globs).
            since: Lower bound on when the entity was last seen.
            until: Upper bound on the same.
            limit: Rows per page (defaults + clamps per the query planner).
            order: ``'asc'`` or ``'desc'`` by last-seen time. Resume with the same order.
            where: Equality filters on declared ``json_index`` paths.
            after: Resume strictly after this anchor; ``None`` is the first page.
            **filters: Equality filters on indexed field dimensions.

        Returns:
            ``(rows, anchor)`` as :meth:`query_page` returns them.

        Raises:
            ConfigError: If ``cls`` has no latest projection.
        """
        stream = self._require_latest(cls, 'query_latest_page')
        window = parse_window(since=since, until=until, limit=limit, order=order)
        return self._select_page(cls, stream, stream.latest_table, window, key or '', filters or None, where, after)

    def iter_latest[M: s.Seared](
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
    ) -> Generator[M]:
        """Stream current state in bounded memory — :meth:`query_latest`, paged.

        The streaming sibling, for a population too large to hand back as one list:
        a floor with a hundred thousand tags has a hundred thousand latest rows.
        Same flush-at-open, keyset resumption and not-a-snapshot properties as
        :meth:`iter` (see there); ``limit=None`` means every matching entity.

        Args:
            cls: A registered class with a latest projection.
            key: Topic key to match (``None`` matches all; ``*`` globs).
            since: Lower bound on when the entity was last seen.
            until: Upper bound on the same.
            limit: Maximum rows in total, or ``None`` for every match.
            order: ``'asc'`` or ``'desc'`` by last-seen time.
            chunk: Rows per page — the memory bound, not a row cap.
            where: Equality filters on declared ``json_index`` paths.
            **filters: Equality filters on indexed field dimensions.

        Yields:
            One decoded ``cls`` instance per matching entity, ordered by last-seen.

        Raises:
            ConfigError: If ``cls`` has no latest projection.
            QueryError: If ``chunk``/``limit`` is invalid, a bound is unparseable,
                or a filter names a non-indexed field.
        """
        if chunk < 1:
            msg = f'chunk must be positive, got {chunk}'
            raise QueryError(msg)
        if limit is not None and limit < 0:
            msg = f'limit must be non-negative, got {limit}'
            raise QueryError(msg)
        stream = self._require_latest(cls, 'iter_latest')
        window = parse_window(since=since, until=until, limit=chunk, order=order)
        self._writer.flush()
        return self._pages(
            cls,
            stream,
            stream.latest_table,
            window,
            key or '',
            filters or None,
            chunk,
            limit,
            where,
        )

    def _require_latest(self, cls: type[s.Seared], what: str) -> Stream:
        """The stream for ``cls``, or a clear error when it keeps no latest projection."""
        stream = self._registry.get(cls)
        if not stream.has_latest:
            msg = f'{what}({cls.__name__}) needs a latest projection (register with latest_key=…)'
            raise ConfigError(msg)
        return stream

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
        stream = self._require_latest(cls, 'latest')
        if set(key) != set(stream.latest_key):
            msg = f'latest({cls.__name__}) needs exactly {list(stream.latest_key)}, got {sorted(key)}'
            raise QueryError(
                msg,
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
