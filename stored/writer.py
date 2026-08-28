"""The batched writer.

A statement per message is wasteful for every backend (DuckDB is columnar and
dislikes per-row inserts; SQLite pays per-transaction overhead), so records are
buffered per table and flushed in batches, by row count or elapsed time, on a
background thread. Idempotency on the ``(_key_expr, _ts_hlc)`` primary key makes
redelivery a no-op.

Backend I/O is serialized through a shared lock (the owning ``Store``'s), so the
periodic flush thread never touches the connection concurrently with a query.
"""
from __future__ import annotations

import threading
from typing import Any

from .backends.base import StorageBackend
from .log import get_logger

_log = get_logger('writer')

_DEFAULT_FLUSH_ROWS = 1000
_DEFAULT_FLUSH_SECS = 1.0


class Writer:
    """Buffers rows and flushes them to the backend in batches.

    Args:
        backend: The storage backend to flush into.
        flush_rows: Flush when a table's buffer reaches this many rows.
        flush_secs: Flush a non-empty buffer at least this often; ``0`` disables
            the periodic flush thread (count- and close-driven flushing only).
        backend_lock: Shared lock serializing backend I/O; a private one is
            created when omitted.
    """

    __slots__ = (
        '_backend', '_flush_rows', '_flush_secs', '_buffers',
        '_buffer_lock', '_backend_lock', '_stop', '_thread', '_latest',
    )

    def __init__(
        self,
        backend: StorageBackend,
        *,
        flush_rows: int = _DEFAULT_FLUSH_ROWS,
        flush_secs: float = _DEFAULT_FLUSH_SECS,
        backend_lock: Any = None,
    ) -> None:
        self._backend = backend
        self._flush_rows = flush_rows
        self._flush_secs = flush_secs
        self._buffers: dict[str, list[dict[str, Any]]] = {}
        # history table -> (latest_table, key_columns, compare_column) for streams
        # that maintain a latest-per-key projection off the same batch.
        self._latest: dict[str, tuple[str, tuple[str, ...], str]] = {}
        self._buffer_lock = threading.Lock()
        self._backend_lock = backend_lock if backend_lock is not None else threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the periodic flush thread (no-op when ``flush_secs`` is 0)."""
        if self._thread is None and self._flush_secs > 0:
            self._thread = threading.Thread(
                target=self._run, name='stored-writer', daemon=True,
            )
            self._thread.start()

    def _run(self) -> None:
        """Flush every ``flush_secs`` until stopped."""
        while not self._stop.wait(self._flush_secs):
            try:
                self.flush()
            except Exception:
                _log.exception('periodic flush failed')

    def register_latest(
        self,
        table: str,
        latest_table: str,
        key_columns: tuple[str, ...],
        compare_column: str,
    ) -> None:
        """Maintain a latest-per-key projection of ``table`` into ``latest_table``.

        On every flush, each history batch is also upserted (newest-wins on
        ``compare_column``) into ``latest_table``, keyed by ``key_columns``.
        """
        self._latest[table] = (latest_table, key_columns, compare_column)

    def enqueue(self, table: str, row: dict[str, Any]) -> None:
        """Buffer ``row`` for ``table``, flushing if the row threshold is hit."""
        flush_now = False
        with self._buffer_lock:
            buffer = self._buffers.setdefault(table, [])
            buffer.append(row)
            if len(buffer) >= self._flush_rows:
                flush_now = True
        if flush_now:
            self.flush()

    def flush(self) -> None:
        """Drain all buffers to the backend now."""
        with self._buffer_lock:
            if not self._buffers:
                return
            pending = self._buffers
            self._buffers = {}
        with self._backend_lock:
            for table, rows in pending.items():
                if not rows:
                    continue
                try:
                    self._backend.append_batch(table, rows)
                    latest = self._latest.get(table)
                    if latest is not None:
                        latest_table, key_columns, compare_column = latest
                        self._backend.upsert_latest(latest_table, rows, key_columns, compare_column)
                except Exception:
                    _log.exception('flush of %d rows to %s failed (dropped)', len(rows), table)

    def close(self) -> None:
        """Stop the flush thread and drain any remaining buffered rows."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=(self._flush_secs or 0.0) + 5.0)
            self._thread = None
        self.flush()


__all__ = ['Writer']
