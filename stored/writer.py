"""The batched writer.

DuckDB is an OLAP engine — per-row inserts are slow — so records are buffered
per table and flushed in batches, by row count or elapsed time, on a background
thread. Idempotency on the ``(_key_expr, _ts_hlc)`` primary key makes redelivery
a no-op.

.. note::
   M0 scaffold: buffering and flush scheduling land in M2.
"""
from __future__ import annotations

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
        flush_secs: Flush a non-empty buffer at least this often.
    """

    __slots__ = ('_backend', '_flush_rows', '_flush_secs', '_buffers')

    def __init__(
        self,
        backend: StorageBackend,
        *,
        flush_rows: int = _DEFAULT_FLUSH_ROWS,
        flush_secs: float = _DEFAULT_FLUSH_SECS,
    ) -> None:
        self._backend = backend
        self._flush_rows = flush_rows
        self._flush_secs = flush_secs
        self._buffers: dict[str, list[dict[str, Any]]] = {}

    def enqueue(self, table: str, row: dict[str, Any]) -> None:
        """Buffer ``row`` for ``table``, flushing if a threshold is reached."""
        raise NotImplementedError('Writer.enqueue lands in M2')

    def flush(self) -> None:
        """Drain all buffers to the backend now."""
        raise NotImplementedError('Writer.flush lands in M2')

    def close(self) -> None:
        """Flush and stop the writer."""
        raise NotImplementedError('Writer.close lands in M2')


__all__ = ['Writer']
