"""DuckDB storage backend (default).

Embedded, columnar, single-writer — a natural fit for the sole-writer
chronicler daemon, and the engine whose native Parquet ``COPY`` makes the
archival roadmap nearly free.

.. note::
   M0 scaffold: the connection is opened, but the storage methods are stubs
   landing in M1 (core store) and M2 (writer + TTL).
"""
from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any

import duckdb

from ..errors import BackendError
from ..log import get_logger

_log = get_logger('backends.duckdb')


class DuckDBBackend:
    """A :class:`~stored.backends.base.StorageBackend` over a DuckDB file.

    Args:
        path: Filesystem path to the DuckDB database (``':memory:'`` for an
            ephemeral store).
    """

    __slots__ = ('_path', '_conn')

    def __init__(self, path: str = 'chronicle.duckdb') -> None:
        self._path = path
        try:
            self._conn = duckdb.connect(path)
        except duckdb.Error as exc:  # pragma: no cover - env-specific
            raise BackendError(f'could not open DuckDB at {path!r}: {exc}') from exc

    @property
    def path(self) -> str:
        """The database file path this backend is bound to."""
        return self._path

    def ensure_table(
        self,
        table: str,
        columns: dict[str, str],
        primary_key: Sequence[str],
    ) -> None:
        """Create/reconcile ``table``. Stub — implemented in M1."""
        raise NotImplementedError('DuckDBBackend.ensure_table lands in M1')

    def append_batch(
        self,
        table: str,
        rows: Sequence[dict[str, Any]],
    ) -> None:
        """Append ``rows`` via the DuckDB appender. Stub — implemented in M1."""
        raise NotImplementedError('DuckDBBackend.append_batch lands in M1')

    def select(
        self,
        sql: str,
        params: Sequence[Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Run a read-only query. Stub — implemented in M1."""
        raise NotImplementedError('DuckDBBackend.select lands in M1')

    def delete_before(
        self,
        table: str,
        column: str,
        cutoff: datetime,
    ) -> int:
        """Delete rows older than ``cutoff``. Stub — implemented in M2."""
        raise NotImplementedError('DuckDBBackend.delete_before lands in M2')

    def close(self) -> None:
        """Close the DuckDB connection."""
        self._conn.close()


__all__ = ['DuckDBBackend']
