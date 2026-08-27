"""DuckDB storage backend (default).

Embedded, columnar, single-writer — a natural fit for the sole-writer
chronicler daemon, and the engine whose native Parquet ``COPY`` makes the
archival roadmap nearly free.
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
        """Create ``table`` if absent and add any missing columns (additive)."""
        col_defs = ', '.join(f'"{name}" {ctype}' for name, ctype in columns.items())
        pk = ', '.join(f'"{col}"' for col in primary_key)
        ddl = f'CREATE TABLE IF NOT EXISTS "{table}" ({col_defs}, PRIMARY KEY ({pk}))'
        try:
            self._conn.execute(ddl)
            existing = {
                r[0]
                for r in self._conn.execute(
                    'SELECT column_name FROM information_schema.columns '
                    'WHERE table_name = ?',
                    [table],
                ).fetchall()
            }
            for name, ctype in columns.items():
                if name not in existing:
                    self._conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{name}" {ctype}')
        except duckdb.Error as exc:
            raise BackendError(f'ensure_table({table!r}) failed: {exc}') from exc

    def append_batch(
        self,
        table: str,
        rows: Sequence[dict[str, Any]],
    ) -> None:
        """Insert ``rows`` in one call, ignoring primary-key conflicts."""
        if not rows:
            return
        cols = list(rows[0].keys())
        col_list = ', '.join(f'"{col}"' for col in cols)
        placeholders = ', '.join(['?'] * len(cols))
        sql = (
            f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholders}) '
            f'ON CONFLICT DO NOTHING'
        )
        params = [[row.get(col) for col in cols] for row in rows]
        try:
            self._conn.executemany(sql, params)
        except duckdb.Error as exc:
            raise BackendError(f'append_batch({table!r}) failed: {exc}') from exc

    def select(
        self,
        sql: str,
        params: Sequence[Any] = (),
    ) -> list[dict[str, Any]]:
        """Run a read-only query and return rows as column-name-keyed dicts."""
        try:
            cursor = self._conn.execute(sql, list(params))
            fetched = cursor.fetchall()
        except duckdb.Error as exc:
            raise BackendError(f'select failed: {exc}') from exc
        names = [desc[0] for desc in cursor.description]
        return [dict(zip(names, values, strict=True)) for values in fetched]

    def delete_before(
        self,
        table: str,
        column: str,
        cutoff: datetime,
    ) -> int:
        """Delete rows of ``table`` whose ``column`` is older than ``cutoff``.

        Returns the number of rows removed (via ``DELETE ... RETURNING``).
        """
        sql = f'DELETE FROM "{table}" WHERE "{column}" < ? RETURNING 1'
        try:
            deleted = self._conn.execute(sql, [cutoff]).fetchall()
        except duckdb.Error as exc:
            raise BackendError(f'delete_before({table!r}) failed: {exc}') from exc
        return len(deleted)

    def close(self) -> None:
        """Close the DuckDB connection."""
        self._conn.close()


__all__ = ['DuckDBBackend']
