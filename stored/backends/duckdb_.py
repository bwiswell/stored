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

from ..dialect import Dialect, DuckDBDialect
from ..errors import BackendError
from ..log import get_logger

_log = get_logger('backends.duckdb')

# Rows per INSERT statement. A single multi-row INSERT is orders of magnitude
# faster than per-row executemany on DuckDB; chunking bounds statement size.
_CHUNK = 1000

_DIALECT = DuckDBDialect()


class DuckDBBackend:
    """A :class:`~stored.backends.base.StorageBackend` over a DuckDB file.

    Args:
        path: Filesystem path to the DuckDB database (``':memory:'`` for an
            ephemeral store).
    """

    __slots__ = ('_path', '_conn', '_pks')

    def __init__(self, path: str = 'chronicle.duckdb') -> None:
        self._path = path
        self._pks: dict[str, tuple[str, ...]] = {}
        try:
            self._conn = duckdb.connect(path)
        except duckdb.Error as exc:  # pragma: no cover - env-specific
            raise BackendError(f'could not open DuckDB at {path!r}: {exc}') from exc

    @property
    def path(self) -> str:
        """The database file path this backend is bound to."""
        return self._path

    @property
    def dialect(self) -> Dialect:
        """DuckDB differs from the baseline only in how it reaches into JSON."""
        return _DIALECT

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
        self._pks[table] = tuple(primary_key)

    def ensure_index(
        self,
        name: str,
        table: str,
        columns: Sequence[str],
    ) -> None:
        """Create the secondary index ``name`` if absent (idempotent DDL)."""
        cols = ', '.join(f'"{col}"' for col in columns)
        try:
            self._conn.execute(f'CREATE INDEX IF NOT EXISTS "{name}" ON "{table}" ({cols})')
        except duckdb.Error as exc:
            raise BackendError(f'ensure_index({name!r}) failed: {exc}') from exc

    def append_batch(
        self,
        table: str,
        rows: Sequence[dict[str, Any]],
    ) -> None:
        """Insert ``rows`` via chunked multi-row INSERTs, ignoring PK conflicts.

        Rows are de-duplicated by primary key within the batch (keeping the last
        occurrence) so a single multi-row INSERT never self-conflicts;
        ``ON CONFLICT DO NOTHING`` then handles conflicts with existing rows.
        """
        if not rows:
            return
        pk = self._pks.get(table)
        if pk:
            deduped: dict[tuple[Any, ...], dict[str, Any]] = {}
            for row in rows:
                deduped[tuple(row.get(col) for col in pk)] = row
            rows = list(deduped.values())

        cols = list(rows[0].keys())
        col_list = ', '.join(f'"{col}"' for col in cols)
        row_ph = '(' + ', '.join(['?'] * len(cols)) + ')'
        try:
            for start in range(0, len(rows), _CHUNK):
                chunk = rows[start:start + _CHUNK]
                values = ', '.join([row_ph] * len(chunk))
                sql = (
                    f'INSERT INTO "{table}" ({col_list}) VALUES {values} '
                    f'ON CONFLICT DO NOTHING'
                )
                params = [row.get(col) for row in chunk for col in cols]
                self._conn.execute(sql, params)
        except duckdb.Error as exc:
            raise BackendError(f'append_batch({table!r}) failed: {exc}') from exc

    def upsert_latest(
        self,
        table: str,
        rows: Sequence[dict[str, Any]],
        key_columns: Sequence[str],
        compare_column: str,
    ) -> None:
        """Upsert rows into ``table`` newest-wins on ``compare_column``.

        ``INSERT … ON CONFLICT(<key>) DO UPDATE … WHERE excluded.<cmp> >= <cmp>``.
        One ``execute`` per row (order-independent newest-wins — an older row can
        never overwrite a newer one) against the ``key_columns`` primary key.
        """
        if not rows:
            return
        cols = list(rows[0].keys())
        col_list = ', '.join(f'"{col}"' for col in cols)
        placeholders = ', '.join(['?'] * len(cols))
        conflict = ', '.join(f'"{col}"' for col in key_columns)
        assignments = ', '.join(f'"{col}" = excluded."{col}"' for col in cols if col not in key_columns)
        sql = (
            f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholders}) '
            f'ON CONFLICT ({conflict}) DO UPDATE SET {assignments} '
            f'WHERE excluded."{compare_column}" >= "{table}"."{compare_column}"'
        )
        try:
            for row in rows:
                self._conn.execute(sql, [row.get(col) for col in cols])
        except duckdb.Error as exc:
            raise BackendError(f'upsert_latest({table!r}) failed: {exc}') from exc

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
