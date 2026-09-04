"""SQLite storage backend (default).

Stdlib ``sqlite3`` — **zero new dependency**, embedded, single-file, and WAL mode
handles the sole-writer chronicler's store-scale write rate. This is ``stored``'s
default backend, matching the ``rio-*`` tiny-surface ethos (workspace persistence
doc 08 §5); DuckDB stays an optional extra for the analytics upgrade path.

**Types.** The core hands column types in DuckDB spelling (``schema.SCALAR_TYPES``);
:data:`_TYPE_MAP` remaps them to SQLite declared types. Temporal values are the only
ones needing care: DuckDB round-trips ``datetime`` natively, SQLite does not, so we
store them as ISO-8601 **text** — lexicographically ordered == chronologically ordered
for a fixed format, so the reaper's ``_issued_at < cutoff`` and the query planner's
range/order comparisons stay correct — and register adapters/converters so they bind
and read back as native ``datetime`` (parity with DuckDB). Registration is module-global
(the stdlib ``sqlite3`` adapter registry is process-wide) and idempotent.

**Thread safety.** The connection is opened ``check_same_thread=False``; every call is
serialized by the caller (``Store`` holds one ``RLock`` across its writer, delivery, and
query threads), so cross-thread use over the single connection is safe.
"""
from __future__ import annotations

import datetime
import decimal
import sqlite3
from collections.abc import Sequence
from typing import Any

from ..dialect import DEFAULT_DIALECT, Dialect
from ..errors import BackendError
from ..log import get_logger

_log = get_logger('backends.sqlite')


def _register_adapters() -> None:
    """Register process-global adapters/converters for the non-native scalar types.

    Idempotent: the stdlib registries are dicts keyed by type / declared-type name, so
    re-registering the same entries is harmless. Timestamps store as ISO-8601 text so
    ordering is correct independent of the converter; the same adapter formats both stored
    values and bound query bounds, keeping range comparisons self-consistent.
    """
    sqlite3.register_adapter(datetime.datetime, lambda v: v.isoformat(sep=' '))
    sqlite3.register_adapter(datetime.date, lambda v: v.isoformat())
    sqlite3.register_adapter(datetime.time, lambda v: v.isoformat())
    sqlite3.register_adapter(decimal.Decimal, str)
    sqlite3.register_adapter(datetime.timedelta, lambda v: repr(v.total_seconds()))
    sqlite3.register_converter('TIMESTAMP', lambda b: datetime.datetime.fromisoformat(b.decode()))
    sqlite3.register_converter('DATE', lambda b: datetime.date.fromisoformat(b.decode()))
    sqlite3.register_converter('TIME', lambda b: datetime.time.fromisoformat(b.decode()))
    sqlite3.register_converter('DECIMAL', lambda b: decimal.Decimal(b.decode()))
    sqlite3.register_converter('INTERVAL', lambda b: datetime.timedelta(seconds=float(b)))


_register_adapters()

# DuckDB column-type spelling -> SQLite declared type. The declared type drives both
# column affinity and (for temporal/decimal) the PARSE_DECLTYPES converter lookup, matched
# on the first word (so ``DECIMAL(38, 9)`` -> ``DECIMAL``). Unknown types pass through.
_TYPE_MAP: dict[str, str] = {
    'BIGINT': 'INTEGER',
    'DOUBLE': 'REAL',
    'BOOLEAN': 'BOOLEAN',
    'VARCHAR': 'TEXT',
    'BLOB': 'BLOB',
    'TIMESTAMP': 'TIMESTAMP',
    'DATE': 'DATE',
    'TIME': 'TIME',
    'INTERVAL': 'INTERVAL',
    'DECIMAL': 'DECIMAL',
}


def _sqlite_type(ctype: str) -> str:
    """Map a core (DuckDB-spelled) column type to its SQLite declared type."""
    base = ctype.split('(', 1)[0].strip().upper()
    return _TYPE_MAP.get(base, ctype)


class SQLiteBackend:
    """A :class:`~stored.backends.base.StorageBackend` over a stdlib ``sqlite3`` file.

    Args:
        path: Filesystem path to the SQLite database (``':memory:'`` for an
            ephemeral store).
    """

    __slots__ = ('_path', '_conn', '_pks')

    def __init__(self, path: str = 'chronicle.db') -> None:
        self._path = path
        self._pks: dict[str, tuple[str, ...]] = {}
        try:
            self._conn = sqlite3.connect(
                path,
                detect_types=sqlite3.PARSE_DECLTYPES,
                check_same_thread=False,
            )
            self._conn.execute('PRAGMA journal_mode=WAL')
        except sqlite3.Error as exc:  # pragma: no cover - env-specific
            raise BackendError(f'could not open SQLite at {path!r}: {exc}') from exc

    @property
    def path(self) -> str:
        """The database file path this backend is bound to."""
        return self._path

    @property
    def dialect(self) -> Dialect:
        """SQLite is the baseline spelling."""
        return DEFAULT_DIALECT

    def ensure_table(
        self,
        table: str,
        columns: dict[str, str],
        primary_key: Sequence[str],
    ) -> None:
        """Create ``table`` if absent and add any missing columns (additive)."""
        col_defs = ', '.join(f'"{name}" {_sqlite_type(ctype)}' for name, ctype in columns.items())
        pk = ', '.join(f'"{col}"' for col in primary_key)
        ddl = f'CREATE TABLE IF NOT EXISTS "{table}" ({col_defs}, PRIMARY KEY ({pk}))'
        try:
            self._conn.execute(ddl)
            existing = {row[1] for row in self._conn.execute(f'PRAGMA table_info("{table}")')}
            for name, ctype in columns.items():
                if name not in existing:
                    self._conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{name}" {_sqlite_type(ctype)}')
            self._conn.commit()
        except sqlite3.Error as exc:
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
            self._conn.commit()
        except sqlite3.Error as exc:
            raise BackendError(f'ensure_index({name!r}) failed: {exc}') from exc

    def append_batch(
        self,
        table: str,
        rows: Sequence[dict[str, Any]],
    ) -> None:
        """Insert ``rows`` in one transaction, ignoring primary-key conflicts.

        Rows are de-duplicated by primary key within the batch (keeping the last
        occurrence); ``INSERT OR IGNORE`` then makes both in-batch and existing-row PK
        conflicts an idempotent no-op — so redelivery (reconnect replays, at-least-once
        quirks) never double-writes.
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
        placeholders = ', '.join('?' for _ in cols)
        sql = f'INSERT OR IGNORE INTO "{table}" ({col_list}) VALUES ({placeholders})'  # noqa: S608 (quoted identifiers)
        params = [tuple(row.get(col) for col in cols) for row in rows]
        try:
            self._conn.executemany(sql, params)
            self._conn.commit()
        except sqlite3.Error as exc:
            raise BackendError(f'append_batch({table!r}) failed: {exc}') from exc

    def upsert_latest(
        self,
        table: str,
        rows: Sequence[dict[str, Any]],
        key_columns: Sequence[str],
        compare_column: str,
    ) -> None:
        """Upsert rows into ``table`` newest-wins on ``compare_column``.

        ``INSERT … ON CONFLICT(<key>) DO UPDATE … WHERE excluded.<cmp> >= <cmp>``
        keeps the row with the greatest ``compare_column`` per key, order-independent:
        each row upserts as its own statement (``executemany``), so an older row can
        never overwrite a newer one — whatever the batch order or redelivery.
        """
        if not rows:
            return
        cols = list(rows[0].keys())
        col_list = ', '.join(f'"{col}"' for col in cols)
        placeholders = ', '.join('?' for _ in cols)
        conflict = ', '.join(f'"{col}"' for col in key_columns)
        assignments = ', '.join(f'"{col}"=excluded."{col}"' for col in cols if col not in key_columns)
        sql = (
            f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholders}) '  # noqa: S608 (quoted identifiers)
            f'ON CONFLICT({conflict}) DO UPDATE SET {assignments} '
            f'WHERE excluded."{compare_column}" >= "{table}"."{compare_column}"'
        )
        params = [tuple(row.get(col) for col in cols) for row in rows]
        try:
            self._conn.executemany(sql, params)
            self._conn.commit()
        except sqlite3.Error as exc:
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
        except sqlite3.Error as exc:
            raise BackendError(f'select failed: {exc}') from exc
        names = [desc[0] for desc in cursor.description]
        return [dict(zip(names, values, strict=True)) for values in fetched]

    def delete_before(
        self,
        table: str,
        column: str,
        cutoff: datetime.datetime,
    ) -> int:
        """Delete rows of ``table`` whose ``column`` is older than ``cutoff``.

        Returns the number of rows removed (``cursor.rowcount``).
        """
        sql = f'DELETE FROM "{table}" WHERE "{column}" < ?'  # noqa: S608 (quoted identifiers)
        try:
            cursor = self._conn.execute(sql, [cutoff])
            self._conn.commit()
        except sqlite3.Error as exc:
            raise BackendError(f'delete_before({table!r}) failed: {exc}') from exc
        return cursor.rowcount

    def close(self) -> None:
        """Close the SQLite connection."""
        self._conn.close()


__all__ = ['SQLiteBackend']
