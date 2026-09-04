"""The :class:`StorageBackend` protocol.

Every backend (DuckDB now, Postgres later) implements this narrow surface. The
core — writer, query planner, TTL reaper — depends on the protocol only, never
on a concrete engine, so a second backend is a drop-in.
"""
from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any, Protocol


class StorageBackend(Protocol):
    """Minimal storage surface the ``stored`` core depends on.

    Implementations own their own connection lifecycle; the core calls
    :meth:`ensure_table` + :meth:`ensure_index` at registration,
    :meth:`append_batch` from the writer, :meth:`select` from the query planner,
    and :meth:`delete_before` from the TTL reaper.
    """

    def ensure_table(
        self,
        table: str,
        columns: dict[str, str],
        primary_key: Sequence[str],
    ) -> None:
        """Create ``table`` if absent and reconcile it toward ``columns``.

        Args:
            table: Table name.
            columns: Ordered mapping of column name to backend column type.
            primary_key: Column names forming the primary key / dedup key.
        """
        ...

    def ensure_index(
        self,
        name: str,
        table: str,
        columns: Sequence[str],
    ) -> None:
        """Create the secondary index ``name`` on ``table`` if it does not exist.

        Args:
            name: Index name (the core derives it; see ``schema.index_specs``).
            table: Table to index.
            columns: Indexed columns, in order.
        """
        ...

    def append_batch(
        self,
        table: str,
        rows: Sequence[dict[str, Any]],
    ) -> None:
        """Append ``rows`` to ``table`` in one transaction (idempotent on PK).

        Args:
            table: Target table.
            rows: Column-keyed row dicts.
        """
        ...

    def upsert_latest(
        self,
        table: str,
        rows: Sequence[dict[str, Any]],
        key_columns: Sequence[str],
        compare_column: str,
    ) -> None:
        """Upsert ``rows`` into a latest-per-key ``table``, newest-wins.

        One row per ``key_columns`` value; on conflict, overwrite only when the
        incoming ``compare_column`` is **at least as new** as the stored one
        (tolerating out-of-order / redelivered batches). ``table`` must have
        ``key_columns`` as its primary key.

        Args:
            table: Target latest-projection table.
            rows: Column-keyed row dicts (the same shape appended to history).
            key_columns: The logical-entity key (the table's primary key).
            compare_column: The temporal column compared for newest-wins.
        """
        ...

    def select(
        self,
        sql: str,
        params: Sequence[Any] = (),
    ) -> list[dict[str, Any]]:
        """Run a read-only ``sql`` query and return all rows as column dicts.

        Args:
            sql: A parameterized SELECT statement.
            params: Positional bind parameters.

        Returns:
            The result rows, each a column-name-keyed dict.
        """
        ...

    def delete_before(
        self,
        table: str,
        column: str,
        cutoff: datetime,
    ) -> int:
        """Delete rows of ``table`` whose ``column`` is older than ``cutoff``.

        Args:
            table: Target table.
            column: Timestamp column to compare.
            cutoff: Rows strictly older than this are removed.

        Returns:
            The number of rows deleted.
        """
        ...

    def close(self) -> None:
        """Flush and close the backend's connection."""
        ...


__all__ = ['StorageBackend']
