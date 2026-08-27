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
    :meth:`ensure_table` at registration, :meth:`append_batch` from the writer,
    :meth:`select` from the query planner, and :meth:`delete_before` from the
    TTL reaper.
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
