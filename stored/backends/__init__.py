"""Storage backends for ``stored``.

The core talks to storage only through :class:`~stored.backends.base.StorageBackend`.
SQLite (stdlib) is the default implementation; DuckDB is an optional extra
(``stored[duckdb]``) for analytics; Postgres is a planned third backend behind the
same protocol.
"""
from __future__ import annotations

from .base import StorageBackend

__all__ = ['StorageBackend']
