"""Storage backends for ``stored``.

The core talks to storage only through :class:`~stored.backends.base.StorageBackend`.
DuckDB is the first (and default) implementation; Postgres is a planned second
backend behind the same protocol.
"""
from __future__ import annotations

from .base import StorageBackend

__all__ = ['StorageBackend']
