"""The :class:`Store` facade — the seared-only core surface.

Persists and queries ``seared`` objects with no mesh involved. The Zenoh
chronicler (``stored.zenoh``) wires a ``Store`` to a ``zeared`` session, but the
``Store`` itself never imports ``zeared``.

.. note::
   M0 scaffold: registration is live; ``record`` / ``query`` / ``prune`` land in
   M1–M2.
"""
from __future__ import annotations

from typing import Any

from .backends.base import StorageBackend
from .backends.duckdb_ import DuckDBBackend
from .errors import ConfigError
from .log import get_logger
from .registry import Stream, StreamRegistry

_log = get_logger('store')


def _make_backend(backend: str, path: str) -> StorageBackend:
    """Construct the named storage backend."""
    if backend == 'duckdb':
        return DuckDBBackend(path)
    raise ConfigError(f'unknown backend {backend!r} (expected one of: duckdb)')


class Store:
    """A persistence store over a pluggable :class:`StorageBackend`.

    Args:
        path: Path to the backing database file.
        backend: Backend name (``'duckdb'``; ``'postgres'`` later).
    """

    __slots__ = ('_backend', '_registry')

    def __init__(self, path: str = 'chronicle.duckdb', *, backend: str = 'duckdb') -> None:
        self._backend = _make_backend(backend, path)
        self._registry = StreamRegistry()

    @property
    def registry(self) -> StreamRegistry:
        """The store's stream registry."""
        return self._registry

    @property
    def backend(self) -> StorageBackend:
        """The underlying storage backend."""
        return self._backend

    def register(
        self,
        cls: type,
        *,
        retention: str | None = None,
        archive: str | None = None,
        index: tuple[str, ...] = (),
    ) -> Stream:
        """Register a message class as a recorded stream.

        Args:
            cls: A ``@s.seared`` / ``@z.zeared`` message class.
            retention: Retention horizon (e.g. ``'7d'``), or ``None``.
            archive: Cold-archival horizon (roadmap), or ``None``.
            index: Extra field names to index as queryable dimensions.

        Returns:
            The registered :class:`Stream`.
        """
        return self._registry.add(
            cls, retention=retention, archive=archive, index=index,
        )

    def record(self, cls: type, msg: Any, *, meta: Any = None) -> None:
        """Persist one message. Stub — implemented in M1."""
        raise NotImplementedError('Store.record lands in M1')

    def query(self, cls: type, **kwargs: Any) -> list[Any]:
        """Query stored history for ``cls``. Stub — implemented in M1."""
        raise NotImplementedError('Store.query lands in M1')

    def flush(self) -> None:
        """Flush buffered writes. Stub — implemented in M2."""
        raise NotImplementedError('Store.flush lands in M2')

    def prune(self) -> int:
        """Force a TTL sweep. Stub — implemented in M2."""
        raise NotImplementedError('Store.prune lands in M2')

    def close(self) -> None:
        """Close the store and its backend."""
        self._backend.close()


__all__ = ['Store']
