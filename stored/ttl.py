"""The TTL reaper.

Unlike zeared's lazy last-value expiry, ``stored`` runs an active sweeper: a
periodic ``DELETE`` of rows older than each stream's retention horizon, followed
by a checkpoint. Needed because medium retention over high-rate streams must
reclaim disk.

.. note::
   M0 scaffold: sweeping and scheduling land in M2.
"""
from __future__ import annotations

from .backends.base import StorageBackend
from .log import get_logger
from .registry import Stream, StreamRegistry

_log = get_logger('ttl')


class Reaper:
    """Periodically expires rows past each stream's retention horizon.

    Args:
        backend: The storage backend to delete from.
        registry: The streams whose retention policies drive expiry.
    """

    __slots__ = ('_backend', '_registry')

    def __init__(self, backend: StorageBackend, registry: StreamRegistry) -> None:
        self._backend = backend
        self._registry = registry

    def sweep_stream(self, stream: Stream) -> int:
        """Expire rows for one ``stream``; return the count deleted."""
        raise NotImplementedError('Reaper.sweep_stream lands in M2')

    def sweep(self) -> int:
        """Expire rows for every stream; return the total count deleted."""
        raise NotImplementedError('Reaper.sweep lands in M2')


__all__ = ['Reaper']
