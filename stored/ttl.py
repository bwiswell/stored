"""The TTL reaper.

Unlike zeared's lazy last-value expiry, ``stored`` runs an active sweeper: a
``DELETE`` of rows older than each stream's retention horizon. It is invoked on
demand here (``Store.prune`` / the ``prune`` CLI); periodic scheduling is wired
into the daemon in M4.
"""
from __future__ import annotations

from ._time import parse_duration, utcnow
from .backends.base import StorageBackend
from .log import get_logger
from .registry import Stream, StreamRegistry

_log = get_logger('ttl')


class Reaper:
    """Expires rows past each stream's retention horizon.

    Args:
        backend: The storage backend to delete from.
        registry: The streams whose retention policies drive expiry.
    """

    __slots__ = ('_backend', '_registry')

    def __init__(self, backend: StorageBackend, registry: StreamRegistry) -> None:
        self._backend = backend
        self._registry = registry

    def sweep_stream(self, stream: Stream) -> int:
        """Expire rows for one ``stream``; return the count deleted.

        A stream with no retention horizon is skipped (returns 0).
        """
        if stream.retention is None:
            return 0
        cutoff = utcnow() - parse_duration(stream.retention)
        removed = self._backend.delete_before(stream.table, stream.time_column, cutoff)
        if removed:
            _log.info(
                'pruned %d rows from %s (retention=%s, by=%s)',
                removed, stream.table, stream.retention, stream.time_column,
            )
        return removed

    def sweep(self) -> int:
        """Expire rows for every registered stream; return the total deleted."""
        return sum(self.sweep_stream(stream) for stream in self._registry.all())


__all__ = ['Reaper']
