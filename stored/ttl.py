"""The TTL reaper.

Unlike zeared's lazy last-value expiry, ``stored`` runs an active sweeper: a
``DELETE`` of rows older than each stream's retention horizon. It is invoked on
demand here (``Store.prune`` / the ``prune`` CLI); periodic scheduling is wired
into the daemon in M4.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ._time import parse_duration, utcnow
from .log import get_logger

if TYPE_CHECKING:
    from .backends.base import StorageBackend
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
        """Expire rows for one ``stream`` (history + latest projection); total deleted.

        The append history expires on ``retention``; a latest-per-key projection
        expires on its own (usually longer) ``latest_retention``. Both key off the
        stream's temporal axis (``time_column``). Horizons left ``None`` are skipped.
        """
        now = utcnow()
        removed = 0
        if stream.retention is not None:
            cutoff = now - parse_duration(stream.retention)
            history = self._backend.delete_before(stream.table, stream.time_column, cutoff)
            if history:
                _log.info(
                    'pruned %d rows from %s (retention=%s, by=%s)',
                    history,
                    stream.table,
                    stream.retention,
                    stream.time_column,
                )
            removed += history
        if stream.has_latest and stream.latest_retention is not None:
            cutoff = now - parse_duration(stream.latest_retention)
            latest = self._backend.delete_before(stream.latest_table, stream.time_column, cutoff)
            if latest:
                _log.info(
                    'pruned %d rows from %s (latest_retention=%s, by=%s)',
                    latest,
                    stream.latest_table,
                    stream.latest_retention,
                    stream.time_column,
                )
            removed += latest
        return removed

    def sweep(self) -> int:
        """Expire rows for every registered stream; return the total deleted."""
        return sum(self.sweep_stream(stream) for stream in self._registry.all())


__all__ = ['Reaper']
