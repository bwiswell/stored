"""The stream registry: which message classes a store records and serves.

A :class:`Stream` binds a message class to its resolved table name and
retention policy; :class:`StreamRegistry` is the per-store collection the
writer, query planner, and TTL reaper consult.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import seared as s

from . import schema
from .errors import RegistrationError


@dataclass(frozen=True, slots=True)
class Stream:
    """A registered stream.

    Attributes:
        cls: The message class recorded on this stream.
        table: The backing table name.
        retention: Retention horizon string (e.g. ``'7d'``), or ``None``.
        archive: Cold-archival horizon string (roadmap), or ``None``.
        index: Extra field names to index as queryable dimensions.
    """

    cls: type[s.Seared]
    table: str
    retention: str | None = None
    archive: str | None = None
    index: tuple[str, ...] = field(default_factory=tuple)


class StreamRegistry:
    """An ordered collection of :class:`Stream` keyed by message class."""

    __slots__ = ('_by_cls',)

    def __init__(self) -> None:
        self._by_cls: dict[type, Stream] = {}

    def add(
        self,
        cls: type[s.Seared],
        *,
        retention: str | None = None,
        archive: str | None = None,
        index: tuple[str, ...] = (),
    ) -> Stream:
        """Register ``cls`` as a stream and return the :class:`Stream`.

        Args:
            cls: A ``@s.seared`` / ``@z.zeared`` message class.
            retention: Retention horizon, or ``None`` to keep forever.
            archive: Cold-archival horizon (roadmap), or ``None``.
            index: Extra field names to index.

        Raises:
            RegistrationError: If ``cls`` is not a seared class or is already
                registered.
        """
        if not (isinstance(cls, type) and issubclass(cls, s.Seared)):
            raise RegistrationError(f'{cls!r} is not a seared class')
        if cls in self._by_cls:
            raise RegistrationError(f'{cls.__name__} is already registered')
        stream = Stream(
            cls=cls,
            table=schema.table_name(cls),
            retention=retention,
            archive=archive,
            index=tuple(index),
        )
        self._by_cls[cls] = stream
        return stream

    def get(self, cls: type[s.Seared]) -> Stream:
        """Return the :class:`Stream` for ``cls``.

        Raises:
            RegistrationError: If ``cls`` is not registered.
        """
        try:
            return self._by_cls[cls]
        except KeyError:
            raise RegistrationError(f'{cls.__name__} is not registered') from None

    def all(self) -> tuple[Stream, ...]:
        """Return every registered stream in insertion order."""
        return tuple(self._by_cls.values())


__all__ = ['Stream', 'StreamRegistry']
