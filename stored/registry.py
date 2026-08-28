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
        time_field: A payload field naming the **domain event time** — retention
            and range queries key off it (via ``_event_at``) instead of the mesh
            ``_issued_at``. ``None`` keeps the default (mesh delivery time).
    """

    cls: type[s.Seared]
    table: str
    retention: str | None = None
    archive: str | None = None
    index: tuple[str, ...] = field(default_factory=tuple)
    time_field: str | None = None

    @property
    def time_column(self) -> str:
        """The temporal column this stream keys retention/queries off.

        ``_event_at`` (the normalized domain event time) when a ``time_field`` is
        set, else ``_issued_at`` (the mesh delivery/issue time).
        """
        return schema.EVENT_AT if self.time_field is not None else schema.ISSUED_AT


def _validate_time_field(cls: type[s.Seared], time_field: str) -> None:
    """Check ``time_field`` names a temporal (instant-bearing) field of ``cls``.

    Raises:
        RegistrationError: If the field is absent or not an ``Int``/``Float``/
            ``DateTime``/``Date`` — kinds that carry an absolute instant.
    """
    for attr, _wire, fld in cls.__seared_fields__:
        if attr == time_field:
            kind = type(fld).__name__
            if kind not in schema.TIME_FIELD_KINDS:
                raise RegistrationError(
                    f'time_field {time_field!r} of {cls.__name__} is a {kind} field; '
                    f'expected one of {sorted(schema.TIME_FIELD_KINDS)}',
                )
            return
    raise RegistrationError(f'time_field {time_field!r} is not a field of {cls.__name__}')


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
        time_field: str | None = None,
    ) -> Stream:
        """Register ``cls`` as a stream and return the :class:`Stream`.

        Args:
            cls: A ``@s.seared`` / ``@z.zeared`` message class.
            retention: Retention horizon, or ``None`` to keep forever.
            archive: Cold-archival horizon (roadmap), or ``None``.
            index: Extra field names to index.
            time_field: A payload field naming the domain event time, or ``None``.

        Raises:
            RegistrationError: If ``cls`` is not a seared class, is already
                registered, or ``time_field`` names no temporal field of ``cls``.
        """
        if not (isinstance(cls, type) and issubclass(cls, s.Seared)):
            raise RegistrationError(f'{cls!r} is not a seared class')
        if cls in self._by_cls:
            raise RegistrationError(f'{cls.__name__} is already registered')
        if time_field is not None:
            _validate_time_field(cls, time_field)
        stream = Stream(
            cls=cls,
            table=schema.table_name(cls),
            retention=retention,
            archive=archive,
            index=tuple(index),
            time_field=time_field,
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
