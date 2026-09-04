"""The stream registry: which message classes a store records and serves.

A :class:`Stream` binds a message class to its resolved table name and
retention policy; :class:`StreamRegistry` is the per-store collection the
writer, query planner, and TTL reaper consult.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import seared as s

from . import schema
from ._time import Duration, duration_text
from .errors import RegistrationError


@dataclass(frozen=True, slots=True)
class Stream:
    """A registered stream.

    Attributes:
        cls: The message class recorded on this stream.
        table: The backing table name.
        retention: Retention horizon, canonicalized to its string form (e.g.
            ``'7d'``) by :meth:`StreamRegistry.add`, or ``None``.
        archive: Cold-archival horizon string (roadmap), or ``None``.
        index: Extra field names to index as queryable dimensions.
        time_field: A payload field naming the **domain event time** — retention
            and range queries key off it (via ``_event_at``) instead of the mesh
            ``_issued_at``. ``None`` keeps the default (mesh delivery time).
        latest_key: Field names forming the **logical entity key** of a latest-per-key
            projection (e.g. ``('source', 'epc')``). Empty means no projection.
        latest_retention: Retention horizon for the latest projection (usually longer
            than ``retention``), canonicalized as ``retention`` is, or ``None`` to
            keep forever.
    """

    cls: type[s.Seared]
    table: str
    retention: str | None = None
    archive: str | None = None
    index: tuple[str, ...] = field(default_factory=tuple)
    time_field: str | None = None
    latest_key: tuple[str, ...] = field(default_factory=tuple)
    latest_retention: str | None = None

    @property
    def time_column(self) -> str:
        """The temporal column this stream keys retention/queries off.

        ``_event_at`` (the normalized domain event time) when a ``time_field`` is
        set, else ``_issued_at`` (the mesh delivery/issue time).
        """
        return schema.EVENT_AT if self.time_field is not None else schema.ISSUED_AT

    @property
    def has_latest(self) -> bool:
        """Whether this stream maintains a latest-per-key projection."""
        return bool(self.latest_key)

    @property
    def latest_table(self) -> str:
        """The backing table name for the latest projection (``latest_<snake>``)."""
        return schema.latest_table_name(self.cls)


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


def _validate_latest_key(cls: type[s.Seared], latest_key: tuple[str, ...]) -> None:
    """Check every ``latest_key`` name is a scalar-column field of ``cls``.

    The latest table's primary key is these columns, so each must project to a
    real column (not a blob-routed complex field).

    Raises:
        RegistrationError: If a key name is not a scalar-column field of ``cls``.
    """
    columns = {attr for attr, _wire, fld in cls.__seared_fields__ if schema.column_type(fld) is not None}
    for name in latest_key:
        if name not in columns:
            raise RegistrationError(
                f'latest_key {name!r} is not a scalar field of {cls.__name__} '
                f'(scalar fields: {sorted(columns)})',
            )


class StreamRegistry:
    """An ordered collection of :class:`Stream` keyed by message class."""

    __slots__ = ('_by_cls',)

    def __init__(self) -> None:
        self._by_cls: dict[type, Stream] = {}

    def add(
        self,
        cls: type[s.Seared],
        *,
        retention: Duration | None = None,
        archive: Duration | None = None,
        index: tuple[str, ...] = (),
        time_field: str | None = None,
        latest_key: tuple[str, ...] = (),
        latest_retention: Duration | None = None,
    ) -> Stream:
        """Register ``cls`` as a stream and return the :class:`Stream`.

        Args:
            cls: A ``@s.seared`` / ``@z.zeared`` message class.
            retention: Retention horizon — a duration string (``'7d'``), a number
                of seconds, or a :class:`datetime.timedelta`; ``None`` keeps forever.
            archive: Cold-archival horizon (roadmap), same forms, or ``None``.
            index: Extra field names to index.
            time_field: A payload field naming the domain event time, or ``None``.
            latest_key: Field names forming a latest-per-key projection's logical key.
            latest_retention: Retention horizon for the latest projection (same
                forms), or ``None``.

        Raises:
            RegistrationError: If ``cls`` is not a seared class, is already
                registered, or ``time_field`` / ``latest_key`` name unsuitable fields.
            ValueError: If a horizon is not a recognized duration. (:meth:`Store.register`
                surfaces this as a ``ConfigError``.)
        """
        if not (isinstance(cls, type) and issubclass(cls, s.Seared)):
            raise RegistrationError(f'{cls!r} is not a seared class')
        if cls in self._by_cls:
            raise RegistrationError(f'{cls.__name__} is already registered')
        if time_field is not None:
            _validate_time_field(cls, time_field)
        if latest_key:
            _validate_latest_key(cls, tuple(latest_key))
        stream = Stream(
            cls=cls,
            table=schema.table_name(cls),
            retention=duration_text(retention) if retention is not None else None,
            archive=duration_text(archive) if archive is not None else None,
            index=tuple(index),
            time_field=time_field,
            latest_key=tuple(latest_key),
            latest_retention=duration_text(latest_retention) if latest_retention is not None else None,
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
