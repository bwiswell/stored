"""Bind a store to typed mesh contracts.

The three consumers of ``stored`` each hand-wrote the same shapes: a subscriber
that records (sometimes normalizing one contract into another), a queryable that
turns a typed request into a time-range read, and a queryable that answers
"newest for this entity" from the latest projection. :class:`Binding` is those
three, declared instead of written.

Everything here is expressed in **zeared vocabulary** — a message class, its
``REQUEST`` payload, a queryable, a projection function. Nothing knows about any
particular contract, topic or category, so a binding serves any ``@zeared`` class.

**The sentinel policy.** Request payloads commonly encode "not provided" as an
empty string or a zero rather than ``None`` (a wire format without optionals).
:data:`UNSET_FALSY` is that convention as a default; pass ``unset=(None,)`` for a
strict one, or any tuple of values a given fleet treats as absent.
"""
from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

import seared as s
import zeared as z

from ..errors import ConfigError
from ..log import get_logger
from ..query import DEFAULT_CHUNK, MAX_LIMIT
from ..store import Store
from .async_store import AsyncStore

if TYPE_CHECKING:
    from zeared import QueryContext, ZenohMeta

_log = get_logger('mesh.binding')

#: The default "not provided" values: the empty-string / zero convention a wire
#: format without optionals falls back on. ``None`` is always absent.
UNSET_FALSY: tuple[Any, ...] = ('', 0, 0.0, None)


def _as_mapping(spec: Sequence[str] | Mapping[str, str]) -> dict[str, str]:
    """Normalize a field spec to ``{request_field: column}`` (a bare name maps to itself)."""
    if isinstance(spec, Mapping):
        return dict(spec)
    return {name: name for name in spec}


def _present(value: Any, unset: tuple[Any, ...]) -> bool:
    """Whether ``value`` counts as provided under the sentinel policy."""
    return not any(value is sentinel or value == sentinel for sentinel in unset)


class Binding:
    """Declares the subscribers and queryables that bind a store to mesh contracts.

    The binding owns only what it declares — the caller owns the store and the
    session. :meth:`close` releases every handle, mirroring
    :class:`stored.zenoh.Chronicler`.

    Handlers are ``async def``: they await the store on a worker thread, so a slow
    read never blocks the event loop the service runs on.

    Args:
        store: The store to bind. A bare :class:`~stored.store.Store` is wrapped in
            an :class:`~stored.mesh.AsyncStore` automatically.
        session: The zeared session to declare on, or ``None`` for the ambient one.
    """

    __slots__ = ('_store', '_session', '_handles')

    def __init__(self, store: AsyncStore | Store, *, session: Any = None) -> None:
        self._store = store if isinstance(store, AsyncStore) else AsyncStore(store)
        self._session = session
        self._handles: list[Any] = []

    @property
    def store(self) -> AsyncStore:
        """The bound store."""
        return self._store

    # -- record ------------------------------------------------------------

    def record[M: z.Message](
        self,
        cls: type[M],
        *,
        store_as: type[s.Seared] | None = None,
        via: Callable[[M], s.Seared] | None = None,
        on_error: Callable[[Exception, bytes], None] | None = None,
    ) -> Any:
        """Subscribe ``cls`` and record every message.

        With ``store_as`` + ``via`` this is *subscribe A, persist B*: several source
        contracts normalize into one row type, which is what makes a unified history
        query across them possible. The mapper stays the caller's — it is domain
        logic that happens to run on the record path.

        A mapped row keeps the **source** message's key expression and HLC timestamp,
        so redelivery of a source message still dedups on the stored primary key.

        Args:
            cls: The contract to subscribe.
            store_as: The registered class to persist as; ``None`` records ``cls``.
            via: ``(message) -> instance`` normalizer, required with ``store_as``.
            on_error: Optional ``on_error(exc, raw)`` for the subscriber.

        Returns:
            The zeared ``Subscriber`` handle (also closed by :meth:`close`).

        Raises:
            ConfigError: If ``store_as`` is given without ``via``, or the target
                class is not registered on the store.
        """
        target = store_as or cls
        if store_as is not None and via is None:
            raise ConfigError(f'record({cls.__name__}, store_as={store_as.__name__}) needs via=<mapper>')
        self._require_registered(target, f'record({cls.__name__})')
        mapper = via

        def _on_message(message: M, meta: ZenohMeta) -> None:
            row = mapper(message) if mapper is not None else message
            self._store.record(target, row, meta=meta)

        handle = cls.on_message(_on_message, session=self._session, on_error=on_error)
        self._handles.append(handle)
        return handle

    # -- serve -------------------------------------------------------------

    def serve_range[M: z.Message](  # noqa: PLR0913 — one keyword per request field it reads
        self,
        cls: type[M],
        *,
        filters: Sequence[str] | Mapping[str, str] = (),
        since: str | None = None,
        until: str | None = None,
        limit: str | None = None,
        default_limit: int | None = None,
        stream: bool = False,
        chunk: int = DEFAULT_CHUNK,
        unset: tuple[Any, ...] = UNSET_FALSY,
        on_error: Callable[[Exception, bytes], None] | None = None,
    ) -> Any:
        """Serve ``cls``'s history as a time-range queryable driven by its ``REQUEST``.

        The request's fields are read by name: ``filters`` become equality filters on
        indexed dimensions, ``since``/``until`` become the window, ``limit`` the cap.
        Any field holding an :data:`unset` sentinel is simply not applied, so one
        request type serves "everything", "one tag", and "one tag in an hour" alike.

        With ``stream=True`` the handler is a generator: rows are replied **as they
        are read**, a page at a time, so neither the historian nor the caller holds
        the result set (a getter using ``z.aquery_iter`` sees each reply as it lands).
        The window is then bounded by ``default_limit``, which defaults to the query
        planner's ``MAX_LIMIT`` rather than to "unbounded" — a caller that abandons a
        query does **not** stop the queryable, so an uncapped stream would leave the
        historian producing rows nobody is reading.

        Args:
            cls: The registered class to serve. Its ``REQUEST`` types the request.
            filters: Request fields that filter — names, or ``{field: column}``.
            since: Request field naming the lower time bound, if any.
            until: Request field naming the upper time bound, if any.
            limit: Request field naming the row cap, if any.
            default_limit: Cap applied when the request omits one. Streaming defaults
                it to ``MAX_LIMIT``; pass an explicit value to raise or lower it.
            stream: Reply row-by-row from a paged walk instead of one materialized
                list. No contract change — the same replies, produced lazily.
            chunk: Rows per page while streaming (the memory bound and thread-hop
                size). Ignored unless ``stream``.
            unset: Values treated as "not provided" (see :data:`UNSET_FALSY`).
            on_error: Optional ``on_error(exc, raw)`` for the queryable.

        Returns:
            The zeared ``Queryable`` handle (also closed by :meth:`close`).

        Raises:
            ConfigError: If ``cls`` is not registered or declares no ``REQUEST``.
        """
        self._require_registered(cls, f'serve_range({cls.__name__})')
        request_cls = self._require_request(cls, 'serve_range')
        columns = _as_mapping(filters)
        cap = default_limit if default_limit is not None else (MAX_LIMIT if stream else None)

        def _read(request: Any) -> dict[str, Any]:
            """The request, read as store keywords — identical for both reply shapes."""
            applied = {
                column: getattr(request, field)
                for field, column in columns.items()
                if _present(getattr(request, field, None), unset)
            }
            return {
                'since': self._bound(request, since, unset),
                'until': self._bound(request, until, unset),
                'limit': self._bound(request, limit, unset) or cap,
                **applied,
            }

        async def _collect(ctx: QueryContext) -> list[M]:
            request = ctx.request
            if not isinstance(request, request_cls):
                _log.warning('%s: query carried no %s payload', cls.__name__, request_cls.__name__)
                return []
            return await self._store.query(cls, **_read(request))

        async def _stream(ctx: QueryContext) -> AsyncIterator[M]:
            request = ctx.request
            if not isinstance(request, request_cls):
                _log.warning('%s: query carried no %s payload', cls.__name__, request_cls.__name__)
                return
            async for row in self._store.iter(cls, chunk=chunk, **_read(request)):
                yield row

        handler = _stream if stream else _collect
        handle = cls.on_query(handler, session=self._session, on_error=on_error)
        self._handles.append(handle)
        return handle

    def serve_latest[R: z.Message](
        self,
        cls: type[R],
        *,
        of: type[s.Seared] | None = None,
        key: Sequence[str] | Mapping[str, str],
        project: Callable[[Any, Any], R] | None = None,
        missing: Callable[[Any], R | None] | None = None,
        unset: tuple[Any, ...] = UNSET_FALSY,
        on_error: Callable[[Exception, bytes], None] | None = None,
    ) -> Any:
        """Serve "newest for this entity" from a latest-per-key projection.

        ``cls`` is the **reply** contract; ``of`` is the class actually stored. When
        they differ — a ``Location`` row answered as a ``LastKnownLocation`` — the
        ``project`` hook shapes the reply, because only the caller knows how a stored
        row becomes its contract. When they are the same class (a row type that
        doubles as its own reply), the projection is the identity and can be omitted.

        Args:
            cls: The reply contract; its ``REQUEST`` types the request.
            of: The stored class holding the projection; defaults to ``cls``.
            key: Request fields forming the entity key — names, or ``{field: column}``.
            project: ``(row, request) -> reply``; required when ``of`` differs from
                ``cls``.
            missing: ``(request) -> reply | None`` when nothing is stored for the key.
                ``None`` replies nothing at all.
            unset: Values treated as "not provided" (see :data:`UNSET_FALSY`).
            on_error: Optional ``on_error(exc, raw)`` for the queryable.

        Returns:
            The zeared ``Queryable`` handle (also closed by :meth:`close`).

        Raises:
            ConfigError: If the stored class is unregistered, has no latest
                projection, declares no ``REQUEST``, or needs a ``project`` hook.
        """
        stored_cls: type[s.Seared] = of or cls
        self._require_registered(stored_cls, f'serve_latest({cls.__name__})')
        stream = self._store.store.registry.get(stored_cls)
        if not stream.has_latest:
            raise ConfigError(
                f'serve_latest({cls.__name__}): {stored_cls.__name__} has no latest projection '
                '(register it with latest_key=…)',
            )
        request_cls = self._require_request(cls, 'serve_latest')
        if project is None and stored_cls is not cls:
            raise ConfigError(
                f'serve_latest({cls.__name__}, of={stored_cls.__name__}) needs project=<row, request -> reply>; '
                'only the caller knows how a stored row becomes this reply',
            )
        shape: Callable[[Any, Any], R] = project if project is not None else (lambda row, _request: row)
        key_columns = _as_mapping(key)

        async def _handler(ctx: QueryContext) -> R | None:
            request = ctx.request
            if not isinstance(request, request_cls):
                _log.warning('%s: query carried no %s payload', cls.__name__, request_cls.__name__)
                return None
            entity = {
                column: getattr(request, field)
                for field, column in key_columns.items()
                if _present(getattr(request, field, None), unset)
            }
            if len(entity) != len(key_columns):
                return missing(request) if missing is not None else None
            row = await self._store.latest(stored_cls, **entity)
            if row is None:
                return missing(request) if missing is not None else None
            return shape(row, request)

        handle = cls.on_query(_handler, session=self._session, on_error=on_error)
        self._handles.append(handle)
        return handle

    # -- lifecycle ---------------------------------------------------------

    def close(self) -> None:
        """Close every declared subscriber and queryable (idempotent)."""
        for handle in self._handles:
            try:
                handle.close()
            except Exception:  # noqa: BLE001 - teardown must not mask the original failure
                _log.exception('error closing binding handle')
        self._handles.clear()

    # -- helpers -----------------------------------------------------------

    def _require_registered(self, cls: type[s.Seared], what: str) -> None:
        """Fail at bind time, not on the first message, when a class is unregistered."""
        try:
            self._store.store.registry.get(cls)
        except Exception as exc:
            raise ConfigError(f'{what}: {cls.__name__} is not registered on the store') from exc

    @staticmethod
    def _require_request(cls: type[z.Message], what: str) -> type:
        """The class's ``REQUEST`` payload type — the thing that makes the binding typed."""
        request_cls = getattr(cls, 'REQUEST', None)
        if request_cls is None:
            raise ConfigError(f'{what}({cls.__name__}): the class declares no REQUEST payload type')
        return request_cls

    @staticmethod
    def _bound(request: Any, field: str | None, unset: tuple[Any, ...]) -> Any:
        """Read one optional request field, or ``None`` when absent/unset."""
        if field is None:
            return None
        value = getattr(request, field, None)
        return value if _present(value, unset) else None


__all__ = ['UNSET_FALSY', 'Binding']
