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

import zeared as z

from ..errors import ConfigError, QueryError
from ..log import get_logger
from ..query import DEFAULT_CHUNK, MAX_LIMIT, decode_anchor, encode_anchor
from .async_store import AsyncStore

if TYPE_CHECKING:
    import seared as s
    from zeared import QueryContext, ZenohMeta

    from ..store import Store

_log = get_logger('mesh.binding')

#: The default "not provided" values: the empty-string / zero convention a wire
#: format without optionals falls back on. ``None`` is always absent.
UNSET_FALSY: tuple[Any, ...] = ('', 0, 0.0, None)


#: What a filter may point at: a fixed column/path name, or a function of the request
#: that picks one per query — for a dimension the *caller* names, such as which zone
#: layer to look inside.
type FilterTarget = str | Callable[[Any], str]


def _effective_limit(
    requested: int | None,
    *,
    default: int | None,
    maximum: int | None,
    streaming: bool,
) -> int | None:
    """The row cap actually applied to one request.

    Three dials, and they answer different questions:

    - ``requested`` — what the caller asked for, or ``None`` when they did not.
    - ``default`` — what an *omitted* limit means.
    - ``maximum`` — the ceiling the **service** imposes, whatever the caller asked,
      including when they asked for nothing.

    The last one is the one a caller cannot talk its way past, which is why it belongs
    on the binding: a gateway collects every reply into a single frame, so an
    unclamped request for a hundred thousand rows is a hundred-thousand-row frame at
    somebody else's expense.

    Args:
        requested: The limit from the request, or ``None``.
        default: The cap for an omitted limit, or ``None`` to leave it to the planner.
        maximum: The service's ceiling, or ``None`` for none beyond the planner's. When
            set, it also bounds a request that names no limit at all.
        streaming: Whether the handler streams (where ``None`` means *unbounded*, so a
            fallback is required rather than optional).

    Returns:
        The limit to pass to the store, or ``None`` to accept the planner's default.
    """
    value = requested if requested is not None else default
    # An OMITTED limit is the most natural way to ask for everything, so a declared
    # ceiling has to answer it too. Left to the planner it would land on DEFAULT_LIMIT,
    # which quietly exceeds a `maximum` set below that — the one case the parameter
    # exists for, failing silently.
    if value is None and (streaming or maximum is not None):
        value = maximum if maximum is not None else MAX_LIMIT
    if value is not None and maximum is not None:
        value = min(value, maximum)
    return value


def _as_mapping(spec: Sequence[str] | Mapping[str, str]) -> dict[str, str]:
    """Normalize a field spec to ``{request_field: field}`` (a bare name maps to itself)."""
    if isinstance(spec, Mapping):
        return dict(spec)
    return {name: name for name in spec}


def _as_targets(spec: Sequence[str] | Mapping[str, FilterTarget]) -> dict[str, FilterTarget]:
    """Normalize a filter spec, whose targets may be computed per request."""
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

    __slots__ = ('_handles', '_session', '_store')

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
        live_only: bool = False,
        on_error: Callable[[Exception, bytes], None] | None = None,
    ) -> Any:
        """Subscribe ``cls`` and record every message.

        With ``store_as`` + ``via`` this is *subscribe A, persist B*: several source
        contracts normalize into one row type, which is what makes a unified history
        query across them possible. The mapper stays the caller's — it is domain
        logic that happens to run on the record path.

        A mapped row keeps the **source** message's key expression and HLC timestamp,
        so redelivery of a source message still dedups on the stored primary key.

        A subscription covers **every template the class declares** — its ``TOPIC``
        and any ``EXTRA_TOPICS``. ``live_only=True`` narrows recording to the
        canonical scope, which is what keeps a historian from re-recording a
        :class:`~stored.mesh.Replayer`'s output as though it were new traffic. It
        compares the sample's key against ``TOPIC``'s literal prefix, so a replay
        scope must differ *before* the first ``{slot}`` — which the scope-segment
        shape gives you anyway.

        Args:
            cls: The contract to subscribe.
            store_as: The registered class to persist as; ``None`` records ``cls``.
            via: ``(message) -> instance`` normalizer, required with ``store_as``.
            live_only: Record only samples arriving on ``cls.TOPIC``'s own scope,
                ignoring the class's other declared templates.
            on_error: Optional ``on_error(exc, raw)`` for the subscriber.

        Returns:
            The zeared ``Subscriber`` handle (also closed by :meth:`close`).

        Raises:
            ConfigError: If ``store_as`` is given without ``via``, the target class
                is not registered, or ``live_only`` is asked of a ``TOPIC`` with no
                literal prefix to compare against.
        """
        target = store_as or cls
        if store_as is not None and via is None:
            msg = f'record({cls.__name__}, store_as={store_as.__name__}) needs via=<mapper>'
            raise ConfigError(msg)
        self._require_registered(target, f'record({cls.__name__})')
        mapper = via
        extras = tuple(getattr(cls, 'EXTRA_TOPICS', ()) or ())
        live_prefix = str(cls.TOPIC).split('{', 1)[0] if live_only else ''
        if live_only and not live_prefix:
            msg = (
                f'record({cls.__name__}, live_only=True): TOPIC {cls.TOPIC!r} starts with a slot, '
                'so there is no literal scope to match on'
            )
            raise ConfigError(
                msg,
            )
        if extras and not live_only:
            _log.warning(
                '%s declares EXTRA_TOPICS %s and is recorded from all of them; '
                'pass live_only=True to record only the canonical scope',
                cls.__name__,
                list(extras),
            )

        def _on_message(message: M, meta: ZenohMeta) -> None:
            if live_prefix and not meta.key_expr.startswith(live_prefix):
                return  # another declared scope (a replay, say) — not this recorder's traffic
            row = mapper(message) if mapper is not None else message
            self._store.record(target, row, meta=meta)

        handle = cls.on_message(_on_message, session=self._session, on_error=on_error)
        self._handles.append(handle)
        return handle

    # -- serve -------------------------------------------------------------

    def serve_range[M: z.Message](
        self,
        cls: type[M],
        *,
        of: type[s.Seared] | None = None,
        project: Callable[[Any, Any], M] | None = None,
        filters: Sequence[str] | Mapping[str, FilterTarget] = (),
        since: str | None = None,
        until: str | None = None,
        limit: str | None = None,
        default_limit: int | None = None,
        max_limit: int | None = None,
        cursor: str | None = None,
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
            cls: The class to serve. Its ``REQUEST`` types the request; it is also the
                stored class unless ``of`` says otherwise.
            of: The stored class to read, when the reply contract differs from it.
            project: ``(row, request) -> reply``; required when ``of`` differs from
                ``cls``.
            filters: Request fields that filter — names, or ``{field: target}``. A
                target may be a callable ``(request) -> target`` when the caller names
                the dimension (which zone layer, say); see :meth:`_split_filters`.
            since: Request field naming the lower time bound, if any.
            until: Request field naming the upper time bound, if any.
            limit: Request field naming the row cap, if any.
            default_limit: Cap applied when the request omits one. Streaming defaults
                it to ``max_limit`` (or ``MAX_LIMIT``); pass an explicit value to raise
                or lower it.
            max_limit: The **service's** ceiling, applied whatever the request asks.
                ``default_limit`` only fills in an omission; this one clamps. Worth
                setting wherever replies are collected into a single frame downstream —
                a caller asking for a hundred thousand rows should not get them.
            cursor: Request **and** reply field naming the page cursor, if the query
                pages. The request's value (empty = first page) resumes the keyset walk
                after the row it names; the **last reply of a full page** carries the
                cursor for the next page, every other reply an empty one — a multi-reply
                queryable has no envelope, so the last row is where a caller reads it.
                The reply class must declare the field; needs a collected reply
                (``stream=False``), since a streamed reply has no last row to stamp.
            stream: Reply row-by-row from a paged walk instead of one materialized
                list. No contract change — the same replies, produced lazily.
            chunk: Rows per page while streaming (the memory bound and thread-hop
                size). Ignored unless ``stream``.
            unset: Values treated as "not provided" (see :data:`UNSET_FALSY`).
            on_error: Optional ``on_error(exc, raw)`` for the queryable.

        Returns:
            The zeared ``Queryable`` handle (also closed by :meth:`close`).

        Raises:
            ConfigError: If the stored class is not registered, ``cls`` declares no
                ``REQUEST``, the reply differs from the row without a ``project``, or
                ``cursor`` names a field the request or reply lacks (or is combined
                with ``stream``).
        """
        stored_cls: type[s.Seared] = of or cls
        what = f'serve_range({cls.__name__})'
        self._require_registered(stored_cls, what)
        request_cls = self._require_request(cls, 'serve_range')
        shape = self._projection(cls, stored_cls, project, what)
        columns, paths, computed = self._split_filters(stored_cls, filters, what)
        paging = self._paging(cls, request_cls, cursor, stream=stream, what=what)

        def _cap(request: Any) -> int | None:
            return _effective_limit(
                self._bound(request, limit, unset),
                default=default_limit,
                maximum=max_limit,
                streaming=stream,
            )

        def _read(request: Any) -> dict[str, Any]:
            """The request, read as store keywords — identical for both reply shapes."""
            applied = {
                column: getattr(request, field)
                for field, column in columns.items()
                if _present(getattr(request, field, None), unset)
            }
            where = {
                path: getattr(request, field)
                for field, path in paths.items()
                if _present(getattr(request, field, None), unset)
            }
            picked_columns, picked_paths = self._resolve(stored_cls, computed, request, unset)
            applied.update(picked_columns)
            where.update(picked_paths)
            return {
                'since': self._bound(request, since, unset),
                'until': self._bound(request, until, unset),
                'limit': _cap(request),
                'where': where or None,
                **applied,
            }

        async def _collect(ctx: QueryContext) -> list[M]:
            request = ctx.request
            if not isinstance(request, request_cls):
                _log.warning('%s: query carried no %s payload', cls.__name__, request_cls.__name__)
                return []
            if paging is None:
                rows = await self._store.query(stored_cls, **_read(request))
                return [shape(row, request) for row in rows]
            after = self._after(request, paging, unset)
            rows, anchor = await self._store.query_page(stored_cls, after=after, **_read(request))
            return self._stamped([shape(row, request) for row in rows], paging, anchor)

        async def _stream(ctx: QueryContext) -> AsyncIterator[M]:
            request = ctx.request
            if not isinstance(request, request_cls):
                _log.warning('%s: query carried no %s payload', cls.__name__, request_cls.__name__)
                return
            async for row in self._store.iter(stored_cls, chunk=chunk, **_read(request)):
                yield shape(row, request)

        handler = _stream if stream else _collect
        handle = cls.on_query(handler, session=self._session, on_error=on_error)
        self._handles.append(handle)
        return handle

    def serve_snapshot[R: z.Message](
        self,
        cls: type[R],
        *,
        of: type[s.Seared] | None = None,
        filters: Sequence[str] | Mapping[str, FilterTarget] = (),
        since: str | None = None,
        until: str | None = None,
        limit: str | None = None,
        default_limit: int | None = None,
        max_limit: int | None = None,
        cursor: str | None = None,
        project: Callable[[Any, Any], R] | None = None,
        stream: bool = False,
        chunk: int = DEFAULT_CHUNK,
        unset: tuple[Any, ...] = UNSET_FALSY,
        on_error: Callable[[Exception, bytes], None] | None = None,
    ) -> Any:
        """Serve **current state**: the newest row of every matching entity.

        :meth:`serve_range` answers "what happened"; this answers "what is the case".
        It is the same declaration pointed at the latest projection — one reply per
        *entity* rather than per observation — which is the shape an operator console
        asks for ("everything in department 5 right now").

        ``filters`` resolve the same way as in :meth:`serve_range`: a target that
        names a declared dimension filters a column, one that names a declared
        ``json_index`` path filters inside a ``Dict``. ``since``/``until`` narrow on
        **last seen**, not on when a row was recorded.

        Args:
            cls: The reply contract; its ``REQUEST`` types the request.
            of: The stored class holding the projection; defaults to ``cls``.
            filters: Request fields that filter — names, or ``{field: target}``.
            since: Request field naming a lower bound on last-seen, if any.
            until: Request field naming an upper bound on last-seen, if any.
            limit: Request field naming the row cap, if any.
            default_limit: Cap applied when the request omits one. Streaming defaults
                it to ``max_limit`` (or ``MAX_LIMIT``), as :meth:`serve_range` does and
                for the same reason.
            max_limit: The service's ceiling, applied whatever the request asks — see
                :meth:`serve_range`. A population snapshot is exactly where an
                unclamped request hurts.
            cursor: Request and reply field naming the page cursor — see
                :meth:`serve_range`; a population is exactly what wants paging.
            project: ``(row, request) -> reply``; required when ``of`` differs from
                ``cls``.
            stream: Reply row-by-row from a paged walk instead of one list.
            chunk: Rows per page while streaming. Ignored unless ``stream``.
            unset: Values treated as "not provided" (see :data:`UNSET_FALSY`).
            on_error: Optional ``on_error(exc, raw)`` for the queryable.

        Returns:
            The zeared ``Queryable`` handle (also closed by :meth:`close`).

        Raises:
            ConfigError: If the stored class is unregistered, keeps no latest
                projection, declares no ``REQUEST``, or needs a ``project`` hook.
        """
        stored_cls: type[s.Seared] = of or cls
        what = f'serve_snapshot({cls.__name__})'
        self._require_registered(stored_cls, what)
        if not self._store.store.registry.get(stored_cls).has_latest:
            msg = f'{what}: {stored_cls.__name__} has no latest projection (register it with latest_key=…)'
            raise ConfigError(
                msg,
            )
        request_cls = self._require_request(cls, 'serve_snapshot')
        shape = self._projection(cls, stored_cls, project, what)
        columns, paths, computed = self._split_filters(stored_cls, filters, what)
        paging = self._paging(cls, request_cls, cursor, stream=stream, what=what)

        def _cap(request: Any) -> int | None:
            return _effective_limit(
                self._bound(request, limit, unset),
                default=default_limit,
                maximum=max_limit,
                streaming=stream,
            )

        def _read(request: Any) -> dict[str, Any]:
            applied = {
                column: getattr(request, field)
                for field, column in columns.items()
                if _present(getattr(request, field, None), unset)
            }
            where = {
                path: getattr(request, field)
                for field, path in paths.items()
                if _present(getattr(request, field, None), unset)
            }
            picked_columns, picked_paths = self._resolve(stored_cls, computed, request, unset)
            applied.update(picked_columns)
            where.update(picked_paths)
            return {
                'since': self._bound(request, since, unset),
                'until': self._bound(request, until, unset),
                'limit': _cap(request),
                'where': where or None,
                **applied,
            }

        async def _collect(ctx: QueryContext) -> list[R]:
            request = ctx.request
            if not isinstance(request, request_cls):
                _log.warning('%s: query carried no %s payload', cls.__name__, request_cls.__name__)
                return []
            if paging is None:
                rows = await self._store.query_latest(stored_cls, **_read(request))
                return [shape(row, request) for row in rows]
            after = self._after(request, paging, unset)
            rows, anchor = await self._store.query_latest_page(stored_cls, after=after, **_read(request))
            return self._stamped([shape(row, request) for row in rows], paging, anchor)

        async def _stream(ctx: QueryContext) -> AsyncIterator[R]:
            request = ctx.request
            if not isinstance(request, request_cls):
                _log.warning('%s: query carried no %s payload', cls.__name__, request_cls.__name__)
                return
            async for row in self._store.iter_latest(stored_cls, chunk=chunk, **_read(request)):
                yield shape(row, request)

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

        It takes no ``max_limit``, and that is not an oversight: a lookup answers one
        entity, so there is no row count for a caller to inflate.

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
            msg = (
                f'serve_latest({cls.__name__}): {stored_cls.__name__} has no latest projection '
                '(register it with latest_key=…)'
            )
            raise ConfigError(
                msg,
            )
        request_cls = self._require_request(cls, 'serve_latest')
        if project is None and stored_cls is not cls:
            msg = (
                f'serve_latest({cls.__name__}, of={stored_cls.__name__}) needs project=<row, request -> reply>; '
                'only the caller knows how a stored row becomes this reply'
            )
            raise ConfigError(
                msg,
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

    @staticmethod
    def _projection[R](
        reply_cls: type[R],
        stored_cls: type[s.Seared],
        project: Callable[[Any, Any], R] | None,
        what: str,
    ) -> Callable[[Any, Any], R]:
        """The row → reply function: the caller's, or the identity when they are one class.

        Raises:
            ConfigError: If the reply contract differs from the stored row and no
                projection was given — only the caller knows how one becomes the other.
        """
        if project is not None:
            return project
        if stored_cls is not reply_cls:
            msg = (
                f'{what} with of={stored_cls.__name__} needs project=<row, request -> reply>; '
                'only the caller knows how a stored row becomes this reply'
            )
            raise ConfigError(
                msg,
            )
        return lambda row, _request: row

    def _split_filters(
        self,
        stored_cls: type[s.Seared],
        filters: Sequence[str] | Mapping[str, FilterTarget],
        what: str,
    ) -> tuple[dict[str, str], dict[str, str], dict[str, Callable[[Any], str]]]:
        """Sort each declared filter into a column filter, a path filter, or a computed one.

        The stream already says which is which — ``index`` names columns,
        ``json_paths`` names paths — so the binding resolves against the declaration
        rather than guessing from the spelling, and a fixed target in neither is a
        **bind-time** error rather than a query that never matches.

        A **callable** target cannot be resolved that early: it is a function of the
        request, for a dimension the caller names (which zone layer to look inside,
        say). Those are resolved per query by :meth:`_resolve`, which means an
        unknown one surfaces as a ``QueryError`` on that request rather than at
        startup — a deliberate trade, since an unrecognized layer *is* a per-request
        condition.

        Args:
            stored_cls: The class whose stream declares the dimensions and paths.
            filters: Request fields that filter — names, or ``{field: target}``.
            what: The caller, for the error message.

        Returns:
            ``({field: column}, {field: path}, {field: target function})``.

        Raises:
            ConfigError: If a fixed target names neither a declared dimension nor a
                declared path.
        """
        stream = self._store.store.registry.get(stored_cls)
        columns: dict[str, str] = {}
        paths: dict[str, str] = {}
        computed: dict[str, Callable[[Any], str]] = {}
        for field, target in _as_targets(filters).items():
            if not isinstance(target, str):  # a function of the request, resolved per query
                computed[field] = target
            elif target in stream.json_paths:
                paths[field] = target
            elif target in stream.index:
                columns[field] = target
            else:
                msg = (
                    f'{what}: {target!r} is neither an indexed dimension {sorted(stream.index)} '
                    f'nor a declared json_index path {sorted(stream.json_paths)} of '
                    f'{stored_cls.__name__}'
                )
                raise ConfigError(
                    msg,
                )
        return columns, paths, computed

    def _resolve(
        self,
        stored_cls: type[s.Seared],
        computed: dict[str, Callable[[Any], str]],
        request: Any,
        unset: tuple[Any, ...],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Apply the computed targets for one request, sorted as columns and paths.

        Args:
            stored_cls: The class whose stream declares the dimensions and paths.
            computed: ``{request field: target function}`` from :meth:`_split_filters`.
            request: The decoded request payload.
            unset: Values treated as "not provided".

        Returns:
            ``({column: value}, {path: value})`` for this request.

        Raises:
            QueryError: If a computed target names nothing the stream declared. The
                query fails visibly rather than answering as though unfiltered.
        """
        stream = self._store.store.registry.get(stored_cls)
        columns: dict[str, Any] = {}
        paths: dict[str, Any] = {}
        for field, pick in computed.items():
            value = getattr(request, field, None)
            if not _present(value, unset):
                continue
            target = pick(request)
            if target in stream.json_paths:
                paths[target] = value
            elif target in stream.index:
                columns[target] = value
            else:
                msg = (
                    f'{target!r} is neither an indexed dimension {sorted(stream.index)} '
                    f'nor a declared json_index path {sorted(stream.json_paths)} of '
                    f'{stored_cls.__name__}'
                )
                raise QueryError(
                    msg,
                )
        return columns, paths

    def _require_registered(self, cls: type[s.Seared], what: str) -> None:
        """Fail at bind time, not on the first message, when a class is unregistered."""
        try:
            self._store.store.registry.get(cls)
        except Exception as exc:
            msg = f'{what}: {cls.__name__} is not registered on the store'
            raise ConfigError(msg) from exc

    @staticmethod
    def _require_request(message_cls: type[z.Message], what: str) -> type:
        """The class's ``REQUEST`` payload type — the thing that makes the binding typed."""
        request_cls = getattr(message_cls, 'REQUEST', None)
        if request_cls is None:
            msg = f'{what}({message_cls.__name__}): the class declares no REQUEST payload type'
            raise ConfigError(msg)
        return request_cls

    @staticmethod
    def _paging(reply_cls: type, request_cls: type, cursor: str | None, *, stream: bool, what: str) -> str | None:
        """Validate a ``cursor`` declaration at bind time: the field on both sides, no streaming.

        Raises:
            ConfigError: If the request or the reply lacks the field, or ``stream`` is set —
                a streamed reply has no last row to carry the next cursor.
        """
        if cursor is None:
            return None
        if stream:
            msg = f'{what}: cursor paging needs a collected reply (stream=False) — a streamed reply has no last row'
            raise ConfigError(msg)
        for side, cls in (('request', request_cls), ('reply', reply_cls)):
            if cursor not in {name for name, _key, _field in getattr(cls, '__seared_fields__', ())}:
                msg = f'{what}: the {side} {cls.__name__} declares no {cursor!r} field to carry the page cursor'
                raise ConfigError(msg)
        return cursor

    @classmethod
    def _after(cls, request: Any, cursor: str, unset: tuple[Any, ...]) -> Any:
        """The anchor a request's cursor resumes after, or ``None`` for the first page.

        Raises:
            QueryError: If the cursor is not one this store produced (surfaces as an error
                reply, never as a silently restarted first page).
        """
        raw = cls._bound(request, cursor, unset)
        return decode_anchor(raw) if raw else None

    @staticmethod
    def _stamped[R](replies: list[R], cursor: str, anchor: Any) -> list[R]:
        """The page's replies with the next cursor on the last one (a full page), else untouched."""
        if anchor is not None and replies:
            setattr(replies[-1], cursor, encode_anchor(anchor))
        return replies

    @staticmethod
    def _bound(request: Any, field: str | None, unset: tuple[Any, ...]) -> Any:
        """Read one optional request field, or ``None`` when absent/unset."""
        if field is None:
            return None
        value = getattr(request, field, None)
        return value if _present(value, unset) else None


__all__ = ['UNSET_FALSY', 'Binding']
