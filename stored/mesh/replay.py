"""Replay recorded history back onto the mesh as ordinary traffic.

A queryable answers *one asker*. A **replay** re-publishes what was recorded so
that any number of ordinary subscribers see it through their ordinary
``on_message`` path — which is what lets a pipeline stage be fed a historical
window with no client code at all: re-run yesterday through a pipeline stage, warm a
cache after a restart, drive a lab run from real recorded traffic.

**Where a replay lands.** Zeared publishes on *declared* templates only, so a
replayable class declares its replay scope in ``EXTRA_TOPICS``:

```python
class Reading(zeared.Message):
    TOPIC = 'live/reading/{source}'
    EXTRA_TOPICS = ('replay/reading/{source}',)
```

That single declaration carries the whole isolation story. Subscribers of the
class receive both scopes, so a consumer that *wants* history gets it for free;
a **recorder** that does not want to re-record a replay says so with
``Binding.record(..., live_only=True)``, which is the one place the distinction
has to be made. Provenance rides the key expression because it is the only
channel available: ``ZenohMeta.origin`` is derived from the local delivery path
and never crosses the wire, so a replayed sample is otherwise indistinguishable
from a live one.

**A replay never retains.** Every publish goes out with ``retain=False``, so
replaying a ``RETAINED`` class cannot clobber the live retained value with a
historical one.

**A row with no key is skipped, not fatal.** zeared refuses a slot value that
would split its key segment (a ``/`` in a single-segment slot), and such a row
has no key to go out on. The replay skips it, logs the first, and counts them
all on ``skipped``; the rows around it still go out. The destination itself is
checked before anything is read, so a topic no row could publish on is a
``ConfigError``, not a replay that skips every row.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
from typing import TYPE_CHECKING, Any

import zeared as z

from ..errors import ConfigError
from ..log import get_logger
from ..query import DEFAULT_CHUNK, TimeBound
from .async_store import AsyncStore

if TYPE_CHECKING:
    from ..store import Store

_log = get_logger('mesh.replay')


def _event_seconds(value: Any) -> float | None:
    """Read a ``time_field`` value as unix seconds, or ``None`` when it has no time."""
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.replace(tzinfo=value.tzinfo or dt.UTC).timestamp()
    if isinstance(value, dt.date):
        return dt.datetime(value.year, value.month, value.day, tzinfo=dt.UTC).timestamp()
    return float(value)


class ReplayHandle:
    """A running replay: how far it has got, and how to stop it early.

    Attributes:
        task: The asyncio task running the replay.
    """

    __slots__ = ('_progress', '_stop', 'task')

    def __init__(self, task: asyncio.Task, stop: asyncio.Event, progress: dict[str, int]) -> None:
        self.task = task
        self._stop = stop
        self._progress = progress

    @property
    def sent(self) -> int:
        """Rows published so far."""
        return self._progress['sent']

    @property
    def skipped(self) -> int:
        """Rows skipped so far, because their key could not be rendered."""
        return self._progress.get('skipped', 0)

    @property
    def done(self) -> bool:
        """Whether the replay has finished (completed, stopped, or failed)."""
        return self.task.done()

    def stop(self) -> None:
        """Ask the replay to stop after the row in flight (idempotent)."""
        self._stop.set()

    async def wait(self) -> int:
        """Await completion and return the number of rows published."""
        return await self.task


class Replayer:
    """Publishes recorded history back onto the mesh.

    ``stored`` ships the *mechanism*; the command contract that starts a replay —
    who may ask, on which topic, with what verbs — belongs to the service, exactly
    as with :class:`~stored.mesh.Binding`'s queryables.

    Args:
        store: The store to read from. A bare :class:`~stored.store.Store` is
            wrapped in an :class:`~stored.mesh.AsyncStore` automatically.
        session: The zeared session to publish on, or ``None`` for the ambient one.
    """

    __slots__ = ('_session', '_store')

    def __init__(self, store: AsyncStore | Store, *, session: Any = None) -> None:
        self._store = store if isinstance(store, AsyncStore) else AsyncStore(store)
        self._session = session

    async def replay[M: z.Message](
        self,
        cls: type[M],
        *,
        topic: str | None = None,
        since: TimeBound = None,
        until: TimeBound = None,
        limit: int | None = None,
        speed: float = 0.0,
        max_rate: float | None = None,
        chunk: int = DEFAULT_CHUNK,
        stop: asyncio.Event | None = None,
        progress: dict[str, int] | None = None,
        **filters: Any,
    ) -> int:
        """Publish a recorded window and return the number of rows sent.

        Args:
            cls: The registered class to replay.
            topic: The declared template to publish on. Defaults to the class's sole
                ``EXTRA_TOPICS`` entry — the replay scope. Pass ``cls.TOPIC``
                explicitly to republish onto the live topic (rarely what you want:
                recorders cannot then tell the replay from live traffic).
            since: Lower time bound of the window.
            until: Upper time bound of the window.
            limit: Maximum rows to publish, or ``None`` for the whole window.
            speed: ``0`` publishes as fast as the mesh accepts (backfill). Above
                zero paces to wall-clock off the stream's event time — ``1.0`` is
                real time, ``10.0`` is ten times faster — which needs a
                ``time_field`` on the stream.
            max_rate: Ceiling in rows per second, applied whatever ``speed`` says.
            chunk: Rows per page read from the store.
            stop: Set this event to end the replay early.
            progress: Optional ``{'sent': int}`` dict updated as rows go out. A
                ``'skipped'`` count is added to it: rows whose key could not be
                rendered, which are not published.
            **filters: Equality filters on indexed field dimensions.

        Returns:
            The number of rows published. Skipped rows are not counted.

        Raises:
            ConfigError: If the destination cannot be resolved, or is one the class
                cannot publish on, or ``speed`` asks for pacing on a stream with no
                event time.
        """
        destination = self._resolve_topic(cls, topic)
        self._check_destination(cls, destination)
        stream = self._store.store.registry.get(cls)
        if speed > 0 and stream.time_field is None:
            msg = (
                f'replay({cls.__name__}, speed={speed}): pacing needs an event time; '
                'register the stream with time_field=… or replay with speed=0'
            )
            raise ConfigError(msg)
        counter = progress if progress is not None else {'sent': 0}
        counter.setdefault('skipped', 0)
        halt = stop if stop is not None else asyncio.Event()
        interval = (1.0 / max_rate) if max_rate else 0.0
        paced = speed > 0 or interval > 0

        _log.info(
            'replay %s → %s (speed=%s, limit=%s)',
            cls.__name__,
            destination,
            speed or 'max',
            limit or 'all',
        )
        if paced:
            sent = await self._paced(
                cls,
                destination,
                stream.time_field,
                speed,
                interval,
                counter,
                halt,
                since=since,
                until=until,
                limit=limit,
                chunk=chunk,
                **filters,
            )
        else:
            sent = await self._as_fast_as_possible(
                cls,
                destination,
                counter,
                halt,
                since=since,
                until=until,
                limit=limit,
                chunk=chunk,
                **filters,
            )
        if counter['skipped']:
            _log.warning(
                'replay %s → %s: skipped %d row(s) with no key to publish on',
                cls.__name__,
                destination,
                counter['skipped'],
            )
        return sent

    def start[M: z.Message](self, cls: type[M], **options: Any) -> ReplayHandle:
        """Run :meth:`replay` as a background task and return its :class:`ReplayHandle`.

        The fire-and-forget form: a service answering a "replay this window" command
        starts one, replies immediately, and reports progress from the handle.

        Args:
            cls: The registered class to replay.
            **options: As :meth:`replay`.

        Returns:
            A handle carrying progress, completion, and :meth:`ReplayHandle.stop`.
        """
        halt = asyncio.Event()
        counter: dict[str, int] = {'sent': 0, 'skipped': 0}
        task = asyncio.create_task(
            self.replay(cls, stop=halt, progress=counter, **options),
            name=f'replay-{cls.__name__}',
        )
        return ReplayHandle(task, halt, counter)

    # -- the two pacing modes ---------------------------------------------

    async def _as_fast_as_possible[M: z.Message](
        self,
        cls: type[M],
        topic: str,
        counter: dict[str, int],
        halt: asyncio.Event,
        *,
        chunk: int,
        **window: Any,
    ) -> int:
        """Backfill: publish a page per thread hop, which is what makes bulk replay quick."""
        page: list[M] = []
        async for row in self._store.iter(cls, chunk=chunk, **window):
            if halt.is_set():
                break
            page.append(row)
            if len(page) >= chunk:
                await self._send_page(cls, page, topic, counter)
                page = []
        if page and not halt.is_set():
            await self._send_page(cls, page, topic, counter)
        return counter['sent']

    async def _paced[M: z.Message](
        self,
        cls: type[M],
        topic: str,
        time_field: str | None,
        speed: float,
        interval: float,
        counter: dict[str, int],
        halt: asyncio.Event,
        *,
        chunk: int,
        **window: Any,
    ) -> int:
        """Wall-clock replay: sleep the (scaled) gap the recording actually had."""
        previous: float | None = None
        async for row in self._store.iter(cls, chunk=chunk, **window):
            if halt.is_set():
                break
            delay = interval
            if speed > 0 and time_field is not None:
                current = _event_seconds(getattr(row, time_field, None))
                if current is not None and previous is not None:
                    delay = max(delay, 0.0, (current - previous) / speed)
                if current is not None:
                    previous = current
            if delay > 0:
                # Sleep, but wake immediately on stop — a paced replay may be long.
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(halt.wait(), delay)
                if halt.is_set():
                    break
            await self._send_row(row, topic, counter)
        return counter['sent']

    # -- helpers -----------------------------------------------------------

    async def _send_page[M: z.Message](
        self,
        cls: type[M],
        page: list[M],
        topic: str,
        counter: dict[str, int],
    ) -> None:
        """Publish one page in a single thread hop.

        A row with no key to publish on makes ``z.batch`` discard the whole page,
        so the page is resent row by row, which skips only that row.
        """
        try:
            await z.asend_batch(cls, page, session=self._session, topic=topic, retain=False)
        except z.TopicError:
            for row in page:
                await self._send_row(row, topic, counter)
            return
        counter['sent'] += len(page)

    async def _send_row(self, row: z.Message, topic: str, counter: dict[str, int]) -> None:
        """Publish one row, or skip it (logging the first) if its key can't be rendered."""
        try:
            await z.asend(row, session=self._session, topic=topic, retain=False)
        except z.TopicError as exc:
            counter['skipped'] += 1
            if counter['skipped'] == 1:
                _log.warning(
                    'replay %s: skipping a row with no key to publish on (%s); later ones are only counted',
                    type(row).__name__,
                    exc,
                )
            return
        counter['sent'] += 1

    @staticmethod
    def _check_destination(message_cls: type[z.Message], topic: str) -> None:
        """Refuse a destination no row could be published on, before anything is read.

        Renders the template with a placeholder for every field, so it fails only
        for the destination itself: undeclared, subscribe-only, or with a slot the
        class doesn't declare. After this, a ``TopicError`` from a send is the
        row's own, and the row is skipped.

        Raises:
            ConfigError: If ``topic`` is not one ``message_cls`` can publish on.
        """
        placeholders = {attr: 'x' for attr, _wire, _field in message_cls.__seared_fields__}
        try:
            message_cls.concrete_topic(topic=topic, **placeholders)
        except z.TopicError as exc:
            msg = f'replay({message_cls.__name__}): cannot publish on {topic!r}: {exc}'
            raise ConfigError(msg) from exc

    @staticmethod
    def _resolve_topic(message_cls: type[z.Message], topic: str | None) -> str:
        """Pick the destination template: the caller's, or the class's sole replay scope."""
        if topic is not None:
            return topic
        extras = tuple(getattr(message_cls, 'EXTRA_TOPICS', ()) or ())
        if len(extras) == 1:
            return extras[0]
        if not extras:
            msg = (
                f'replay({message_cls.__name__}): no replay scope — declare one in EXTRA_TOPICS, '
                'or pass topic= explicitly (cls.TOPIC republishes onto the live topic)'
            )
            raise ConfigError(msg)
        msg = f'replay({message_cls.__name__}): {len(extras)} declared EXTRA_TOPICS; pass topic= to choose one'
        raise ConfigError(msg)


__all__ = ['ReplayHandle', 'Replayer']
