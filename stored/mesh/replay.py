"""Replay recorded history back onto the mesh as ordinary traffic.

A queryable answers *one asker*. A **replay** re-publishes what was recorded so
that any number of ordinary subscribers see it through their ordinary
``on_message`` path — which is what lets a pipeline stage be fed a historical
window with no client code at all: re-run yesterday through the collator, warm a
cache after a restart, drive a lab run from real recorded traffic.

**Where a replay lands.** Zeared publishes on *declared* templates only, so a
replayable class declares its replay scope in ``EXTRA_TOPICS``:

```python
class Location(zeared.Message):
    TOPIC = 'rio/telemetry/location/{source}'
    EXTRA_TOPICS = ('rio/replay/telemetry/location/{source}',)
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
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime
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
    if isinstance(value, datetime.datetime):
        return value.replace(tzinfo=value.tzinfo or datetime.UTC).timestamp()
    if isinstance(value, datetime.date):
        return datetime.datetime(value.year, value.month, value.day, tzinfo=datetime.UTC).timestamp()
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
            progress: Optional ``{'sent': int}`` dict updated as rows go out.
            **filters: Equality filters on indexed field dimensions.

        Returns:
            The number of rows published.

        Raises:
            ConfigError: If the destination cannot be resolved, or ``speed`` asks for
                pacing on a stream with no event time.
        """
        destination = self._resolve_topic(cls, topic)
        stream = self._store.store.registry.get(cls)
        if speed > 0 and stream.time_field is None:
            msg = (
                f'replay({cls.__name__}, speed={speed}): pacing needs an event time; '
                'register the stream with time_field=… or replay with speed=0'
            )
            raise ConfigError(msg)
        counter = progress if progress is not None else {'sent': 0}
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
            return await self._paced(
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
        return await self._as_fast_as_possible(
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
        counter: dict[str, int] = {'sent': 0}
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
            await z.asend(row, session=self._session, topic=topic, retain=False)
            counter['sent'] += 1
        return counter['sent']

    # -- helpers -----------------------------------------------------------

    async def _send_page[M: z.Message](
        self,
        cls: type[M],
        page: list[M],
        topic: str,
        counter: dict[str, int],
    ) -> None:
        """Publish one page in a single thread hop."""
        await z.asend_batch(cls, page, session=self._session, topic=topic, retain=False)
        counter['sent'] += len(page)

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
