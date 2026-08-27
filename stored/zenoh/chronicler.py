"""The chronicler: bind a store to a zeared session.

For each added stream it declares a subscriber (``on_message`` → record, using
the 2-arg callback so the HLC timestamp arrives) and, for non-RETAINED classes,
a queryable (``on_query`` → serve history).
"""
from __future__ import annotations

import threading
from typing import Any

import zeared as z

from ..errors import RegistrationError
from ..log import get_logger
from ..store import Store
from .serve import make_query_handler

_log = get_logger('zenoh.chronicler')


class Chronicler:
    """Records mesh traffic into a :class:`~stored.store.Store` and serves it back.

    The caller owns the session and the store; the chronicler owns only the
    subscribers and queryables it declares.

    Args:
        store: The store to record into and query from.
        session: An open zeared session (timestamping enabled).
    """

    __slots__ = ('_store', '_session', '_subscribers', '_queryables', '_stop')

    def __init__(self, store: Store, session: Any) -> None:
        self._store = store
        self._session = session
        self._subscribers: list[Any] = []
        self._queryables: list[Any] = []
        self._stop = threading.Event()

    def add(
        self,
        cls: type[z.Message],
        *,
        retention: str | None = None,
        serve: bool = True,
        index: tuple[str, ...] = (),
        on_error: Any = None,
    ) -> None:
        """Record ``cls``, and (if ``serve``) serve its history.

        Args:
            cls: A ``@z.zeared`` message class.
            retention: Retention horizon for the stream when first registered.
            serve: Whether to declare a history queryable (ignored for RETAINED
                classes, which cannot host an ``on_query``).
            index: Extra field names to index as queryable dimensions.
            on_error: Optional ``on_error(exc, raw)`` for the subscriber/queryable.
        """
        try:
            stream = self._store.registry.get(cls)
        except RegistrationError:
            stream = self._store.register(cls, retention=retention, index=index)

        def _record(msg: Any, meta: Any) -> None:
            self._store.record(cls, msg, meta=meta)

        self._subscribers.append(
            cls.on_message(_record, session=self._session, on_error=on_error),
        )

        if not serve:
            return
        if cls.RETAINED:
            _log.warning(
                '%s is RETAINED; skipping history queryable (on_query forbidden)',
                cls.__name__,
            )
            return
        handler = make_query_handler(self._store, stream)
        self._queryables.append(
            cls.on_query(handler, session=self._session, on_error=on_error),
        )

    def run(self) -> None:
        """Block, recording and serving, until :meth:`stop` (or SIGINT)."""
        self._stop.clear()
        try:
            self._stop.wait()
        except KeyboardInterrupt:
            pass
        finally:
            self.close()

    def stop(self) -> None:
        """Signal :meth:`run` to return."""
        self._stop.set()

    def close(self) -> None:
        """Tear down all subscribers and queryables (idempotent)."""
        self._stop.set()
        for handle in (*self._subscribers, *self._queryables):
            try:
                handle.close()
            except Exception:
                _log.exception('error closing chronicler handle')
        self._subscribers.clear()
        self._queryables.clear()


__all__ = ['Chronicler']
