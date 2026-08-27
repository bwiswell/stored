"""The chronicler: bind a store to a zeared session.

For each added stream it declares a subscriber (``on_message`` → record, using
the 2-arg callback so the HLC timestamp arrives) and, for non-RETAINED classes,
a queryable (``on_query`` → serve history).

.. note::
   M0 scaffold: subscribe/serve wiring lands in M3.
"""
from __future__ import annotations

from typing import Any

from ..log import get_logger
from ..store import Store

_log = get_logger('zenoh.chronicler')


class Chronicler:
    """Records mesh traffic into a :class:`~stored.store.Store` and serves it back.

    Args:
        store: The store to record into and query from.
        session: An open zeared session (timestamping enabled).
    """

    __slots__ = ('_store', '_session', '_subscribers', '_queryables')

    def __init__(self, store: Store, session: Any) -> None:
        self._store = store
        self._session = session
        self._subscribers: list[Any] = []
        self._queryables: list[Any] = []

    def add(
        self,
        cls: type,
        *,
        retention: str | None = None,
        serve: bool = True,
        index: tuple[str, ...] = (),
    ) -> None:
        """Record ``cls``, and (if ``serve``) serve its history. Stub — M3."""
        raise NotImplementedError('Chronicler.add lands in M3')

    def run(self) -> None:
        """Block, recording and serving until stopped. Stub — M3/M4."""
        raise NotImplementedError('Chronicler.run lands in M3')

    def close(self) -> None:
        """Tear down subscribers and queryables. Stub — M3."""
        raise NotImplementedError('Chronicler.close lands in M3')


__all__ = ['Chronicler']
