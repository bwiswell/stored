"""Zenoh session wiring for the chronicler.

Opens a managed session with timestamping enabled — the chronicler depends on
Zenoh HLC timestamps for ordering and dedup, so ``timestamping=True`` is
mandatory, not optional.

.. note::
   M0 scaffold: session construction lands in M3.
"""
from __future__ import annotations

from typing import Any


def open_session(zenoh_config: dict[str, Any]) -> Any:
    """Open a managed, timestamped zeared session from ``zenoh_config``.

    Args:
        zenoh_config: Connection spec mapped onto ``z.SessionConfig``.

    Returns:
        An open managed ``zeared`` session.
    """
    raise NotImplementedError('zenoh.session.open_session lands in M3')


__all__ = ['open_session']
