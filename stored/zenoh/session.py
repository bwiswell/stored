"""Zenoh session wiring for the chronicler.

Opens a session with timestamping enabled — the chronicler depends on Zenoh HLC
timestamps for ordering and dedup. zeared's factories turn it on; don't turn it
off through ``zenoh_overrides``. With ``auto_reconnect`` set, the session is a
``zeared.ManagedSession``, which reconnects, and re-fetches retained values when
its link comes back.
"""

from __future__ import annotations

from typing import Any

import zeared as z


def open_session(zenoh_config: dict[str, Any]) -> Any:
    """Open a timestamped zeared session from a ``zenoh_config`` mapping.

    Args:
        zenoh_config: Connection spec mapped onto :class:`zeared.SessionConfig`
            (must include ``mode``, e.g. ``{'mode': 'peer'}`` or
            ``{'mode': 'client', 'router': 'tcp/host:7447'}``).

    Returns:
        An open, timestamped session: a ``zenoh.Session``, or a
        ``zeared.ManagedSession`` when ``auto_reconnect`` is set.
    """
    return z.open(z.SessionConfig.load(zenoh_config))


__all__ = ['open_session']
