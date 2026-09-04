"""Zenoh session wiring for the chronicler.

Opens a session with timestamping enabled — the chronicler depends on Zenoh HLC
timestamps for ordering and dedup, so timestamping (zeared's default) is
mandatory, not optional.
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
        An open ``zeared`` session (timestamping enabled by the factory default).
    """
    return z.open(z.SessionConfig.load(zenoh_config))


__all__ = ['open_session']
