"""Zenoh session wiring for the chronicler.

Opens a session with timestamping enabled — the chronicler depends on Zenoh HLC
timestamps for ordering and dedup. zeared's factories turn it on; don't turn it
off through ``zenoh_overrides``. With ``auto_reconnect`` set, the session is a
``zeared.ManagedSession``, which reconnects, and re-fetches retained values when
its link comes back.

The connection comes from the config's ``[zenoh]`` table, or, when it has none,
from zeared's ``ZEARED_SESSION_*`` environment variables (0.4.1). The two don't
layer, as stored's own config doesn't (``-c`` reads the TOML alone): a table is
used whole.
"""

from __future__ import annotations

from typing import Any

import zeared as z

from ..errors import ConfigError


def session_config(zenoh_config: dict[str, Any]) -> z.SessionConfig:
    """The ``SessionConfig`` the daemon opens, from ``[zenoh]`` or the environment.

    A non-empty ``zenoh_config`` is loaded whole; an empty one (no ``[zenoh]``
    table, or an environment-only config, which never maps it) reads
    ``ZEARED_SESSION_MODE``, ``ZEARED_SESSION_ROUTER`` and the rest through
    ``SessionConfig.from_env``.

    Args:
        zenoh_config: The config's ``[zenoh]`` table, as a mapping.

    Returns:
        The connection spec.

    Raises:
        ConfigError: If the chosen source gives no ``mode``, naming where to set
            one.
        seared.ValidationError: If a value is wrong, such as a ``mode`` other
            than ``peer`` or ``client``.
    """
    if zenoh_config:
        where = '[zenoh] sets no mode: add mode = "peer" or mode = "client"'
        load = lambda: z.SessionConfig.load(zenoh_config)  # noqa: E731
    else:
        where = 'no session mode: give the config a [zenoh] table with a mode, or set ZEARED_SESSION_MODE'
        load = z.SessionConfig.from_env
    try:
        return load()
    except z.ValidationError as exc:
        if exc.path == ('mode',) and exc.reason == 'required key missing':
            raise ConfigError(where) from exc
        raise


def open_session(zenoh_config: dict[str, Any]) -> Any:
    """Open a timestamped zeared session, from ``[zenoh]`` or the environment (see :func:`session_config`).

    Args:
        zenoh_config: The config's ``[zenoh]`` table, as a mapping; empty to read
            ``ZEARED_SESSION_*`` instead.

    Returns:
        An open, timestamped session: a ``zenoh.Session``, or a
        ``zeared.ManagedSession`` when ``auto_reconnect`` is set.

    Raises:
        ConfigError: If neither source gives a ``mode``.
    """
    return z.open(session_config(zenoh_config))


__all__ = ['open_session', 'session_config']
