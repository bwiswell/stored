"""Logging setup for ``stored``.

Stdlib ``logging`` only — the daemon logs to stdout, which systemd routes to
journald. No colorlog / rich, per the workspace convention.
"""

from __future__ import annotations

import logging
import sys

_LOG_FORMAT = '%(asctime)s %(levelname)-8s %(name)s: %(message)s'


def configure(level: str = 'INFO') -> None:
    """Configure root logging to stdout at ``level``.

    Idempotent enough for a daemon entrypoint: replaces existing handlers so a
    re-invocation does not double-log.

    Args:
        level: A logging level name (e.g. ``'INFO'``, ``'DEBUG'``).
    """
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(logging.Formatter(_LOG_FORMAT))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())


def get_logger(name: str) -> logging.Logger:
    """Return the ``stored``-namespaced logger for ``name``.

    Args:
        name: A dotted suffix (e.g. ``'writer'``) under the ``stored`` root.

    Returns:
        The corresponding :class:`logging.Logger`.
    """
    return logging.getLogger(f'stored.{name}')


__all__ = ['configure', 'get_logger']
