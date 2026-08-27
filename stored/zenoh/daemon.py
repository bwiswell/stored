"""The standalone chronicler daemon.

Loads a :class:`~stored.config.StoredConfig`, opens a store and a timestamped
session, resolves each configured stream's class, wires a
:class:`~stored.zenoh.chronicler.Chronicler`, and runs until SIGTERM/SIGINT —
then flushes, releases the session, and closes the store.

.. note::
   M0 scaffold: the run loop and lifecycle land in M4.
"""
from __future__ import annotations

from ..config import StoredConfig


def run(config: StoredConfig) -> int:
    """Run the chronicler daemon to completion.

    Args:
        config: The loaded configuration.

    Returns:
        A process exit code.
    """
    raise NotImplementedError('zenoh.daemon.run lands in M4')


__all__ = ['run']
