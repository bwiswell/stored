"""``stored`` — a seared-flavored persistence layer and Zenoh chronicler.

The public surface is the seared-only **core**: :class:`Store` (persist and
query seared objects), the :class:`~stored.config.StoredConfig` /
:class:`~stored.config.StreamSpec` config classes, and the error hierarchy. The
optional Zenoh chronicler lives under ``stored.zenoh`` (requires the ``zenoh``
extra) and is not imported here.
"""
from __future__ import annotations

from .config import StoredConfig, StreamSpec
from .errors import (
    BackendError,
    ConfigError,
    QueryError,
    RegistrationError,
    SchemaError,
    StoredError,
    WriterError,
)
from .store import Store

__version__ = '0.2.1'

__all__ = [
    'Store',
    'StoredConfig',
    'StreamSpec',
    'StoredError',
    'ConfigError',
    'RegistrationError',
    'SchemaError',
    'BackendError',
    'WriterError',
    'QueryError',
    '__version__',
]
