"""``stored`` — a zeared-native persistence layer and Zenoh chronicler.

The public surface is the seared-only **core**: :class:`Store` (persist and
query seared objects), the :class:`~stored.config.StoredConfig` /
:class:`~stored.config.StreamSpec` config classes, and the error hierarchy.

``zeared`` is a core dependency (plan 02), so the :class:`Chronicler` — the
Zenoh layer that records mesh traffic into a store and serves it back — is a
first-class export. It is re-exported **lazily** (PEP 562 ``__getattr__``): the
name resolves on first access, so a Store-only ``import stored`` never pulls in
``zeared`` / ``zenoh`` at import time. It also remains available under its own
namespace as :class:`stored.zenoh.Chronicler`.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

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

if TYPE_CHECKING:
    from .zenoh import Chronicler

__version__ = '0.2.15'

__all__ = [
    'Store',
    'Chronicler',
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


def __getattr__(name: str) -> object:
    """Lazily resolve the top-level ``Chronicler`` re-export (PEP 562).

    Keeps a Store-only ``import stored`` transport-free at import time: ``zeared``
    / ``zenoh`` are only imported when ``stored.Chronicler`` is actually accessed.
    """
    if name == 'Chronicler':
        from .zenoh import Chronicler

        return Chronicler
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
