"""Exception hierarchy for ``stored``.

All errors raised by the package derive from :class:`StoredError`, so callers
can ``except StoredError`` to catch anything the persistence layer throws.
"""

from __future__ import annotations


class StoredError(Exception):
    """Base class for every error raised by ``stored``."""


class ConfigError(StoredError):
    """Configuration is missing or invalid."""


class RegistrationError(StoredError):
    """A message class could not be registered as a stream."""


class SchemaError(StoredError):
    """A seared class could not be mapped to a table, or a table has drifted."""


class BackendError(StoredError):
    """The storage backend failed (connect, DDL, read, or write)."""


class WriterError(StoredError):
    """The batched writer failed to buffer or flush rows."""


class QueryError(StoredError):
    """A history query could not be planned or executed."""


__all__ = [
    'BackendError',
    'ConfigError',
    'QueryError',
    'RegistrationError',
    'SchemaError',
    'StoredError',
    'WriterError',
]
