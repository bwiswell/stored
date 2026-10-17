"""Exception hierarchy for ``stored``.

Every error ``stored`` raises itself derives from :class:`StoredError`. Two
come through from its dependencies instead, unwrapped:

- ``seared.ValidationError`` from a read, when a stored row no longer loads
  into its class (a field retyped lossily since it was written, or a required
  field added);
- ``zeared.TopicError`` when a class is first subscribed or served, when its
  ``TOPIC`` can't be used (a ``Path`` or base64 ``Bytes`` single-segment slot).

A batch the writer fails to flush is logged and dropped, not raised: the
writer thread must outlive one bad batch.
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


class QueryError(StoredError):
    """A history query could not be planned or executed."""


__all__ = [
    'BackendError',
    'ConfigError',
    'QueryError',
    'RegistrationError',
    'SchemaError',
    'StoredError',
]
