"""Resolve configured streams into a store — the core (transport-free) wiring.

``migrate`` and ``prune`` are pure storage verbs: they need each stream's table and
retention, not a mesh. So resolving a ``StreamSpec``'s ``'module:ClassName'`` to its
class and registering it lives here in the core, validated at the **seared** level —
the store records ``seared`` objects, and the mesh-only ``zeared.Message`` distinction
(a topic to publish/serve on) is the chronicler's concern, not storage's. So
``migrate``/``prune`` run against the pure storage core, importing no ``zeared``.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

import seared as s

from .errors import ConfigError

if TYPE_CHECKING:
    import zeared as z

    from .config import StoredConfig, StreamSpec
    from .store import Store


def resolve_stream_class(spec: StreamSpec) -> type[s.Seared]:
    """Resolve a ``'module:ClassName'`` stream spec to its ``seared`` class.

    Args:
        spec: The stream spec whose ``cls`` import path to resolve.

    Returns:
        The resolved ``@s.seared`` / ``@z.zeared`` class.

    Raises:
        ConfigError: If the path is malformed, unimportable, or not a seared class.
    """
    module_path, sep, cls_name = spec.cls.partition(':')
    if not sep or not module_path or not cls_name:
        msg = f"stream cls {spec.cls!r} must be 'module:ClassName'"
        raise ConfigError(msg)
    try:
        module = importlib.import_module(module_path)
        obj = getattr(module, cls_name)
    except (ImportError, AttributeError) as exc:
        msg = f'cannot import stream class {spec.cls!r}: {exc}'
        raise ConfigError(msg) from exc
    if not (isinstance(obj, type) and issubclass(obj, s.Seared)):
        msg = f'stream cls {spec.cls!r} is not a seared class'
        raise ConfigError(msg)
    return obj


def resolve_message_class(spec: StreamSpec) -> type[z.Message]:
    """Resolve a stream spec to its zeared ``Message`` class, as the chronicler records it (0.4.2).

    :func:`resolve_stream_class`, then the check that the class is a Message:
    what ``run`` records, and what ``validate-config`` checks. zeared is
    imported here rather than at module load, so ``import stored.streams``
    stays transport-free; a class that is a Message has imported it anyway.

    Args:
        spec: The stream spec whose ``cls`` import path to resolve.

    Returns:
        The resolved ``@z.zeared`` Message class.

    Raises:
        ConfigError: If the path is malformed, unimportable, or not a zeared Message.
    """
    cls = resolve_stream_class(spec)
    import zeared as z  # lazy: only a chronicler's streams need the transport

    if not issubclass(cls, z.Message):
        msg = f'stream cls {spec.cls!r} is not a zeared Message class'
        raise ConfigError(msg)
    return cls


def register_stream(store: Store, spec: StreamSpec, cls: type[s.Seared]) -> None:
    """Register ``cls`` into ``store`` with everything ``spec`` declares.

    Retention, the archive horizon, indexes, the event-time field and the latest
    projection. The one
    place a configured stream becomes a registration, so the daemon's ``run`` and
    the ``migrate`` / ``prune`` verbs register it identically.

    Args:
        store: The store to register into.
        spec: The configured stream.
        cls: Its class, already resolved (the daemon resolves to a zeared
            ``Message``; the storage verbs, to any seared class).
    """
    latest = spec.latest
    store.register(
        cls,
        retention=spec.retention,
        archive=spec.archive,
        index=tuple(spec.index),
        time_field=spec.time_field,
        latest_key=tuple(latest.key) if latest is not None else (),
        latest_retention=latest.retention if latest is not None else None,
    )


def register_streams(store: Store, config: StoredConfig) -> None:
    """Resolve and register every configured stream into ``store`` (no session).

    Used by the ``migrate`` and ``prune`` CLI verbs, which need the tables and
    retention policies but not a mesh.
    """
    for spec in config.streams:
        register_stream(store, spec, resolve_stream_class(spec))


__all__ = ['register_stream', 'register_streams', 'resolve_message_class', 'resolve_stream_class']
