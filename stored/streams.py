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


def register_streams(store: Store, config: StoredConfig) -> None:
    """Resolve and register every configured stream into ``store`` (no session).

    Used by the ``migrate`` and ``prune`` CLI verbs, which need the tables and
    retention policies but not a mesh.
    """
    for spec in config.streams:
        cls = resolve_stream_class(spec)
        latest = spec.latest
        store.register(
            cls,
            retention=spec.retention,
            index=tuple(spec.index),
            time_field=spec.time_field,
            latest_key=tuple(latest.key) if latest is not None else (),
            latest_retention=latest.retention if latest is not None else None,
        )


__all__ = ['register_streams', 'resolve_stream_class']
