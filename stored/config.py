"""Declarative configuration for ``stored``.

A ``@s.seared`` dataclass is the single source of truth, populated from the
environment (12-factor) with an optional TOML file for local/dev convenience
(``StoredConfig.from_toml`` is auto-attached by seared). No ``pydantic`` — the
serializer is the config layer, per the workspace convention.
"""
from __future__ import annotations

import os

import seared as s

_ENV_PREFIX_DEFAULT = 'STORED_'


def _split_csv(value: str) -> list[str]:
    """Split a comma-separated env value into a stripped, non-empty list."""
    return [item.strip() for item in value.split(',') if item.strip()]


@s.seared
class LatestSpec(s.Seared):
    """A stream's latest-per-key projection (durable last-known state).

    Attributes:
        key: Field names forming the logical-entity key (e.g. ``['source', 'epc']``).
        retention: Retention horizon for the projection (usually longer than the
            stream's history ``retention``); ``None`` keeps it forever.
    """

    key:       list       = s.Str(many=True, default_factory=list)
    retention: str | None = s.Str(default=None)


@s.seared
class StreamSpec(s.Seared):
    """One recorded stream: a message class plus its retention policy.

    Attributes:
        cls: Import path of the message class, ``'module:ClassName'``. The class
            must be a ``@s.seared`` / ``@z.zeared`` class.
        retention: Retention horizon (e.g. ``'7d'``, ``'48h'``); ``None`` keeps
            rows forever.
        archive: Cold-archival horizon (roadmap); ``None`` disables archival.
        index: Extra field names to index as queryable dimensions.
        time_field: A payload field naming the domain event time — retention and
            range queries key off it instead of the mesh delivery time. ``None``
            keeps the default (mesh ``_issued_at``).
        latest: A latest-per-key projection, or ``None`` for history only.
    """

    cls:        str               = s.Str(required=True)
    retention:  str | None        = s.Str(default=None)
    archive:    str | None        = s.Str(default=None)
    index:      list              = s.Str(many=True, default_factory=list)
    time_field: str | None        = s.Str(default=None)
    latest:     LatestSpec | None = s.T(LatestSpec, default=None)


@s.seared
class StoredConfig(s.Seared):
    """Top-level configuration for a ``stored`` store or chronicler daemon.

    Attributes:
        db_path: Path to the backing database file.
        backend: Storage backend name (``'sqlite'`` default; ``'duckdb'`` via the
            extra; ``'postgres'`` later).
        streams: Streams to record and serve.
        zenoh: Raw connection spec passed through to ``z.SessionConfig``.
        identity: This instance's name on the mesh (required).
        flush_rows: Writer flush threshold in buffered rows.
        flush_secs: Writer flush threshold in seconds.
        prune_interval: Seconds between TTL sweeps (0 disables periodic pruning).
        log_level: Root logging level name.
    """

    db_path:        str   = s.Str(default='chronicle.db')
    backend:        str   = s.Str(default='sqlite')
    streams:        list  = s.T(StreamSpec, many=True, default_factory=list)
    zenoh:          dict  = s.Dict(default_factory=dict)
    identity:       str   = s.Str(required=True)
    flush_rows:     int   = s.Int(default=1000)
    flush_secs:     float = s.Float(default=1.0)
    prune_interval: float = s.Float(default=300.0)
    log_level:      str   = s.Str(default='INFO')

    @classmethod
    def from_env(cls, prefix: str = _ENV_PREFIX_DEFAULT) -> StoredConfig:
        """Build a ``StoredConfig`` from environment variables.

        Scalar fields are read as ``{prefix}{FIELD_NAME}`` (uppercase). The
        ``streams`` list is not env-mapped — supply it via TOML
        (``StoredConfig.from_toml``) or programmatically; nested-list env
        nesting is intentionally out of scope.

        Args:
            prefix: Env-var prefix (default ``'STORED_'``).

        Returns:
            A validated ``StoredConfig``.

        Raises:
            seared.ValidationError: If required ``IDENTITY`` is absent.
        """
        data: dict = {}
        for env_key, field_name, coercer in (
            ('DB_PATH', 'db_path', str),
            ('BACKEND', 'backend', str),
            ('IDENTITY', 'identity', str),
            ('FLUSH_ROWS', 'flush_rows', int),
            ('FLUSH_SECS', 'flush_secs', float),
            ('PRUNE_INTERVAL', 'prune_interval', float),
            ('LOG_LEVEL', 'log_level', str),
        ):
            raw = os.environ.get(f'{prefix}{env_key}')
            if raw is None or raw == '':
                continue
            data[field_name] = coercer(raw)
        return cls.load(data)


__all__ = ['StoredConfig', 'StreamSpec', 'LatestSpec']
