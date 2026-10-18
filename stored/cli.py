"""Command-line interface for ``stored`` (``python -m stored``).

Stdlib ``argparse`` only — the surface is small: ``run`` (chronicler daemon),
``validate-config``, ``prune`` (force a TTL sweep), and ``migrate`` (reconcile
tables to current schemas).
"""

from __future__ import annotations

import argparse
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING

from . import log
from .config import StoredConfig, check_identity

if TYPE_CHECKING:
    from collections.abc import Sequence


def _load_config(path: str | None) -> StoredConfig:
    """Load config from a TOML ``path`` if given, else from the environment."""
    if path is not None:
        with Path(path).open('rb') as handle:
            data = tomllib.load(handle)
        return StoredConfig.load(data)
    return StoredConfig.from_env()


def _cmd_validate_config(args: argparse.Namespace) -> int:
    """Load and check the configuration, and summarize it.

    Beyond the load, it checks what ``run`` would refuse at start: the
    identity, and a ``[zenoh]`` table's values, loaded as ``run`` loads them
    (0.4.1); and the backend and every stream, registered as ``run``
    registers them (0.4.2). With no table, ``run`` reads the session from
    ``ZEARED_SESSION_*``, which this leaves unchecked: the summary says
    ``zenoh=env``. It imports only what the config names: zeared for a table
    or a stream, each stream's module, and the backend.
    """
    cfg = _load_config(args.config)
    check_identity(cfg.identity)
    if cfg.zenoh:
        from .zenoh.session import session_config  # lazy: only a table needs the transport

        session_config(cfg.zenoh)
    _check_streams(cfg)
    print(  # noqa: T201 - CLI user-facing output
        f'ok: identity={cfg.identity!r} backend={cfg.backend!r} db_path={cfg.db_path!r} streams={len(cfg.streams)} '
        f'zenoh={"table" if cfg.zenoh else "env"}',
    )
    return 0


def _check_streams(cfg: StoredConfig) -> None:
    """Register every stream into an in-memory store of the configured backend, as ``run`` would (0.4.2).

    Each class resolves as a zeared Message, which imports its module; each
    registration meets every check ``run``'s does (a duration, a ``time_field``,
    a ``latest`` key, an index, a duplicate), and the store's construction
    refuses an unknown backend. Nothing is written to disk.

    Raises:
        ConfigError: A stream class that won't resolve, a bad duration, or an
            unknown backend.
        RegistrationError: A registration ``run`` would refuse.
    """
    from .store import Store
    from .streams import register_stream, resolve_message_class

    store = Store(':memory:', backend=cfg.backend)
    try:
        for spec in cfg.streams:
            register_stream(store, spec, resolve_message_class(spec))
    finally:
        store.close()


def _cmd_run(args: argparse.Namespace) -> int:
    """Run the chronicler daemon."""
    cfg = _load_config(args.config)
    log.configure(cfg.log_level)
    from .zenoh import daemon  # lazy: keeps validate-config/prune/migrate transport-free

    return daemon.run(cfg)


def _cmd_prune(args: argparse.Namespace) -> int:
    """Force a one-off TTL sweep and report the row count removed."""
    cfg = _load_config(args.config)
    from .store import Store
    from .streams import register_streams

    store = Store(cfg.db_path, backend=cfg.backend)
    try:
        register_streams(store, cfg)
        removed = store.prune()
    finally:
        store.close()
    print(f'pruned {removed} rows')  # noqa: T201 - CLI user-facing output
    return 0


def _cmd_migrate(args: argparse.Namespace) -> int:
    """Create/reconcile stream tables to their classes' current schemas.

    Registration runs the backend's additive ``ensure_table`` (new columns are
    added; destructive changes remain a manual step).
    """
    cfg = _load_config(args.config)
    from .store import Store
    from .streams import register_streams

    store = Store(cfg.db_path, backend=cfg.backend)
    try:
        register_streams(store, cfg)
        tables = [stream.table for stream in store.registry.all()]
    finally:
        store.close()
    print(f'reconciled {len(tables)} table(s): {", ".join(tables) or "(none)"}')  # noqa: T201
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level argument parser."""
    parser = argparse.ArgumentParser(prog='stored', description=__doc__)
    parser.add_argument(
        '-c',
        '--config',
        default=None,
        help='Path to a TOML config file (default: read from the environment).',
    )
    sub = parser.add_subparsers(dest='command', required=True)

    sub.add_parser('run', help='Run the chronicler daemon.').set_defaults(
        func=_cmd_run,
    )
    sub.add_parser('validate-config', help='Load and check configuration.').set_defaults(
        func=_cmd_validate_config,
    )
    sub.add_parser('prune', help='Force a TTL sweep.').set_defaults(func=_cmd_prune)
    sub.add_parser('migrate', help='Reconcile tables to current schemas.').set_defaults(
        func=_cmd_migrate,
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint.

    Args:
        argv: Argument vector (defaults to ``sys.argv[1:]``).

    Returns:
        A process exit code.
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


__all__ = ['build_parser', 'main']
