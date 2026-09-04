"""The standalone chronicler daemon.

Loads a :class:`~stored.config.StoredConfig`, opens a store and a timestamped
session, resolves each configured stream's class, wires a
:class:`~stored.zenoh.chronicler.Chronicler`, and runs until SIGTERM/SIGINT —
then flushes, releases the session, and closes the store.
"""

from __future__ import annotations

import contextlib
import importlib
import os
import signal
import socket
import threading
from typing import TYPE_CHECKING, Any

import zeared as z

from ..errors import ConfigError
from ..log import configure, get_logger
from ..store import Store
from .chronicler import Chronicler
from .session import open_session

if TYPE_CHECKING:
    from ..config import StoredConfig, StreamSpec

_log = get_logger('zenoh.daemon')


def _resolve(spec: StreamSpec) -> type[z.Message]:
    """Resolve a ``'module:ClassName'`` stream spec to its zeared Message class.

    Raises:
        ConfigError: If the path is malformed, unimportable, or not a Message.
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
    if not (isinstance(obj, type) and issubclass(obj, z.Message)):
        msg = f'stream cls {spec.cls!r} is not a zeared Message class'
        raise ConfigError(msg)
    return obj


def _sd_notify(state: str) -> None:
    """Best-effort ``sd_notify`` — a no-op when not run under systemd."""
    addr = os.environ.get('NOTIFY_SOCKET')
    if not addr:
        return
    if addr.startswith('@'):
        addr = '\0' + addr[1:]
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
            sock.connect(addr)
            sock.sendall(state.encode())
    except OSError:
        _log.warning('sd_notify(%s) failed', state)


class Daemon:
    """Owns the store, session, chronicler, and reaper for a running chronicler.

    An injected ``session`` / ``store`` (for tests) is used as-is and left for
    the caller to close; otherwise the daemon builds and owns them.

    Args:
        config: The loaded configuration.
        session: Optional pre-opened session (test injection).
        store: Optional pre-built store (test injection).
    """

    __slots__ = (
        '_chronicler',
        '_config',
        '_injected_session',
        '_injected_store',
        '_owns_session',
        '_owns_store',
        '_reaper_stop',
        '_reaper_thread',
        '_session',
        '_stop',
        '_store',
    )

    def __init__(self, config: StoredConfig, *, session: Any = None, store: Store | None = None) -> None:
        self._config = config
        self._injected_session = session
        self._injected_store = store
        self._store: Store | None = None
        self._session: Any = None
        self._chronicler: Chronicler | None = None
        self._owns_store = False
        self._owns_session = False
        self._reaper_stop = threading.Event()
        self._reaper_thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        """Open resources, register + subscribe every stream, signal readiness."""
        cfg = self._config
        configure(cfg.log_level)

        if self._injected_store is not None:
            store = self._injected_store
        else:
            store = Store(
                cfg.db_path,
                backend=cfg.backend,
                flush_rows=cfg.flush_rows,
                flush_secs=cfg.flush_secs,
            )
            self._owns_store = True

        if self._injected_session is not None:
            session = self._injected_session
        else:
            session = open_session(cfg.zenoh)
            self._owns_session = True

        chronicler = Chronicler(store, session)
        for spec in cfg.streams:
            cls = _resolve(spec)
            chronicler.add(cls, retention=spec.retention, index=tuple(spec.index))
            _log.info('recording %s (retention=%s)', cls.__name__, spec.retention)

        self._store = store
        self._session = session
        self._chronicler = chronicler

        if cfg.prune_interval > 0:
            self._reaper_thread = threading.Thread(
                target=self._reaper_loop,
                args=(store, cfg.prune_interval),
                name='stored-reaper',
                daemon=True,
            )
            self._reaper_thread.start()

        _sd_notify('READY=1')
        _log.info('stored chronicler ready (%d stream(s))', len(cfg.streams))

    def _reaper_loop(self, store: Store, interval: float) -> None:
        """Sweep expired rows every ``interval`` seconds until stopped."""
        while not self._reaper_stop.wait(interval):
            try:
                removed = store.prune()
                if removed:
                    _log.info('reaper removed %d expired row(s)', removed)
            except Exception:  # noqa: BLE001 — a daemon loop outlives any single failure
                _log.exception('reaper sweep failed')

    def wait(self) -> None:
        """Block until :meth:`stop` (or a shutdown signal)."""
        self._stop.wait()

    def stop(self) -> None:
        """Signal :meth:`wait` to return."""
        self._stop.set()

    def shutdown(self) -> None:
        """Tear down in order: reaper → chronicler → session → store."""
        _sd_notify('STOPPING=1')
        self._reaper_stop.set()
        if self._reaper_thread is not None:
            self._reaper_thread.join(timeout=(self._config.prune_interval or 0.0) + 5.0)
            self._reaper_thread = None
        if self._chronicler is not None:
            self._chronicler.close()
        if self._session is not None and self._owns_session:
            try:
                z.release(session=self._session)
            except Exception:  # noqa: BLE001 — a daemon loop outlives any single failure
                _log.exception('error releasing session')
        if self._store is not None and self._owns_store:
            self._store.close()


def _install_signals(daemon: Daemon) -> None:
    """Route SIGTERM/SIGINT to ``daemon.stop`` (skipped off the main thread)."""

    def _handler(signum: int, _frame: Any) -> None:
        _log.info('signal %d received; shutting down', signum)
        daemon.stop()

    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(ValueError):  # not the main thread (e.g. under a test runner)
            signal.signal(sig, _handler)


def run(config: StoredConfig) -> int:
    """Run the chronicler daemon to completion.

    Args:
        config: The loaded configuration.

    Returns:
        A process exit code.
    """
    daemon = Daemon(config)
    daemon.start()
    _install_signals(daemon)
    try:
        daemon.wait()
    except KeyboardInterrupt:
        pass
    finally:
        daemon.shutdown()
    return 0


__all__ = ['Daemon', 'run']
