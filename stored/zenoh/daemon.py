"""The standalone chronicler daemon.

Loads a :class:`~stored.config.StoredConfig`, opens a store and a timestamped
session, resolves each configured stream's class, wires a
:class:`~stored.zenoh.chronicler.Chronicler`, declares its presence (a liveliness
token at ``stored/presence/{identity}``), and runs until SIGTERM/SIGINT — then
withdraws the token, flushes, releases the session, and closes the store.
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
from ..streams import register_stream
from .chronicler import Chronicler
from .presence import ChroniclerPresence
from .session import open_session

if TYPE_CHECKING:
    from ..config import StoredConfig, StreamSpec

_log = get_logger('zenoh.daemon')

#: Characters a key segment can't hold literally: ``/`` splits it, and the rest are
#: Zenoh's wildcard and reserved characters, so ``*`` would declare a token on every
#: identity at once.
_NOT_IN_A_SEGMENT = frozenset('/*$?#')


def _check_identity(identity: str) -> None:
    """Refuse an identity that can't be the last segment of the presence key.

    Raises:
        ConfigError: If ``identity`` is empty, or holds a character a key segment
            can't hold literally.
    """
    if not identity:
        msg = 'identity is empty; it names this daemon on the mesh (stored/presence/{identity})'
        raise ConfigError(msg)
    found = sorted(_NOT_IN_A_SEGMENT.intersection(identity))
    if found:
        msg = (
            f'identity {identity!r} holds {"".join(found)!r}; it must be one key segment (stored/presence/{{identity}})'
        )
        raise ConfigError(msg)


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


def _watchdog_interval() -> float | None:
    """Seconds between watchdog pings, or ``None`` when systemd wants none (0.4.1).

    systemd sets ``WATCHDOG_USEC`` when the unit has ``WatchdogSec=``, and
    ``WATCHDOG_PID`` to the process it expects pings from. Pinging at half the
    period leaves a missed tick's slack, as ``sd_watchdog_enabled(3)`` advises.
    """
    usec = os.environ.get('WATCHDOG_USEC')
    if not usec:
        return None
    pid = os.environ.get('WATCHDOG_PID')
    if pid and pid != str(os.getpid()):
        return None
    try:
        period = int(usec) / 1_000_000
    except ValueError:
        _log.warning('ignoring WATCHDOG_USEC=%r: not a number of microseconds', usec)
        return None
    return period / 2 if period > 0 else None


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
        '_presence',
        '_reaper_stop',
        '_reaper_thread',
        '_session',
        '_stop',
        '_store',
        '_watchdog_stop',
        '_watchdog_thread',
    )

    def __init__(self, config: StoredConfig, *, session: Any = None, store: Store | None = None) -> None:
        self._config = config
        self._injected_session = session
        self._injected_store = store
        self._store: Store | None = None
        self._session: Any = None
        self._chronicler: Chronicler | None = None
        self._presence: z.PresenceToken | None = None
        self._owns_store = False
        self._owns_session = False
        self._reaper_stop = threading.Event()
        self._reaper_thread: threading.Thread | None = None
        self._watchdog_stop = threading.Event()
        self._watchdog_thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        """Open resources, register + subscribe every stream, declare presence, signal readiness."""
        cfg = self._config
        configure(cfg.log_level)
        _check_identity(cfg.identity)

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
            # The whole spec, as `migrate` and `prune` register it. The chronicler
            # registers a class itself only when it finds it unregistered, and
            # then with retention and indexes alone.
            register_stream(store, spec, cls)
            chronicler.add(cls)
            _log.info(
                'recording %s (retention=%s, time_field=%s, latest=%s)',
                cls.__name__,
                spec.retention,
                spec.time_field,
                spec.latest.key if spec.latest is not None else None,
            )

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

        interval = _watchdog_interval()
        if interval is not None:
            self._watchdog_thread = threading.Thread(
                target=self._watchdog_loop,
                args=(store, interval),
                name='stored-watchdog',
                daemon=True,
            )
            self._watchdog_thread.start()

        # Last before READY: the token says the daemon is recording, so it goes up
        # once every stream is subscribed.
        self._presence = ChroniclerPresence.declare_presence(session=session, identity=cfg.identity)

        _sd_notify('READY=1')
        _log.info('stored chronicler ready (%d stream(s)) as %s', len(cfg.streams), self._presence.key)

    def _reaper_loop(self, store: Store, interval: float) -> None:
        """Sweep expired rows every ``interval`` seconds until stopped."""
        while not self._reaper_stop.wait(interval):
            try:
                removed = store.prune()
                if removed:
                    _log.info('reaper removed %d expired row(s)', removed)
            except Exception:  # noqa: BLE001 — a daemon loop outlives any single failure
                _log.exception('reaper sweep failed')

    def _watchdog_tick(self, store: Store, timeout: float) -> bool:
        """Ping systemd's watchdog if the store can still do its work; whether it pinged.

        A store that is wedged, or whose writer has stopped, gets no ping, so
        systemd restarts the daemon once ``WatchdogSec`` passes without one.
        """
        problem = store.health(timeout)
        if problem is not None:
            _log.warning('watchdog: not pinging, since %s', problem)
            return False
        _sd_notify('WATCHDOG=1')
        return True

    def _watchdog_loop(self, store: Store, interval: float) -> None:
        """Tick every ``interval`` seconds until stopped, giving the store half of it."""
        while not self._watchdog_stop.wait(interval):
            try:
                self._watchdog_tick(store, interval / 2)
            except Exception:  # noqa: BLE001 — a daemon loop outlives any single failure
                _log.exception('watchdog tick failed')

    def wait(self) -> None:
        """Block until :meth:`stop` (or a shutdown signal)."""
        self._stop.wait()

    def stop(self) -> None:
        """Signal :meth:`wait` to return."""
        self._stop.set()

    def shutdown(self) -> None:
        """Tear down in order: watchdog → presence → reaper → chronicler → session → store."""
        _sd_notify('STOPPING=1')
        self._watchdog_stop.set()
        if self._watchdog_thread is not None:
            self._watchdog_thread.join(timeout=5.0)
            self._watchdog_thread = None
        # First, so watchers see the daemon go as it stops recording. Releasing the
        # session would withdraw it too, but not an injected session's.
        if self._presence is not None:
            self._presence.undeclare()
            self._presence = None
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
