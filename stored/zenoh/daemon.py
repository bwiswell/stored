"""The standalone chronicler daemon.

Loads a :class:`~stored.config.StoredConfig`, opens a store and a timestamped
session, resolves each configured stream's class, wires a
:class:`~stored.zenoh.chronicler.Chronicler`, declares its presence (a liveliness
token at ``stored/presence/{identity}``), and runs until SIGTERM/SIGINT — then
withdraws the token, flushes, releases the session, and closes the store.
"""

from __future__ import annotations

import contextlib
import os
import signal
import socket
import threading
from typing import TYPE_CHECKING, Any

import zeared as z

from ..config import check_identity
from ..log import configure, get_logger
from ..store import Store
from ..streams import register_stream, resolve_message_class
from .chronicler import Chronicler
from .presence import ChroniclerPresence
from .session import open_session

if TYPE_CHECKING:
    from ..config import StoredConfig

_log = get_logger('zenoh.daemon')


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
        '_link_handles',
        '_owns_session',
        '_owns_store',
        '_presence',
        '_reaper_stop',
        '_reaper_thread',
        '_session',
        '_status',
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
        self._link_handles: list[Any] = []
        self._status = ''
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
        check_identity(cfg.identity)

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
            cls = resolve_message_class(spec)
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
        self._link_handles = self._watch_link(session)

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
        self._status = f'recording {len(cfg.streams)} stream(s) as {self._presence.key}'
        _sd_notify(f'STATUS={self._status}')
        _log.info('stored chronicler ready: %s', self._status)

    def _watch_link(self, session: Any) -> list[Any]:
        """Report the mesh link's state to the log and systemd, for a session that can lose it (0.4.1).

        Only a ``zeared.ManagedSession`` has the signals (``auto_reconnect`` in
        ``[zenoh]``). It reconnects, re-fetches retained values and keeps the
        presence token by itself, so a lost link changes the status and
        nothing else: the watchdog keeps pinging, since a restart would undo
        that work, and a hub restart would restart every daemon at once.
        """
        if not isinstance(session, z.ManagedSession):
            return []
        return [session.on_link_lost(self._link_lost), session.on_link_restored(self._link_restored)]

    def _link_lost(self, _session: Any) -> None:
        _log.warning('mesh link lost: nothing is recorded until the session reconnects')
        _sd_notify('STATUS=mesh link lost; reconnecting')

    def _link_restored(self, _session: Any) -> None:
        _log.info('mesh link restored: %s', self._status)
        _sd_notify(f'STATUS={self._status}')

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
        """Tear down in order: link watch → watchdog → presence → reaper → chronicler → session → store."""
        _sd_notify('STOPPING=1')
        for handle in self._link_handles:
            handle.cancel()
        self._link_handles = []
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
