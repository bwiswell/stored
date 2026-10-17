from __future__ import annotations

import os
import socket
import threading
import time

import pytest
import zeared as z
from _support_messages import Beacon, Sighting

from stored import Store, StoredConfig
from stored.config import StreamSpec
from stored.errors import ConfigError
from stored.zenoh import ChroniclerPresence
from stored.zenoh.daemon import Daemon, _resolve, _sd_notify, _watchdog_interval


def wait(seconds: float = 0.2):
    time.sleep(seconds)


# -- stream resolution ----------------------------------------------------


def test_resolve_ok():
    assert _resolve(StreamSpec.load({'cls': '_support_messages:Beacon'})) is Beacon


def test_resolve_rejects_bad_format():
    with pytest.raises(ConfigError):
        _resolve(StreamSpec.load({'cls': 'no-colon-here'}))


def test_resolve_rejects_non_message():
    with pytest.raises(ConfigError):
        _resolve(StreamSpec.load({'cls': 'stored.config:StoredConfig'}))


def test_resolve_rejects_missing_module():
    with pytest.raises(ConfigError):
        _resolve(StreamSpec.load({'cls': 'no.such.module:Thing'}))


# -- sd_notify ------------------------------------------------------------


def test_sd_notify_noop_without_socket(monkeypatch):
    monkeypatch.delenv('NOTIFY_SOCKET', raising=False)
    _sd_notify('READY=1')  # must not raise


# -- daemon lifecycle (injected session + store) --------------------------


def _config():
    return StoredConfig.load(
        {
            'identity': 'd1',
            'zenoh': {'mode': 'peer'},
            'streams': [{'cls': '_support_messages:Beacon', 'retention': '7d'}],
            'prune_interval': 0,
        }
    )


def test_daemon_records_and_serves(session):
    z.session = session
    store = Store(':memory:', flush_secs=0)
    daemon = Daemon(_config(), session=session, store=store)
    try:
        daemon.start()
        assert store.registry.get(Beacon).retention == '7d'

        Beacon(id=1, v=5).send()
        Beacon(id=1, v=6).send()
        wait()

        served = Beacon.query(id=1, params={'limit': '10'}, timeout=2.0)
        assert {b.v for b in served} == {5, 6}
    finally:
        daemon.stop()
        daemon.shutdown()
        store.close()


def test_daemon_applies_the_whole_stream_spec(session):
    # Until 0.3.5 `run` registered a stream with its retention and indexes only,
    # so a configured `time_field` and `[streams.latest]` did nothing there.
    z.session = session
    config = StoredConfig.load(
        {
            'identity': 'd1',
            'zenoh': {'mode': 'peer'},
            'streams': [
                {
                    'cls': '_support_messages:Sighting',
                    'retention': '7d',
                    'time_field': 'at',
                    'latest': {'key': ['id'], 'retention': '30d'},
                }
            ],
            'prune_interval': 0,
        }
    )
    store = Store(':memory:', flush_secs=0)
    daemon = Daemon(config, session=session, store=store)
    try:
        daemon.start()
        stream = store.registry.get(Sighting)
        assert (stream.time_field, stream.latest_key, stream.latest_retention) == ('at', ('id',), '30d')

        Sighting(id=1, at=1000.0).send()
        Sighting(id=1, at=2000.0).send()
        wait()

        # The projection keeps the newest by event time.
        assert store.latest(Sighting, id=1).at == 2000.0
        # Retention keys on the event time: both rows are decades past '7d',
        # though they were delivered a moment ago. The projection, on '30d', too.
        assert store.counts(Sighting) == (2, 1)
        store.prune()
        assert store.counts(Sighting) == (0, 0)
    finally:
        daemon.stop()
        daemon.shutdown()
        store.close()


def test_daemon_holds_its_presence_while_it_runs(session):
    # Health is systemd supervision plus this token: up once every stream is
    # subscribed, and down as the daemon shuts down, before its session goes.
    z.session = session
    online: list[str] = []
    offline: list[str] = []
    sub = ChroniclerPresence.on_message(
        lambda _msg: None,
        session=session,
        on_online=lambda meta: online.append(meta.captures['identity']),
        on_offline=lambda meta: offline.append(meta.captures['identity']),
    )
    store = Store(':memory:', flush_secs=0)
    daemon = Daemon(_config(), session=session, store=store)
    try:
        wait()
        daemon.start()
        wait()
        assert (online, offline) == (['d1'], [])

        daemon.stop()
        daemon.shutdown()
        wait()
        assert offline == ['d1']
    finally:
        sub.close()
        store.close()


@pytest.mark.parametrize('identity', ['', 'site/chronicler', '*', 'r-$*', 'what?', 'no#1'])
def test_an_identity_that_cannot_name_its_key_is_refused_before_anything_opens(identity, tmp_path):
    db_path = tmp_path / 'chronicle.db'
    config = StoredConfig.load({'identity': identity, 'db_path': str(db_path), 'prune_interval': 0})
    with pytest.raises(ConfigError, match='identity'):
        Daemon(config).start()
    assert not db_path.exists()


def test_run_composes_start_wait_shutdown(monkeypatch):
    import stored.zenoh.daemon as daemon_mod

    calls = []

    class FakeDaemon:
        def __init__(self, config):
            calls.append('init')

        def start(self):
            calls.append('start')

        def wait(self):
            calls.append('wait')

        def shutdown(self):
            calls.append('shutdown')

    monkeypatch.setattr(daemon_mod, 'Daemon', FakeDaemon)
    monkeypatch.setattr(daemon_mod, '_install_signals', lambda daemon: None)

    assert daemon_mod.run(_config()) == 0
    assert calls == ['init', 'start', 'wait', 'shutdown']


def test_daemon_shutdown_leaves_injected_store_open(session):
    # Injected store/session are the caller's to close; shutdown must not.
    store = Store(':memory:', flush_secs=0)
    daemon = Daemon(_config(), session=session, store=store)
    try:
        daemon.start()
        daemon.shutdown()
        # Still usable — shutdown did not close the injected store.
        assert store.query(Beacon) == []
    finally:
        store.close()


# -- the systemd watchdog (0.4.1) -------------------------------------------


@pytest.fixture
def notify(tmp_path, monkeypatch):
    """A datagram socket standing in for systemd's, read with ``notify.recv()``."""
    path = tmp_path / 'notify.sock'
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    sock.bind(str(path))
    sock.settimeout(2.0)
    monkeypatch.setenv('NOTIFY_SOCKET', str(path))

    class Receiver:
        def recv(self):
            return sock.recv(256).decode()

        def drain(self, seconds):
            """Every message that arrives within ``seconds``."""
            got = []
            deadline = time.monotonic() + seconds
            while (left := deadline - time.monotonic()) > 0:
                sock.settimeout(left)
                try:
                    got.append(sock.recv(256).decode())
                except TimeoutError:
                    break
            return got

    try:
        yield Receiver()
    finally:
        sock.close()


class TestWatchdogInterval:
    def test_none_unless_systemd_asks(self, monkeypatch):
        monkeypatch.delenv('WATCHDOG_USEC', raising=False)
        assert _watchdog_interval() is None

    def test_half_the_period(self, monkeypatch):
        monkeypatch.setenv('WATCHDOG_USEC', '30000000')
        monkeypatch.delenv('WATCHDOG_PID', raising=False)
        assert _watchdog_interval() == 15.0

    def test_only_for_the_process_systemd_names(self, monkeypatch):
        monkeypatch.setenv('WATCHDOG_USEC', '30000000')
        monkeypatch.setenv('WATCHDOG_PID', str(os.getpid()))
        assert _watchdog_interval() == 15.0
        monkeypatch.setenv('WATCHDOG_PID', str(os.getpid() + 1))
        assert _watchdog_interval() is None

    def test_a_period_that_isnt_a_number_is_ignored(self, monkeypatch):
        monkeypatch.setenv('WATCHDOG_USEC', 'soon')
        monkeypatch.delenv('WATCHDOG_PID', raising=False)
        assert _watchdog_interval() is None


class TestWatchdogTick:
    def test_a_healthy_store_is_pinged(self, notify):
        store = Store(':memory:', flush_secs=0)
        try:
            assert Daemon(_config(), store=store)._watchdog_tick(store, 1.0)
            assert notify.recv() == 'WATCHDOG=1'
        finally:
            store.close()

    def test_a_wedged_store_is_not(self, notify):
        store = Store(':memory:', flush_secs=0)
        held, release = threading.Event(), threading.Event()

        def hold():
            with store._lock:
                held.set()
                release.wait(5)

        holder = threading.Thread(target=hold)
        holder.start()
        try:
            held.wait(5)
            assert not Daemon(_config(), store=store)._watchdog_tick(store, 0.05)
            assert notify.drain(0.2) == []
        finally:
            release.set()
            holder.join()
            store.close()


def test_the_daemon_pings_while_it_runs_and_stops_on_shutdown(session, notify, monkeypatch):
    monkeypatch.setenv('WATCHDOG_USEC', '200000')  # a 0.2 s period: a ping every 0.1 s
    monkeypatch.delenv('WATCHDOG_PID', raising=False)
    z.session = session
    store = Store(':memory:', flush_secs=0)
    daemon = Daemon(_config(), session=session, store=store)
    try:
        daemon.start()
        assert notify.recv() == 'READY=1'
        time.sleep(0.35)
        daemon.stop()
        daemon.shutdown()
        assert daemon._watchdog_thread is None
        sent = notify.drain(0.3)
        stopping = sent.index('STOPPING=1')
        assert 'WATCHDOG=1' in sent[:stopping]
        assert 'WATCHDOG=1' not in sent[stopping:]
    finally:
        store.close()
