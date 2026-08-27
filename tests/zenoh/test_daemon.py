from __future__ import annotations

import time

import pytest
import zeared as z
from _support_messages import Beacon

from stored import Store, StoredConfig
from stored.config import StreamSpec
from stored.errors import ConfigError
from stored.zenoh.daemon import Daemon, _resolve, _sd_notify


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
    return StoredConfig.load({
        'identity': 'd1',
        'zenoh': {'mode': 'peer'},
        'streams': [{'cls': '_support_messages:Beacon', 'retention': '7d'}],
        'prune_interval': 0,
    })


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
