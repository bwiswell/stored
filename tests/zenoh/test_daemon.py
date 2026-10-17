from __future__ import annotations

import time

import pytest
import zeared as z
from _support_messages import Beacon, Sighting

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
