"""Core stream-resolution tests — deliberately transport-free (no ``zeared`` needed).

These exercise the seared-level resolver that lets ``migrate``/``prune`` run on the
SQLite core with no ``zenoh`` extra installed. They use a plain ``seared`` class
(``StoredConfig``) as the fixture — which the daemon's stricter *Message*-level
resolver would reject — precisely to prove the storage core does not require a mesh
``Message`` (see ``tests/zenoh/test_daemon.py`` for the Message-level checks).
"""
import pytest

from stored import Store, StoredConfig
from stored.config import StreamSpec
from stored.errors import ConfigError
from stored.streams import register_streams, resolve_stream_class


def test_resolve_ok_for_plain_seared_class():
    resolved = resolve_stream_class(StreamSpec.load({'cls': 'stored.config:StoredConfig'}))
    assert resolved is StoredConfig


def test_resolve_rejects_bad_format():
    with pytest.raises(ConfigError):
        resolve_stream_class(StreamSpec.load({'cls': 'no-colon-here'}))


def test_resolve_rejects_non_seared():
    with pytest.raises(ConfigError):
        resolve_stream_class(StreamSpec.load({'cls': 'builtins:int'}))


def test_resolve_rejects_missing_module():
    with pytest.raises(ConfigError):
        resolve_stream_class(StreamSpec.load({'cls': 'no.such.module:Thing'}))


def test_register_streams_creates_table_and_retention(tmp_path):
    config = StoredConfig.load({
        'identity': 'x',
        'streams': [{'cls': 'stored.config:StoredConfig', 'retention': '7d'}],
    })
    store = Store(str(tmp_path / 'c.db'))
    try:
        register_streams(store, config)
        assert store.registry.get(StoredConfig).retention == '7d'
    finally:
        store.close()
