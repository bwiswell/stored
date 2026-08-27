import pytest
import seared as s

from stored.config import StoredConfig, StreamSpec


def test_defaults_via_load():
    cfg = StoredConfig.load({'identity': 'chronicler-1'})
    assert cfg.identity == 'chronicler-1'
    assert cfg.backend == 'duckdb'
    assert cfg.db_path == 'chronicle.duckdb'
    assert cfg.flush_rows == 1000
    assert cfg.flush_secs == 1.0
    assert cfg.streams == []


def test_identity_required():
    with pytest.raises(s.ValidationError):
        StoredConfig.load({})


def test_from_env(monkeypatch):
    monkeypatch.setenv('STORED_IDENTITY', 'c1')
    monkeypatch.setenv('STORED_BACKEND', 'duckdb')
    monkeypatch.setenv('STORED_FLUSH_ROWS', '500')
    monkeypatch.setenv('STORED_LOG_LEVEL', 'DEBUG')
    cfg = StoredConfig.from_env()
    assert cfg.identity == 'c1'
    assert cfg.flush_rows == 500
    assert cfg.log_level == 'DEBUG'


def test_stream_spec_defaults():
    spec = StreamSpec.load({'cls': 'pkg.mod:Telemetry'})
    assert spec.cls == 'pkg.mod:Telemetry'
    assert spec.retention is None
    assert spec.index == []
