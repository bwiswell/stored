import pytest
import seared as s

from stored.config import StoredConfig, StreamSpec, check_identity
from stored.errors import ConfigError


def test_defaults_via_load():
    cfg = StoredConfig.load({'identity': 'chronicler-1'})
    assert cfg.identity == 'chronicler-1'
    assert cfg.backend == 'sqlite'
    assert cfg.db_path == 'chronicle.db'
    assert cfg.flush_rows == 1000
    assert cfg.flush_secs == 1.0
    assert cfg.streams == []


def test_identity_required():
    with pytest.raises(s.ValidationError):
        StoredConfig.load({})


@pytest.mark.parametrize('identity', ['chronicler-1', 'site.a_b', 'résumé'])
def test_check_identity_passes_one_key_segment(identity):
    check_identity(identity)


@pytest.mark.parametrize(
    ('identity', 'reason'),
    [
        ('', 'identity is empty'),
        ('site/chronicler', "holds '/'"),
        ('*', r"holds '\*'"),
        ('r-$*', r"holds '\$\*'"),
        ('what?', r"holds '\?'"),
        ('no#1', "holds '#'"),
    ],
)
def test_check_identity_refuses_what_cannot_be_one_key_segment(identity, reason):
    # Pure string, so validate-config can run it with no transport imported.
    with pytest.raises(ConfigError, match=reason):
        check_identity(identity)


def test_from_env(monkeypatch):
    monkeypatch.setenv('STORED_IDENTITY', 'c1')
    monkeypatch.setenv('STORED_BACKEND', 'sqlite')
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
    assert spec.time_field is None
    assert spec.latest is None


def test_stream_spec_latest_projection_parses():
    spec = StreamSpec.load(
        {
            'cls': 'pkg.mod:Location',
            'time_field': 'observed_at',
            'latest': {'key': ['source', 'item'], 'retention': '30d'},
        }
    )
    assert spec.time_field == 'observed_at'
    assert spec.latest is not None
    assert spec.latest.key == ['source', 'item']
    assert spec.latest.retention == '30d'
