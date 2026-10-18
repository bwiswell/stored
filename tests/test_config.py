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


class TestUnknownKeys:
    """0.4.2: a key no config class declares is refused, anywhere in the file."""

    def test_every_typo_is_named_in_one_error(self):
        data = {
            'identity': 'c1',
            'retension': '7d',
            'streams': [{'cls': 'm:C', 'retension': '1d', 'latest': {'key': ['id'], 'kye': ['x']}}],
        }
        named = r'^StoredConfig\.retension: unknown key \(2 more: streams\[0\]\.retension, streams\[0\]\.latest\.kye\)$'
        with pytest.raises(s.ValidationError, match=named):
            StoredConfig.load(data)

    def test_a_section_refuses_them_loaded_alone(self):
        with pytest.raises(s.ValidationError, match=r'StreamSpec\.clss: unknown key'):
            StreamSpec.load({'clss': 'm:C'})

    def test_the_zenoh_tables_keys_are_left_to_zeared(self):
        # A Dict: its keys are data to seared. zeared's SessionConfig checks them
        # where the table is loaded (`run`, `validate-config`).
        cfg = StoredConfig.load({'identity': 'c1', 'zenoh': {'mode': 'peer', 'auto_reconect': True}})
        assert cfg.zenoh == {'mode': 'peer', 'auto_reconect': True}

    def test_from_env_reads_only_the_variables_it_knows(self, monkeypatch):
        monkeypatch.setenv('STORED_IDENTITY', 'c1')
        monkeypatch.setenv('STORED_RETENSION', '7d')  # not one of them, so never read
        assert StoredConfig.from_env().identity == 'c1'


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
