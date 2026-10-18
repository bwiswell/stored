import shlex
import sqlite3
from importlib.metadata import entry_points
from pathlib import Path

import pytest
import seared as s

from stored.cli import build_parser, main
from stored.errors import ConfigError, RegistrationError

_UNIT = Path(__file__).parent.parent / 'systemd' / 'stored.service'


def test_validate_config(tmp_path, capsys):
    toml = tmp_path / 'c.toml'
    toml.write_text('identity = "x"\n')
    assert main(['-c', str(toml), 'validate-config']) == 0
    assert 'identity=' in capsys.readouterr().out


def _validate(tmp_path, toml):
    path = tmp_path / 'c.toml'
    path.write_text(toml)
    return main(['-c', str(path), 'validate-config'])


@pytest.mark.parametrize(
    ('toml', 'source'),
    [
        ('identity = "x"\n', 'env'),
        ('identity = "x"\n[zenoh]\nmode = "client"\nrouter = "tcp/hub:7447"\n', 'table'),
    ],
)
def test_validate_config_says_where_the_session_comes_from(tmp_path, capsys, toml, source):
    assert _validate(tmp_path, toml) == 0
    assert capsys.readouterr().out.rstrip().endswith(f' zenoh={source}')


def test_validate_config_refuses_the_identity_run_would(tmp_path):
    with pytest.raises(ConfigError, match='one key segment'):
        _validate(tmp_path, 'identity = "site/chronicler"\n')


@pytest.mark.parametrize(
    ('table', 'where'),
    [
        ('mode = "bogus"\n', r'SessionConfig\.mode'),
        ('mode = "peer"\nmax_attempts = "three"\n', r'SessionConfig\.max_attempts'),
    ],
)
def test_validate_config_checks_a_zenoh_table(tmp_path, table, where):
    # Until 0.4.1 the table went unread until `run`, so these passed.
    with pytest.raises(s.ValidationError, match=where):
        _validate(tmp_path, f'identity = "x"\n[zenoh]\n{table}')


def test_validate_config_refuses_a_zenoh_table_without_a_mode(tmp_path):
    with pytest.raises(ConfigError, match=r'^\[zenoh\] sets no mode'):
        _validate(tmp_path, 'identity = "x"\n[zenoh]\nrouter = "tcp/hub:7447"\n')


def test_validate_config_refuses_a_misspelt_key(tmp_path):
    # 0.4.2: the config classes refuse unknown keys, so `run` would refuse it too.
    with pytest.raises(s.ValidationError, match=r'StoredConfig\.prune_intervl: unknown key'):
        _validate(tmp_path, 'identity = "x"\nprune_intervl = 60\n')


def test_validate_config_refuses_a_misspelt_zenoh_key(tmp_path):
    # Through zeared 0.4.2's strict SessionConfig, which loads the table.
    with pytest.raises(s.ValidationError, match=r'SessionConfig\.auto_reconect: unknown key'):
        _validate(tmp_path, 'identity = "x"\n[zenoh]\nmode = "peer"\nauto_reconect = true\n')


def test_validate_config_leaves_the_environment_unchecked(tmp_path, monkeypatch, capsys):
    # With no table, `run` reads ZEARED_SESSION_*. validate-config imports no
    # transport then, so it doesn't check them; the summary says where they're from.
    monkeypatch.setenv('ZEARED_SESSION_MODE', 'bogus')
    assert _validate(tmp_path, 'identity = "x"\n') == 0
    assert 'zenoh=env' in capsys.readouterr().out


class TestValidateConfigStreams:
    """0.4.2: validate-config registers every stream as `run` would, into an in-memory store."""

    @staticmethod
    def _stream(body):
        return f'identity = "x"\ndb_path = "never.db"\n[[streams]]\n{body}'

    def test_a_good_stream_passes(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        assert _validate(tmp_path, self._stream('cls = "_support_messages:Sighting"\ntime_field = "at"\n')) == 0
        assert 'streams=1' in capsys.readouterr().out
        assert not (tmp_path / 'never.db').exists()  # in memory: nothing written

    @pytest.mark.parametrize(
        ('body', 'error', 'match'),
        [
            ('cls = "_support_messages:Beecon"\n', ConfigError, 'cannot import stream class'),
            ('cls = "stored.config:StoredConfig"\n', ConfigError, 'is not a zeared Message class'),
            ('cls = "_support_messages:Beacon"\nretention = "7dd"\n', ConfigError, 'invalid duration'),
            ('cls = "_support_messages:Beacon"\ntime_field = "seen"\n', RegistrationError, "time_field 'seen' is not"),
            ('cls = "_support_messages:Beacon"\n[streams.latest]\nkey = ["nope"]\n', RegistrationError, 'nope'),
        ],
        ids=['unimportable', 'not-a-message', 'bad-duration', 'no-such-time-field', 'latest-key'],
    )
    def test_what_run_would_refuse_it_refuses(self, tmp_path, body, error, match):
        # Until 0.4.2 each of these passed validate-config and failed `run`.
        with pytest.raises(error, match=match):
            _validate(tmp_path, self._stream(body))

    def test_a_duplicate_stream_is_refused(self, tmp_path):
        twice = 'cls = "_support_messages:Beacon"\n[[streams]]\ncls = "_support_messages:Beacon"\n'
        with pytest.raises(RegistrationError, match='already registered'):
            _validate(tmp_path, self._stream(twice))

    def test_an_unknown_backend_is_refused(self, tmp_path):
        with pytest.raises(ConfigError, match="unknown backend 'postgress'"):
            _validate(tmp_path, 'identity = "x"\nbackend = "postgress"\n')


def test_migrate_creates_tables(tmp_path, capsys):
    # A plain seared class as the stream, so ``migrate`` is exercised on the pure
    # SQLite core with no ``zenoh`` extra installed (the CLI storage verbs must not
    # require a mesh). Any @s.seared class works — the store records seared objects.
    db = tmp_path / 'c.db'
    toml = tmp_path / 'c.toml'
    toml.write_text(
        f'identity = "x"\ndb_path = "{db}"\n[[streams]]\ncls = "stored.config:StoredConfig"\n',
    )
    assert main(['-c', str(toml), 'migrate']) == 0
    assert 'stream_stored_config' in capsys.readouterr().out

    conn = sqlite3.connect(str(db))
    names = [r[0] for r in conn.execute("select name from sqlite_master where type='table'").fetchall()]
    conn.close()
    assert 'stream_stored_config' in names


def test_the_stored_command_is_installed():
    # The README and docs run `stored -c … <verb>`; until 0.3.5 no such command
    # existed, and only `python -m stored` worked.
    (script,) = entry_points(group='console_scripts', name='stored')
    assert script.value == 'stored.cli:main'


def test_the_systemd_unit_starts_the_daemon():
    # Read the unit's ExecStart and parse it with the CLI's own parser, so the two
    # can't drift apart: until 0.3.5 it passed `--config` after `run`, which the
    # parser rejects, so the unit couldn't start.
    (line,) = [ln for ln in _UNIT.read_text().splitlines() if ln.startswith('ExecStart=')]
    program, *argv = shlex.split(line.removeprefix('ExecStart='))
    assert program.endswith('/stored')
    args = build_parser().parse_args(argv)
    assert (args.command, args.config) == ('run', '/etc/stored/stored.toml')
