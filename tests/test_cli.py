import shlex
import sqlite3
from importlib.metadata import entry_points
from pathlib import Path

from stored.cli import build_parser, main

_UNIT = Path(__file__).parent.parent / 'systemd' / 'stored.service'


def test_validate_config(tmp_path, capsys):
    toml = tmp_path / 'c.toml'
    toml.write_text('identity = "x"\n')
    assert main(['-c', str(toml), 'validate-config']) == 0
    assert 'identity=' in capsys.readouterr().out


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
