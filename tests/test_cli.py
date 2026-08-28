import sqlite3

from stored.cli import main


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
        'identity = "x"\n'
        f'db_path = "{db}"\n'
        '[[streams]]\n'
        'cls = "stored.config:StoredConfig"\n',
    )
    assert main(['-c', str(toml), 'migrate']) == 0
    assert 'stream_stored_config' in capsys.readouterr().out

    conn = sqlite3.connect(str(db))
    names = [r[0] for r in conn.execute("select name from sqlite_master where type='table'").fetchall()]
    conn.close()
    assert 'stream_stored_config' in names
