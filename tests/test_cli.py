import duckdb

from stored.cli import main


def test_validate_config(tmp_path, capsys):
    toml = tmp_path / 'c.toml'
    toml.write_text('identity = "x"\n')
    assert main(['-c', str(toml), 'validate-config']) == 0
    assert 'identity=' in capsys.readouterr().out


def test_migrate_creates_tables(tmp_path, capsys):
    db = tmp_path / 'c.duckdb'
    toml = tmp_path / 'c.toml'
    toml.write_text(
        'identity = "x"\n'
        f'db_path = "{db}"\n'
        '[[streams]]\n'
        'cls = "_support_messages:Beacon"\n',
    )
    assert main(['-c', str(toml), 'migrate']) == 0
    assert 'stream_beacon' in capsys.readouterr().out

    conn = duckdb.connect(str(db))
    names = [r[0] for r in conn.execute('select table_name from information_schema.tables').fetchall()]
    conn.close()
    assert 'stream_beacon' in names
