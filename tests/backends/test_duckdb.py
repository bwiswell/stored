from stored.backends.duckdb_ import DuckDBBackend


def test_open_memory_and_close():
    backend = DuckDBBackend(':memory:')
    assert backend.path == ':memory:'
    backend.close()


def test_open_file_and_close(tmp_path):
    path = str(tmp_path / 'c.duckdb')
    backend = DuckDBBackend(path)
    assert backend.path == path
    backend.close()
