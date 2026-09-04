import stored


def test_version():
    assert stored.__version__ == '0.2.17'


def test_public_surface():
    assert hasattr(stored, 'Store')
    assert hasattr(stored, 'Chronicler')
    assert hasattr(stored, 'StoredConfig')
    assert hasattr(stored, 'StreamSpec')


def test_error_hierarchy():
    assert issubclass(stored.ConfigError, stored.StoredError)
    assert issubclass(stored.BackendError, stored.StoredError)
    assert issubclass(stored.QueryError, stored.StoredError)
