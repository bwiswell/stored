import tomllib
from pathlib import Path

import stored

_ROOT = Path(__file__).parent.parent


def test_the_version_is_stamped_the_same_in_both_places():
    project = tomllib.loads((_ROOT / 'pyproject.toml').read_text())
    # The release workflow tags from one and refuses to run if they differ;
    # consumers read the other. Failing here catches it before the merge.
    assert project['project']['version'] == stored.__version__


def test_public_surface():
    assert hasattr(stored, 'Store')
    assert hasattr(stored, 'Chronicler')
    assert hasattr(stored, 'StoredConfig')
    assert hasattr(stored, 'StreamSpec')


def test_error_hierarchy():
    assert issubclass(stored.ConfigError, stored.StoredError)
    assert issubclass(stored.BackendError, stored.StoredError)
    assert issubclass(stored.QueryError, stored.StoredError)
