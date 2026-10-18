import importlib
import inspect
import pkgutil
import tomllib
import typing
from pathlib import Path

import pytest

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


#: Optional dependencies a module may import at load (the ``[duckdb]`` extra).
_OPTIONAL = frozenset({'duckdb'})


def _exported_classes() -> list[type]:
    """Every class a stored module exports in its ``__all__``, each once.

    A module whose optional dependency isn't installed is passed over.
    """
    found: dict[str, type] = {}
    for info in pkgutil.walk_packages(stored.__path__, 'stored.'):
        try:
            module = importlib.import_module(info.name)
        except ModuleNotFoundError as exc:
            if exc.name in _OPTIONAL:
                continue
            raise
        for name in getattr(module, '__all__', ()):
            obj = getattr(module, name)
            if inspect.isclass(obj) and obj.__module__.startswith('stored'):
                found[f'{obj.__module__}.{obj.__qualname__}'] = obj
    return [found[key] for key in sorted(found)]


@pytest.mark.parametrize('cls', _exported_classes(), ids=lambda c: f'{c.__module__}.{c.__qualname__}')
def test_every_exported_classs_annotations_resolve(cls):
    # 0.4.3: `LatestMeta.run_start` named `dt`, imported only for type checking,
    # so `typing.get_type_hints` raised `NameError` on it; on seared 0.4.1 and
    # 0.4.2 it did on every config class too (seared's base). An annotation
    # naming a type-checking-only import, here or in a sibling, fails this.
    typing.get_type_hints(cls)
