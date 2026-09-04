import subprocess
import sys


def test_store_only_import_stays_transport_free():
    # zeared is a core dependency now, but the top-level Chronicler re-export is
    # lazy (PEP 562), so a Store-only ``import stored`` must not eagerly pull in
    # the transport. Checked in a fresh subprocess so an unrelated import in this
    # process can't mask a regression.
    code = (
        'import stored, sys; '
        "assert 'zeared' not in sys.modules, 'bare import pulled zeared'; "
        "assert 'zenoh' not in sys.modules, 'bare import pulled zenoh'"
    )
    subprocess.run([sys.executable, '-c', code], check=True)


def test_bare_import_does_not_pull_duckdb():
    # DuckDB stays an optional extra with a lazy backend import — merely importing
    # stored (or selecting the default SQLite backend) must not load it, even when
    # the dev env has the extra installed.
    code = "import stored, sys; assert 'duckdb' not in sys.modules, 'bare import pulled duckdb'"
    subprocess.run([sys.executable, '-c', code], check=True)


def test_chronicler_resolves_at_top_level():
    # The lazy re-export makes ``stored.Chronicler`` resolve on access, importing
    # the zenoh layer (and thus zeared) only then.
    code = (
        'import stored, sys; '
        "assert 'zeared' not in sys.modules; "
        'c = stored.Chronicler; '
        'from stored.zenoh import Chronicler; '
        'assert c is Chronicler; '
        "assert 'zeared' in sys.modules, 'accessing Chronicler should import zeared'"
    )
    subprocess.run([sys.executable, '-c', code], check=True)


def test_async_store_import_stays_transport_free():
    # ``stored.mesh`` will grow zeared-dependent bindings, but AsyncStore is asyncio
    # over the core and must stay importable without the transport — the invariant
    # that keeps ``stored.mesh``'s zeared names lazy as the layer fills in.
    code = (
        'from stored.mesh import AsyncStore; import sys; '
        "assert 'zeared' not in sys.modules, 'stored.mesh pulled zeared'; "
        "assert 'zenoh' not in sys.modules, 'stored.mesh pulled zenoh'"
    )
    subprocess.run([sys.executable, '-c', code], check=True)
