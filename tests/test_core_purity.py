import subprocess
import sys


def test_core_import_does_not_pull_zeared():
    # The seared-only core must never import the transport. Checked in a fresh
    # subprocess so an unrelated import in this process can't mask a regression.
    code = (
        'import stored, sys; '
        "assert 'zeared' not in sys.modules, 'core imported zeared'; "
        "assert 'zenoh' not in sys.modules, 'core imported zenoh'"
    )
    subprocess.run([sys.executable, '-c', code], check=True)


def test_zenoh_layer_imports_with_extra():
    # The dev env installs the zenoh extra, so the guarded subpackage imports.
    code = 'import stored.zenoh; assert hasattr(stored.zenoh, "Chronicler")'
    subprocess.run([sys.executable, '-c', code], check=True)
