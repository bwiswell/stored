"""The optional Zenoh chronicler layer.

Importing this subpackage requires the ``zenoh`` extra (``stored[zenoh]``),
which brings in ``zeared``. The core (``stored``) never imports it, so
persistence works with no transport installed.
"""
from __future__ import annotations

try:
    import zeared as _zeared  # noqa: F401  (probe only)
except ImportError as exc:  # pragma: no cover - exercised only without the extra
    raise ImportError(
        "stored.zenoh requires the 'zenoh' extra — install stored[zenoh] "
        '(which provides zeared).',
    ) from exc

from .chronicler import Chronicler

__all__ = ['Chronicler']
