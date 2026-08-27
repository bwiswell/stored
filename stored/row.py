"""Convert between message instances and table rows.

:func:`build_row` turns a decoded message plus its Zenoh metadata into a
column-keyed dict; :func:`rehydrate` reverses that into a typed instance for
query replies.

.. note::
   M0 scaffold: both are stubs landing in M1 (core store).
"""
from __future__ import annotations

from typing import Any

from .registry import Stream


def build_row(stream: Stream, msg: Any, meta: Any) -> dict[str, Any]:
    """Build a table row from ``msg`` and its ``meta``.

    Args:
        stream: The registered stream ``msg`` belongs to.
        msg: A decoded message instance.
        meta: The Zenoh metadata (``ZenohMeta``-shaped: ``timestamp``,
            ``issued_at``, ``key_expr``, ``source_info``, ``schema``), or
            ``None`` for a non-mesh record.

    Returns:
        A column-keyed row dict (meta columns + scalar fields + ``_payload``).
    """
    raise NotImplementedError('row.build_row lands in M1')


def rehydrate(stream: Stream, columns: dict[str, Any]) -> Any:
    """Reconstruct a typed message instance from a stored ``columns`` row.

    Args:
        stream: The registered stream to rebuild an instance for.
        columns: A column-keyed row dict as returned by the backend.

    Returns:
        An instance of ``stream.cls``.
    """
    raise NotImplementedError('row.rehydrate lands in M1')


__all__ = ['build_row', 'rehydrate']
