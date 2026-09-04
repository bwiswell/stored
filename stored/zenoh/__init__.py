"""The Zenoh chronicler layer.

``zeared`` is a core dependency of ``stored`` (plan 02), so this subpackage
always imports. It wires the seared-only core's :class:`~stored.store.Store` to a
``zeared`` subscriber (record) and queryable (serve history). The core itself
imports no ``zeared`` at runtime — a Store-only ``import stored`` stays
transport-free (the top-level ``Chronicler`` re-export is lazy).
"""

from __future__ import annotations

from .chronicler import Chronicler

__all__ = ['Chronicler']
