"""Tests for ``stored/errors.py``: one base class, and every error exported under it."""

from __future__ import annotations

import pytest

import stored
from stored import errors


@pytest.mark.parametrize('name', errors.__all__)
def test_every_error_is_a_stored_error_and_exported_at_the_top(name):
    cls = getattr(errors, name)
    assert issubclass(cls, errors.StoredError)
    assert getattr(stored, name) is cls


def test_the_base_is_an_exception():
    assert issubclass(errors.StoredError, Exception)
