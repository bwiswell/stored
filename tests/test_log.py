"""Tests for ``stored/log.py``: one stdout handler at the configured level, and loggers under ``stored``."""

from __future__ import annotations

import logging

import pytest

from stored.log import configure, get_logger


@pytest.fixture
def root():
    """The root logger, put back as it was afterwards (``configure`` replaces its handlers)."""
    logger = logging.getLogger()
    handlers, level = logger.handlers[:], logger.level
    try:
        yield logger
    finally:
        logger.handlers[:] = handlers
        logger.setLevel(level)


def test_get_logger_names_a_logger_under_stored():
    assert get_logger('writer').name == 'stored.writer'


def test_configure_logs_to_stdout_at_the_level(root, capsys):
    configure('debug')
    get_logger('test').debug('hello')

    assert root.level == logging.DEBUG
    assert 'DEBUG    stored.test: hello' in capsys.readouterr().out


def test_configure_again_replaces_its_handler_rather_than_adding_one(root):
    configure('INFO')
    configure('WARNING')

    assert len(root.handlers) == 1
    assert root.level == logging.WARNING
