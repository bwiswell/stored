"""Tests for ``stored/zenoh/session.py``: the chronicler's session, timestamped, from a config mapping."""

from __future__ import annotations

import os
import time

import pytest
import zeared as z

from stored.errors import ConfigError
from stored.zenoh.session import open_session, session_config

#: A peer that finds no other: the suite's sessions share one process.
ISOLATED = {'mode': 'peer', 'zenoh_overrides': {'scouting/multicast/enabled': False}}


@z.zeared
class Ping(z.Message):
    TOPIC = 'test/session/ping'
    n: int = z.Int(default=0)


def test_what_the_session_delivers_carries_a_timestamp():
    # The row builder dedups and orders on the HLC timestamp, so the session must stamp.
    session = open_session(ISOLATED)
    stamps: list[str | None] = []
    try:
        sub = Ping.on_message(lambda _msg, meta: stamps.append(meta.timestamp), session=session)
        time.sleep(0.2)
        Ping(n=1).send(session=session)
        time.sleep(0.3)
        sub.close()

        assert len(stamps) == 1
        assert stamps[0] is not None
    finally:
        z.release(session=session)


def test_auto_reconnect_opens_a_managed_session():
    session = open_session({**ISOLATED, 'auto_reconnect': True})
    try:
        assert isinstance(session, z.ManagedSession)
    finally:
        z.release(session=session)


# -- where the connection comes from (0.4.1) ---------------------------------


@pytest.fixture
def no_session_env(monkeypatch):
    """No ``ZEARED_SESSION_*`` variable set, whatever the environment running the tests holds."""
    for key in [k for k in os.environ if k.startswith('ZEARED_SESSION_')]:
        monkeypatch.delenv(key)
    return monkeypatch


class TestSessionConfig:
    def test_a_table_is_used(self, no_session_env):
        assert session_config({'mode': 'peer'}).mode.value == 'peer'

    def test_with_no_table_the_environment_is_read(self, no_session_env):
        no_session_env.setenv('ZEARED_SESSION_MODE', 'client')
        no_session_env.setenv('ZEARED_SESSION_ROUTER', 'tcp/hub:7447')
        config = session_config({})
        assert (config.mode.value, config.router) == ('client', 'tcp/hub:7447')

    def test_a_table_wins_whole_over_the_environment(self, no_session_env):
        no_session_env.setenv('ZEARED_SESSION_MODE', 'client')
        no_session_env.setenv('ZEARED_SESSION_ROUTER', 'tcp/hub:7447')
        config = session_config({'mode': 'peer'})
        assert (config.mode.value, config.router) == ('peer', None)

    def test_with_neither_the_error_says_where_to_set_a_mode(self, no_session_env):
        with pytest.raises(ConfigError, match=r'set ZEARED_SESSION_MODE$'):
            session_config({})

    def test_a_table_without_a_mode_says_so(self, no_session_env):
        with pytest.raises(ConfigError, match=r'^\[zenoh\] sets no mode'):
            session_config({'router': 'tcp/hub:7447'})

    def test_a_wrong_value_keeps_zeareds_error(self, no_session_env):
        with pytest.raises(z.ValidationError, match=r'SessionConfig\.mode'):
            session_config({'mode': 'bogus'})

    def test_open_session_opens_what_the_environment_names(self, no_session_env):
        no_session_env.setenv('ZEARED_SESSION_MODE', 'client')
        no_session_env.setattr(z, 'open', lambda config: config)  # no session: what it would open
        assert open_session({}).mode.value == 'client'
