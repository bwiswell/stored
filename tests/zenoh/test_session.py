"""Tests for ``stored/zenoh/session.py``: the chronicler's session, timestamped, from a config mapping."""

from __future__ import annotations

import time

import zeared as z

from stored.zenoh.session import open_session

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
