from __future__ import annotations

import time

import zeared as z

from stored.zenoh import ChroniclerPresence


class Events:
    def __init__(self):
        self.online: list[tuple[str, z.Origin]] = []
        self.offline: list[str] = []

    def on_online(self, meta):
        self.online.append((meta.captures['identity'], meta.origin))

    def on_offline(self, meta):
        self.offline.append(meta.captures['identity'])


def wait(seconds: float = 0.3):
    time.sleep(seconds)


def test_the_key_is_the_identity_under_stored_presence():
    assert ChroniclerPresence.LIVELINESS
    assert ChroniclerPresence.concrete_topic(identity='chronicler-1') == 'stored/presence/chronicler-1'


def test_a_watcher_sees_a_declared_token_come_and_go(session):
    events = Events()
    sub = ChroniclerPresence.on_message(
        lambda _msg: None,
        session=session,
        on_online=events.on_online,
        on_offline=events.on_offline,
    )
    try:
        wait()
        token = ChroniclerPresence.declare_presence(session=session, identity='c1')
        wait()
        assert events.online == [('c1', z.Origin.LIVE)]

        token.undeclare()
        wait()
        assert events.offline == ['c1']
    finally:
        sub.close()
