from __future__ import annotations

import time

import zeared as z

from stored import Store
from stored.zenoh import Chronicler


@z.zeared
class Telemetry(z.Message):
    TOPIC = 'robot/{id}/telemetry'
    id: int = z.Int(required=True)
    x: float = z.Float(required=True)


@z.zeared
class Config(z.Message):
    TOPIC = 'cfg/{id}'
    RETAINED = True
    id: int = z.Int(required=True)
    v: int = z.Int(default=0)


def wait(seconds: float = 0.2):
    time.sleep(seconds)


def test_subscriber_records_sends(session):
    z.session = session
    store = Store(':memory:', flush_secs=0)
    chronicler = Chronicler(store, session)
    try:
        chronicler.add(Telemetry)
        wait()
        Telemetry(id=7, x=1.5).send()
        Telemetry(id=7, x=2.5).send()
        wait()

        rows = store.query(Telemetry)
        assert {r.x for r in rows} == {1.5, 2.5}
        assert all(r.id == 7 for r in rows)
    finally:
        chronicler.close()
        store.close()


def test_history_served_over_mesh(session):
    z.session = session
    store = Store(':memory:', flush_secs=0)
    chronicler = Chronicler(store, session)
    try:
        chronicler.add(Telemetry)
        wait()
        Telemetry(id=7, x=1.5).send()
        Telemetry(id=7, x=2.5).send()
        wait()

        replies = Telemetry.query(id=7, params={'limit': '10'}, timeout=2.0)
        assert {r.x for r in replies} == {1.5, 2.5}
    finally:
        chronicler.close()
        store.close()


def test_retained_class_skips_queryable(session):
    z.session = session
    store = Store(':memory:', flush_secs=0)
    chronicler = Chronicler(store, session)
    try:
        chronicler.add(Config)
        assert len(chronicler._subscribers) == 1
        assert len(chronicler._queryables) == 0
    finally:
        chronicler.close()
        store.close()
