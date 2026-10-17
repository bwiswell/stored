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


# -- retained replays -------------------------------------------------------
# Each new chronicler fetches the retained values already on the mesh. The reply
# carries the value's original timestamp (zeared 0.4.0), so its row is the
# original's and the insert is a no-op. Under 0.3.x it carried none, a fresh
# ``_ts_hlc`` was synthesized, and every start recorded each value again.


def restart(store, session, times=3):
    for _ in range(times):
        chronicler = Chronicler(store, session)
        chronicler.add(Config)
        wait(0.5)
        chronicler.close()


def test_restarts_record_a_retained_value_once(session):
    z.session = session
    Config(id=1, v=5).send()
    wait()
    store = Store(':memory:', flush_secs=0)
    try:
        restart(store, session)
        assert [(r.id, r.v) for r in store.query(Config)] == [(1, 5)]
    finally:
        store.close()


def test_restarts_after_a_live_record_add_no_row(session):
    z.session = session
    store = Store(':memory:', flush_secs=0)
    try:
        chronicler = Chronicler(store, session)
        chronicler.add(Config)
        wait()
        Config(id=2, v=9).send()
        wait()
        chronicler.close()
        restart(store, session)
        assert [(r.id, r.v) for r in store.query(Config)] == [(2, 9)]
    finally:
        store.close()
