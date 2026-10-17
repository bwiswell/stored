"""The core read primitives serve request/reply contracts, with no mesh at all.

A service (async, in real life) answers a request by mapping it to a `Store` call and
the result to a reply, using only `Store.latest` (single entity) and `Store.query`
(time range): no `zeared`, just the primitives and a small mapping function the
service owns. `stored.mesh.Binding` declares the same shape over the mesh.
"""

import datetime as dt

import seared as s

from stored import Store


@s.seared
class Ping(s.Seared):
    """A live reading (also the time-range reply body)."""

    source: str = s.Str(required=True)
    item: str = s.Str(required=True)
    x: float = s.Float(default=0.0)
    y: float = s.Float(default=0.0)
    observed_at: float = s.Float(required=True)  # unix seconds (domain event time)


@s.seared
class LastKnown(s.Seared):
    """A last-known reply the service builds from a stored row."""

    source: str = s.Str(required=True)
    item: str = s.Str(required=True)
    found: bool = s.Bool(default=False)
    x: float = s.Float(default=0.0)
    y: float = s.Float(default=0.0)
    observed_at: float = s.Float(default=0.0)


# --- consumer-owned mappings (this is the service's job, not stored's) ----------


def serve_last_known(store: Store, source: str, item: str) -> LastKnown:
    """Answer a {source, item} request from the latest-per-key projection."""
    row = store.latest(Ping, source=source, item=item)
    if row is None:
        return LastKnown(source=source, item=item, found=False)
    return LastKnown(source=source, item=item, found=True, x=row.x, y=row.y, observed_at=row.observed_at)


def serve_history(store: Store, item: str, from_ts: float, to_ts: float) -> list[Ping]:
    """Answer an {item, from_ts, to_ts} request as a time-ordered history slice."""
    return store.query(Ping, item=item, since=from_ts, until=to_ts, order='asc')


# --- the proof ------------------------------------------------------------------


def _store(tmp_path) -> Store:
    store = Store(str(tmp_path / 'c.db'))
    store.register(
        Ping,
        retention='7d',
        time_field='observed_at',
        latest_key=('source', 'item'),
        index=('item',),
    )
    return store


def test_last_known_served_from_latest_projection(tmp_path):
    store = _store(tmp_path)
    try:
        now = dt.datetime.now(dt.UTC).timestamp()
        store.record(Ping, Ping(source='site-a', item='A', x=1.0, y=1.0, observed_at=now - 100))
        store.record(Ping, Ping(source='site-a', item='A', x=2.0, y=2.0, observed_at=now))  # newest
        store.record(Ping, Ping(source='site-a', item='A', x=9.0, y=9.0, observed_at=now - 50))  # older

        reply = serve_last_known(store, 'site-a', 'A')
        assert reply.found
        assert (reply.x, reply.y) == (2.0, 2.0)  # the newest observation, whatever the order
        assert reply.observed_at == now

        miss = serve_last_known(store, 'site-a', 'GONE')
        assert not miss.found
    finally:
        store.close()


def test_time_range_served_from_history_with_unix_bounds(tmp_path):
    store = _store(tmp_path)
    try:
        now = dt.datetime.now(dt.UTC).timestamp()
        store.record(Ping, Ping(source='site-a', item='A', observed_at=now - 7200))  # 2h ago (out of range)
        store.record(Ping, Ping(source='site-a', item='A', observed_at=now - 1800))  # 30m ago
        store.record(Ping, Ping(source='site-a', item='A', observed_at=now - 60))  # 1m ago
        store.record(Ping, Ping(source='site-a', item='B', observed_at=now - 120))  # other tag

        # unix-seconds bounds map straight through; the item filter isolates the tag.
        window = serve_history(store, 'A', from_ts=now - 3600, to_ts=now)
        assert [p.observed_at for p in window] == [now - 1800, now - 60]
        assert all(p.item == 'A' for p in window)
    finally:
        store.close()
