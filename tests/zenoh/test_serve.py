import time

import zeared as z

from stored import Store
from stored.zenoh.serve import make_query_handler


@z.zeared
class Telemetry(z.Message):
    TOPIC = 'robot/{id}/telemetry'
    id: int = z.Int(required=True)
    x: float = z.Float(required=True)


@z.zeared
class Tagged(z.Message):
    TOPIC = 'tag/{name}/read'
    name: str = z.Str(required=True)
    n: int = z.Int(required=True)


class FakeCtx:
    def __init__(self, key_expr, params):
        self.key_expr = key_expr
        self.params = params
        self.errors = []

    def reply_err(self, message):
        self.errors.append(message)


def test_handler_returns_matching_history():
    store = Store(':memory:', flush_secs=0)
    try:
        stream = store.register(Telemetry)
        store.record(Telemetry, Telemetry(id=7, x=1.0), key='robot/7/telemetry')
        store.record(Telemetry, Telemetry(id=7, x=2.0), key='robot/7/telemetry')
        store.record(Telemetry, Telemetry(id=8, x=9.0), key='robot/8/telemetry')
        store.flush()

        handler = make_query_handler(store, stream)
        ctx = FakeCtx('robot/7/telemetry', {})
        replies = handler(ctx)

        assert {r.x for r in replies} == {1.0, 2.0}
        assert not ctx.errors
    finally:
        store.close()


def test_handler_respects_limit_param():
    store = Store(':memory:', flush_secs=0)
    try:
        stream = store.register(Telemetry)
        for i in range(4):
            store.record(Telemetry, Telemetry(id=7, x=float(i)), key='robot/7/telemetry')
        store.flush()

        handler = make_query_handler(store, stream)
        ctx = FakeCtx('robot/7/telemetry', {'limit': '2'})

        assert len(handler(ctx)) == 2
    finally:
        store.close()


def test_handler_bad_limit_replies_err():
    store = Store(':memory:', flush_secs=0)
    try:
        stream = store.register(Telemetry)
        handler = make_query_handler(store, stream)
        ctx = FakeCtx('robot/7/telemetry', {'limit': 'abc'})

        assert handler(ctx) is None
        assert ctx.errors
    finally:
        store.close()


def test_a_row_with_no_key_is_skipped_and_the_rest_still_go_out(session):
    # A single-segment slot holding '/' has no key to reply on (zeared refuses it). Such a
    # row can only arrive through Store.record or a via= mapper, which keep the key given.
    store = Store(':memory:', flush_secs=0)
    stream = store.register(Tagged)
    for i, name in enumerate(['a', 'x/y', 'b']):
        store.record(Tagged, Tagged(name=name, n=i), key=f'tag/r{i}/read')
    store.flush()
    # Keep types, not exceptions: a kept reply failure's traceback holds the query open
    # until the getter times out.
    reply_failures: list[type] = []
    error_replies: list[Exception] = []
    queryable = Tagged.on_query(
        make_query_handler(store, stream),
        session=session,
        on_error=lambda exc, _raw: reply_failures.append(type(exc.__cause__)),
    )
    try:
        time.sleep(0.2)
        replies = Tagged.query(
            session=session,
            timeout=2.0,
            on_error=lambda exc, _raw: error_replies.append(exc),
        )

        assert sorted(r.n for r in replies) == [0, 2]
        assert not error_replies, 'a bad row must not turn the answer into an error'
        assert reply_failures == [z.TopicError]
    finally:
        queryable.close()
        store.close()
