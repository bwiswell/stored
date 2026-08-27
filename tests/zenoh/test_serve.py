import zeared as z

from stored import Store
from stored.zenoh.serve import make_query_handler


@z.zeared
class Telemetry(z.Message):
    TOPIC = 'robot/{id}/telemetry'
    id: int = z.Int(required=True)
    x: float = z.Float(required=True)


class FakeCtx:
    def __init__(self, key_expr, params):
        self.key_expr = key_expr
        self.params = params
        self.replies = []
        self.errors = []

    def reply(self, instance):
        self.replies.append(instance)

    def reply_err(self, message):
        self.errors.append(message)


def test_handler_streams_matching_history():
    store = Store(':memory:', flush_secs=0)
    try:
        stream = store.register(Telemetry)
        store.record(Telemetry, Telemetry(id=7, x=1.0), key='robot/7/telemetry')
        store.record(Telemetry, Telemetry(id=7, x=2.0), key='robot/7/telemetry')
        store.record(Telemetry, Telemetry(id=8, x=9.0), key='robot/8/telemetry')
        store.flush()

        handler = make_query_handler(store, stream)
        ctx = FakeCtx('robot/7/telemetry', {})
        handler(ctx)

        assert {r.x for r in ctx.replies} == {1.0, 2.0}
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
        handler(ctx)

        assert len(ctx.replies) == 2
    finally:
        store.close()


def test_handler_bad_limit_replies_err():
    store = Store(':memory:', flush_secs=0)
    try:
        stream = store.register(Telemetry)
        handler = make_query_handler(store, stream)
        ctx = FakeCtx('robot/7/telemetry', {'limit': 'abc'})
        handler(ctx)

        assert ctx.errors
        assert not ctx.replies
    finally:
        store.close()
