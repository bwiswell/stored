from stored.writer import Writer


class FakeBackend:
    def __init__(self):
        self.batches = []

    def append_batch(self, table, rows):
        self.batches.append((table, list(rows)))


def test_flush_on_row_threshold():
    backend = FakeBackend()
    writer = Writer(backend, flush_rows=2, flush_secs=0)
    writer.enqueue('t', {'a': 1})
    assert backend.batches == []
    writer.enqueue('t', {'a': 2})
    assert len(backend.batches) == 1
    assert backend.batches[0][0] == 't'
    assert len(backend.batches[0][1]) == 2
    writer.close()


def test_flush_on_close():
    backend = FakeBackend()
    writer = Writer(backend, flush_rows=100, flush_secs=0)
    writer.enqueue('t', {'a': 1})
    assert backend.batches == []
    writer.close()
    assert len(backend.batches) == 1


def test_manual_flush_is_idempotent_when_empty():
    backend = FakeBackend()
    writer = Writer(backend, flush_rows=100, flush_secs=0)
    writer.enqueue('t', {'a': 1})
    writer.flush()
    writer.flush()
    assert len(backend.batches) == 1
    writer.close()


def test_flush_error_is_swallowed():
    class BadBackend:
        def append_batch(self, table, rows):
            raise RuntimeError('boom')

    writer = Writer(BadBackend(), flush_rows=1, flush_secs=0)
    writer.enqueue('t', {'a': 1})  # flush triggered; error logged, not raised
    writer.close()
