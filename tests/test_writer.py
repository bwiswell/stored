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
            msg = 'boom'
            raise RuntimeError(msg)

    writer = Writer(BadBackend(), flush_rows=1, flush_secs=0)
    writer.enqueue('t', {'a': 1})  # flush triggered; error logged, not raised
    writer.close()


def test_a_row_with_no_compare_value_stays_out_of_the_projection():
    class LatestBackend(FakeBackend):
        def __init__(self):
            super().__init__()
            self.upserts = []

        def upsert_latest(self, table, rows, key_columns, compare_column, *, run_gap_s=None):
            self.upserts.append((table, [row['id'] for row in rows]))

    backend = LatestBackend()
    writer = Writer(backend, flush_rows=100, flush_secs=0)
    writer.register_latest('t', 'latest_t', ('id',), '_event_at')
    writer.enqueue('t', {'id': 1, '_event_at': None})
    writer.enqueue('t', {'id': 2, '_event_at': 1000.0})
    writer.flush()
    writer.enqueue('t', {'id': 3, '_event_at': None})  # a batch with nothing timed: no upsert at all
    writer.close()

    assert [[row['id'] for row in rows] for _table, rows in backend.batches] == [[1, 2], [3]]  # history: every row
    assert backend.upserts == [('latest_t', [2])]
