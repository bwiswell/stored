import seared as s

from stored import Store


@s.seared
class Msg(s.Seared):
    id: int = s.Int(required=True)


def test_register_creates_stream(tmp_path):
    store = Store(str(tmp_path / 'c.duckdb'))
    try:
        stream = store.register(Msg, retention='1d', index=('id',))
        assert stream.table == 'stream_msg'
        assert store.registry.get(Msg).retention == '1d'
    finally:
        store.close()
