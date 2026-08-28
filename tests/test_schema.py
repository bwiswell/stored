import seared as s

from stored import schema


@s.seared
class Sample(s.Seared):
    id:   int   = s.Int(required=True)
    name: str   = s.Str(default='')
    x:    float = s.Float(default=0.0)


def test_table_name_snake_cases():
    assert schema.table_name(Sample) == 'stream_sample'


def test_primary_key_and_meta_columns():
    assert schema.PRIMARY_KEY == ('_key_expr', '_ts_hlc')
    for col in ('_key_expr', '_ts_hlc', '_issued_at', '_source', '_payload'):
        assert col in schema.META_COLUMNS


def test_event_at_is_a_timestamp_meta_column():
    assert schema.META_COLUMNS['_event_at'] == 'TIMESTAMP'
    assert schema.EVENT_AT == '_event_at'
    assert schema.ISSUED_AT == '_issued_at'


def test_derive_columns_maps_scalars():
    cols = schema.derive_columns(Sample)
    assert cols['id'] == 'BIGINT'
    assert cols['name'] == 'VARCHAR'
    assert cols['x'] == 'DOUBLE'
    # meta columns are present and come first
    assert '_key_expr' in cols
    assert list(cols)[:2] == ['_key_expr', '_ts_hlc']
