import seared as s

from stored import schema


@s.seared
class Sample(s.Seared):
    id: int = s.Int(required=True)
    name: str = s.Str(default='')
    x: float = s.Float(default=0.0)


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


def test_index_specs_temporal_first_then_dimensions():
    specs = schema.index_specs('stream_sample', '_event_at', ('id', 'name'))
    assert specs[0] == ('idx_stream_sample_time', ('_event_at', '_ts_hlc', '_key_expr'))
    assert specs[1:] == (
        ('idx_stream_sample_id', ('id',)),
        ('idx_stream_sample_name', ('name',)),
    )


def test_index_specs_without_dimensions_is_temporal_only():
    assert schema.index_specs('stream_sample', '_issued_at', ()) == (
        ('idx_stream_sample_time', ('_issued_at', '_ts_hlc', '_key_expr')),
    )


@s.seared
class Aliased(s.Seared):
    zones: dict = s.Dict(data_key='zn', default_factory=dict)
    name: str = s.Str(default='')


def test_wire_path_passes_an_unaliased_head_through():
    assert schema.wire_path(Aliased, 'name') == 'name'
    assert schema.wire_path(Sample, 'id') == 'id'


def test_wire_path_rewrites_an_aliased_head():
    """``_payload`` is written in wire spelling, so the path has to be too."""
    assert schema.wire_path(Aliased, 'zones.department') == 'zn.department'
    assert schema.wire_path(Aliased, 'zones') == 'zn'


def test_wire_path_rejects_an_unknown_head():
    import pytest

    from stored.errors import SchemaError

    with pytest.raises(SchemaError, match='not a field'):
        schema.wire_path(Aliased, 'nope.key')


def test_json_index_specs_names_after_the_declared_path():
    specs = schema.json_index_specs('stream_loc', {'zones.department': 'zn.department'})
    assert specs == (('idx_stream_loc_json_zones_department', 'zn.department'),)


def test_json_index_specs_slugs_anything_unsafe():
    specs = schema.json_index_specs('t', {'a.b-c': 'a.b-c'})
    assert specs[0][0] == 'idx_t_json_a_b_c'


def test_json_index_specs_is_empty_without_declarations():
    assert schema.json_index_specs('t', {}) == ()
