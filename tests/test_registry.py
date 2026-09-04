import datetime

import pytest
import seared as s

from stored.errors import RegistrationError
from stored.registry import StreamRegistry


@s.seared
class Msg(s.Seared):
    id: int = s.Int(required=True)


@s.seared
class Zoned(s.Seared):
    id:    int  = s.Int(required=True)
    zones: dict = s.Dict(data_key='zn', default_factory=dict)
    label: str  = s.Str(default='')


@s.seared
class Obs(s.Seared):
    id:          int   = s.Int(required=True)
    observed_at: float = s.Float(required=True)
    label:       str   = s.Str(default='')


def test_add_and_get():
    reg = StreamRegistry()
    stream = reg.add(Msg, retention='7d', index=('id',))
    assert reg.get(Msg) is stream
    assert stream.table == 'stream_msg'
    assert stream.retention == '7d'
    assert stream.index == ('id',)
    assert reg.all() == (stream,)


def test_add_canonicalizes_horizon_units():
    reg = StreamRegistry()
    stream = reg.add(Msg, retention=datetime.timedelta(days=7), latest_key=('id',), latest_retention=86400)
    assert stream.retention == '604800s'
    assert stream.latest_retention == '86400s'


def test_add_rejects_bad_horizon():
    with pytest.raises(ValueError):
        StreamRegistry().add(Msg, retention=-1)


def test_duplicate_raises():
    reg = StreamRegistry()
    reg.add(Msg)
    with pytest.raises(RegistrationError):
        reg.add(Msg)


def test_non_seared_raises():
    reg = StreamRegistry()

    class Plain:
        pass

    with pytest.raises(RegistrationError):
        reg.add(Plain)


def test_get_unregistered_raises():
    reg = StreamRegistry()
    with pytest.raises(RegistrationError):
        reg.get(Msg)


def test_default_time_column_is_issued_at():
    assert StreamRegistry().add(Msg).time_column == '_issued_at'


def test_time_field_selects_event_column():
    stream = StreamRegistry().add(Obs, time_field='observed_at')
    assert stream.time_field == 'observed_at'
    assert stream.time_column == '_event_at'


def test_time_field_unknown_raises():
    with pytest.raises(RegistrationError):
        StreamRegistry().add(Obs, time_field='nope')


def test_time_field_non_temporal_raises():
    with pytest.raises(RegistrationError):
        StreamRegistry().add(Obs, time_field='label')


def test_default_has_no_latest_projection():
    assert not StreamRegistry().add(Msg).has_latest


def test_latest_key_sets_projection():
    stream = StreamRegistry().add(Obs, latest_key=('id',), latest_retention='30d')
    assert stream.has_latest
    assert stream.latest_key == ('id',)
    assert stream.latest_retention == '30d'
    assert stream.latest_table == 'latest_obs'


def test_latest_key_unknown_field_raises():
    with pytest.raises(RegistrationError):
        StreamRegistry().add(Obs, latest_key=('nope',))


def test_json_index_records_the_translated_paths():
    stream = StreamRegistry().add(Zoned, json_index=('zones.department',))
    assert stream.json_paths == {'zones.department': 'zn.department'}


def test_json_index_defaults_to_nothing():
    assert StreamRegistry().add(Zoned).json_paths == {}


def test_json_index_rejects_an_unknown_head():
    with pytest.raises(RegistrationError, match='not a field'):
        StreamRegistry().add(Zoned, json_index=('nope.key',))


def test_json_index_rejects_a_non_dict_head():
    """A path reaches into a Dict; a scalar already has a column."""
    with pytest.raises(RegistrationError, match='is a Str field'):
        StreamRegistry().add(Zoned, json_index=('label.key',))


def test_json_index_rejects_the_bare_dict():
    with pytest.raises(RegistrationError, match='names the whole'):
        StreamRegistry().add(Zoned, json_index=('zones',))
