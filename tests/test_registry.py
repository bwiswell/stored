import pytest
import seared as s

from stored.errors import RegistrationError
from stored.registry import StreamRegistry


@s.seared
class Msg(s.Seared):
    id: int = s.Int(required=True)


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
