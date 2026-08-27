import pytest
import seared as s

from stored.errors import RegistrationError
from stored.registry import StreamRegistry


@s.seared
class Msg(s.Seared):
    id: int = s.Int(required=True)


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
