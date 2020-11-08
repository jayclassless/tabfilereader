import pytest

from tabfilereader.record import make_record_type


def fields(obj):
    return sorted([
        field
        for field in dir(obj)
        if not field.startswith('_')
    ])


def test_create():
    rectype = make_record_type(['foo', 'bar'])
    rec = rectype()
    assert fields(rec) == ['bar', 'foo']


def test_empty():
    rectype = make_record_type(['foo', 'bar'])
    rec = rectype()
    assert rec.foo is None
    assert rec.bar is None


def test_init():
    rectype = make_record_type(['foo', 'bar'])
    rec = rectype(foo=123)
    assert rec.foo == 123
    assert rec.bar is None


def test_getitem():
    rectype = make_record_type(['foo', 'bar'])
    rec = rectype(foo=123)
    assert rec['foo'] == 123
    assert rec['bar'] is None
    with pytest.raises(KeyError):
        rec['baz']


def test_contains():
    rectype = make_record_type(['foo', 'bar'])
    rec = rectype()
    assert 'foo' in rec
    assert 'baz' not in rec


def test_len():
    rectype = make_record_type(['foo', 'bar'])
    rec = rectype()
    assert len(rec) == 2


def test_iter():
    rectype = make_record_type(['foo', 'bar'])
    rec = rectype()
    assert list(rec) == ['bar', 'foo']


def test_eq():
    rectype = make_record_type(['foo', 'bar'])

    rec1 = rectype(foo=123, bar='baz')
    rec2 = rectype(foo=123, bar='baz')
    assert rec1 == rec2

    rec1 = rectype(foo=123, bar='baz')
    rec2 = rectype(foo=456, bar='baz')
    assert rec1 != rec2

    other_rectype = make_record_type(['foo', 'bar', 'baz'])
    rec1 = rectype(foo=123, bar='baz')
    rec2 = other_rectype(foo=123, bar='baz')
    assert rec1 != rec2


def test_eq_dict():
    rectype = make_record_type(['foo', 'bar'])

    rec1 = rectype(foo=123, bar='baz')
    rec2 = {'foo': 123, 'bar': 'baz'}
    assert rec1 == rec2

    rec1 = rectype(foo=123, bar='baz')
    rec2 = {'foo': 456, 'bar': 'baz'}
    assert rec1 != rec2


def test_eq_other():
    rectype = make_record_type(['foo', 'bar'])

    rec1 = rectype(foo=123, bar='baz')
    rec2 = (123, 'baz')
    assert rec1 != rec2


def test_asdict():
    rectype = make_record_type(['foo', 'bar'])

    rec1 = rectype(foo=123, bar='baz')._asdict()
    assert isinstance(rec1, dict)

    rec2 = {'foo': 123, 'bar': 'baz'}
    assert rec1 == rec2

