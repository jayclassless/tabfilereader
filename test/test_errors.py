from tabfilereader import RecordErrors


def test_add():
    errors = RecordErrors()
    errors.add('foo', 'bar')
    assert len(errors) == 1
    assert errors['foo'] == 'bar'


def test_add_exception():
    errors = RecordErrors()
    errors.add('foo', ValueError('bar'))
    assert len(errors) == 1
    assert errors['foo'] == 'bar'


def test_contains():
    errors = RecordErrors()
    assert 'foo' not in errors
    errors.add('foo', 'bar')
    assert 'foo' in errors


def test_iter():
    errors = RecordErrors()
    errors.add('foo', 'red')
    errors.add('bar', 'blue')
    assert list(errors) == ['foo', 'bar']


def test_eq():
    errors1 = RecordErrors()
    errors1.add('foo', 'red')

    errors2 = RecordErrors()
    errors2.add('foo', 'red')

    assert errors1 == errors2

    errors2.add('bar', 'blue')
    assert errors1 != errors2


def test_eq_dict():
    errors1 = RecordErrors()
    errors1.add('foo', 'red')

    errors2 = {'foo': 'red'}

    assert errors1 == errors2

    errors2['bar'] = 'blue'
    assert errors1 != errors2


def test_eq_other():
    errors = RecordErrors()
    errors.add('foo', 'red')

    assert errors != 123


def test_bool():
    errors = RecordErrors()
    assert bool(errors) is False

    errors.add('foo', 'red')
    assert bool(errors) is True

