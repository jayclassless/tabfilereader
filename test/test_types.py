
from datetime import date, time, datetime, timezone
from decimal import Decimal

import pytest

from tabfilereader import (
    StringType,
    IntegerType,
    FloatType,
    DecimalType,
    BooleanType,
    DateType,
    TimeType,
    DateTimeType,
    JsonType,
    JsonObjectType,
    JsonArrayType,
    Base64Type,
)


GOOD_VALUES = (
    (StringType(), 'foo', 'foo'),

    (IntegerType(), '123', 123),
    (IntegerType(), '-123', -123),
    (IntegerType(), '12.34', 12),

    (FloatType(), '123', 123.0),
    (FloatType(), '12.34', 12.34),
    (FloatType(), '-12.34', -12.34),

    (DecimalType(), '123', Decimal('123')),
    (DecimalType(), '12.34', Decimal('12.34')),
    (DecimalType(), '-12.34', Decimal('-12.34')),

    (BooleanType(), 'true', True),
    (BooleanType(), 'True', True),
    (BooleanType(), 'yEs', True),
    (BooleanType(), 'y', True),
    (BooleanType(), '1', True),
    (BooleanType(), 'false', False),
    (BooleanType(), 'fAlSe', False),
    (BooleanType(), 'nO', False),
    (BooleanType(), 'N', False),
    (BooleanType(), '0', False),

    (DateType(), '2020-05-22', date(2020, 5, 22)),
    (DateType('%m/%d/%Y'), '5/22/2020', date(2020, 5, 22)),
    (DateType(['%Y-%m-%d', '%m/%d/%Y']), '5/22/2020', date(2020, 5, 22)),
    (DateType(['%Y-%m-%d', '%m/%d/%Y']), '2020-05-22', date(2020, 5, 22)),

    (DateTimeType(), '2020-05-22T12:34:56', datetime(2020, 5, 22, 12, 34, 56)),
    (DateTimeType(), '2020-05-22T12:34:56.123456', datetime(2020, 5, 22, 12, 34, 56, 123456)),
    (DateTimeType(), '2020-05-22T12:34:56+0000', datetime(2020, 5, 22, 12, 34, 56, tzinfo=timezone.utc)),
    (DateTimeType(), '2020-05-22T12:34:56.123456+0000', datetime(2020, 5, 22, 12, 34, 56, 123456, tzinfo=timezone.utc)),
    (DateTimeType('%I:%M:%S%p %m/%d/%Y'), '1:34:56pm 5/22/2020', datetime(2020, 5, 22, 13, 34, 56)),
    (DateTimeType(['%Y-%m-%d', '%I:%M:%S%p %m/%d/%Y']), '1:34:56pm 5/22/2020', datetime(2020, 5, 22, 13, 34, 56)),

    (JsonType(), '123', 123),
    (JsonType(), '"foo"', 'foo'),
    (JsonType(), 'true', True),
    (JsonType(), '[1,2,3]', [1,2,3]),
    (JsonType(), '{"foo": 1, "bar": false}', {'foo': 1, 'bar': False}),

    (JsonArrayType(), '[1,2,3]', [1,2,3]),

    (JsonObjectType(), '{"foo": 1, "bar": false}', {'foo': 1, 'bar': False}),

    (Base64Type(), 'Zm9v', b'foo'),
)


@pytest.mark.parametrize('datatype,value,expected', GOOD_VALUES)
def test_good_values(datatype, value, expected):
    assert datatype(value) == expected


BAD_VALUES = (
    (IntegerType(), 'foo'),

    (FloatType(), 'foo'),

    (DecimalType(), 'foo'),

    (BooleanType(), 'foo'),

    (DateType(), 'foo'),

    (TimeType(), 'foo'),

    (DateTimeType(), 'foo'),

    (JsonType(), 'foo'),

    (JsonArrayType(), 'foo'),
    (JsonArrayType(), '123'),

    (JsonObjectType(), 'foo'),
    (JsonObjectType(), '123'),

    (Base64Type(), 'foo'),
)


@pytest.mark.parametrize('datatype,value', BAD_VALUES)
def test_bad_values(datatype, value):
    with pytest.raises(ValueError):
        datatype(value)

