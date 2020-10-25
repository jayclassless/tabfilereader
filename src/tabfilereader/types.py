
import base64
import binascii
import datetime
import decimal
import json

from typing import Union, Sequence, Optional, Any


class StringType:
    """
    A data type for Columns that coerces values to ``str``.
    """

    def __call__(self, value: str) -> str:
        return str(value)


class IntegerType:
    """
    A data type for Columns that coerces values to ``int``.
    """

    def __call__(self, value: str) -> int:
        return int(value.split('.', 1)[0])


class FloatType:
    """
    A data type for Columns that coerces values to ``float``.
    """

    def __call__(self, value: str) -> float:
        return float(value)


class DecimalType:
    """
    A data type for Columns that coerces values to ``decimal.Decimal``.
    """

    def __call__(self, value: str) -> decimal.Decimal:
        try:
            return decimal.Decimal(value)
        except decimal.InvalidOperation as exc:
            raise ValueError('Not a valid decimal') from exc


class BooleanType:
    """
    A data type for Columns that coerces values to ``bool``.

    ``true``, ``t``, ``yes``, ``y``, and ``1`` are interpreted as ``True``.

    ``false``, ``f``, ``no``, ``n``, and ``0`` are interpreted as ``False``.
    """

    VALUE_MAP = {
        'TRUE': True,
        'T': True,
        'YES': True,
        'Y': True,
        '1': True,
        'FALSE': False,
        'F': False,
        'NO': False,
        'N': False,
        '0': False,
    }

    def __call__(self, value: str) -> bool:
        val = self.VALUE_MAP.get(value.upper())
        if val is None:
            raise ValueError('Not a valid boolean')
        return val


class DateTimeTypeBase:
    """
    A base class for date/time-oriented data types.

    Don't use this class directly in ``Schema`` definitions.

    :param fmt:
        The Python ``strptime()`` format string or sequence of strings to
        parse.
    """

    formats: Sequence[str]

    def __init__(self, fmt: Optional[Union[str, Sequence[str]]] = None):
        if isinstance(fmt, str):
            self.formats = [fmt]
        elif fmt is not None:
            self.formats = fmt

    def __call__(self, value: str) -> Union[
            datetime.date, datetime.time, datetime.datetime]:
        for fmt in self.formats:
            try:
                return self.convert(value, fmt)
            except ValueError:
                continue
        raise ValueError(
            'Does not match accepted %s formats: %s"' % (
                self.__class__.__name__,
                ', '.join(self.formats),
            )
        )

    def convert(self, value: str, fmt: str) -> Any:
        raise NotImplementedError()


class DateType(DateTimeTypeBase):
    """
    A data type for Columns that coerces values to ``datetime.date``.

    By default, allows the following formats:

    * ``YYYY-MM-DD``

    :param fmt:
        The Python ``strptime()`` format string or sequence of strings to
        parse.
    """

    formats: Sequence[str] = [
        '%Y-%m-%d',
    ]

    def convert(self, value: str, fmt: str) -> datetime.date:
        return datetime.datetime.strptime(value, fmt).date()


class ExcelDateType(DateType):
    """
    A data type for Columns that coerces values to ``datetime.date``.

    This is specifically aimed at handling Excel file oddities.
    """

    formats: Sequence[str] = [
        '%Y-%m-%dT%H:%M:%S',
    ]


class TimeType(DateTimeTypeBase):
    """
    A data type for Columns that coerces values to ``datetime.time``.

    By default, allows the following formats:

    * ``HH:MM``
    * ``HH:MM:SS``
    * ``HH:MM:SS.FFFFFF``

    :param fmt:
        The Python ``strptime()`` format string or sequence of strings to
        parse.
    """

    formats: Sequence[str] = [
        '%H:%M',
        '%H:%M:%S',
        '%H:%M:%S.%f',
    ]

    def convert(self, value: str, fmt: str) -> datetime.time:
        return datetime.datetime.strptime(value, fmt).time()


class ExcelTimeType(TimeType):
    """
    A data type for Columns that coerces values to ``datetime.time``.

    This is specifically aimed at handling Excel file oddities.
    """

    formats: Sequence[str] = [
        '%Y-%m-%dT%H:%M:%S',
    ]


class DateTimeType(DateTimeTypeBase):
    """
    A data type for Columns that coerces values to ``datetime.datetime``.

    By default, allows the following formats:

    * ``YYYY-MM-DDTHH:MM:SS``
    * ``YYYY-MM-DDTHH:MM:SS+HHMM``
    * ``YYYY-MM-DDTHH:MM:SS.FFFFFF``
    * ``YYYY-MM-DDTHH:MM:SS.FFFFFF+HHMM``

    :param fmt:
        The Python ``strptime()`` format string or sequence of strings to
        parse.
    """

    formats: Sequence[str] = [
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%dT%H:%M:%S%z',
        '%Y-%m-%dT%H:%M:%S.%f',
        '%Y-%m-%dT%H:%M:%S.%f%z',
    ]

    def convert(self, value: str, fmt: str) -> datetime.datetime:
        return datetime.datetime.strptime(value, fmt)


class ExcelDateTimeType(DateTimeType):
    """
    A data type for Columns that coerces values to ``datetime.datetime``.

    This is specifically aimed at handling Excel file oddities.
    """


class JsonType:
    """
    A data type for Columns that allows JSON-encoded values.
    """

    def __call__(self, value: str) -> Any:
        try:
            return json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError('Not a JSON-encoded value') from exc


class JsonObjectType(JsonType):
    """
    A data type for Columns that only allows JSON-encoded objects and coerces
    them to ``dict``.
    """

    def __call__(self, value: str) -> dict:
        parsed = super().__call__(value)
        if not isinstance(parsed, dict):
            raise ValueError('Not a JSON-encoded object')
        return parsed


class JsonArrayType(JsonType):
    """
    A data type for Columns that only allows JSON-encoded arrays and coerces
    them to ``list``.
    """

    def __call__(self, value: str) -> list:
        parsed = super().__call__(value)
        if not isinstance(parsed, list):
            raise ValueError('Not a JSON-encoded array')
        return parsed


class Base64Type:
    """
    A data type for Columns that only allows base64-encoded values and coerces
    them to ``bytes``.
    """

    def __call__(self, value: str) -> bytes:
        try:
            return base64.b64decode(value)
        except binascii.Error as exc:
            raise ValueError('Not a base64-encoded value') from exc

