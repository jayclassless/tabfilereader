#
# Copyright (c) 2020, Jason Simeone
#

import re

from collections import Counter, abc
from typing import Union, Sequence, Callable, Any, Mapping, List, Type, Tuple

from .errors import TabFileReaderError
from .types import StringType
from .util import RegexType


class Column:
    """
    Defines the properties of a column in a schema.

    :param location:
        Specifies where in the file the column is located. Can be specified in
        several ways:

        * A zero-based integer that corresponds to the column's position.
        * A string that indicates the name of the column that will be found
          in the first record of the file.
        * A compiled regular expression that will be used to match against
          the names in the first record of the file.
        * A sequence of strings or compiled regular expressions that will be
          used to match against the names in the first record in the file.
    :param required: Whether or not this column must exist in the file.
    :param data_type:
        A callable that will translate the string read from the file and
        coerce it to the desired Python data type. If the value cannot be
        coerced, the callable should raise a ``ValueError``.
    :param data_required: Whether or not this column must contain a value.
    """

    location: Union[int, Sequence[RegexType]]

    def __init__(
            self,
            location: Union[
                str, int, RegexType, Sequence[Union[str, RegexType]]],
            required: bool = True,
            data_type: Callable[[str], Any] = StringType(),
            data_required: bool = False):

        if isinstance(location, (str, RegexType)):
            location = [location]
        if isinstance(location, abc.Sequence):
            self.location = [
                pat if isinstance(pat, RegexType)
                else re.compile(f'^{re.escape(pat)}$')
                for pat in location
            ]
        else:
            self.location = location

        self.required = required
        self.data_type = data_type or str
        self.data_required = data_required

    def process_value(self, value: str) -> Any:
        """
        Parses/Coerces a value from a file according to this column's
        ``data_type``.

        :param value: The raw value read from the file.
        """

        if not value:
            if self.data_required:
                raise ValueError('A value is required')
            return None
        return self.data_type(value)


OptionsDefType = Mapping[str, Tuple[Type, Any]]


SCHEMA_OPTIONS: OptionsDefType = {
    'ignore_unknown_columns': (bool, False),
    'ignore_empty_records': (bool, False),
}


def _validate_columns(class_name: str, columns: Mapping[str, Column]) -> None:
    if not columns:
        raise TabFileReaderError(
            f'No columns defined for {class_name}'
        )

    locations: List[Union[str, int]] = []
    for column in columns.values():
        if isinstance(column.location, int):
            locations.append(column.location)
        else:
            locations += [
                loc.pattern if isinstance(loc, RegexType) else loc
                for loc in column.location
            ]

    location_types = list(map(type, locations))
    if any([lt != location_types[0] for lt in location_types]):
        raise TabFileReaderError(
            f'Column locations in {class_name} must be all header-based'
            ' or all position-based'
        )

    dupe_locations = [
        str(key)
        for key, value in Counter(locations).items()
        if value > 1
    ]
    if dupe_locations:
        raise TabFileReaderError(
            f'Duplicated column locations specified for {class_name}:'
            f" {', '.join(dupe_locations)}"
        )


class Schema:
    """
    The base class for defining the structure and behavior of data to be read
    from a file.

    This class should be subclassed in your code. The properties defined on the
    subclass should be instances of the ``Column`` class.

    Subclasses accept the following class keyword arguments:

    * ``ignore_unknown_columns``: Whether or not columns found in the file that
      do not correspond to columns defined on this class will be ignored.
      Defaults to ``False``.
    * ``ignore_empty_records``: Whether or not completely empty records in a
      file are ignored by tabfilereader. Defaults to ``False``.
    """

    _columns: Mapping[str, Column]
    _options: Mapping[str, Any]

    def __init_subclass__(cls, **kwargs: Any):
        # Gather the options
        cls._options = {}
        for name, option in SCHEMA_OPTIONS.items():
            if name in kwargs:
                value = kwargs.pop(name)
                if not isinstance(value, option[0]):
                    raise ValueError(
                        'Invalid value specified for option %s' % (name,)
                    )
                cls._options[name] = value
            else:
                cls._options[name] = option[1]

        if kwargs:
            raise TabFileReaderError(
                f"Invalid keyword arguments: {', '.join(kwargs.keys())}"
            )

        super().__init_subclass__()

        # Catalog the column names
        cls._columns = {}
        for name in dir(cls):
            attr = getattr(cls, name)
            if isinstance(attr, Column):
                cls._columns[name] = attr

        # Make sure the column definitions make sense together
        _validate_columns(cls.__name__, cls._columns)

