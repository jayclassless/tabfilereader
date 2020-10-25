#
# Copyright (c) 2020, Jason Simeone
#

from typing import Sequence, Type, Any, Iterator


class CsvRecordBase:
    """
    The base class for records that are read by tabfilereader.

    The column values can be accessed on this class as attributes (e.g.,
    ``record.my_column``) or item lookups (e.g., ``record['my_column']``).
    """

    __slots__: Sequence[str] = ()

    def __init__(self, **kwargs: Any):
        for field in self.__class__.__slots__:
            setattr(self, field, kwargs.get(field, None))

    def _asdict(self) -> dict:
        """
        Returns a dict representation of the values contained in this record.
        """

        return {
            field: self[field]
            for field in self
        }

    def __getitem__(self, key: str) -> Any:
        if key in self.__class__.__slots__:
            return getattr(self, key)
        raise KeyError(key)

    def __contains__(self, item: str) -> bool:
        return item in self.__class__.__slots__

    def __len__(self) -> int:
        return len(self.__class__.__slots__)

    def __iter__(self) -> Iterator:
        return iter(self.__class__.__slots__)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, (CsvRecordBase, dict)):
            return NotImplemented

        if sorted(self) != sorted(other):
            return False

        for field in self:
            if self[field] != other[field]:
                return False

        return True

    def __repr__(self) -> str:
        return '%s(%s)' % (
            self.__class__.__name__,
            ', '.join(
                f'{field}={repr(getattr(self, field))}'
                for field in self.__class__.__slots__
            ),
        )


def make_csvrecord_type(fields: Sequence[str]) -> Type:
    """
    Creates a CsvRecord type that handles the specified field names.

    :param fields: The field names to support in the record.
    """

    return type(
        'CsvRecord',
        (CsvRecordBase,),
        {
            '__slots__': sorted(fields),
        },
    )

