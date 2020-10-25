#
# Copyright (c) 2020, Jason Simeone
#

from typing import MutableMapping, Union, Iterator


class TabFileReaderError(Exception):
    """
    An exception raised by tabfilereader.
    """


class RecordErrors:
    """
    A collection of the errors encountered while reading a record from a file.

    This class can be accessed like a dict, where the keys are the column
    names.
    """

    def __init__(self) -> None:
        self._errors: MutableMapping[str, str] = {}

    def add(self, column: str, error: Union[Exception, str]) -> None:
        """
        Adds an error to the collection.

        :param column: The column where the error was encountered.
        :param error: The error that occurred.
        """

        if isinstance(error, Exception):
            error = str(error)
        self._errors[column] = error

    def __getitem__(self, key: str) -> str:
        return self._errors[key]

    def __contains__(self, item: str) -> bool:
        return item in self._errors

    def __len__(self) -> int:
        return len(self._errors)

    def __iter__(self) -> Iterator:
        return iter(self._errors)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, RecordErrors):
            return self._errors == other._errors
        if isinstance(other, dict):
            return self._errors == other
        return NotImplemented

    def __bool__(self) -> bool:
        return len(self._errors) > 0

    def __repr__(self) -> str:
        return repr(self._errors)

