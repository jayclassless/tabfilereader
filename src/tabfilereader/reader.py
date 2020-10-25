#
# Copyright (c) 2020, Jason Simeone
#

import csv
import io
import mmap
import os
import re
import tempfile

from collections import Counter
from datetime import time
from pathlib import Path
from typing import (
    Iterator,
    Sequence,
    Mapping,
    Union,
    Type,
    Optional,
    Tuple,
    Dict,
    Any,
)

import ezodf
import xlrd3

from .errors import TabFileReaderError, RecordErrors
from .record import CsvRecordBase, make_csvrecord_type
from .schema import Schema
from .util import RegexType


ColumnMapType = Mapping[int, str]


class Reader:
    schema: Type[Schema]

    # pylint: disable=protected-access

    def __init__(
            self,
            source_file: Union[io.IOBase, Path, str]):
        self._column_map: Optional[ColumnMapType] = None
        self._expects_header_record = not isinstance(
            list(self.schema._columns.values())[0].location,
            int,
        )
        self._record_type = make_csvrecord_type(
            list(self.schema._columns.keys())
        )
        self._records_read = 0
        if isinstance(source_file, str):
            source_file = Path(source_file)
        self._init_reader(source_file)

    def _init_reader(self, source_file: Union[io.IOBase, Path]) -> None:
        raise NotImplementedError()

    def _read_next_record(self) -> Sequence[str]:
        raise NotImplementedError()

    @property
    def column_map(self) -> Optional[ColumnMapType]:
        if self._column_map is not None:
            return dict(self._column_map)
        return None

    @property
    def records_read(self) -> int:
        return self._records_read

    def __iter__(self) -> Iterator:
        return self

    def __next__(self) -> Tuple[CsvRecordBase, RecordErrors]:
        raw: Sequence[str] = self._read_next_record()

        if not self._column_map:
            self._column_map = self._generate_column_map(raw)
            if self._expects_header_record:
                raw = self._read_next_record()

        while self.schema._options['ignore_empty_records'] and not raw:
            raw = self._read_next_record()

        rec = self._record_type()
        errors = RecordErrors()
        for idx, name in self._column_map.items():
            column = self.schema._columns[name]
            if idx >= len(raw):
                if column.required:
                    errors.add(name, 'Column missing from record')
            else:
                try:
                    setattr(
                        rec,
                        name,
                        column.process_value(raw[idx]),
                    )
                except ValueError as exc:
                    errors.add(name, exc)

        self._records_read += 1
        return rec, errors

    def _generate_column_map(
            self,
            first_record: Sequence[str]) -> ColumnMapType:
        cmap = {}

        if self._expects_header_record:
            desired = {
                location: name
                for name, column in self.schema._columns.items()
                for location in column.location
            }
            unknown = []

            for idx, column in enumerate(first_record):
                for regex, target in desired.items():
                    if regex.match(column):
                        cmap[idx] = target
                        break
                else:
                    unknown.append(column)

            dupes = [
                key
                for key, value in Counter(cmap.values()).items()
                if value > 1
            ]
            if dupes:
                raise TabFileReaderError(
                    'Multiple locations found targetting columns:'
                    f" {', '.join(dupes)}"
                )

        else:
            cmap = {
                column.location: name
                for name, column in self.schema._columns.items()
                if column.location < len(first_record)
            }

            unknown = [
                str(idx)
                for idx in range(len(first_record))
                if idx not in cmap
            ]

        if unknown and not self.schema._options['ignore_unknown_columns']:
            raise TabFileReaderError(
                'Unknown columns encountered in data:'
                f" {', '.join(unknown)}"
            )

        missing = []
        for name, col in self.schema._columns.items():
            if col.required and name not in cmap.values():
                missing.append(name)
        if missing:
            raise TabFileReaderError(
                'Could not identify locations for required columns:'
                f" {', '.join(missing)}"
            )

        return cmap


class CsvDialect(csv.Dialect):
    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)
        super().__init__()


class CsvReader(Reader):
    encoding: Optional[str] = None
    delimiter: str = ','
    doublequote: bool = True
    escapechar: Optional[str] = None
    quotechar: str = '"'
    quoting: int = csv.QUOTE_MINIMAL
    lineterminator: str = '\r\n'
    skipinitialspace: bool = False

    def _init_reader(self, source_file: Union[io.IOBase, Path]) -> None:
        dialect = CsvDialect(
            delimiter=self.delimiter,
            doublequote=self.doublequote,
            escapechar=self.escapechar,
            quotechar=self.quotechar,
            quoting=self.quoting,
            lineterminator=self.lineterminator,
            skipinitialspace=self.skipinitialspace,
        )

        if isinstance(source_file, Path):
            self._reader = csv.reader(
                source_file.open(
                    mode='r',
                    newline='',
                    encoding=self.encoding,
                ),
                dialect,
            )

        elif isinstance(source_file, io.BufferedIOBase):
            self._reader = csv.reader(
                io.TextIOWrapper(
                    source_file,
                    encoding=self.encoding,
                ),
                dialect,
            )

        elif isinstance(source_file, io.TextIOBase):
            self._reader = csv.reader(source_file, dialect)

        else:  # pragma: no cover
            raise ValueError('Invalid source_file')

    def _read_next_record(self) -> Sequence[str]:
        return next(self._reader)


class ExcelReader(Reader):
    worksheet: Union[int, str, RegexType] = 0
    encoding: Optional[str] = None
    on_demand: bool = True

    def _init_reader(self, source_file: Union[io.IOBase, Path]) -> None:
        wb_kwargs: Dict[str, Any] = {
            'encoding_override': self.encoding,
            'on_demand': self.on_demand,
        }

        if isinstance(source_file, Path):
            wb_kwargs['filename'] = str(source_file)

        elif isinstance(source_file, io.IOBase):
            try:
                fileno = source_file.fileno()
            except OSError:
                # We're dealing with an in-memory stream. Write it to a file
                # so it can be mmap'ed,
                self._tmpfile = tempfile.TemporaryFile()
                buf = source_file.readline()
                while buf:
                    self._tmpfile.write(buf)
                    buf = source_file.readline()
                self._tmpfile.seek(0)
                fileno = self._tmpfile.fileno()
            wb_kwargs['file_contents'] = mmap.mmap(
                fileno,
                os.fstat(fileno).st_size,
                access=mmap.ACCESS_READ,
            )

        else:  # pragma: no cover
            raise ValueError('Invalid source_file')

        self._book = xlrd3.open_workbook(**wb_kwargs)
        self._rows = self._find_sheet().get_rows()

    def _find_sheet(self) -> xlrd3.sheet.Sheet:
        try:
            if isinstance(self.worksheet, int):
                return self._book.sheet_by_index(self.worksheet)

            if isinstance(self.worksheet, RegexType):
                # pylint: disable=no-member
                for name in self._book.sheet_names():
                    if self.worksheet.match(name):
                        return self._book.sheet_by_name(name)
                raise ValueError(
                    f'No worksheet matches {self.worksheet.pattern}'
                )

            return self._book.sheet_by_name(self.worksheet)
        except (IndexError, ValueError, xlrd3.XLRDError) as exc:
            raise TabFileReaderError(
                'Specified worksheet does not exist'
            ) from exc

    def _read_next_record(self) -> Sequence[str]:
        row = next(self._rows)

        record = []
        for cell in row:
            if cell.ctype in (xlrd3.XL_CELL_NUMBER, xlrd3.XL_CELL_BOOLEAN):
                record.append(str(cell.value))

            elif cell.ctype == xlrd3.XL_CELL_DATE:
                record.append(xlrd3.xldate.xldate_as_datetime(
                    cell.value,
                    self._book.datemode,
                ).isoformat())

            elif cell.ctype == xlrd3.XL_CELL_ERROR:
                record.append(
                    xlrd3.biffh.error_text_from_code[cell.value]
                )

            else:
                record.append(cell.value)

        return record


class OdsReader(Reader):
    worksheet: Union[int, str, RegexType] = 0

    def _init_reader(self, source_file: Union[io.IOBase, Path]) -> None:
        if isinstance(source_file, Path):
            self._doc = ezodf.opendoc(source_file)

        elif isinstance(source_file, io.BufferedReader):
            self._doc = ezodf.opendoc(io.BytesIO(source_file.read()))

        else:
            self._doc = ezodf.opendoc(source_file)

        self._rows = self._find_sheet().rows()

    def _find_sheet(self) -> ezodf.Sheet:
        try:
            if isinstance(self.worksheet, RegexType):
                # pylint: disable=no-member
                for sheet in self._doc.sheets:
                    if self.worksheet.match(sheet.name):
                        return sheet
                raise ValueError(
                    f'No worksheet matches {self.worksheet.pattern}'
                )
            return self._doc.sheets[self.worksheet]
        except (IndexError, KeyError, ValueError) as exc:
            raise TabFileReaderError(
                'Specified worksheet does not exist'
            ) from exc

    def _read_next_record(self) -> Sequence[str]:
        row = next(self._rows)

        record = []
        for cell in row:
            if cell.value_type == 'time':
                record.append(self._parse_time(cell.value))
            elif cell.value is None:
                record.append('')
            else:
                record.append(str(cell.value))

        return record

    RE_TIME = re.compile(
        r'^PT(?P<hour>\d{2})H'
        r'(?P<minute>\d{2})M'
        r'(?P<second>\d{2})'
        r'(,(?P<frac>\d+))?S$'
    )

    def _parse_time(self, value: str) -> str:
        match = self.RE_TIME.match(value)
        if match:
            parts = match.groupdict()
            return time(
                int(parts['hour']),
                int(parts['minute']),
                int(parts['second']),
                int(parts['frac']) if parts['frac'] else 0,
            ).isoformat()
        return value

