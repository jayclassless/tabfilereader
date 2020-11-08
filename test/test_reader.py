import io
import re

from copy import deepcopy
from datetime import date, time, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from tabfilereader import *


DATA = Path(__file__).parent / 'data'


class ReaderTest:
    reader_type = None
    reader_kwargs = {}
    schema = None
    source = None
    expected = []
    error = None
    read_error = None

    def test_reader(self):
        if isinstance(self.source, list):
            src = io.StringIO('\n'.join(self.source))
        else:
            src = self.source

        if self.error:
            with pytest.raises(self.error[0], match=self.error[1]):
                self.reader = self.reader_type.open(src, self.schema, **self.reader_kwargs)
            return
        self.reader = self.reader_type.open(src, self.schema, **self.reader_kwargs)

        assert self.reader.records_read == 0
        assert self.reader.column_map is None

        parsed = []
        if self.read_error:
            with pytest.raises(self.read_error[0], match=self.read_error[1]):
                for idx, result in enumerate(self.reader):
                    assert self.reader.records_read == idx + 1
                    parsed.append(result)
        else:
            for idx, result in enumerate(self.reader):
                assert self.reader.records_read == idx + 1
                parsed.append(result)

        assert self.reader.records_read == len(parsed)
        assert len(parsed) == len(self.expected)

        for actual, expected in zip(parsed, self.expected):
            assert actual[0] == expected[0]
            assert actual[1] == expected[1]

class CsvReaderTest(ReaderTest):
    reader_type = CsvReader


class EmptyTest(ReaderTest):
    class schema(Schema):
        col1 = Column(0)
    expected = []

class TestEmptyCsv(EmptyTest):
    reader_type = CsvReader
    source = DATA / 'empty.csv'

class TestEmptyXls(EmptyTest):
    reader_type = ExcelReader
    source = DATA / 'empty.xls'

class TestEmptyXlsx(EmptyTest):
    reader_type = ExcelReader
    source = DATA / 'empty.xlsx'

class TestEmptyOds(EmptyTest):
    reader_type = OdsReader
    source = DATA / 'empty.ods'


class EmptyHeaderTest(ReaderTest):
    class schema(Schema):
        col1 = Column('column_1')
        col2 = Column('column_2', data_type=IntegerType())

    expected = []

    def test_reader(self):
        super().test_reader()
        assert self.reader.column_map == {0: 'col1', 1: 'col2'}

class TestEmptyCsv(EmptyHeaderTest):
    reader_type = CsvReader
    source = DATA / 'empty_header.csv'

class TestEmptyXls(EmptyHeaderTest):
    reader_type = ExcelReader
    source = DATA / 'empty_header.xls'

class TestEmptyXlsx(EmptyHeaderTest):
    reader_type = ExcelReader
    source = DATA / 'empty_header.xlsx'

class TestEmptyOds(EmptyHeaderTest):
    reader_type = OdsReader
    source = DATA / 'empty_header.ods'


class SimpleTest(ReaderTest):
    class schema(Schema):
        col1 = Column(0)
        col2 = Column(1, data_type=IntegerType())

    expected = [
        (
            {'col1': 'foo', 'col2': 123},
            {},
        ),
        (
            {'col1': 'bar', 'col2': None},
            {},
        ),
    ]

    def test_reader(self):
        super().test_reader()
        assert self.reader.column_map == {0: 'col1', 1: 'col2'}

class TestSimpleCsv(SimpleTest):
    reader_type = CsvReader
    source = DATA / 'simple.csv'

class TestSimpleXls(SimpleTest):
    reader_type = ExcelReader
    source = DATA / 'simple.xls'

class TestSimpleXlsx(SimpleTest):
    reader_type = ExcelReader
    source = DATA / 'simple.xlsx'

class TestSimpleOOds(SimpleTest):
    reader_type = OdsReader
    source = DATA / 'simple.ods'


class SimpleHeaderTest(SimpleTest):
    class schema(Schema):
        col1 = Column('column_1')
        col2 = Column('column_2', data_type=IntegerType())

    def test_reader(self):
        super().test_reader()
        assert self.reader.column_map == {0: 'col1', 1: 'col2'}

class TestSimpleHeaderCsv(SimpleHeaderTest):
    reader_type = CsvReader
    source = DATA / 'simple_header.csv'

class TestSimpleHeaderXls(SimpleHeaderTest):
    reader_type = ExcelReader
    source = DATA / 'simple_header.xls'

class TestSimpleHeaderXlsx(SimpleHeaderTest):
    reader_type = ExcelReader
    source = DATA / 'simple_header.xlsx'

class TestSimpleHeaderOds(SimpleHeaderTest):
    reader_type = OdsReader
    source = DATA / 'simple_header.ods'



class AllTypesTest(ReaderTest):
    class schema(Schema):
        colstr = Column('string')
        colint = Column('integer', data_type=IntegerType())
        colfloat = Column('float', data_type=FloatType())
        coldecimal = Column('decimal', data_type=DecimalType())
        colbool = Column('bool', data_type=BooleanType())
        coldate = Column('date', data_type=DateType())
        coltime = Column('time', data_type=TimeType())
        coldatetime = Column('datetime', data_type=DateTimeType())
        coljson = Column('json', data_type=JsonType())
        coljsonobj = Column('jsonobject', data_type=JsonObjectType())
        coljsonarr = Column('jsonarray', data_type=JsonArrayType())
        colb64 = Column('base64', data_type=Base64Type())
    expected = [
        (
            {
                'colstr': 'good',
                'colint': 123,
                'colfloat': 12.34,
                'coldecimal': Decimal('12.34'),
                'colbool': True,
                'coldate': date(2020, 5, 22),
                'coltime': time(12, 34, 56),
                'coldatetime': datetime(2020, 5, 22, 12, 34, 56),
                'coljson': {'foo': 123},
                'coljsonobj': {'foo': 123},
                'coljsonarr': ['foo', 123],
                'colb64': b'hello world',
            },
            {},
        ),
        (
            {
                'colstr': 'bad',
                'colint': None,
                'colfloat': None,
                'coldecimal': None,
                'colbool': None,
                'coldate': None,
                'coltime': None,
                'coldatetime': None,
                'coljson': None,
                'coljsonobj': None,
                'coljsonarr': None,
                'colb64': None,
            },
            {
                'colint': "invalid literal for int() with base 10: 'bad'",
                'colfloat': "could not convert string to float: 'bad'",
                'coldecimal': 'Not a valid decimal',
                'colbool': 'Not a valid boolean',
                'coldate': 'Does not match accepted DateType formats: %Y-%m-%d"',
                'coltime': 'Does not match accepted TimeType formats: %H:%M, %H:%M:%S, %H:%M:%S.%f"',
                'coldatetime': 'Does not match accepted DateTimeType formats: %Y-%m-%dT%H:%M:%S, %Y-%m-%dT%H:%M:%S%z, %Y-%m-%dT%H:%M:%S.%f, %Y-%m-%dT%H:%M:%S.%f%z"',
                'coljson': 'Not a JSON-encoded value',
                'coljsonobj': 'Not a JSON-encoded value',
                'coljsonarr': 'Not a JSON-encoded value',
                'colb64': 'Not a base64-encoded value',
            },
        ),
        (
            {
                'colstr': 'missing',
                'colint': None,
                'colfloat': None,
                'coldecimal': None,
                'colbool': None,
                'coldate': None,
                'coltime': None,
                'coldatetime': None,
                'coljson': None,
                'coljsonobj': None,
                'coljsonarr': None,
                'colb64': None,
            },
            {},
        ),
    ]

class TestAllTypesCsv(AllTypesTest):
    reader_type = CsvReader
    source = DATA / 'all_types.csv'

class TestAllTypesOds(AllTypesTest):
    reader_type = OdsReader
    source = DATA / 'all_types.ods'

class ExcelAllTypesTest(AllTypesTest):
    class schema(AllTypesTest.schema):
        coldate = Column('date', data_type=ExcelDateType())
        coltime = Column('time', data_type=ExcelTimeType())
    reader_type = ExcelReader
    expected = deepcopy(AllTypesTest.expected)
ExcelAllTypesTest.expected[1][1]['coldate'] = 'Does not match accepted ExcelDateType formats: %Y-%m-%dT%H:%M:%S"'
ExcelAllTypesTest.expected[1][1]['coltime'] = 'Does not match accepted ExcelTimeType formats: %Y-%m-%dT%H:%M:%S"'

class TestAllTypesXls(ExcelAllTypesTest):
    source = DATA / 'all_types.xls'

class TestAllTypesXlsx(ExcelAllTypesTest):
    reader_type = ExcelReader
    source = DATA / 'all_types.xlsx'


class BadFormulaTest(ReaderTest):
    class schema(Schema):
        col1 = Column(0)
    expected = [
        (
            {'col1': '#NAME?'},
            {},
        ),
    ]

class TestBadFormulaXls(BadFormulaTest):
    reader_type = ExcelReader
    source = DATA / 'bad_formula.xls'

class TestBadFormulaXslx(BadFormulaTest):
    reader_type = ExcelReader
    source = DATA / 'bad_formula.xlsx'

class TestBadFormulaOds(BadFormulaTest):
    reader_type = OdsReader
    source = DATA / 'bad_formula.ods'


class MultiSheetTest(ReaderTest):
    class schema(Schema):
        col1 = Column(0)

    def test_reader(self):
        super().test_reader()
        assert self.reader.column_map == {0: 'col1'}

class MultiSheetIntegerTest(MultiSheetTest):
    reader_kwargs = {
        'worksheet': 2,
    }
    expected = [
        (
            {'col1': 'three'},
            {},
        ),
    ]

class TestMultiSheetIntegerXls(MultiSheetIntegerTest):
    reader_type = ExcelReader
    source = DATA / 'multisheet.xls'

class TestMultiSheetIntegerXlsx(MultiSheetIntegerTest):
    reader_type = ExcelReader
    source = DATA / 'multisheet.xlsx'

class TestMultiSheetIntegerOds(MultiSheetIntegerTest):
    reader_type = OdsReader
    source = DATA / 'multisheet.ods'

class MultiSheetNameTest(MultiSheetTest):
    reader_kwargs = {
        'worksheet': 'Sheet1',
    }
    expected = [
        (
            {'col1': 'one'},
            {},
        ),
    ]

class TestMultiSheetNameXls(MultiSheetNameTest):
    reader_type = ExcelReader
    source = DATA / 'multisheet.xls'

class TestMultiSheetNameXlsx(MultiSheetNameTest):
    reader_type = ExcelReader
    source = DATA / 'multisheet.xlsx'

class TestMultiSheetNameOds(MultiSheetNameTest):
    reader_type = OdsReader
    source = DATA / 'multisheet.ods'

class MultiSheetRegexTest(MultiSheetTest):
    reader_kwargs = {
        'worksheet': re.compile('.* Other Sheet$'),
    }
    expected = [
        (
            {'col1': 'two'},
            {},
        ),
    ]

class TestMultiSheetRegexXls(MultiSheetRegexTest):
    reader_type = ExcelReader
    source = DATA / 'multisheet.xls'

class TestMultiSheetRegexXlsx(MultiSheetRegexTest):
    reader_type = ExcelReader
    source = DATA / 'multisheet.xlsx'

class TestMultiSheetRegexOds(MultiSheetRegexTest):
    reader_type = OdsReader
    source = DATA / 'multisheet.ods'


class TestPathStringSourceCsv(SimpleTest):
    reader_type = CsvReader
    source = str(DATA / 'simple.csv')

class TestPathStringSourceExcel(SimpleTest):
    reader_type = ExcelReader
    source = str(DATA / 'simple.xls')

class TestPathStringSourceOds(SimpleTest):
    reader_type = OdsReader
    source = str(DATA / 'simple.ods')


class TestTextFileIOSourceCsv(SimpleTest):
    reader_type = CsvReader
    source = (DATA / 'simple.csv').open('r')

class TestTextMemoryIOSourceCsv(SimpleTest):
    reader_type = CsvReader
    source = io.StringIO((DATA / 'simple.csv').open('r').read())


class TestBinaryFileIOSourceCsv(SimpleTest):
    reader_type = CsvReader
    source = (DATA / 'simple.csv').open('rb')

class TestBinaryMemoryIOSourceCsv(SimpleTest):
    reader_type = CsvReader
    source = io.StringIO((DATA / 'simple.csv').open('r').read())

class TestBinaryFileIOSourceExcel(SimpleTest):
    reader_type = ExcelReader
    source = (DATA / 'simple.xls').open('rb')

class TestBinaryMemoryIOSourceExcel(SimpleTest):
    reader_type = ExcelReader
    source = io.BytesIO((DATA / 'simple.xls').open('rb').read())

class TestBinaryFileIOSourceOds(SimpleTest):
    reader_type = OdsReader
    source = (DATA / 'simple.ods').open('rb')

class TestBinaryMemoryIOSourceOds(SimpleTest):
    reader_type = OdsReader
    source = io.BytesIO((DATA / 'simple.ods').open('rb').read())


class TestIgnoreEmptyRecords(CsvReaderTest):
    source = [
        'foo,123',
        '',
        '',
        'bar,',
        '',
    ]
    class schema(Schema, ignore_empty_records=True):
        col1 = Column(0)
        col2 = Column(1, data_type=int)
    expected = [
        (
            {'col1': 'foo', 'col2': 123},
            {},
        ),
        (
            {'col1': 'bar', 'col2': None},
            {},
        ),
    ]


class TestBadValueType(CsvReaderTest):
    source = [
        'column_1,column_2',
        'foo,red',
        'bar,',
    ]
    class schema(Schema):
        col1 = Column('column_1')
        col2 = Column('column_2', data_type=int)
    expected = [
        (
            {'col1': 'foo', 'col2': None},
            {'col2': "invalid literal for int() with base 10: 'red'"},
        ),
        (
            {'col1': 'bar', 'col2': None},
            {},
        ),
    ]


class TestFileMissingColumn(CsvReaderTest):
    source = [
        'column_1',
        'foo',
        'bar,',
    ]
    class schema(Schema):
        col1 = Column('column_1')
        col2 = Column('column_2', data_type=int)
    read_error = (
        TabFileReaderError,
        'Could not identify locations for required columns: col2',
    )


class TestUnknownColumn(CsvReaderTest):
    source = [
        'foo,123',
        'bar,',
    ]
    class schema(Schema):
        col1 = Column(0)
    read_error = (
        TabFileReaderError,
        'Unknown columns encountered in data: 1',
    )

class TestHeadersUnknownColumn(CsvReaderTest):
    source = [
        'column_1,column_2',
        'foo,123',
        'bar,',
    ]
    class schema(Schema):
        col1 = Column('column_1')
    read_error = (
        TabFileReaderError,
        'Unknown columns encountered in data: column_2',
    )

class TestUnknownColumnAllowed(CsvReaderTest):
    source = [
        'foo,123',
        'bar,',
    ]
    class schema(Schema, ignore_unknown_columns=True):
        col1 = Column(0)
    expected = [
        (
            {'col1': 'foo'},
            {},
        ),
        (
            {'col1': 'bar'},
            {},
        ),
    ]

class TestHeadersUnknownColumnAllowed(CsvReaderTest):
    source = [
        'column_1,column_2',
        'foo,123',
        'bar,',
    ]
    class schema(Schema, ignore_unknown_columns=True):
        col1 = Column('column_1')
    expected = [
        (
            {'col1': 'foo'},
            {},
        ),
        (
            {'col1': 'bar'},
            {},
        ),
    ]


class TestMultipleLocations(CsvReaderTest):
    source = [
        'column_1,column_2',
        'foo,123',
        'bar,',
    ]
    class schema(Schema):
        col1 = Column(['column_1', 'column_2'])
    read_error = (
        TabFileReaderError,
        'Multiple locations found targetting columns: col1',
    )


class TestRecordMissingColumn(CsvReaderTest):
    source = [
        'column_1,column_2,column_3',
        'foo,123,red',
        'bar,456',
        'baz',
    ]
    class schema(Schema):
        col1 = Column('column_1')
        col2 = Column('column_2', data_type=int)
        col3 = Column('column_3', required=False)
    expected = [
        (
            {'col1': 'foo', 'col2': 123, 'col3': 'red'},
            {},
        ),
        (
            {'col1': 'bar', 'col2': 456, 'col3': None},
            {},
        ),
        (
            {'col1': 'baz', 'col2': None, 'col3': None},
            {'col2': 'Column missing from record'},
        ),
    ]


class TestHeadersRegex(CsvReaderTest):
    source = [
        'column_1,column$2',
        'foo,123',
        'bar,',
    ]
    class schema(Schema):
        col1 = Column(re.compile('^[a-z]+_1$'))
        col2 = Column('column$2', data_type=int)
    expected = [
        (
            {'col1': 'foo', 'col2': 123},
            {},
        ),
        (
            {'col1': 'bar', 'col2': None},
            {},
        ),
    ]


class WorksheetBadIntegerTest(ReaderTest):
    class schema(Schema):
        col1 = Column(0)
    reader_kwargs = {
        'worksheet': 42,
    }
    error = (
        TabFileReaderError,
        'Specified worksheet does not exist',
    )

class TestWorksheetBadIntegerExcel(WorksheetBadIntegerTest):
    reader_type = ExcelReader
    source = DATA / 'multisheet.xls'

class TestWorksheetBadIntegerOds(WorksheetBadIntegerTest):
    reader_type = OdsReader
    source = DATA / 'multisheet.ods'


class WorksheetBadNameTest(ReaderTest):
    class schema(Schema):
        col1 = Column(0)
    reader_kwargs = {
        'worksheet': 'Does Not Exist',
    }
    error = (
        TabFileReaderError,
        'Specified worksheet does not exist',
    )

class TestWorksheetBadNameExcel(WorksheetBadNameTest):
    reader_type = ExcelReader
    source = DATA / 'multisheet.xls'

class TestWorksheetBadNameOds(WorksheetBadNameTest):
    reader_type = OdsReader
    source = DATA / 'multisheet.ods'


class WorksheetBadRegexTest(ReaderTest):
    class schema(Schema):
        col1 = Column(0)
    reader_kwargs = {
        'worksheet': re.compile('^zzznope'),
    }
    error = (
        TabFileReaderError,
        'Specified worksheet does not exist',
    )

class TestWorksheetBadRegexExcel(WorksheetBadRegexTest):
    reader_type = ExcelReader
    source = DATA / 'multisheet.xls'

class TestWorksheetBadRegexOds(WorksheetBadRegexTest):
    reader_type = OdsReader
    source = DATA / 'multisheet.ods'
