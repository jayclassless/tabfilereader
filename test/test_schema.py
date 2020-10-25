import re

import pytest

from tabfilereader import Column, Schema, TabFileReaderError


def test_column_process():
    col = Column(0)
    assert col.process_value('foo') == 'foo'
    assert col.process_value('') is None

    col = Column(0, data_type=int)
    assert col.process_value('123') == 123
    assert col.process_value('') is None
    with pytest.raises(ValueError, match='invalid literal for int'):
        col.process_value('foo')

    col = Column(0, data_required=True)
    assert col.process_value('foo') == 'foo'
    with pytest.raises(ValueError, match='value is required'):
        col.process_value('')


def test_schema():
    class TestSchema(Schema):
        col1 = Column('foo')
        col2 = Column('bar')


def test_schema_options():
    class TestSchema(Schema, ignore_unknown_columns=True):
        col1 = Column('foo')

    class TestSchema2(Schema, ignore_unknown_columns=False):
        col1 = Column('foo')


def test_schema_options_bad():
    with pytest.raises(ValueError, match='Invalid value specified'):
        class TestSchema(Schema, ignore_unknown_columns='foo'):
            col1 = Column('foo')

    with pytest.raises(TabFileReaderError, match='Invalid keyword arguments'):
        class TestSchema(Schema, foo=False):
            col1 = Column('foo')


def test_schema_no_columns():
    with pytest.raises(TabFileReaderError, match='No columns defined'):
        class TestSchema(Schema):
            pass


def test_schema_mixed_column_location():
    with pytest.raises(TabFileReaderError, match='must be all header-based or all position-based'):
        class TestSchema(Schema):
            col1 = Column(0)
            col2 = Column('foo')

    with pytest.raises(TabFileReaderError, match='must be all header-based or all position-based'):
        class TestSchema(Schema):
            col1 = Column(0)
            col2 = Column(re.compile('foo'))


def test_schema_dupe_location():
    with pytest.raises(TabFileReaderError, match='Duplicated column locations'):
        class TestSchema(Schema):
            col1 = Column(0)
            col2 = Column(0)

    with pytest.raises(TabFileReaderError, match='Duplicated column locations'):
        class TestSchema(Schema):
            col1 = Column('foo')
            col2 = Column('foo')

