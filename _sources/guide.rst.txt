*******
Welcome
*******

.. image:: https://img.shields.io/pypi/v/tabfilereader.svg
   :target: https://pypi.python.org/pypi/tabfilereader
.. image:: https://img.shields.io/pypi/l/tabfilereader.svg
   :target: https://pypi.python.org/pypi/tabfilereader
.. image:: https://github.com/jayclassless/tabfilereader/workflows/Test/badge.svg
   :target: https://github.com/jayclassless/tabfilereader/actions
.. image:: https://github.com/jayclassless/tabfilereader/workflows/Docs/badge.svg
   :target: https://jayclassless.github.io/tabfilereader/


Overview
========
``tabfilereader`` is a small library to make reading flat, tabular data from
files a bit less tedious.

At its base, to use ``tabfilereader``, you simply define your Schema, then use
it to open a Reader. You can then iterate through the Reader to retrieve
records from the file.

    >>> import tabfilereader as tfr
    >>> class MySchema(tfr.Schema):
    ...     column1 = tfr.Column('column_1')
    ...     column2 = tfr.Column('column_2', data_type=tfr.IntegerType(), data_required=True)
    >>> reader = tfr.CsvReader.open('test/data/simple_header.csv', MySchema)
    >>> for record, errors in reader:
    ...     print(record)
    Record(column1='foo', column2=123)
    Record(column1='bar', column2=None)


Schemas
=======
Schema classes tell ``tabfilereader`` what columns to expect in the file, and
what datatypes the values contained in them should be cast as. You create your
schemas by defining a class that inherits from ``tabfilereader.Schema``. In
this class, you define properties that are instances of
``tabfilereader.Column``, which specify where columns are in the file, and what
their datatype is. An example::

    >>> import re
    >>> class ExampleSchema(tfr.Schema):
    ...     first = tfr.Column('First Name')
    ...     last = tfr.Column('Last Name', data_required=True)
    ...     birthdate = tfr.Column(re.compile(r'^Birth.*'), data_type=tfr.DateType())
    ...     weight = tfr.Column('Weight', data_type=tfr.FloatType(), required=False)

Columns require at least one argument that tells ``tabfilereader`` how to find
the column in the file. For files where the first record contains column names,
you can specify either:

* The exact name of the column as a string.
* An ``re.Pattern`` that will match the column name.
* A sequence of strings or ``re.Pattern`` objects that the column could
  possibly be named as.

For files that do not contain a header record, you specify the column's
location with an zero-based integer index.

Columns also take a series of optional parameters:

``required``
    To indicate whether or not it is required that this column exists in the
    file. Defaults to ``True``.

``data_required``
    To indicate whether or not the column must have a value for every record in
    the file. Defaults to ``False``.

``data_type``
    With this parameter, you can provide a ``callable`` that will receive a
    string value from the file and return a parsed and properly-typed value. If
    the value is invalid, the callable should throw a ``ValueError``.
    ``tabfilereader`` provides an array of pre-defined Types that you can use
    here for the most common data types (numbers, dates, strings, etc).
    See the API documentation for all the available pre-defined Types. This
    parameter defaults to ``tabfilereader.StringType()`` if not specified.

There are also a handful of optional parameteres that can be declared on the
Schema itself. The available options are:

``ignore_unknown_columns``
    To indicate what should be done if a Reader finds columns in the file that
    are not declared in the Schema. Defaults to ``False``, which means the
    Reader will throw an exception.

``ignore_empty_records``
    To indicate what should be done if a Reader encounters a record with no
    columns whatsoever. Defaults to ``False``, which means the reader will
    return a record that is full of errors. This option is particularly useful
    for CSV files when people are a bit sloppy with their newlines at the end
    of a file.

To set these Schema-level options, pass them as keyword arguments in the class
declaration::

    >>> class SchemaWithOptions(tfr.Schema, ignore_unknown_columns=True):
    ...     column1 = tfr.Column('column_1')


Readers
=======
Readers use the Schemas to interpret the contents of the tabular files.
``tabfilereader`` provides the following Readers to handle various types of
files:

``CsvReader``
    Handles Comma Separated Value files (or similarly-constructed files; TSV,
    etc).

``ExcelReader``
    Handles Excel spreadsheets; either XLS- or XLSX-formatted.

``OdsReader``
    Handles OpenDocumentFormat spreadsheets.

Readers can be created by either calling the ``open()`` classmethod on the
specific Reader class you want to use, or by defining your own Reader class
that inherits from one provided by ``tabfilereader`` like so::

    >>> class MyReader(tfr.CsvReader):
    ...     schema = MySchema
    ...     delimiter = '|'

    >>> reader = MyReader('test/data/simple_header_pipe.csv')

Each reader allows for a variety of optional parameters (like ``delimiter`` in
the example above). See the API documentation for a full listing of the options
for each.

Readers are iterable. Each iteration returns a tuple of two values. The first
value is a Record that contains the values from the file. The second value is
a collection of all the errors encountered when trying to parse the values in
the columns.

    >>> record, errors = next(reader)
    >>> record.column1
    'foo'
    >>> record['column2']
    123
    >>> bool(errors)
    False

    >>> record, errors = next(reader)
    >>> record.column1
    'bar'
    >>> record['column2'] is None
    True
    >>> bool(errors)
    True
    >>> errors['column2']
    'A value is required'


License
=======
This project is released under the terms of the `MIT License`_.

.. _MIT License: https://opensource.org/licenses/MIT

