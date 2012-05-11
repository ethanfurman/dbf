"""
=========
Copyright
=========
    - Copyright: 2008-2012 Ad-Mail, Inc -- All rights reserved.
    - Author: Ethan Furman
    - Contact: ethanf@admailinc.com
    - Organization: Ad-Mail, Inc.
    - Version: 0.93.000 as of 11 May 2012

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    - Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    - Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    - Neither the name of Ad-Mail, Inc nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY Ad-Mail, Inc ''AS IS'' AND ANY
EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL Ad-Mail, Inc BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-------
Summary
-------

Python package for reading/writing dBase III and VFP 6 tables and memos

Goals:  programming style with databases
    - table = dbf.Table('table name' [, 'fielddesc[; fielddesc[; ....]]]')
        - fielddesc examples:  name C(30); age N(3,0); wisdom M; marriage D
    - record = [ table.current() | table[int] | table.append() | table.[next|prev|top|bottom|goto]() ]
    - record.field | record['field'] accesses the field

NOTE:   Of the VFP data types, auto-increment and variable-length character fields are not implemented.
        Record data is not written to disk until record.write_record() is called; there is a utility
        generator Write to help with this:

            for record in Write(table):
                record.some_field = some_data

        at the start of each loop the previous record is written to disk.

Examples:

    Create a test table:
        table = dbf.Table('temptable', 'name C(30); age N(3,0); birth D')

    Populate it:
        for datum in (
                ('John Doe', 31, dbf.Date(1979, 9,13)),
                ('Ethan Furman', 102, dbf.Date(1909, 4, 1)),
                ('Jane Smith', 57, dbf.Date(1954, 7, 2)),
                ('John Adams', 44, dbf.Date(1967, 1, 9)),
                ):
            table.append(datum)

    Export to csv:
        table.export(filename='filename', header=False)

    Iterate over it:
        for record in table:
            print "%s was born on %s, so s/he is %d years of age" % (record.name, record.birth, record.age)

    Create a new table from a csv file (all character fields now):
        table = dbf.from_csv('filename.csv') # this has field names of f0, f1, f2, etc
    or
        table = dbf.from_csv('filename.csv', field_names="name age birth".split())


    table = dbf.Table('temptable')  #reopen original file

    Sort it:
        name_index = table.create_index(lambda rec: rec.name)
        for record in name_index:
            print record.name

    Search using the sort:
        first = name_index.index(match=('John ',), partial=True) # or IndexError
    or
        first = name_index.find(match=('John ',), partial=True)  # or -1

    Delete a record:
        table[1].delete_record()

    Check if a record has been marked as deleted:
        record = table[1] # for example
        if record.has_been_deleted:
            print "table.pack() will physically remove this record! (and all other deleted records)"

    Ignore deleted records:
        table.use_deleted = False

    Write records:
        record.write_record()  # returns 1 if the record was actually written, 0 otherwise

    Specify data to write with write_record():
        record.write_record(age=39)

    Access record fields via attribute access:
        for record in table:
            print record.name
            record.name = 'The Black Knight was here!'      # not saved without a record.write_record() !
            print record.name

    or dictionary-style access:
        for record in table:
            print record['age']
            record['age'] = 29 # perpetual age of some ;)   # not saved without a record.write_record() !
            print record['age']

    and let's not forget index-style access:
        for record in table:
            print record[2]
            record[2] = dbf.Date.today() # just born?       # not saved without a record.write_record() !
            print record[2]

    can even use slices if you like:
        table = dbf.from_csv('filename.csv', field_names="name age birth".split())
        table[0][:2] = table[1][:2] # first record's first two fields are now the same as the second record's
        for record in table:
            print record, '\n'

    Primitive SQL (work in progress):
        records = table.sql("select * where name[0] == 'J'")
        for rec in records:
            print rec, '\n'

Field Types  -->  Python data types
  Dbf
    Character       unicode
    Date            datetime.date
    Logical         boolean
    Memo            unicode (same as character)
    Numeric         if N(x, 0) int; if N(x, 1+) float
  FP
    Float           same as Numeric
    General         binary data
    Photo           binary data
  VFP
    Currency        Decimal
    Double          float
    Integer         int
    DateTime        datetime.datetime
  Note: if any of the above are empty (nothing ever stored in that field) None is returned

"""
version = (0, 93, 0)

__all__ = (
        'Table', 'List', 'Date', 'DateTime', 'Time',
        'DbfError', 'DataOverflow', 'FieldMissing', 'NonUnicode',
        'DbfWarning', 'Eof', 'Bof', 'DoNotIndex',
        'Null', 'Char', 'Date', 'DateTime', 'Time', 'Logical', 'Quantum',
        'NullDate', 'NullDateTime', 'NullTime', 
        'Write',
        'Truth', 'Falsth', 'Unknown', 'NoneType', 'Decimal',
        'guess_table_type', 'table_type',
        'add_fields', 'delete_fields', 'get_fields', 'rename_field',
        'export', 'first_record', 'from_csv', 'info', 'structure',
        )


import codecs
import csv
import datetime
import locale
import os
import struct
import sys
import time
import weakref

from array import array
from bisect import bisect_left, bisect_right
import decimal
from decimal import Decimal
from math import floor
from types import NoneType

LOGICAL_BAD_IS_FALSE = True # if bad data in logical fields, return False? (else raise error)

input_decoding = locale.getdefaultlocale()[1]    # treat non-unicode data as ...
default_codepage = input_decoding or 'ascii'     # if no codepage specified on dbf creation, use this
temp_dir = os.environ.get("DBF_TEMP") or os.environ.get("TEMP") or ""

default_type = 'db3'    # default format if none specified
sql_user_functions = {}      # user-defined sql functions

#class Context(object):
#    "used to hold user-configurable settings"
#    def __init__(
#            record_type=_DbfRecord,
#            auto_write_on_deletion=True,
#            auto_write_in_loops=True,
#            logical_unknown_is_None=True,
#            ):
#        pass

# 2.7 constructs
if sys.version_info[:2] < (2, 7):
    # Decimal does not accept float inputs until 2.7
    def Decimal(val=0):
        if isinstance(val, float):
            return decimal.Decimal(str(val))
        return decimal.Decimal(val)

    bytes = str

# 2.6 constructs
if sys.version_info[:2] < (2, 6):
    # define next()
    def next(iterator):
        return iterator.next()

    # 2.6+ property for 2.5-,
    # define our own property type
    class property(object):
        "2.6 properties for 2.5-"    
        def __init__(self, fget=None, fset=None, fdel=None, doc=None):
            self.fget = fget
            self.fset = fset
            self.fdel = fdel
            self.__doc__ = doc or fget.__doc__
        def __call__(self, func):
            self.fget = func
            if not self.__doc__:
                self.__doc__ = fget.__doc__
        def __get__(self, obj, objtype=None):
            if obj is None:
                return self         
            if self.fget is None:
                raise AttributeError("unreadable attribute")
            return self.fget(obj)
        def __set__(self, obj, value):
            if self.fset is None:
                raise AttributeError("can't set attribute")
            self.fset(obj, value)
        def __delete__(self, obj):
            if self.fdel is None:
                raise AttributeError("can't delete attribute")
            self.fdel(obj)
        def setter(self, func):
            self.fset = func
            return self
        def deleter(self, func):
            self.fdel = func
            return self

# 2.5 constructs

try:
    all
except NameError:
    def all(iterable):
        for element in iterable:
            if not element:
                return False
        return True

    def any(iterable):
        for element in iterable:
            if element:
                return True
        return False
    SEEK_SET, SEEK_CUR, SEEK_END = range(3)
else:
    SEEK_SET, SEEK_CUR, SEEK_END = os.SEEK_SET, os.SEEK_CUR, os.SEEK_END


try:
    from collections import defaultdict
except ImportError:
    class defaultdict(dict):
        def __init__(self, default_factory=None, *a, **kw):
            if (default_factory is not None and
                not hasattr(default_factory, '__call__')):
                raise TypeError('first argument must be callable')
            dict.__init__(self, *a, **kw)
            self.default_factory = default_factory
        def __getitem__(self, key):
            try:
                return dict.__getitem__(self, key)
            except KeyError:
                return self.__missing__(key)
        def __missing__(self, key):
            if self.default_factory is None:
                raise KeyError(key)
            self[key] = value = self.default_factory()
            return value
        def __reduce__(self):
            if self.default_factory is None:
                args = tuple()
            else:
                args = self.default_factory,
            return type(self), args, None, None, self.iteritems()
        def copy(self):
            return self.__copy__()
        def __copy__(self):
            return type(self)(self.default_factory, self)
        def __deepcopy__(self, memo):
            import copy
            return type(self)(self.default_factory,
                              copy.deepcopy(self.items()))
        def __repr__(self):
            return 'defaultdict(%s, %s)' % (self.default_factory,
                                            dict.__repr__(self))
# other constructs
class MutableDefault(object):
    """Lives in the class, and on first access calls the supplied factory and
    maps the result into the instance it was called on"""
    def __init__(self, func):
        self._name = func.__name__
        self.func = func
    def __call__(self):
        return self
    def __get__(self, instance, owner):
        result = self.func()
        if instance is not None:
            setattr(instance, self._name, result)
        return result
    def __repr__(self):
        result = self.func()
        return "MutableDefault(%r)" % (result, )


def none(*args, **kwargs):
    return None
# Constants
SYSTEM = 0x01
NULLABLE = 0x02
BINARY = 0x04
 #AUTOINC = 0x0c         # not currently supported (not vfp 6)

TYPE = 0
START = 1
LENGTH = 2
END = 3
DECIMALS = 4
FLAGS = 5
CLASS = 6
EMPTY = 7
NULL = 8

FIELD_FLAGS = {
        'null':NULLABLE,
        'binary':BINARY,
        'nocptrans':BINARY,
        #'autoinc':AUTOINC,

        NULLABLE:'null',
        BINARY:'binary',
        SYSTEM:'system',
        #AUTOINC:'autoinc',
        }

YES_I_AM_SURE = 42      # any true value will do

# warnings and errors
class DbfError(Exception):
    "Fatal errors elicit this response."
class DataOverflow(DbfError):
    "Data too large for field"
    def __init__(self, message, data=None):
        DbfError.__init__(self, message)
        self.data = data
class BadData(DbfError):
    "bad data in table"
    def __init__(self, message, data=None):
        DbfError.__init__(self, message)
        self.data = data
class FieldMissing(KeyError, DbfError):
    "Field does not exist in table"
    def __init__(self, fieldname):
        KeyError.__init__(self, '%s:  no such field in table' % fieldname)
        DbfError.__init__(self, '%s:  no such field in table' % fieldname)
        self.data = fieldname
class NonUnicode(DbfError):
    "Data for table not in unicode"
    def __init__(self, message=None):
        DbfError.__init__(self, message)
class DbfWarning(Exception):
    "Normal operations elicit this response"
class Eof(DbfWarning, StopIteration):
    "End of file reached"
    message = 'End of file reached'
    def __init__(self):
        StopIteration.__init__(self, self.message)
        DbfWarning.__init__(self, self.message)
class Bof(DbfWarning, StopIteration):
    "Beginning of file reached"
    message = 'Beginning of file reached'
    def __init__(self):
        StopIteration.__init__(self, self.message)
        DbfWarning.__init__(self, self.message)
class DoNotIndex(DbfWarning):
    "Returned by indexing functions to suppress a record from becoming part of the index"
    message = 'Not indexing record'
    def __init__(self):
        DbfWarning.__init__(self, self.message)
# wrappers around datetime and logical objects to allow null values
Unknown = Other = object() # gets replaced later by their final values
class NullType(object):
    "Null object -- any interaction returns Null"
    def _null(self, *args, **kwargs):
        return self
    __eq__ = __ne__ = __ge__ = __gt__ = __le__ = __lt__ = _null
    __add__ = __iadd__ = __radd__ = _null
    __sub__ = __isub__ = __rsub__ = _null
    __mul__ = __imul__ = __rmul__ = _null
    __div__ = __idiv__ = __rdiv__ = _null
    __mod__ = __imod__ = __rmod__ = _null
    __pow__ = __ipow__ = __rpow__ = _null
    __and__ = __iand__ = __rand__ = _null
    __xor__ = __ixor__ = __rxor__ = _null
    __or__ = __ior__ = __ror__ = _null
    __truediv__ = __itruediv__ = __rtruediv__ = _null
    __floordiv__ = __ifloordiv__ = __rfloordiv__ = _null
    __lshift__ = __ilshift__ = __rlshift__ = _null
    __rshift__ = __irshift__ = __rrshift__ = _null
    __neg__ = __pos__ = __abs__ = __invert__ = _null
    __call__ = __getattr__ = _null

    def __divmod__(self, other):
        return self, self
    __rdivmod__ = __divmod__

    if sys.version_info[:2] >= (2, 6):
        __hash__ = None
    else:
        def __hash__(self):
            raise TypeError("unhashable type: 'Null'")

    def __new__(cls):
        return cls.null
    def __nonzero__(self):
        return False
    def __repr__(self):
        return '<null>'
    def __setattr__(self, name, value):
        return None
    def __setitem___(self, index, value):
        return None
    def __str__(self):
        return ''
NullType.null = object.__new__(NullType)
Null = NullType()

class Char(str):
    "adds null capable str constructs"
    def __new__(cls, text=''):
        if not isinstance(text, (str, unicode, cls)):
            raise ValueError("Unable to automatically coerce %r to Char" % text)
        return str.__new__(cls, text)
    def __eq__(self, other):
        "ignores trailing whitespace"
        if not isinstance(other, (self.__class__, str)):
            return NotImplemented
        return self.rstrip() == other.rstrip()
    def __ge__(self, other):
        "ignores trailing whitespace"
        if not isinstance(other, (self.__class__, str)):
            return NotImplemented
        return self.rstrip() >= other.rstrip()
    def __gt__(self, other):
        "ignores trailing whitespace"
        if not isinstance(other, (self.__class__, str)):
            return NotImplemented
        return self.rstrip() > other.rstrip()
    def __le__(self, other):
        "ignores trailing whitespace"
        if not isinstance(other, (self.__class__, str)):
            return NotImplemented
        return self.rstrip() <= other.rstrip()
    def __lt__(self, other):
        "ignores trailing whitespace"
        if not isinstance(other, (self.__class__, str)):
            return NotImplemented
        return self.rstrip() < other.rstrip()
    def __ne__(self, other):
        "ignores trailing whitespace"
        if not isinstance(other, (self.__class__, str)):
            return NotImplemented
        return self.rstrip() != other.rstrip()
    def __nonzero__(self):
        "ignores trailing whitespace"
        return bool(self.rstrip())
    def __str__(self):
        return self.rstrip()

class Date(object):
    "adds null capable datetime.date constructs"
    __slots__ = ['_date']
    def __new__(cls, year=None, month=0, day=0):
        """date should be either a datetime.date or date/month/day should all be appropriate integers"""
        if year is None or year is Null:
            return cls._null_date
        nd = object.__new__(cls)
        if isinstance(year, (datetime.date)):
            nd._date = year
        elif isinstance(year, (Date)):
            nd._date = year._date
        else:
            nd._date = datetime.date(year, month, day)
        return nd
    def __add__(self, other):
        if self and isinstance(other, (datetime.timedelta)):
            return Date(self._date + other)
        else:
            return NotImplemented
    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._date == other._date
        if isinstance(other, datetime.date):
            return self._date == other
        if isinstance(other, type(None)):
            return self._date is None
        return NotImplemented
    def __getattr__(self, name):
        return self._date.__getattribute__(name)
    def __ge__(self, other):
        if isinstance(other, (datetime.date)):
            return self._date >= other
        elif isinstance(other, (Date)):
            if other:
                return self._date >= other._date
            return False
        return NotImplemented
    def __gt__(self, other):
        if isinstance(other, (datetime.date)):
            return self._date > other
        elif isinstance(other, (Date)):
            if other:
                return self._date > other._date
            return True
        return NotImplemented
    def __hash__(self):
        return hash(self._date)
    def __le__(self, other):
        if self:
            if isinstance(other, (datetime.date)):
                return self._date <= other
            elif isinstance(other, (Date)):
                if other:
                    return self._date <= other._date
                return False
        else:
            if isinstance(other, (datetime.date)):
                return True
            elif isinstance(other, (Date)):
                if other:
                    return True
                return True
        return NotImplemented
    def __lt__(self, other):
        if self:
            if isinstance(other, (datetime.date)):
                return self._date < other
            elif isinstance(other, (Date)):
                if other:
                    return self._date < other._date
                return False
        else:
            if isinstance(other, (datetime.date)):
                return True
            elif isinstance(other, (Date)):
                if other:
                    return True
                return False
        return NotImplemented
    def __ne__(self, other):
        if self:
            if isinstance(other, (datetime.date)):
                return self._date != other
            elif isinstance(other, (Date)):
                if other:
                    return self._date != other._date
                return True
        else:
            if isinstance(other, (datetime.date)):
                return True
            elif isinstance(other, (Date)):
                if other:
                    return True
                return False
        return NotImplemented
    def __nonzero__(self):
        return bool(self._date)
    __radd__ = __add__
    def __rsub__(self, other):
        if self and isinstance(other, (datetime.date)):
            return other - self._date
        elif self and isinstance(other, (Date)):
            return other._date - self._date
        elif self and isinstance(other, (datetime.timedelta)):
            return Date(other - self._date)
        else:
            return NotImplemented
    def __repr__(self):
        if self:
            return "Date(%d, %d, %d)" % self.timetuple()[:3]
        else:
            return "Date()"
    def __str__(self):
        if self:
            return self.isoformat()
        return ""
    def __sub__(self, other):
        if self and isinstance(other, (datetime.date)):
            return self._date - other
        elif self and isinstance(other, (Date)):
            return self._date - other._date
        elif self and isinstance(other, (datetime.timedelta)):
            return Date(self._date - other)
        else:
            return NotImplemented
    def date(self):
        if self:
            return self._date
        return None
    @classmethod
    def fromordinal(cls, number):
        if number:
            return cls(datetime.date.fromordinal(number))
        return cls()
    @classmethod
    def fromtimestamp(cls, timestamp):
        return cls(datetime.date.fromtimestamp(timestamp))
    @classmethod
    def fromymd(cls, yyyymmdd):
        if yyyymmdd in ('', '        ','no date'):
            return cls()
        return cls(datetime.date(int(yyyymmdd[:4]), int(yyyymmdd[4:6]), int(yyyymmdd[6:])))
    def strftime(self, format):
        if self:
            return self._date.strftime(format)
        return ''
    @classmethod
    def today(cls):
        return cls(datetime.date.today())
    def ymd(self):
        if self:
            return "%04d%02d%02d" % self.timetuple()[:3]
        else:
            return '        '
Date.max = Date(datetime.date.max)
Date.min = Date(datetime.date.min)
Date._null_date = object.__new__(Date)
Date._null_date._date = None
NullDate = Date()

class DateTime(object):
    "adds null capable datetime.datetime constructs"
    __slots__ = ['_datetime']
    def __new__(cls, year=None, month=0, day=0, hour=0, minute=0, second=0, microsec=0):
        """year may be a datetime.datetime"""
        if year is None or year is Null:
            return cls._null_datetime
        ndt = object.__new__(cls)
        if isinstance(year, (datetime.datetime)):
            ndt._datetime = year
        elif isinstance(year, (DateTime)):
            ndt._datetime = year._datetime
        elif year is not None:
            ndt._datetime = datetime.datetime(year, month, day, hour, minute, second, microsec)
        return ndt
    def __add__(self, other):
        if self and isinstance(other, (datetime.timedelta)):
            return DateTime(self._datetime + other)
        else:
            return NotImplemented
    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._datetime == other._datetime
        if isinstance(other, datetime.date):
            return self._datetime == other
        if isinstance(other, type(None)):
            return self._datetime is None
        return NotImplemented
    def __getattr__(self, name):
        if self:
            attribute = self._datetime.__getattribute__(name)
            return attribute
        else:
            raise AttributeError('null DateTime object has no attribute %s' % name)
    def __ge__(self, other):
        if self:
            if isinstance(other, (datetime.datetime)):
                return self._datetime >= other
            elif isinstance(other, (DateTime)):
                if other:
                    return self._datetime >= other._datetime
                return False
        else:
            if isinstance(other, (datetime.datetime)):
                return False
            elif isinstance(other, (DateTime)):
                if other:
                    return False
                return True
        return NotImplemented
    def __gt__(self, other):
        if self:
            if isinstance(other, (datetime.datetime)):
                return self._datetime > other
            elif isinstance(other, (DateTime)):
                if other:
                    return self._datetime > other._datetime
                return True
        else:
            if isinstance(other, (datetime.datetime)):
                return False
            elif isinstance(other, (DateTime)):
                if other:
                    return False
                return False
        return NotImplemented
    def __hash__(self):
        return self._datetime.__hash__()
    def __le__(self, other):
        if self:
            if isinstance(other, (datetime.datetime)):
                return self._datetime <= other
            elif isinstance(other, (DateTime)):
                if other:
                    return self._datetime <= other._datetime
                return False
        else:
            if isinstance(other, (datetime.datetime)):
                return True
            elif isinstance(other, (DateTime)):
                if other:
                    return True
                return True
        return NotImplemented
    def __lt__(self, other):
        if self:
            if isinstance(other, (datetime.datetime)):
                return self._datetime < other
            elif isinstance(other, (DateTime)):
                if other:
                    return self._datetime < other._datetime
                return False
        else:
            if isinstance(other, (datetime.datetime)):
                return True
            elif isinstance(other, (DateTime)):
                if other:
                    return True
                return False
        return NotImplemented
    def __ne__(self, other):
        if self:
            if isinstance(other, (datetime.datetime)):
                return self._datetime != other
            elif isinstance(other, (DateTime)):
                if other:
                    return self._datetime != other._datetime
                return True
        else:
            if isinstance(other, (datetime.datetime)):
                return True
            elif isinstance(other, (DateTime)):
                if other:
                    return True
                return False
        return NotImplemented
    def __nonzero__(self):
        return bool(self._datetime)
    __radd__ = __add__
    def __rsub__(self, other):
        if self and isinstance(other, (datetime.datetime)):
            return other - self._datetime
        elif self and isinstance(other, (DateTime)):
            return other._datetime - self._datetime
        elif self and isinstance(other, (datetime.timedelta)):
            return DateTime(other - self._datetime)
        else:
            return NotImplemented
    def __repr__(self):
        if self:
            return "DateTime(%d, %d, %d, %d, %d, %d, %d)" % (
                self._datetime.timetuple()[:6] + (self._datetime.microsecond, )
                )
        else:
            return "DateTime()"
    def __str__(self):
        if self:
            return self.isoformat()
        return ""
    def __sub__(self, other):
        if self and isinstance(other, (datetime.datetime)):
            return self._datetime - other
        elif self and isinstance(other, (DateTime)):
            return self._datetime - other._datetime
        elif self and isinstance(other, (datetime.timedelta)):
            return DateTime(self._datetime - other)
        else:
            return NotImplemented
    @classmethod
    def combine(cls, date, time):
        if Date(date) and Time(time):
            return cls(date.year, date.month, date.day, time.hour, time.minute, time.second, time.microsecond)
        return cls()
    def date(self):
        if self:
            return Date(self.year, self.month, self.day)
        return Date()
    def datetime(self):
        if self:
            return self._datetime
        return None
    @classmethod    
    def fromordinal(cls, number):
        if number:
            return cls(datetime.datetime.fromordinal(number))
        else:
            return cls()
    @classmethod
    def fromtimestamp(cls, timestamp):
        return DateTime(datetime.datetime.fromtimestamp(timestamp))
    @classmethod
    def now(cls):
        return cls(datetime.datetime.now())
    def time(self):
        if self:
            return Time(self.hour, self.minute, self.second, self.microsecond)
        return Time()
    @classmethod
    def utcnow(cls):
        return cls(datetime.datetime.utcnow())
    @classmethod
    def today(cls):
        return cls(datetime.datetime.today())
DateTime.max = DateTime(datetime.datetime.max)
DateTime.min = DateTime(datetime.datetime.min)
DateTime._null_datetime = object.__new__(DateTime)
DateTime._null_datetime._datetime = None
NullDateTime = DateTime()

class Time(object):
    "adds null capable datetime.time constructs"
    __slots__ = ['_time']
    def __new__(cls, hour=None, minute=0, second=0, microsec=0):
        """hour may be a datetime.time"""
        if hour is None or hour is Null:
            return cls._null_time
        nt = object.__new__(cls)
        if isinstance(hour, (datetime.time)):
            nt._time = hour
        elif isinstance(hour, (Time)):
            nt._time = hour._time
        elif hour is not None:
            nt._time = datetime.time(hour, minute, second, microsec)
        return nt
    def __add__(self, other):
        if self and isinstance(other, (datetime.timedelta)):
            t = self._time
            t = datetime.datetime(2012, 6, 27, t.hour, t.minute, t.second, t.microsecond)
            t += other
            return Time(t.hour, t.minute, t.second, t.microsecond)
        else:
            return NotImplemented
    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._time == other._time
        if isinstance(other, datetime.time):
            return self._time == other
        if isinstance(other, type(None)):
            return self._time is None
        return NotImplemented
    def __getattr__(self, name):
        if self:
            attribute = self._time.__getattribute__(name)
            return attribute
        else:
            raise AttributeError('null Time object has no attribute %s' % name)
    def __ge__(self, other):
        if self:
            if isinstance(other, (datetime.time)):
                return self._time >= other
            elif isinstance(other, (Time)):
                if other:
                    return self._time >= other._time
                return False
        else:
            if isinstance(other, (datetime.time)):
                return False
            elif isinstance(other, (Time)):
                if other:
                    return False
                return True
        return NotImplemented
    def __gt__(self, other):
        if self:
            if isinstance(other, (datetime.time)):
                return self._time > other
            elif isinstance(other, (DateTime)):
                if other:
                    return self._time > other._time
                return True
        else:
            if isinstance(other, (datetime.time)):
                return False
            elif isinstance(other, (Time)):
                if other:
                    return False
                return False
        return NotImplemented
    def __hash__(self):
        return self._datetime.__hash__()
    def __le__(self, other):
        if self:
            if isinstance(other, (datetime.time)):
                return self._time <= other
            elif isinstance(other, (Time)):
                if other:
                    return self._time <= other._time
                return False
        else:
            if isinstance(other, (datetime.time)):
                return True
            elif isinstance(other, (Time)):
                if other:
                    return True
                return True
        return NotImplemented
    def __lt__(self, other):
        if self:
            if isinstance(other, (datetime.time)):
                return self._time < other
            elif isinstance(other, (Time)):
                if other:
                    return self._time < other._time
                return False
        else:
            if isinstance(other, (datetime.time)):
                return True
            elif isinstance(other, (Time)):
                if other:
                    return True
                return False
        return NotImplemented
    def __ne__(self, other):
        if self:
            if isinstance(other, (datetime.time)):
                return self._time != other
            elif isinstance(other, (Time)):
                if other:
                    return self._time != other._time
                return True
        else:
            if isinstance(other, (datetime.time)):
                return True
            elif isinstance(other, (Time)):
                if other:
                    return True
                return False
        return NotImplemented
    def __nonzero__(self):
        return bool(self._time)
    __radd__ = __add__
    def __rsub__(self, other):
        if self and isinstance(other, (Time, datetime.time)):
            t = self._time
            t = datetime.datetime(2012, 6, 27, t.hour, t.minute, t.second, t.microsecond)
            other = datetime.datetime(2012, 6, 27, other.hour, other.minute, other.second, other.microsecond)
            other -= t
            return other
        else:
            return NotImplemented
    def __repr__(self):
        if self:
            return "Time(%d, %d, %d, %d)" % (self.hour, self.minute, self.second, self.microsecond)
        else:
            return "Time()"
    def __str__(self):
        if self:
            return self.isoformat()
        return ""
    def __sub__(self, other):
        if self and isinstance(other, (Time, datetime.time)):
            t = self._time
            t = datetime.datetime(2012, 6, 27, t.hour, t.minute, t.second, t.microsecond)
            o = datetime.datetime(2012, 6, 27, other.hour, other.minute, other.second, other.microsecond)
            t -= other
            return Time(t.hour, t.minute, t.second, t.microsecond)
        elif self and isinstance(other, (datetime.timedelta)):
            t = self._time
            t = datetime.datetime(2012, 6, 27, t.hour, t.minute, t.second, t.microsecond)
            t -= other
            return Time(t.hour, t.minute, t.second, t.microsecond)
        else:
            return NotImplemented
    @staticmethod
    def now():
        return DateTime.now().time()
Time.max = Time(datetime.time.max)
Time.min = Time(datetime.time.min)
Time._null_time = object.__new__(Time)
Time._null_time._time = None
NullTime = Time()

class Logical(object):
    """return type for Logical fields; 
    can take the values of True, False, or None/Null"""
    def __new__(cls, value=None):
        if value is None or value is Null or value is Other or value is Unknown:
            return cls.unknown
        elif isinstance(value, (str, unicode)):
            if value.lower() in ('t','true','y','yes','on'):
                return cls.true
            elif value.lower() in ('f','false','n','no','off'):
                return cls.false
            elif value.lower() in ('?','unknown','null','none',' ',''):
                return cls.unknown
            else:
                raise ValueError('unknown value for Logical: %s' % value)
        else:
            return (cls.false, cls.true)[bool(value)]
    def __add__(x, y):
        if isinstance(y, type(None)) or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(y)
        except Exception:
            return NotImplemented
        return int(x) + i

    __radd__ = __iadd__ = __add__
    def __sub__(x, y):
        if isinstance(y, type(None)) or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(y)
        except Exception:
            return NotImplemented
        return int(x) - i
    __isub__ = __sub__
    def __rsub__(y, x):
        if isinstance(x, type(None)) or x is Unknown or y is Unknown:
            return Unknown
        try:
            i = int(x)
        except Exception:
            return NotImplemented
        return i - int(y)
    def __mul__(x, y):
        if x == 0 or y == 0:
            return 0
        elif isinstance(y, type(None)) or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(y)
        except Exception:
            return NotImplemented
        return int(x) * i
    __rmul__ = __imul__ = __mul__
    def __div__(x, y):
        if isinstance(y, type(None)) or y == 0 or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(y)
        except Exception:
            return NotImplemented
        return int(x).__div__(i)
    __idiv__ = __div__
    def __rdiv__(y, x):
        if isinstance(x, type(None)) or y == 0 or x is Unknown or y is Unknown:
            return Unknown
        try:
            i = int(x)
        except Exception:
            return NotImplemented
        return i.__div__(int(y))
    def __truediv__(x, y):
        if isinstance(y, type(None)) or y == 0 or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(y)
        except Exception:
            return NotImplemented
        return int(x).__truediv__(i)
    __itruediv__ = __truediv__
    def __rtruediv__(y, x):
        if isinstance(x, type(None)) or y == 0 or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(x)
        except Exception:
            return NotImplemented
        return i.__truediv__(int(y))
    def __floordiv__(x, y):
        if isinstance(y, type(None)) or y == 0 or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(y)
        except Exception:
            return NotImplemented
        return int(x).__floordiv__(i)
    __ifloordiv__ = __floordiv__
    def __rfloordiv__(y, x):
        if isinstance(x, type(None)) or y == 0 or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(x)
        except Exception:
            return NotImplemented
        return i.__floordiv__(int(y))
    def __divmod__(x, y):
        if isinstance(y, type(None)) or y == 0 or y is Unknown or x is Unknown:
            return (Unknown, Unknown)
        try:
            i = int(y)
        except Exception:
            return NotImplemented
        return divmod(int(x), i)
    def __rdivmod__(y, x):
        if isinstance(x, type(None)) or y == 0 or y is Unknown or x is Unknown:
            return (Unknown, Unknown)
        try:
            i = int(x)
        except Exception:
            return NotImplemented
        return divmod(i, int(y))
    def __mod__(x, y):
        if isinstance(y, type(None)) or y == 0 or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(y)
        except Exception:
            return NotImplemented
        return int(x) % i
    __imod__ = __mod__
    def __rmod__(y, x):
        if isinstance(x, type(None)) or y == 0 or x is Unknown or y is Unknown:
            return Unknown
        try:
            i = int(x)
        except Exception:
            return NotImplemented
        return i % int(y)
    def __pow__(x, y):
        if not isinstance(y, (x.__class__, bool, type(None), int)):
            return NotImplemented
        if isinstance(y, type(None)) or y is Unknown:
            return Unknown
        i = int(y)
        if i == 0:
            return 1
        if x is Unknown:
            return Unknown
        return int(x) ** i
    __ipow__ = __pow__
    def __rpow__(y, x):
        if not isinstance(x, (y.__class__, bool, type(None), int)):
            return NotImplemented
        if y is Unknown:
            return Unknown
        i = int(y)
        if i == 0:
            return 1
        if x is Unknown or isinstance(x, type(None)):
            return Unknown
        return int(x) ** i
    def __lshift__(x, y):
        if isinstance(y, type(None)) or x is Unknown or y is Unknown:
            return Unknown
        return int(x.value) << int(y)
    __ilshift__ = __lshift__
    def __rlshift__(y, x):
        if isinstance(x, type(None)) or x is Unknown or y is Unknown:
            return Unknown
        return int(x) << int(y)
    def __rshift__(x, y):
        if isinstance(y, type(None)) or x is Unknown or y is Unknown:
            return Unknown
        return int(x.value) >> int(y)
    __irshift__ = __rshift__
    def __rrshift__(y, x):
        if isinstance(x, type(None)) or x is Unknown or y is Unknown:
            return Unknown
        return int(x) >> int(y)
    def __neg__(x):
        "NEG (negation)"
        if x in (Truth, Falsth):
            return -x.value
        return Unknown
    def __pos__(x):
        "POS (posation)"
        if x in (Truth, Falsth):
            return +x.value
        return Unknown
    def __abs__(x):
        if x in (Truth, Falsth):
            return abs(x.value)
        return Unknown
    def __invert__(x):
        if x in (Truth, Falsth):
            return ~x.value
        return Unknown
    def __complex__(x):
        if x.value is None:
            raise ValueError("unable to return complex() of %r" % x)
        return complex(x.value)
    def __int__(x):
        if x.value is None:
            raise ValueError("unable to return int() of %r" % x)
        return int(x.value)
    def __long__(x):
        if x.value is None:
            raise ValueError("unable to return long() of %r" % x)
        return long(x.value)
    def __float__(x):
        if x.value is None:
            raise ValueError("unable to return float() of %r" % x)
        return float(x.value)
    def __oct__(x):
        if x.value is None:
            raise ValueError("unable to return oct() of %r" % x)
        return oct(x.value)
    def __hex__(x):
        if x.value is None:
            raise ValueError("unable to return hex() of %r" % x)
        return hex(x.value)
        
    def __and__(x, y):
        """AND (conjunction) x & y:
        True iff both x, y are True
        False iff at least one of x, y is False
        Unknown otherwise"""
        if (isinstance(x, int) and not isinstance(x, bool)) or (isinstance(y, int) and not isinstance(y, bool)):
            if x == 0 or y == 0:
                return 0
            elif x is Unknown or y is Unknown:
                return Unknown
            return int(x) & int(y)
        elif x in (False, Falsth) or y in (False, Falsth):
            return Falsth
        elif x in (True, Truth) and y in (True, Truth):
            return Truth
        elif isinstance(x, type(None)) or isinstance(y, type(None)) or y is Unknown or x is Unknown:
            return Unknown
        return NotImplemented
    __rand__ = __and__
    def __or__(x, y):
        "OR (disjunction): x | y => True iff at least one of x, y is True"
        if (isinstance(x, int) and not isinstance(x, bool)) or (isinstance(y, int) and not isinstance(y, bool)):
            if x is Unknown or y is Unknown:
                return Unknown
            return int(x) | int(y)
        elif x in (True, Truth) or y in (True, Truth):
            return Truth
        elif x in (False, Falsth) and y in (False, Falsth):
            return Falsth
        elif isinstance(x, type(None)) or isinstance(y, type(None)) or y is Unknown or x is Unknown:
            return Unknown
        return NotImplemented
    __ror__ = __or__
    def __xor__(x, y):
        "XOR (parity) x ^ y: True iff only one of x,y is True"
        if (isinstance(x, int) and not isinstance(x, bool)) or (isinstance(y, int) and not isinstance(y, bool)):
            if x is Unknown or y is Unknown:
                return Unknown
            return int(x) ^ int(y)
        elif x in (True, Truth, False, Falsth) and y in (True, Truth, False, Falsth):
            return {
                    (True, True)  : Falsth,
                    (True, False) : Truth,
                    (False, True) : Truth,
                    (False, False): Falsth,
                   }[(x, y)]
        elif isinstance(x, type(None)) or isinstance(y, type(None)) or y is Unknown or x is Unknown:
            return Unknown
        return NotImplemented
    __rxor__ = __xor__
    def __nonzero__(x):
        if x is Unknown:
            raise TypeError('True/False value of %r is unknown' % x)
        return x.value is True
    def __eq__(x, y):
        if isinstance(y, x.__class__):
            return x.value == y.value
        elif isinstance(y, (bool, type(None), int)):
            return x.value == y
        return NotImplemented
    def __ge__(x, y):
        if isinstance(y, type(None)) or x is Unknown or y is Unknown:
            return x.value == None
        elif isinstance(y, x.__class__):
            return x.value >= y.value
        elif isinstance(y, (bool, int)):
            return x.value >= y
        return NotImplemented
    def __gt__(x, y):
        if isinstance(y, type(None)) or x is Unknown or y is Unknown:
            return False
        elif isinstance(y, x.__class__):
            return x.value > y.value
        elif isinstance(y, (bool, int)):
            return x.value > y
        return NotImplemented
    def __le__(x, y):
        if isinstance(y, type(None)) or x is Unknown or y is Unknown:
            return x.value == None
        elif isinstance(y, x.__class__):
            return x.value <= y.value
        elif isinstance(y, (bool, int)):
            return x.value <= y
        return NotImplemented
    def __lt__(x, y):
        if isinstance(y, type(None)) or x is Unknown or y is Unknown:
            return False
        elif isinstance(y, x.__class__):
            return x.value < y.value
        elif isinstance(y, (bool, int)):
            return x.value < y
        return NotImplemented
    def __ne__(x, y):
        if isinstance(y, x.__class__):
            return x.value != y.value
        elif isinstance(y, (bool, type(None), int)):
            return x.value != y
        return NotImplemented
    def __hash__(x):
        return hash(x.value)
    def __index__(x):
        if x.value is False:
            return 0
        elif x.value is True:
            return 1
        return 2
    def __repr__(x):
        return "Logical(%r)" % x.string
    def __str__(x):
        return x.string
Logical.true = object.__new__(Logical)
Logical.true.value = True
Logical.true.string = 'T'
Logical.false = object.__new__(Logical)
Logical.false.value = False
Logical.false.string = 'F'
Logical.unknown = object.__new__(Logical)
Logical.unknown.value = None
Logical.unknown.string = '?'
Truth = Logical(True)
Falsth = Logical(False)
Unknown = Logical()

class Quantum(object):
    """return type for Logical fields; implements boolean algebra
    can take the values of True, False, or None/Null/Unknown/Other"""
    def __new__(cls, value=None):
        if value is None or value is Null or value is Other or value is Unknown:
            return cls.unknown
        elif isinstance(value, (str, unicode)):
            if value.lower() in ('t','true','y','yes','on'):
                return cls.true
            elif value.lower() in ('f','false','n','no','off'):
                return cls.false
            elif value.lower() in ('?','unknown','null','none',' ',''):
                return cls.unknown
            else:
                raise ValueError('unknown value for Quantum: %s' % value)
        else:
            return (cls.false, cls.true)[bool(value)]
    def A(x, y):
        "OR (disjunction): x | y => True iff at least one of x, y is True"
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        if x.value is True or y is not Other and y == True:
            return x.true
        elif x.value is False and y is not Other and y == False:
            return x.false
        return Other
    def _C_material(x, y):
        "IMP (material implication) x >> y => False iff x == True and y == False"
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        if (x.value is False
            or (x.value is True and y is not Other and y == True)):
            return x.true
        elif x.value is True and y is not Other and y == False:
            return False
        return Other
    def _C_material_reversed(y, x):
        "IMP (material implication) x >> y => False iff x = True and y = False"
        if not isinstance(x, (y.__class__, bool, NullType, type(None))):
            return NotImplemented
        if (x is not Other and x == False
            or (x is not Other and x == True and y.value is True)):
            return y.true
        elif x is not Other and x == True and y.value is False:
            return y.false
        return Other
    def _C_relevant(x, y):
        "IMP (relevant implication) x >> y => True iff both x, y are True, False iff x == True and y == False, Other if x is False"
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        if x.value is True and y is not Other and y == True:
            return x.true
        if x.value is True and y is not Other and y == False:
            return x.false
        return Other
    def _C_relevant_reversed(y, x):
        "IMP (relevant implication) x >> y => True iff both x, y are True, False iff x == True and y == False, Other if y is False"
        if not isinstance(x, (y.__class__, bool, NullType, type(None))):
            return NotImplemented
        if x is not Other and x == True and y.value is True:
            return y.true
        if x is not Other and x == True and y.value is False:
            return y.false
        return Other
    def D(x, y):
        "NAND (negative AND) x.D(y): False iff x and y are both True"
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        if x.value is False or y is not Other and y == False:
            return x.true
        elif x.value is True and y is not Other and y == True:
            return x.false
        return Other
    def E(x, y):
        "EQV (equivalence) x.E(y): True iff x and y are the same"
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        elif (
            (x.value is True and y is not Other and y == True)
            or
            (x.value is False and y is not Other and y == False)
            ):
            return x.true
        elif (
            (x.value is True and y is not Other and y == False)
            or
            (x.value is False and y is not Other and y == True)
            ):
            return x.false
        return Other
    def J(x, y):
        "XOR (parity) x ^ y: True iff only one of x,y is True"
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        if (
            (x.value is True and y is not Other and y == False)
            or
            (x.value is False and y is not Other and y == True)
            ):
            return x.true
        if (
            (x.value is False and y is not Other and y == False)
            or
            (x.value is True and y is not Other and y == True)
            ):
            return x.false
        return Other
    def K(x, y):
        "AND (conjunction) x & y: True iff both x, y are True"
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        if x.value is True and y is not Other and y == True:
            return x.true
        elif x.value is False or y is not Other and y == False:
            return x.false
        return Other
    def N(x):
        "NEG (negation) -x: True iff x = False"
        if x is x.true:
            return x.false
        elif x is x.false:
            return x.true
        return Other
    @classmethod
    def set_implication(cls, method):
        "sets IMP to material or relevant"
        if not isinstance(method, (str, unicode)) or method.lower() not in ('material','relevant'):
            raise ValueError("method should be 'material' (for strict boolean) or 'relevant', not %r'" % method)
        if method.lower() == 'material':
            cls.C = cls._C_material
            cls.__rshift__ = cls._C_material
            cls.__rrshift__ = cls._C_material_reversed
        elif method.lower() == 'relevant':
            cls.C = cls._C_relevant
            cls.__rshift__ = cls._C_relevant
            cls.__rrshift__ = cls._C_relevant_reversed
    def __eq__(x, y):
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        if (
            (x.value is True and y is not Other and y == True)
            or
            (x.value is False and y is not Other and y == False)
            ):
            return x.true
        elif (
            (x.value is True and y is not Other and y == False)
            or
            (x.value is False and y is not Other and y == True)
            ):
            return x.false
        return Other
    def __hash__(x):
        return hash(x.value)
    def __index__(x):
        if x.value is False:
            return 0
        if x.value is True:
            return 1
        if x.value is None:
            return 2
    def __ne__(x, y):
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        if (
            (x.value is True and y is not Other and y == False)
            or
            (x.value is False and y is not Other and y == True)
            ):
            return x.true
        elif (
            (x.value is True and y is not Other and y == True)
            or
            (x.value is False and y is not Other and y == False)
            ):
            return x.false
        return Other
    def __nonzero__(x):
        if x is Other:
            raise TypeError('True/False value of %r is unknown' % x)
        return x.value is True
    def __repr__(x):
        return "Quantum(%r)" % x.string
    def __str__(x):
        return x.string
    __add__ = A
    __and__ = K
    __mul__ = K
    __neg__ = N
    __or__ = A
    __radd__ = A
    __rand__ = K
    __rshift__ = None
    __rmul__ = K
    __ror__ = A
    __rrshift__ = None
    __rxor__ = J
    __xor__ = J
Quantum.true = object.__new__(Quantum)
Quantum.true.value = True
Quantum.true.string = 'Y'
Quantum.false = object.__new__(Quantum)
Quantum.false.value = False
Quantum.false.string = 'N'
Quantum.unknown = object.__new__(Quantum)
Quantum.unknown.value = None
Quantum.unknown.string = '?'
Quantum.set_implication('material')
On = Quantum(True)
Off = Quantum(False)
Other = Quantum()

# Internal classes
class _DbfRecord(object):
    """Provides routines to extract and save data within the fields of a dbf record."""
    __slots__ = ['_recnum', '_layout', '_data', '_dirty', '__weakref__']
    def _retrieve_field_value(self, index, name):
        """calls appropriate routine to convert value stored in field from array
        @param record_data: the data portion of the record
        @type record_data: array of characters
        @param fielddef: description of the field definition
        @type fielddef: dictionary with keys 'type', 'start', 'length', 'end', 'decimals', and 'flags'
        @returns: python data stored in field"""
        fielddef = self._layout[name]
        flags = fielddef[FLAGS]
        nullable = flags & NULLABLE
        binary = flags & BINARY
        if nullable:
            byte, bit = divmod(index, 8)
            null_def = self._layout['_nullflags']
            null_data = self._data[null_def[START]:null_def[END]]
            try:
                if ord(null_data[byte]) >> bit & 1:
                    return Null
            except IndexError:
                print null_data
                print index
                print byte, bit
                print len(self._data), self._data
                print null_def
                print null_data
                raise

        record_data = self._data[fielddef[START]:fielddef[END]]
        field_type = fielddef[TYPE]
        retrieve = self._layout.fieldtypes[field_type]['Retrieve']
        datum = retrieve(record_data, fielddef, self._layout.memo, self._layout.decoder)
        return datum
    def _update_field_value(self, index, name, value):
        "calls appropriate routine to convert value to ascii bytes, and save it in record"
        fielddef = self._layout[name]
        field_type = fielddef[TYPE]
        flags = fielddef[FLAGS]
        binary = flags & BINARY
        nullable = flags & NULLABLE
        update = self._layout.fieldtypes[field_type]['Update']
        if nullable:
            byte, bit = divmod(index, 8)
            null_def = self._layout['_nullflags']
            null_data = self._data[null_def[START]:null_def[END]].tostring()
            null_data = [ord(c) for c in null_data]
            if value is Null:
                null_data[byte] |= 1 << bit
                value = None
            else:
                null_data[byte] &= 0xff ^ 1 << bit
            null_data = array('c', [chr(n) for n in null_data])
            self._data[null_def[START]:null_def[END]] = null_data
        if value is not Null:
            bytes = array('c', update(value, fielddef, self._layout.memo, self._layout.input_decoder, self._layout.encoder))
            size = fielddef[LENGTH]
            if len(bytes) > size:
                raise DataOverflow("tried to store %d bytes in %d byte field" % (len(bytes), size))
            blank = array('c', ' ' * size)
            start = fielddef[START]
            end = start + size
            blank[:len(bytes)] = bytes[:]
            self._data[start:end] = blank[:]
        self._dirty = True
        self._layout._dirty = True
    def _update_disk(self, location='', data=None):
        layout = self._layout
        if not layout.inmemory:
            header = layout.header
            if self._recnum < 0:
                raise DbfError("Attempted to update record that has been packed")
            if location == '':
                location = self._recnum * header.record_length + header.start
            if data is None:
                data = self._data
            layout.dfd.seek(location)
            layout.dfd.write(data)
            self._dirty = False
        if layout.table() is not None:  # is None when table is being destroyed
            for index in self.record_table._indexen:
                index(self)
    def __new__(cls, recnum, layout, kamikaze='', _fromdisk=False):
        """record = ascii array of entire record; layout=record specification; memo = memo object for table"""
        record = object.__new__(cls)
        record._dirty = False
        record._recnum = recnum
        record._layout = layout
        header = layout.header
        if layout.blankrecord is None and not _fromdisk:
            record._create_blank_record()
        record._data = layout.blankrecord
        if recnum == -1:                    # not a disk-backed record
            return record
        elif type(kamikaze) == array:
            record._data = kamikaze[:]
        elif type(kamikaze) == str:
            record._data = array('c', kamikaze)
        else:
            record._data = kamikaze._data[:]
        if record._data and record._data[0] not in (' ','*'):
            raise DbfError("record data not correct -- first character should be a ' ' or a '*'.")
        datalen = len(record._data)
        if datalen < header.record_length:
            record._data.extend(layout.blankrecord[datalen:])
        elif datalen > header.record_length:
            record._data = record._data[:header.record_length]
        if not _fromdisk and not layout.inmemory:
            record._update_disk()
        return record
    def __contains__(self, key):
        return key in self._layout.user_fields or key in ['record_number','delete_flag']
    def __iter__(self):
        return (self[field] for field in self._layout.user_fields)
    def __getattr__(self, name):
        if name[0:2] == '__' and name[-2:] == '__':
            raise AttributeError, 'Method %s is not implemented.' % name
        elif name == 'record_number':
            return self._recnum
        elif name == 'delete_flag':
            return self._data[0] != ' '
        elif not name in self._layout.fields:
            raise FieldMissing(name)
        try:
            index = self._layout.fields.index(name)
            value = self._retrieve_field_value(index, name)
            return value
        except DbfError, error:
            error.message = "field --%s-- is %s -> %s" % (name, self._layout.fieldtypes[fielddef['type']]['Type'], error.message)
            raise
    def __getitem__(self, item):
        if isinstance(item, (int, long)):
            fields = self._layout.user_fields
            field_count = len(fields)
            if not -field_count <= item < field_count:
                raise IndexError("Field offset %d is not in record" % item)
            return self[fields[item]]
        elif isinstance(item, slice):
            sequence = []
            for index in self._layout.fields[item]:
                sequence.append(self[index])
            return sequence
        elif isinstance(item, (str, unicode)):
            return self.__getattr__(item)
        else:
            raise TypeError("%r is not a field name" % item)
    def __len__(self):
        return self._layout.user_field_count
    def __setattr__(self, name, value):
        if name in self.__slots__:
            object.__setattr__(self, name, value)
            return
        elif not name in self._layout.fields:
            raise FieldMissing(name)
        index = self._layout.fields.index(name)
        try:
            self._update_field_value(index, name, value)
        except DbfError, error:
            fielddef = self._layout[name]
            message = "%s (%s) = %r --> %s" % (name, self._layout.fieldtypes[fielddef[TYPE]]['Type'], value, error.message)
            data = name
            err_cls = error.__class__
            raise err_cls(message, data)
    def __setitem__(self, name, value):
        if isinstance(name, (str, unicode)):
            self.__setattr__(name, value)
        elif isinstance(name, (int, long)):
            self.__setattr__(self._layout.fields[name], value)
        elif isinstance(name, slice):
            sequence = []
            for field in self._layout.fields[name]:
                sequence.append(field)
            if len(sequence) != len(value):
                raise DbfError("length of slices not equal")
            for field, val in zip(sequence, value):
                self[field] = val
        else:
            raise TypeError("%s is not a field name" % name)
    def __str__(self):
        result = []
        for seq, field in enumerate(self.field_names):
            result.append("%3d - %-10s: %r" % (seq, field, self[field]))
        return '\n'.join(result)
    def __repr__(self):
        return self._data.tostring()
    #@classmethod
    def _create_blank_record(self):
        "creates a blank record data chunk"
        layout = self._layout
        ondisk = layout.ondisk
        layout.ondisk = False
        self._data = array('c', ' ' * layout.header.record_length)
        layout.memofields = []
        for index, name in enumerate(layout.fields):
            if name == '_nullflags':
                self._data[layout['_nullflags'][START]:layout['_nullflags'][END]] = array('c', chr(0) * layout['_nullflags'][LENGTH])
        for index, name in enumerate(layout.fields):
            if name != '_nullflags':
                self._update_field_value(index, name, None)
                if layout[name][TYPE] in layout.memotypes:
                    layout.memofields.append(name)
        layout.blankrecord = self._data[:]
        layout.ondisk = ondisk
    def delete_record(self):
        "marks record as deleted"
        self._data[0] = '*'
        self._dirty = True
        return self
    @property
    def field_names(self):
        "fields in table/record"
        return self._layout.user_fields[:]
    def gather_fields(self, dictionary, drop=False):
        """saves a dictionary into a record's fields
        keys with no matching field will raise a FieldMissing exception unless drop_missing = True
        if an Exception occurs the record is restored before reraising"""
        old_data = self._data[:]
        try:
            for key, value in dictionary.items():
                if not key in self.field_names:
                    if drop:
                        continue
                    raise FieldMissing(key)
                self.__setattr__(key, value)
        except:
            self._data[:] = old_data
            raise
        return self
    @property
    def has_been_deleted(self):
        "marked for deletion?"
        return self._data[0] == '*'
    @property
    def record_number(self):
        "physical record number"
        return self._recnum
    @property
    def record_table(self):
        "table associated with record"
        table = self._layout.table()
        if table is None:
            raise DbfError("table is no longer available")
        return table
    def reindex(self):
        "rerun all indices with this record"
        for dbfindex in self._layout.table()._indexen:
            dbfindex(self)
    def reset_record(self, keep_fields=None):
        "blanks record, except for field in keep_fields"
        if keep_fields is None:
            keep_fields = []
        keep = {}
        for field in keep_fields:
            keep[field] = self[field]
        if self._layout.blankrecord == None:
            self._create_blank_record()
        self._data[:] = self._layout.blankrecord[:]
        for field in keep_fields:
            self[field] = keep[field]
        self._dirty = True
        return self
    def scatter_fields(self, blank=False):
        "returns a dictionary of fieldnames and values which can be used with gather_fields().  if blank is True, values are empty."
        keys = self._layout.user_fields
        if blank:
            values = []
            layout = self._layout
            for key in keys:
                fielddef = layout[key]
                empty = fielddef[EMPTY]
                values.append(empty())
        else:
            values = [self[field] for field in keys]
        return dict(zip(keys, values))
    def undelete_record(self):
        "marks record as active"
        self._data[0] = ' '
        self._dirty = True
        return self
    def write_record(self, _force=False, **kwargs):
        "write record data to disk (updates indices)"
        if kwargs:
            self.gather_fields(kwargs)
        if self._dirty or _force:
            self._update_disk()
            return 1
        return 0
class _DbfMemo(object):
    """Provides access to memo fields as dictionaries
       must override _init, _get_memo, and _put_memo to
       store memo contents to disk"""
    def _init(self):
        "initialize disk file usage"
    def _get_memo(self, block):
        "retrieve memo contents from disk"
    def _put_memo(self, data):
        "store memo contents to disk"
    def _zap(self):
        "resets memo structure back to zero memos"
        self.memory.clear()
        self.nextmemo = 1
    def __init__(self, meta):
        ""
        self.meta = meta
        self.memory = {}
        self.nextmemo = 1
        self._init()
        self.meta.newmemofile = False
    def get_memo(self, block):
        "gets the memo in block"
        if self.meta.ignorememos or not block:
            return ''
        if self.meta.ondisk:
            return self._get_memo(block)
        else:
            return self.memory[block]
    def put_memo(self, data):
        "stores data in memo file, returns block number"
        if self.meta.ignorememos or data == '':
            return 0
        if self.meta.inmemory:
            thismemo = self.nextmemo
            self.nextmemo += 1
            self.memory[thismemo] = data
        else:
            thismemo = self._put_memo(data)
        return thismemo
class _Db3Memo(_DbfMemo):
    def _init(self):
        "dBase III specific"
        self.meta.memo_size= 512
        self.record_header_length = 2
        if self.meta.ondisk and not self.meta.ignorememos:
            if self.meta.newmemofile:
                self.meta.mfd = open(self.meta.memoname, 'w+b')
                self.meta.mfd.write(pack_long_int(1) + '\x00' * 508)
            else:
                try:
                    self.meta.mfd = open(self.meta.memoname, 'r+b')
                    self.meta.mfd.seek(0)
                    next = self.meta.mfd.read(4)
                    self.nextmemo = unpack_long_int(next)
                except Exception, exc:
                    raise DbfError("memo file appears to be corrupt: %r" % exc.args)
    def _get_memo(self, block):
        block = int(block)
        self.meta.mfd.seek(block * self.meta.memo_size)
        eom = -1
        data = ''
        while eom == -1:
            newdata = self.meta.mfd.read(self.meta.memo_size)
            if not newdata:
                return data
            data += newdata
            eom = data.find('\x1a\x1a')
        return data[:eom]
    def _put_memo(self, data):
        data = data
        length = len(data) + self.record_header_length  # room for two ^Z at end of memo
        blocks = length // self.meta.memo_size
        if length % self.meta.memo_size:
            blocks += 1
        thismemo = self.nextmemo
        self.nextmemo = thismemo + blocks
        self.meta.mfd.seek(0)
        self.meta.mfd.write(pack_long_int(self.nextmemo))
        self.meta.mfd.seek(thismemo * self.meta.memo_size)
        self.meta.mfd.write(data)
        self.meta.mfd.write('\x1a\x1a')
        double_check = self._get_memo(thismemo)
        if len(double_check) != len(data):
            uhoh = open('dbf_memo_dump.err','wb')
            uhoh.write('thismemo: %d' % thismemo)
            uhoh.write('nextmemo: %d' % self.nextmemo)
            uhoh.write('saved: %d bytes' % len(data))
            uhoh.write(data)
            uhoh.write('retrieved: %d bytes' % len(double_check))
            uhoh.write(double_check)
            uhoh.close()
            raise DbfError("unknown error: memo not saved")
        return thismemo
    def _zap(self):
        if self.meta.ondisk and not self.meta.ignorememos:
            mfd = self.meta.mfd
            mfd.seek(0)
            mfd.truncate(0)
            mfd.write(pack_long_int(1) + '\x00' * 508)
            mfd.flush()

class _VfpMemo(_DbfMemo):
    def _init(self):
        "Visual Foxpro 6 specific"
        if self.meta.ondisk and not self.meta.ignorememos:
            self.record_header_length = 8
            if self.meta.newmemofile:
                if self.meta.memo_size == 0:
                    self.meta.memo_size = 1
                elif 1 < self.meta.memo_size < 33:
                    self.meta.memo_size *= 512
                self.meta.mfd = open(self.meta.memoname, 'w+b')
                nextmemo = 512 // self.meta.memo_size
                if nextmemo * self.meta.memo_size < 512:
                    nextmemo += 1
                self.nextmemo = nextmemo
                self.meta.mfd.write(pack_long_int(nextmemo, bigendian=True) + '\x00\x00' + \
                        pack_short_int(self.meta.memo_size, bigendian=True) + '\x00' * 504)
            else:
                try:
                    self.meta.mfd = open(self.meta.memoname, 'r+b')
                    self.meta.mfd.seek(0)
                    header = self.meta.mfd.read(512)
                    self.nextmemo = unpack_long_int(header[:4], bigendian=True)
                    self.meta.memo_size = unpack_short_int(header[6:8], bigendian=True)
                except Exception, exc:
                    raise DbfError("memo file appears to be corrupt: %r" % exc.args)
    def _get_memo(self, block):
        self.meta.mfd.seek(block * self.meta.memo_size)
        header = self.meta.mfd.read(8)
        length = unpack_long_int(header[4:], bigendian=True)
        return self.meta.mfd.read(length)
    def _put_memo(self, data):
        data = data
        self.meta.mfd.seek(0)
        thismemo = unpack_long_int(self.meta.mfd.read(4), bigendian=True)
        self.meta.mfd.seek(0)
        length = len(data) + self.record_header_length
        blocks = length // self.meta.memo_size
        if length % self.meta.memo_size:
            blocks += 1
        self.meta.mfd.write(pack_long_int(thismemo+blocks, bigendian=True))
        self.meta.mfd.seek(thismemo*self.meta.memo_size)
        self.meta.mfd.write('\x00\x00\x00\x01' + pack_long_int(len(data), bigendian=True) + data)
        return thismemo
    def _zap(self):
        if self.meta.ondisk and not self.meta.ignorememos:
            mfd = self.meta.mfd
            mfd.seek(0)
            mfd.truncate(0)
            nextmemo = 512 // self.meta.memo_size
            if nextmemo * self.meta.memo_size < 512:
                nextmemo += 1
            self.nextmemo = nextmemo
            mfd.write(pack_long_int(nextmemo, bigendian=True) + '\x00\x00' + \
                    pack_short_int(self.meta.memo_size, bigendian=True) + '\x00' * 504)
            mfd.flush()

class DbfCsv(csv.Dialect):
    "csv format for exporting tables"
    delimiter = ','
    doublequote = True
    escapechar = None
    lineterminator = '\n'
    quotechar = '"'
    skipinitialspace = True
    quoting = csv.QUOTE_NONNUMERIC
csv.register_dialect('dbf', DbfCsv)

# Routines for saving, retrieving, and creating fields

VFPTIME = 1721425

def pack_short_int(value, bigendian=False):
        "Returns a two-bye integer from the value, or raises DbfError"
        # 256 / 65,536
        if value > 65535:
            raise DataOverflow("Maximum Integer size exceeded.  Possible: 65535.  Attempted: %d" % value)
        if bigendian:
            return struct.pack('>H', value)
        else:
            return struct.pack('<H', value)
def pack_long_int(value, bigendian=False):
        "Returns a four-bye integer from the value, or raises DbfError"
        # 256 / 65,536 / 16,777,216
        if value > 4294967295:
            raise DataOverflow("Maximum Integer size exceeded.  Possible: 4294967295.  Attempted: %d" % value)
        if bigendian:
            return struct.pack('>L', value)
        else:
            return struct.pack('<L', value)
def pack_str(string):
        "Returns an 11 byte, upper-cased, null padded string suitable for field names; raises DbfError if the string is bigger than 10 bytes"
        if len(string) > 10:
            raise DbfError("Maximum string size is ten characters -- %s has %d characters" % (string, len(string)))
        return struct.pack('11s', string.upper())       
def unpack_short_int(bytes, bigendian=False):
        "Returns the value in the two-byte integer passed in"
        if bigendian:
            return struct.unpack('>H', bytes)[0]
        else:
            return struct.unpack('<H', bytes)[0]
def unpack_long_int(bytes, bigendian=False):
        "Returns the value in the four-byte integer passed in"
        if bigendian:
            return int(struct.unpack('>L', bytes)[0])
        else:
            return int(struct.unpack('<L', bytes)[0])
def unpack_str(chars):
        "Returns a normal, lower-cased string from a null-padded byte string"
        field = struct.unpack('%ds' % len(chars), chars)[0]
        name = []
        for ch in field:
            if ch == '\x00':
                break
            name.append(ch.lower())
        return ''.join(name)
def unsupported_type(something, *ignore):
    "called if a data type is not supported for that style of table"
    return something
def retrieve_character(bytes, fielddef, memo, decoder):
    "Returns the string in bytes as fielddef[CLASS] or fielddef[EMPTY]"
    data = bytes.tostring()
    if not data.strip():
        cls = fielddef[EMPTY]
        if cls is NoneType:
            return None
        return cls(data)
    if fielddef[FLAGS] & BINARY:
        return data
    return fielddef[CLASS](decoder(data)[0])
def update_character(string, fielddef, memo, decoder, encoder):
    "returns the string as bytes (not unicode) as fielddef[CLASS] or fielddef[EMPTY]"
    if string == None:
        return fielddef[LENGTH] * ' '
    if fielddef[FLAGS] & BINARY:
        if not isinstance(string, str):
            raise BadData('binary field: %r not in bytes format' % string)
        string = str(string)
        return string
    else:
        if not isinstance(string, unicode):
            if not isinstance(string, str):
                raise BadData("unable to coerce %r(%r) to string" % (type(string), string))
            string = decoder(string)[0]
        return encoder(string)[0]
def retrieve_currency(bytes, fielddef, *ignore):
    "returns the currency value in bytes"
    value = struct.unpack('<q', bytes)[0]
    return fielddef[CLASS](("%de-4" % value).strip())
def update_currency(value, *ignore):
    "returns the value to be stored in the record's disk data"
    if value == None:
        value = 0
    currency = int(value * 10000)
    if not -9223372036854775808 < currency < 9223372036854775808:
        raise DataOverflow("value %s is out of bounds" % value)
    return struct.pack('<q', currency)
def retrieve_date(bytes, fielddef, *ignore):
    "Returns the ascii coded date as fielddef[CLASS] or fielddef[EMPTY]"
    text = bytes.tostring()
    if text == '        ':
        cls = fielddef[EMPTY]
        if cls is NoneType:
            return None
        return cls()
    year = int(text[0:4])
    month = int(text[4:6])
    day = int(text[6:8])
    return fielddef[CLASS](year, month, day)
def update_date(moment, *ignore):
    "returns the Date or datetime.date object ascii-encoded (yyyymmdd)"
    if moment == None:
        return '        '
    return "%04d%02d%02d" % moment.timetuple()[:3]
def retrieve_double(bytes, fielddef, *ignore):
    "Returns the double in bytes as fielddef[CLASS] ('default' == float)"
    typ = fielddef[CLASS]
    if typ == 'default':
        typ = float
    return typ(struct.unpack('<d', bytes)[0])
def update_double(value, *ignore):
    "returns the value to be stored in the record's disk data"
    if value == None:
        value = 0
    return struct.pack('<d', float(value))
def retrieve_integer(bytes, fielddef, *ignore):
    "Returns the binary number stored in bytes in little-endian format as fielddef[CLASS]"
    typ = fielddef[CLASS]
    if typ == 'default':
        typ = int
    return typ(struct.unpack('<i', bytes)[0])
def update_integer(value, *ignore):
    "returns value in little-endian binary format"
    if value == None:
        value = 0
    try:
        value = int(value)
    except Exception:
        raise DbfError("incompatible type: %s(%s)" % (type(value), value))
    if not -2147483648 < value < 2147483647:
        raise DataOverflow("Integer size exceeded.  Possible: -2,147,483,648..+2,147,483,647.  Attempted: %d" % value)
    return struct.pack('<i', int(value))
def retrieve_logical(bytes, fielddef, *ignore):
    "Returns True if bytes is 't', 'T', 'y', or 'Y', None if '?', and False otherwise"
    cls = fielddef[CLASS]
    empty = fielddef[EMPTY]
    bytes = bytes.tostring()
    if bytes in 'tTyY':
        return cls(True)
    elif bytes in 'fFnN':
        return cls(False)
    elif bytes in '? ':
        if empty is NoneType:
            return None
        return empty()
    elif LOGICAL_BAD_IS_FALSE:
        return False
    else:
        raise BadData('Logical field contained %r' % bytes)
    return typ(bytes)
def update_logical(data, *ignore):
    "Returns 'T' if logical is True, 'F' if False, '?' otherwise"
    if data is Unknown or data is None or data is Null or data is Other:
        return '?'
    if data == True:
        return 'T'
    if data == False:
        return 'F'
    raise ValueError("unable to automatically coerce %r to Logical" % data)
def retrieve_memo(bytes, fielddef, memo, decoder):
    "Returns the block of data from a memo file"
    stringval = bytes.tostring().strip()
    if not stringval:
        cls = fielddef[EMPTY]
        if cls is NoneType:
            return None
        return cls()
    block = int(stringval)
    data = memo.get_memo(block)
    if fielddef[FLAGS] & BINARY:
        return data
    return fielddef[CLASS](decoder(data)[0])
def update_memo(string, fielddef, memo, decoder, encoder):
    "Writes string as a memo, returns the block number it was saved into"
    if string == None:
        string = ''
    if fielddef[FLAGS] & BINARY:
        if not isinstance(string, str):
            raise BadData('binary field: %r not in bytes format' % string)
        string = str(string)
    else:
        if not isinstance(string, unicode):
            if not isinstance(string, str):
                raise BadData("unable to coerce %r(%r) to string" % (type(string), string))
            string = decoder(string)[0]
        string = encoder(string)[0]
    block = memo.put_memo(string)
    if block == 0:
        block = ''
    return "%*s" % (fielddef[LENGTH], block)
def retrieve_numeric(bytes, fielddef, *ignore):
    "Returns the number stored in bytes as integer if field spec for decimals is 0, float otherwise"
    string = bytes.tostring().strip()
    cls = fielddef[CLASS]
    if not string or string[0:1] == '*':  # value too big to store (Visual FoxPro idiocy)
        cls = fielddef[EMPTY]
        if cls is NoneType:
            return None
        return cls()
    if cls == 'default':
        if fielddef[DECIMALS] == 0:
            return int(string)
        else:
            return float(string)
    else:
        return cls(string.strip())
def update_numeric(value, fielddef, *ignore):
    "returns value as ascii representation, rounding decimal portion as necessary"
    if value == None:
        return fielddef[LENGTH] * ' '
    try:
        value = float(value)
    except Exception:
        raise DbfError("incompatible type: %s(%s)" % (type(value), value))
    decimalsize = fielddef[DECIMALS]
    if decimalsize:
        decimalsize += 1
    maxintegersize = fielddef[LENGTH]-decimalsize
    integersize = len("%.0f" % floor(value))
    if integersize > maxintegersize:
        raise DataOverflow('Integer portion too big')
    return "%*.*f" % (fielddef[LENGTH], fielddef[DECIMALS], value)
def retrieve_vfp_datetime(bytes, fielddef, *ignore):
    """returns the date/time stored in bytes; dates <= 01/01/1981 00:00:00
    may not be accurate;  BC dates are nulled."""
    # two four-byte integers store the date and time.
    # millesecords are discarded from time
    if bytes == array('c','\x00' * 8):
        cls = fielddef[EMPTY]
        if cls is NoneType:
            return None
        return cls()
    cls = fielddef[CLASS]
    time = unpack_long_int(bytes[4:])
    microseconds = (time % 1000) * 1000
    time = time // 1000                      # int(round(time, -3)) // 1000 discard milliseconds
    hours = time // 3600
    mins = time % 3600 // 60
    secs = time % 3600 % 60
    time = datetime.time(hours, mins, secs, microseconds)
    possible = unpack_long_int(bytes[:4])
    possible -= VFPTIME
    possible = max(0, possible)
    try:
        date = datetime.date.fromordinal(possible)
    except:
        print
        print bytes
        print possible
    return cls(date.year, date.month, date.day, time.hour, time.minute, time.second, time.microsecond)
def update_vfp_datetime(moment, *ignore):
    """sets the date/time stored in moment
    moment must have fields year, month, day, hour, minute, second, microsecond"""
    bytes = ['\x00'] * 8
    if moment:
        hour = moment.hour
        minute = moment.minute
        second = moment.second
        millisecond = moment.microsecond // 1000       # convert from millionths to thousandths
        time = ((hour * 3600) + (minute * 60) + second) * 1000 + millisecond
        bytes[4:] = update_integer(time)
        bytes[:4] = update_integer(moment.toordinal() + VFPTIME)
    return ''.join(bytes)
def retrieve_vfp_memo(bytes, fielddef, memo, decoder):
    "Returns the block of data from a memo file"
    block = struct.unpack('<i', bytes)[0]
    if not block:
        cls =  fielddef[EMPTY]
        if cls is NoneType:
            return None
        return cls()
    data = memo.get_memo(block)
    if fielddef[FLAGS] & BINARY:
        return data
    return fielddef[CLASS](decoder(data)[0])
def update_vfp_memo(string, fielddef, memo, decoder, encoder):
    "Writes string as a memo, returns the block number it was saved into"
    if string == None:
        string = ''
    if fielddef[FLAGS] & BINARY:
        if not isinstance(string, str):
            raise BadData('binary field: %r not in bytes format' % string)
        string = str(string)
    else:
        if not isinstance(string, unicode):
            if not isinstance(string, str):
                raise BadData("unable to coerce %r(%r) to string" % (type(string), string))
            string = decoder(string)[0]
        string = encoder(string)[0]
    block = memo.put_memo(string)
    return struct.pack('<i', block)
def add_character(format, flags):
    if format[0][0] != '(' or format[0][-1] != ')' or any([f not in flags for f in format[1:]]):
        raise DbfError("Format for Character field creation is <C(n)%s>, not <C%s>" % field_spec_error_text(format, flags))
    length = int(format[0][1:-1])
    if not 0 < length < 255:
        raise ValueError
    decimals = 0
    flag = 0
    for f in format[1:]:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def add_date(format, flags):
    if any([f not in flags for f in format[1:]]):
        raise DbfError("Format for Date field creation is <D%s>, not <D%s>" % field_spec_error_text(format, flags))
    length = 8
    decimals = 0
    flag = 0
    flag = 0
    for f in format:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def add_logical(format, flags):
    if any([f not in flags for f in format[1:]]):
        raise DbfError("Format for Logical field creation is <L%s>, not <L%s>" % field_spec_error_text(format, flags))
    length = 1
    decimals = 0
    flag = 0
    for f in format:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def add_memo(format, flags):
    if any(f not in flags for f in format[1:]):
        raise DbfError("Format for Memo field creation is <M(n)%s>, not <M%s>" % field_spec_error_text(format, flags))
    length = 10
    decimals = 0
    flag = 0
    for f in format:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def add_numeric(format, flags):
    if len(format) > 1 or format[0][0] != '(' or format[0][-1] != ')' or any(f not in flags for f in format[1:]):
        raise DbfError("Format for Numeric field creation is <N(s,d)%s>, not <N%s>" % field_spec_error_text(format, flags))
    length, decimals = format[0][1:-1].split(',')
    length = int(length)
    decimals = int(decimals)
    flag = 0
    for f in format[1:]:
        flag |= FIELD_FLAGS[f]
    if not 0 < length < 18:
        raise DbfError("Numeric fields must be between 1 and 17 digits, not %d" % length)
    if decimals and not 0 < decimals <= length - 2:
        raise DbfError("Decimals must be between 0 and Length-2 (Length: %d, Decimals: %d)" % (length, decimals))
    return length, decimals, flag
def add_vfp_currency(format, flags):
    if any(f not in flags for f in format[1:]):
        raise DbfError("Format for Currency field creation is <Y%s>, not <Y%s>" % field_spec_error_text(format, flags))
    length = 8
    decimals = 0
    flag = 0
    for f in format:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def add_vfp_datetime(format, flags):
    if any(f not in flags for f in format[1:]):
        raise DbfError("Format for DateTime field creation is <T%s>, not <T%s>" % field_spec_error_text(format, flags))
    length = 8
    decimals = 8
    flag = 0
    for f in format:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def add_vfp_double(format, flags):
    if any(f not in flags for f in format[1:]):
        raise DbfError("Format for Double field creation is <B%s>, not <B%s>" % field_spec_error_text(format, flags))
    length = 8
    decimals = 0
    flag = 0
    for f in format:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def add_vfp_integer(format, flags):
    if any(f not in flags for f in format[1:]):
        raise DbfError("Format for Integer field creation is <I%s>, not <I%s>" % field_spec_error_text(format, flags))
    length = 4
    decimals = 0
    flag = 0
    for f in format:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def add_vfp_memo(format, flags):
    if any(f not in flags for f in format[1:]):
        raise DbfError("Format for Memo field creation is <M%s>, not <M%s>" % field_spec_error_text(format, flags))
    length = 4
    decimals = 0
    flag = 0
    for f in format:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def add_vfp_numeric(format, flags):
    if format[0][0] != '(' or format[0][-1] != ')' or any(f not in flags for f in format[1:]):
        raise DbfError("Format for Numeric field creation is <N(s,d)%s>, not <N%s>" % field_spec_error_text(format, flags))
    length, decimals = format[0][1:-1].split(',')
    length = int(length)
    decimals = int(decimals)
    flag = 0
    for f in format[1:]:
        flag |= FIELD_FLAGS[f]
    if not 0 < length < 21:
        raise DbfError("Numeric fields must be between 1 and 20 digits, not %d" % length)
    if decimals and not 0 < decimals <= length - 2:
        raise DbfError("Decimals must be between 0 and Length-2 (Length: %d, Decimals: %d)" % (length, decimals))
    return length, decimals, flag
def field_spec_error_text(format, flags):
    "generic routine for error text for the add...() functions"
    flg = ''
    if flags:
        flg = ' [ ' + ' | '.join(flags) + ' ]'
    frmt = ''
    if format:
        frmt = ' ' + ' '.join(format)
    return flg, frmt

def ezip(*iters):
    "extends all iters to longest one, using last value from each as necessary"
    iters = [iter(x) for x in iters]
    last = [None] * len(iters)
    while "any iters have items left":
        alive = len(iters)
        for i, iterator in enumerate(iters):
            try:
                value = next(iterator)
                last[i] = value
            except StopIteration:
                alive -= 1
        if alive:
            yield tuple(last)
            alive = len(iters)
            continue
        break

# Public classes

class FieldType(tuple):
    "tuple with named attributes for representing a field's dbf type and python class"
    __slots__= ()
    def __new__(cls, *args):
        if len(args) != 2:
            raise TypeError("%s should be called with Type and Class" % cls.__name__)
        return tuple.__new__(cls, args)
    @property
    def type(self):
        return self[0]
    @property
    def cls(self):
        return self[1]

class DbfTable(object):
    """Provides a framework for dbf style tables."""
    _version = 'basic memory table'
    _versionabbr = 'dbf'
    _max_fields = 255
    _max_records = 4294967296
    @MutableDefault
    def _field_types():
        return {
                'C' : {
                        'Type':'Character', 'Init':add_character, 'Blank':str, 'Retrieve':retrieve_character, 'Update':update_character,
                        'Class':unicode, 'Empty':unicode, 'flags':tuple(),
                        },
                'D' : { 
                        'Type':'Date', 'Init':add_date, 'Blank':Date, 'Retrieve':retrieve_date, 'Update':update_date,
                        'Class':datetime.date, 'Empty':none, 'flags':tuple(),
                        },
                'L' : { 
                        'Type':'Logical', 'Init':add_logical, 'Blank':Logical, 'Retrieve':retrieve_logical, 'Update':update_logical,
                        'Class':bool, 'Empty':none, 'flags':tuple(),
                        },
                'M' : { 
                        'Type':'Memo', 'Init':add_memo, 'Blank':str, 'Retrieve':retrieve_memo, 'Update':update_memo,
                        'Class':unicode, 'Empty':unicode, 'flags':tuple(),
                        },
                'N' : { 
                        'Type':'Numeric', 'Init':add_numeric, 'Blank':int, 'Retrieve':retrieve_numeric, 'Update':update_numeric,
                        'Class':'default', 'Empty':none, 'flags':tuple(),
                        },
                }
    _memoext = ''
    _memotypes = tuple('M', )
    _memoClass = _DbfMemo
    _yesMemoMask = ''
    _noMemoMask = ''
    _fixed_fields = ('M','D','L')           # always same length in table
    _variable_fields = ('C', 'N')           # variable length in table
    _binary_fields = tuple()                # as in non-unicode character
    _character_fields = ('C', 'M')          # field representing character data
    _decimal_fields = ('N', )               # text-based numeric fields
    _numeric_fields = ('N', )               # fields representing a number
    _currency_fields = tuple()
    _date_fields = ('D', )
    _datetime_fields = tuple()
    _logical_fields = ('L', )
    _dbfTableHeader = array('c', '\x00' * 32)
    _dbfTableHeader[0] = '\x00'             # table type - none
    _dbfTableHeader[8:10] = array('c', pack_short_int(33))
    _dbfTableHeader[10] = '\x01'            # record length -- one for delete flag
    _dbfTableHeader[29] = '\x00'            # code page -- none, using plain ascii
    _dbfTableHeader = _dbfTableHeader.tostring()
    _dbfTableHeaderExtra = ''
    _supported_tables = []
    _read_only = False
    _meta_only = False
    _use_deleted = True
    backup = False

    class _DbfLists(object):
        "implements the weakref structure for DbfLists"
        def __init__(self):
            self._lists = set()
        def __iter__(self):
            self._lists = set([s for s in self._lists if s() is not None])    
            return (s() for s in self._lists if s() is not None)
        def __len__(self):
            self._lists = set([s for s in self._lists if s() is not None])
            return len(self._lists)
        def add(self, new_list):
            self._lists.add(weakref.ref(new_list))
            self._lists = set([s for s in self._lists if s() is not None])
    class _Indexen(object):
        "implements the weakref structure for seperate indexes"
        def __init__(self):
            self._indexen = set()
        def __iter__(self):
            self._indexen = set([s for s in self._indexen if s() is not None])    
            return (s() for s in self._indexen if s() is not None)
        def __len__(self):
            self._indexen = set([s for s in self._indexen if s() is not None])
            return len(self._indexen)
        def add(self, new_list):
            self._indexen.add(weakref.ref(new_list))
            self._indexen = set([s for s in self._indexen if s() is not None])
    class _MetaData(dict):
        "per table values"
        blankrecord = None
        current = -1            # current record pointer
        dfd = None              # file handle
        fields = None           # field names
        field_count = 0         # number of fields
        filename = None         # name of .dbf file
        ignorememos = False     # True when memos should be ignored
        memoname = None         # name of .dbt/.fpt file
        mfd = None              # file handle
        memo = None             # memo object
        memofields = None       # field names of Memo type
        newmemofile = False     # True when memo file needs to be created
        nulls = None            # non-None when Nullable fields present
        user_fields = None      # not counting SYSTEM fields
        user_field_count = 0    # also not counting SYSTEM fields
        _dirty = False          # updates last_update field on close if True
    class _TableHeader(object):
        "represents the data block that defines a tables type and layout"
        def __init__(self, data, pack_date, unpack_date):
            if len(data) != 32:
                raise DbfError('table header should be 32 bytes, but is %d bytes' % len(data))
            self.packDate = pack_date
            self.unpackDate = unpack_date
            self._data = array('c', data + '\x0d')
        def codepage(self, cp=None):
            "get/set code page of table"
            if cp is None:
                return self._data[29]
            else:
                cp, sd, ld = _codepage_lookup(cp)
                self._data[29] = cp                    
                return cp
        @property
        def data(self):
            "main data structure"
            date = self.packDate(Date.today())
            self._data[1:4] = array('c', date)
            return self._data.tostring()
        @data.setter
        def data(self, bytes):
            if len(bytes) < 32:
                raise DbfError("length for data of %d is less than 32" % len(bytes))
            self._data[:] = array('c', bytes)
        @property
        def extra(self):
            "extra dbf info (located after headers, before data records)"
            fieldblock = self._data[32:]
            for i in range(len(fieldblock)//32+1):
                cr = i * 32
                if fieldblock[cr] == '\x0d':
                    break
            else:
                raise DbfError("corrupt field structure")
            cr += 33    # skip past CR
            return self._data[cr:].tostring()
        @extra.setter
        def extra(self, data):
            fieldblock = self._data[32:]
            for i in range(len(fieldblock)//32+1):
                cr = i * 32
                if fieldblock[cr] == '\x0d':
                    break
            else:
                raise DbfError("corrupt field structure")
            cr += 33    # skip past CR
            self._data[cr:] = array('c', data)                             # extra
            self._data[8:10] = array('c', pack_short_int(len(self._data)))  # start
        @property
        def field_count(self):
            "number of fields (read-only)"
            fieldblock = self._data[32:]
            for i in range(len(fieldblock)//32+1):
                cr = i * 32
                if fieldblock[cr] == '\x0d':
                    break
            else:
                raise DbfError("corrupt field structure")
            return len(fieldblock[:cr]) // 32
        @property
        def fields(self):
            "field block structure"
            fieldblock = self._data[32:]
            for i in range(len(fieldblock)//32+1):
                cr = i * 32
                if fieldblock[cr] == '\x0d':
                    break
            else:
                raise DbfError("corrupt field structure")
            return fieldblock[:cr].tostring()
        @fields.setter
        def fields(self, block):
            fieldblock = self._data[32:]
            for i in range(len(fieldblock)//32+1):
                cr = i * 32
                if fieldblock[cr] == '\x0d':
                    break
            else:
                raise DbfError("corrupt field structure")
            cr += 32    # convert to indexing main structure
            fieldlen = len(block)
            if fieldlen % 32 != 0:
                raise DbfError("fields structure corrupt: %d is not a multiple of 32" % fieldlen)
            self._data[32:cr] = array('c', block)                           # fields
            self._data[8:10] = array('c', pack_short_int(len(self._data)))   # start
            fieldlen = fieldlen // 32
            recordlen = 1                                     # deleted flag
            for i in range(fieldlen):
                recordlen += ord(block[i*32+16])
            self._data[10:12] = array('c', pack_short_int(recordlen))
        @property
        def record_count(self):
            "number of records (maximum 16,777,215)"
            return unpack_long_int(self._data[4:8].tostring())
        @record_count.setter
        def record_count(self, count):
            self._data[4:8] = array('c', pack_long_int(count))
        @property
        def record_length(self):
            "length of a record (read_only) (max of 65,535)"
            return unpack_short_int(self._data[10:12].tostring())
        @property
        def start(self):
            "starting position of first record in file (must be within first 64K)"
            return unpack_short_int(self._data[8:10].tostring())
        @start.setter
        def start(self, pos):
            self._data[8:10] = array('c', pack_short_int(pos))
        @property
        def update(self):
            "date of last table modification (read-only)"
            return self.unpackDate(self._data[1:4].tostring())
        @property
        def version(self):
            "dbf version"
            return self._data[0]
        @version.setter
        def version(self, ver):
            self._data[0] = ver
    class _Table(object):
        "implements the weakref table for records"
        def __init__(self, count, meta):
            self._meta = meta
            self._weakref_list = [weakref.ref(lambda x: None)] * count
        def __getitem__(self, index):
            maybe = self._weakref_list[index]()
            if maybe is None:
                meta = self._meta
                header = meta.header
                if index < 0:
                    index += header.record_count
                size = header.record_length
                location = index * size + header.start
                meta.dfd.seek(location)
                if meta.dfd.tell() != location:
                    raise ValueError("unable to seek to offset %d in file" % location)
                bytes = meta.dfd.read(size)
                if not bytes:
                    raise ValueError("unable to read record data from %s at location %d" % (meta.filename, location))
                maybe = _DbfRecord(recnum=index, layout=meta, kamikaze=bytes, _fromdisk=True)
                self._weakref_list[index] = weakref.ref(maybe)
            return maybe
        def append(self, record):
            self._weakref_list.append(weakref.ref(record))
        def clear(self):
            self._weakref_list[:] = []
        def flush(self):
            for maybe in self._weakref_list:
                maybe = maybe()
                if maybe is not None and maybe._dirty:
                    maybe.write_record()            
        def pop(self):
            return self._weakref_list.pop()
    class DbfIterator(object):
        "returns records using current index"
        def __init__(self, table, update=False):
            "if update is True records are written during the loop"
            self._table = table
            self._index = -1
            self._more_records = True
            self._record = None
            self._update = update
        def __iter__(self):
            return self
        def next(self):
            while True:
                if self._record is not None and self._update:
                    self._record.write_record()
                    self._record = None
                if not self._more_records:
                    break
                self._index += 1
                if self._index >= len(self._table):
                    self._more_records = False
                    break
                record = self._record = self._table[self._index]
                if not self._table.use_deleted and record.has_been_deleted:
                    continue
                return record
            raise StopIteration
    def _build_header_fields(self):
        "constructs fieldblock for disk table"
        fieldblock = array('c', '')
        memo = False
        nulls = False
        meta = self._meta
        header = meta.header
        header.version = chr(ord(header.version) & ord(self._noMemoMask))
        meta.fields = [f for f in meta.fields if f != '_nullflags']
        for field in meta.fields:
            layout = meta[field]
            if meta.fields.count(field) > 1:
                raise DbfError("corrupted field structure (noticed in _build_header_fields)")
            fielddef = array('c', '\x00' * 32)
            fielddef[:11] = array('c', pack_str(meta.encoder(field)[0]))
            try:
                fielddef[11] = layout[TYPE]
            except TypeError:
                print
                print repr(meta[field])
                print repr(layout[TYPE])
            fielddef[12:16] = array('c', pack_long_int(layout[START]))
            fielddef[16] = chr(layout[LENGTH])
            fielddef[17] = chr(layout[DECIMALS])
            fielddef[18] = chr(layout[FLAGS])
            fieldblock.extend(fielddef)
            if layout[TYPE] in meta.memotypes:
                memo = True
            if layout[FLAGS] & NULLABLE:
                nulls = True
        if memo:
            header.version = chr(ord(header.version) | ord(self._yesMemoMask))
            if meta.memo is None:
                meta.memo = self._memoClass(meta)
        else:
            if os.path.exists(meta.memoname):
                if meta.mfd is not None:
                    print 1, meta.mfd.name
                    print 2, meta.mfd.closed
                    meta.mfd.close()

                os.remove(meta.memoname)
            meta.memo = None
        if nulls:
            start = layout[START] + layout[LENGTH]
            length, one_more = divmod(len(meta.fields), 8)
            if one_more:
                length += 1
            fielddef = array('c', '\x00' * 32)
            fielddef[:11] = array('c', pack_str('_nullflags'))
            fielddef[11] = '0'
            fielddef[12:16] = array('c', pack_long_int(start))
            fielddef[16] = chr(length)
            fielddef[17] = chr(0)
            fielddef[18] = chr(BINARY | SYSTEM)
            fieldblock.extend(fielddef)
            meta.fields.append('_nullflags')
            nullflags = (
                    '0',                # type
                    start,              # start
                    length,             # length
                    start + length,     # end
                    0,                  # decimals
                    BINARY | SYSTEM,    # flags
                    none,               # class
                    none,               # empty
                    )
            meta['_nullflags'] = nullflags
        header.fields = fieldblock.tostring()
        meta.user_fields = [f for f in meta.fields if not meta[f][FLAGS] & SYSTEM]
        meta.user_field_count = len(meta.user_fields)

    def _check_memo_integrity(self):
        "memory memos are simple dicts"
        pass
    def _initialize_fields(self):
        "builds the FieldList of names, types, and descriptions from the disk file"
        old_fields = defaultdict(dict)
        missing = object()
        meta = self._meta
        for name in meta.fields:
            old_fields[name]['type'] = meta[name][TYPE]
            old_fields[name]['class'] = meta[name][CLASS]
            old_fields[name]['empty'] = meta[name][EMPTY]
        meta.fields[:] = []
        offset = 1
        fieldsdef = meta.header.fields
        if len(fieldsdef) % 32 != 0:
            raise DbfError("field definition block corrupt: %d bytes in size" % len(fieldsdef))
        if len(fieldsdef) // 32 != self._meta.header.field_count:
            raise DbfError("Header shows %d fields, but field definition block has %d fields" % (self._meta.header.field_count, len(fieldsdef)//32))
        for i in range(self._meta.header.field_count):
            fieldblock = meta.decoder(fieldsdef[i*32:(i+1)*32])
            name = unpack_str(fieldblock[:11])
            type = fieldblock[11]
            if not type in meta.fieldtypes:
                raise DbfError("Unknown field type: %s" % type)
            start = offset
            length = ord(fieldblock[16])
            offset += length
            end = start + length
            decimals = ord(fieldblock[17])
            flags = ord(fieldblock[18])
            if name in meta.fields:
                raise DbfError('Duplicate field name found: %s' % name)
            meta.fields.append(name)
            if name in old_fields and old_fields[name]['type'] == type:
                cls = old_fields[name]['class']
                empty = old_fields[name]['empty']
            else:
                cls = meta.fieldtypes[type]['Class']
                empty = meta.fieldtypes[type]['Empty']
            meta[name] = (
                    type,
                    start,
                    length,
                    end,
                    decimals,
                    flags,
                    cls,
                    empty,
                    )
        self._meta.user_fields = [f for f in self._meta.fields if not self._meta[f][FLAGS] & SYSTEM]
        self._meta.user_field_count = len(self._meta.user_fields)

    def _field_layout(self, i):
        "Returns field information Name Type(Length[,Decimals])"
        name = self._meta.fields[i]
        fielddef = self._meta[name]
        type = fielddef[TYPE]
        length = fielddef[LENGTH]
        decimals = fielddef[DECIMALS]
        set_flags = fielddef[FLAGS]
        flags = []
        for flg in BINARY, NULLABLE, SYSTEM:
            if flg & set_flags == flg:
                flags.append(FIELD_FLAGS[flg])
                set_flags &= 255 ^ flg
        if flags:
            flags = ' ' + ' '.join(flags)
        else:
            flags = ''
        if type in self._decimal_fields:
            description = "%s %s(%d,%d)%s" % (name, type, length, decimals, flags)
        elif type in self._fixed_fields:
            description = "%s %s%s" % (name, type, flags)
        else:
            description = "%s %s(%d)%s" % (name, type, length, flags)
        return description
    def _load_table(self):
        "loads the records from disk to memory"
        if self._meta_only:
            raise DbfError("%s has been closed, records are unavailable" % self.filename)
        dfd = self._meta.dfd
        header = self._meta.header
        dfd.seek(header.start)
        allrecords = dfd.read()                     # kludge to get around mysterious errno 0 problems
        dfd.seek(0)
        length = header.record_length
        for i in range(header.record_count):
            record_data = allrecords[length*i:length*i+length]
            self._table.append(_DbfRecord(i, self._meta, allrecords[length*i:length*i+length], _fromdisk=True))
        dfd.seek(0)
    def _list_fields(self, specs, sep=','):
        "standardizes field specs"
        if specs is None:
            specs = self.field_names
        elif isinstance(specs, str):
            specs = specs.split(sep)
        else:
            specs = list(specs)
        specs = [s.strip() for s in specs]
        return specs
    @staticmethod
    def _pack_date(date):
            "Returns a group of three bytes, in integer form, of the date"
            return "%c%c%c" % (date.year-1900, date.month, date.day)
    @staticmethod
    def _unpack_date(bytestr):
            "Returns a Date() of the packed three-byte date passed in"
            year, month, day = struct.unpack('<BBB', bytestr)
            year += 1900
            return Date(year, month, day)
    def _update_disk(self, headeronly=False):
        "synchronizes the disk file with current data"
        if self._meta.inmemory:
            return
        meta = self._meta
        header = meta.header
        fd = meta.dfd
        fd.seek(0)
        fd.write(header.data)
        eof = header.start + header.record_count * header.record_length
        if not headeronly:
            for record in self:
                if record._update_disk():
                    fd.flush()
            fd.truncate(eof)
        if 'db3' in self._versionabbr:
            fd.seek(0, SEEK_END)
            fd.write('\x1a')        # required for dBase III
            fd.flush()
            fd.truncate(eof + 1)

    def __contains__(self, key):
        "field name based"
        return key in self.field_names or key in ['record_number','delete_flag']
    def __enter__(self):
        return self
    def __exit__(self, *exc_info):
        self.close()
    def __getattr__(self, name):
        if name in (
                'memotypes',
                'fixed_fields',
                'variable_fields',
                'character_fields',
                'numeric_fields',
                'decimal_fields',
                'currency_fields',
                ):
            return getattr(self, '_'+name)
        if name in ('_table'):
                if self._meta.ondisk:
                    self._table = self._Table(len(self), self._meta)
                else:
                    self._table = []
                    self._load_table()
        return object.__getattribute__(self, name)
    def __getitem__(self, value):
        if type(value) == int:
            if not -self._meta.header.record_count <= value < self._meta.header.record_count: 
                raise IndexError("Record %d is not in table." % value)
            return self._table[value]
        elif type(value) == slice:
            sequence = List(desc='%s -->  %s' % (self.filename, value), field_names=self.field_names)
            self._dbflists.add(sequence)
            for index in range(len(self))[value]:
                record = self._table[index]
                if self.use_deleted is True or not record.has_been_deleted:
                    sequence.append(record)
            return sequence
        else:
            raise TypeError('type <%s> not valid for indexing' % type(value))
    def __init__(self, filename=':memory:', field_specs=None, memo_size=128, ignore_memos=False, 
                 read_only=False, keep_memos=False, meta_only=False, codepage=None, 
                 default_data_types={}, field_data_types={},    # e.g. 'name':str, 'age':float
                 ):
        """open/create dbf file
        filename should include path if needed
        field_specs can be either a ;-delimited string or a list of strings
        memo_size is always 512 for db3 memos
        ignore_memos is useful if the memo file is missing or corrupt
        read_only will load records into memory, then close the disk file
        keep_memos will also load any memo fields into memory
        meta_only will ignore all records, keeping only basic table information
        codepage will override whatever is set in the table itself"""
        if filename[0] == filename[-1] == ':':
            if field_specs is None:
                raise DbfError("field list must be specified for memory tables")
        elif type(self) is DbfTable:
            raise DbfError("only memory tables supported")
        self._dbflists = self._DbfLists()
        self._indexen = self._Indexen()
        self._meta = meta = self._MetaData()
        meta.max_fields = self._max_fields
        meta.max_records = self._max_records
        meta.table = weakref.ref(self)
        meta.filename = filename
        meta.fields = []
        meta.user_fields = []
        meta.user_field_count = 0
        meta.fieldtypes = fieldtypes = self._field_types
        meta.fixed_fields = self._fixed_fields
        meta.variable_fields = self._variable_fields
        meta.character_fields = self._character_fields
        meta.decimal_fields = self._decimal_fields
        meta.numeric_fields = self._numeric_fields
        meta.memotypes = self._memotypes
        meta.ignorememos = meta.original_ignorememos = ignore_memos
        meta.memo_size = memo_size
        meta.input_decoder = codecs.getdecoder(input_decoding)      # from ascii to unicode
        meta.output_encoder = codecs.getencoder(input_decoding)     # and back to ascii
        meta.header = header = self._TableHeader(self._dbfTableHeader, self._pack_date, self._unpack_date)
        header.extra = self._dbfTableHeaderExtra
        #header.data        #force update of date
        for field, types in default_data_types.items():
            if not isinstance(types, tuple):
                types = (types, )
            for result_name, result_type in ezip(('Class','Empty','Null'), types):
                fieldtypes[field][result_name] = result_type
        if filename[0] == filename[-1] == ':':
            self._table = []
            meta.ondisk = False
            meta.inmemory = True
            meta.memoname = filename
        else:
            base, ext = os.path.splitext(filename)
            if ext == '':
                meta.filename =  base + '.dbf'
            meta.memoname = base + self._memoext
            meta.ondisk = True
            meta.inmemory = False
        if codepage is not None:
            cp, sd, ld = _codepage_lookup(codepage)
            self._meta.decoder = codecs.getdecoder(sd) 
            self._meta.encoder = codecs.getencoder(sd)
        if field_specs:
            if meta.ondisk:
                meta.dfd = open(meta.filename, 'w+b')
                meta.newmemofile = True
            if codepage is None:
                header.codepage(default_codepage)
                cp, sd, ld = _codepage_lookup(header.codepage())
                meta.decoder = codecs.getdecoder(sd) 
                meta.encoder = codecs.getencoder(sd)
            self.add_fields(field_specs)
        else:
            try:
                dfd = meta.dfd = open(meta.filename, 'r+b')
            except IOError, e:
                raise DbfError(str(e))
            dfd.seek(0)
            meta.header = header = self._TableHeader(dfd.read(32), self._pack_date, self._unpack_date)
            if not header.version in self._supported_tables:
                dfd.close()
                dfd = None
                raise DbfError(
                    "%s does not support %s [%x]" % 
                    (self._version,
                    version_map.get(header.version, 'Unknown: %s' % header.version),
                    ord(header.version)))
            if codepage is None:
                cp, sd, ld = _codepage_lookup(header.codepage())
                self._meta.decoder = codecs.getdecoder(sd) 
                self._meta.encoder = codecs.getencoder(sd)
            fieldblock = dfd.read(header.start - 32)
            for i in range(len(fieldblock)//32+1):
                fieldend = i * 32
                if fieldblock[fieldend] == '\x0d':
                    break
            else:
                raise DbfError("corrupt field structure in header")
            if len(fieldblock[:fieldend]) % 32 != 0:
                raise DbfError("corrupt field structure in header")
            header.fields = fieldblock[:fieldend]
            header.extra = fieldblock[fieldend+1:]  # skip trailing \r
            self._initialize_fields()
            self._check_memo_integrity()
            meta.current = -1
            if len(self) > 0:
                meta.current = 0
            dfd.seek(0)
            if meta_only:
                self.close(keep_table=False, keep_memos=False)
            elif read_only:
                self.close(keep_table=True, keep_memos=keep_memos)

        for field in meta.fields:
            field_type = meta[field][TYPE]
            default_field_type = (
                fieldtypes[field_type]['Class'], 
                fieldtypes[field_type]['Empty'], 
                )
            specific_field_type = field_data_types.get(field)
            if specific_field_type is not None and not isinstance(specific_field_type, tuple):
                specific_field_type = (specific_field_type, )
            classes = []
            for result_name, result_type in ezip(
                    ('class','empty'),
                    specific_field_type or default_field_type,
                    ):
                classes.append(result_type)
            meta[field] = meta[field][:-2] + tuple(classes)

        
    def __iter__(self):
        "iterates over the table's records"
        return self.DbfIterator(self)           
    def __len__(self):
        "returns number of records in table"
        return self._meta.header.record_count
    def __nonzero__(self):
        "True if table has any records"
        return self._meta.header.record_count != 0
    def __repr__(self):
        if self._read_only:
            return __name__ + ".Table('%s', read_only=True)" % self._meta.filename
        elif self._meta_only:
            return __name__ + ".Table('%s', meta_only=True)" % self._meta.filename
        else:
            return __name__ + ".Table('%s')" % self._meta.filename
    def __str__(self):
        if self._read_only:
            status = "read-only"
        elif self._meta_only:
            status = "meta-only"
        else:
            status = "read/write"
        str =  """
        Table:         %s
        Type:          %s
        Codepage:      %s
        Status:        %s
        Last updated:  %s
        Record count:  %d
        Field count:   %d
        Record length: %d """ % (self.filename, version_map.get(self._meta.header.version, 
            'unknown - ' + hex(ord(self._meta.header.version))), self.codepage, status, 
            self.last_update, len(self), self.field_count, self.record_length)
        str += "\n        --Fields--\n"
        for i in range(len(self.field_names)):
            str += "%11d) %s\n" % (i, self._field_layout(i))
        return str
    @property
    def codepage(self):
        "code page used for text translation"
        return "%s (%s)" % code_pages[self._meta.header.codepage()]
    @codepage.setter
    def codepage(self, cp):
        cp = code_pages[self._meta.header.codepage(cp)][0]
        self._meta.decoder = codecs.getdecoder(cp) 
        self._meta.encoder = codecs.getencoder(cp)
        self._update_disk(headeronly=True)
    @property
    def field_count(self):
        "the number of user fields in the table"
        return self._meta.user_field_count
    @property
    def field_names(self):
        "a list of the user fields in the table"
        return self._meta.user_fields[:]
    @property
    def filename(self):
        "table's file name, including path (if specified on open)"
        return self._meta.filename
    @property
    def last_update(self):
        "date of last update"
        return self._meta.header.update
    @property
    def memoname(self):
        "table's memo name (if path included in filename on open)"
        return self._meta.memoname
    @property
    def record_length(self):
        "number of bytes in a record (including deleted flag and null field size"
        return self._meta.header.record_length
    @property
    def record_number(self):
        "offset of record in table"
        return self._meta.current
    @property
    def supported_tables(self):
        "allowable table types"
        return self._supported_tables
    @property
    def use_deleted(self):
        "process or ignore deleted records"
        return self._use_deleted
    @use_deleted.setter
    def use_deleted(self, new_setting):
        self._use_deleted = new_setting
    @property
    def version(self):
        "returns the dbf type of the table"
        return self._version
    def add_fields(self, field_specs):
        """adds field(s) to the table layout; format is Name Type(Length,Decimals)[; Name Type(Length,Decimals)[...]]
        backup table is created with _backup appended to name
        then zaps table, recreates current structure, and copies records back from the backup"""
        meta = self._meta
        header = meta.header
        fields = self.structure() + self._list_fields(field_specs, sep=';')
        if len(fields) > meta.max_fields:
            raise DbfError(
                    "Adding %d more field%s would exceed the limit of %d" 
                    % (len(fields), ('','s')[len(fields)==1], meta.max_fields)
                    )
        old_table = None
        if self:
            old_table = self.create_backup()
            self.zap(YES_I_AM_SURE)
        if meta.mfd is not None and not meta.ignorememos:
            meta.mfd.close()
            meta.mfd = None
            meta.memo = None
        if not meta.ignorememos:
            meta.newmemofile = True
        offset = 1
        for name in meta.fields:
            del meta[name]
        meta.fields[:] = []

        meta.blankrecord = None
        for field in fields:
            field = field.lower()
            pieces = field.split()
            name = pieces.pop(0)
            if '(' in pieces[0]:
                loc = pieces[0].index('(')
                pieces.insert(0, pieces[0][:loc])
                pieces[1] = pieces[1][loc:]
            format = pieces.pop(0).upper()
            if pieces and '(' in pieces[0]:
                for i, p in enumerate(pieces):
                    if ')' in p:
                        pieces[0:i+1] = [''.join(pieces[0:i+1])]
                        break
            if name[0] == '_' or name[0].isdigit() or not name.replace('_','').isalnum():
                raise DbfError("%s invalid:  field names must start with a letter, and can only contain letters, digits, and _" % name)
            name = unicode(name)
            if name in meta.fields:
                raise DbfError("Field '%s' already exists" % name)
            field_type = format.encode('ascii')
            if len(name) > 10:
                raise DbfError("Maximum field name length is 10.  '%s' is %d characters long." % (name, len(name)))
            if not field_type in meta.fieldtypes.keys():
                raise DbfError("Unknown field type:  %s" % field_type)
            init = self._meta.fieldtypes[field_type]['Init']
            flags = self._meta.fieldtypes[field_type]['flags']
            length, decimals, flags = init(pieces, flags)
            start = offset
            end = offset + length
            offset = end
            meta.fields.append(name)
            cls = meta.fieldtypes[field_type]['Class']
            empty = meta.fieldtypes[field_type]['Empty']
            meta[name] = (
                    field_type,
                    start,
                    length,
                    end,
                    decimals,
                    flags,
                    cls,
                    empty,
                    )
        self._build_header_fields()
        self._update_disk()
        if old_table is not None:
            old_table.open()
            for record in old_table:
                self.append(record.scatter_fields())
            old_table.close()

    def allow_nulls(self, fields):
        "set fields to allow null values"
        if self._versionabbr in ('db3', ):
            raise DbfError("Nullable fields are not allowed in %s tables" % self._version)
        meta = self._meta
        header = meta.header
        fields = self._list_fields(fields)
        missing = set(fields) - set(self.field_names)
        if missing:
            raise FieldMissing(', '.join(missing))
        if len(self.field_names) + 1 > meta.max_fields:
            raise DbfError(
                    "Adding the hidden _nullflags field would exceed the limit of %d fields for this table" 
                    % (meta.max_fields, )
                    )
        old_table = None
        if self:
            old_table = self.create_backup()
            self.zap(YES_I_AM_SURE)
        if meta.mfd is not None and not meta.ignorememos:
            meta.mfd.close()
            meta.mfd = None
            meta.memo = None
        if not meta.ignorememos:
            meta.newmemofile = True
        for field in fields:
            specs = list(meta[field])
            specs[FLAGS] |= NULLABLE
            meta[field] = tuple(specs)
        meta.blankrecord = None
        self._build_header_fields()
        self._update_disk()
        if old_table is not None:
            old_table.open()
            for record in old_table:
                self.append(record.scatter_fields())
            old_table.close()

    def append(self, kamikaze='', drop=False, multiple=1):
        "adds <multiple> blank records, and fills fields with dict/tuple values if present"
        if not self.field_count:
            raise DbfError("No fields defined, cannot append")
        empty_table = len(self) == 0
        dictdata = False
        tupledata = False
        meta = self._meta
        header = meta.header
        if not isinstance(kamikaze, _DbfRecord):
            if isinstance(kamikaze, dict):
                dictdata = kamikaze
                kamikaze = ''
            elif isinstance(kamikaze, tuple):
                tupledata = kamikaze
                kamikaze = ''
        newrecord = _DbfRecord(recnum=header.record_count, layout=meta, kamikaze=kamikaze)
        self._table.append(newrecord)
        header.record_count += 1
        try:
            if dictdata:
                newrecord.gather_fields(dictdata, drop=drop)
            elif tupledata:
                for index, item in enumerate(tupledata):
                    try:
                        newrecord[index] = item
                    except IndexError, exc:
                        raise DbfError("table %s has %d fields, incoming data has %d fields" %
                            (self.filename, len(newrecord), len(tupledata)))
            elif kamikaze == str:
                for field in meta.memofields:
                    newrecord[field] = ''
            elif kamikaze:
                for field in meta.memofields:
                    newrecord[field] = kamikaze[field]
            newrecord.write_record()
        except Exception:
            self._table.pop()     # discard failed record
            header.record_count = header.record_count - 1
            self._update_disk()
            raise
        multiple -= 1
        if multiple:
            data = newrecord._data
            single = header.record_count
            total = single + multiple
            while single < total:
                multi_record = _DbfRecord(single, meta, kamikaze=data)
                self._table.append(multi_record)
                for field in meta.memofields:
                    multi_record[field] = newrecord[field]
                single += 1
                multi_record.write_record()
            header.record_count = total   # += multiple
            meta.current = header.record_count - 1
            newrecord = multi_record
        self._update_disk(headeronly=True)
        if empty_table:
            meta.current = 0
        return newrecord
    def bof(self, _move=False):
        "moves record pointer to previous usable record; returns True if no more usable records"
        current = self._meta.current
        try:
            while self._meta.current > 0:
                self._meta.current -= 1
                if self.use_deleted or not self.current().has_been_deleted:
                    break
            else:
                self._meta.current = -1
                return True
            return False
        finally:
            if not _move:
                self._meta.current = current
    def bottom(self, get_record=False):
        """sets record pointer to bottom of table (end of table)
        if get_record, seeks to and returns last (non-deleted) record
        DbfError if table is empty
        Bof if all records deleted and use_deleted is False"""
        self._meta.current = self._meta.header.record_count
        if get_record:
            try:
                return self.prev()
            except Bof:
                self._meta.current = self._meta.header.record_count
                raise Eof()
    def close(self, keep_table=False, keep_memos=False):
        """closes disk files, flushing record data to disk
        ensures table data is available if keep_table
        ensures memo data is available if keep_memos"""
        if not self._meta.inmemory:
            self._table.flush()
        self._meta.inmemory = True
        if keep_table:
            replacement_table = []
            for record in self._table:
                replacement_table.append(record)
            self._table = replacement_table
        else:
            if self._meta.ondisk:
                self._meta_only = True
        if self._meta.mfd is not None:
            if not keep_memos:
                self._meta.ignorememos = True
            else:
                memo_fields = []
                for field in self.field_names:
                    if self.is_memotype(field):
                        memo_fields.append(field)
                for record in self:
                    for field in memo_fields:
                        record[field] = record[field]
            self._meta.mfd.close()
            self._meta.mfd = None
        if self._meta.ondisk:
            self._meta.dfd.close()
            self._meta.dfd = None
        if keep_table:
            self._read_only = True
        self._meta.ondisk = False
    def create_backup(self, new_name=None):
        "creates a backup table"
        if self.filename[0] == self.filename[-1] == ':' and new_name is None:
            new_name = self.filename[:-1] + '_backup:'
        if new_name is None:
            upper = self.filename.isupper()
            name, ext = os.path.splitext(os.path.split(self.filename)[1])
            extra = ('_backup', '_BACKUP')[upper]
            new_name = os.path.join(temp_dir, name + extra + ext)
        bkup = self.__class__(new_name, self.structure())
        for record in self:
            bkup.append(record)
        bkup.close()
        self.backup = new_name
        return bkup
    def create_index(self, key):
        "creates an in-memory index using the function key"
        return Index(self, key)
    def current(self, index=False):
        "returns current logical record, or its index"
        if self._meta.current < 0:
            raise Bof()
        elif self._meta.current >= self._meta.header.record_count:
            raise Eof()
        if index:
            return self._meta.current
        return self._table[self._meta.current]
    def delete_fields(self, doomed):
        """removes field(s) from the table
        creates backup files with _backup appended to the file name,
        then modifies current structure"""
        doomed = self._list_fields(doomed)
        meta = self._meta
        header = meta.header
        for victim in doomed:
            if victim not in meta.user_fields:
                raise DbfError("field %s not in table -- delete aborted" % victim)
        old_table = None
        if self:
            old_table = self.create_backup()
            self.zap(YES_I_AM_SURE)
        if meta.mfd is not None and not meta.ignorememos:
            meta.mfd.close()
            meta.mfd = None
            meta.memo = None
        if not meta.ignorememos:
            meta.newmemofile = True
        if '_nullflags' in meta.fields:
            doomed.append('_nullflags')
        for victim in doomed:
            layout = meta[victim]
            meta.fields.pop(meta.fields.index(victim))
            start = layout[START]
            end = layout[END]
            for field in meta.fields:
                if meta[field][START] == end:
                    specs = list(meta[field])
                    end = specs[END]                    #self._meta[field][END]
                    specs[START] = start                #self._meta[field][START] = start
                    specs[END] = start + specs[LENGTH]  #self._meta[field][END] = start + self._meta[field][LENGTH]
                    start = specs[END]                  #self._meta[field][END]
                    meta[field] =  tuple(specs)
        self._build_header_fields()
        self._update_disk()
        for name in list(meta):
            if name not in meta.fields:
                del meta[name]
        if old_table is not None:
            old_table.open()
            for record in old_table:
                self.append(record.scatter_fields(), drop=True)
            old_table.close()

    def disallow_nulls(self, fields):
        "set fields to not allow null values"
        meta = self._meta
        fields = self._list_fields(fields)
        missing = set(fields) - set(self.field_names)
        if missing:
            raise FieldMissing(', '.join(missing))
        old_table = None
        if self:
            old_table = self.create_backup()
            self.zap(YES_I_AM_SURE)
        if meta.mfd is not None and not meta.ignorememos:
            meta.mfd.close()
            meta.mfd = None
            meta.memo = None
        if not meta.ignorememos:
            meta.newmemofile = True
        for field in fields:
            specs = list(meta[field])
            specs[FLAGS] &= 0xff ^ NULLABLE
            meta[field] =  tuple(specs)
        meta.blankrecord = None
        self._build_header_fields()
        self._update_disk()
        if old_table is not None:
            old_table.open()
            for record in old_table:
                self.append(record.scatter_fields())
            old_table.close()

    def eof(self, _move=False):
        "moves record pointer to next usable record; returns True if no more usable records"
        current = self._meta.current
        try:
            while self._meta.current < self._meta.header.record_count - 1:
                self._meta.current += 1
                if self.use_deleted or not self.current().has_been_deleted:
                    break
            else:
                self._meta.current = self._meta.header.record_count
                return True
            return False
        finally:
            if not _move:
                self._meta.current = current
    def export(self, records=None, filename=None, field_specs=None, format='csv', header=True):
        """writes the table using CSV or tab-delimited format, using the filename
        given if specified, otherwise the table name"""
        if filename is not None:
            path, filename = os.path.split(filename)
        else:
            path, filename = os.path.split(self.filename)
        filename = os.path.join(path, filename)
        field_specs = self._list_fields(field_specs)
        if records is None:
            records = self
        format = format.lower()
        if format not in ('csv', 'tab', 'fixed'):
            raise DbfError("export format: csv, tab, or fixed -- not %s" % format)
        if format == 'fixed':
            format = 'txt'
        base, ext = os.path.splitext(filename)
        if ext.lower() in ('', '.dbf'):
            filename = base + "." + format[:3]
        fd = open(filename, 'w')
        try:
            if format == 'csv':
                csvfile = csv.writer(fd, dialect='dbf')
                if header:
                    csvfile.writerow(field_specs)
                for record in records:
                    fields = []
                    for fieldname in field_specs:
                        data = record[fieldname]
                        if isinstance(data, (str, unicode)):
                            fields.append(self._meta.encoder(data)[0])
                        else:
                            fields.append(data)
                    csvfile.writerow(fields)
            elif format == 'tab':
                if header:
                    fd.write('\t'.join(field_specs) + '\n')
                for record in records:
                    fields = []
                    for fieldname in field_specs:
                        data = record[fieldname]
                        if isinstance(data, (str, unicode)):
                            fields.append(self._meta.encoder(data)[0])
                        else:
                            fields.append(str(data))
                    fd.write('\t'.join(fields) + '\n')
            else: # format == 'fixed'
                header = open("%s_layout.txt" % os.path.splitext(filename)[0], 'w')
                header.write("%-15s  Size\n" % "Field Name")
                header.write("%-15s  ----\n" % ("-" * 15))
                sizes = []
                for field in field_specs:
                    size = self.size(field)[0]
                    sizes.append(size)
                    header.write("%-15s  %3d\n" % (field, size))
                header.write('\nTotal Records in file: %d\n' % len(records))
                header.close()
                for record in records:
                    fields = []
                    for i, fieldname in enumerate(field_specs):
                        data = record[fieldname]
                        if isinstance(data, (str, unicode)):
                            fields.append("%-*s" % (sizes[i], self._meta.encoder(data)[0]))
                        else:
                            fields.append("%-*s" % (sizes[i], data))
                    fd.write(''.join(fields) + '\n')
        finally:
            fd.close()
            fd = None
        return len(records)
    def find(self, command):
        "uses exec to perform queries on the table"
        possible = List(desc="%s -->  %s" % (self.filename, command), field_names=self.field_names)
        self._dbflists.add(possible)
        result = {}
        select = 'result["keep"] = %s' % command
        g = {}
        use_deleted = self.use_deleted
        for record in self:
            result['keep'] = False
            g['result'] = result
            exec select in g, record
            if result['keep']:
                possible.append(record)
            record.write_record()
        return possible
    def get_record(self, recno):
        "returns record at physical_index[recno]"
        return self._table[recno]
    def goto(self, criteria):
        """changes the record pointer to the first matching (non-deleted) record
        criteria should be either a tuple of tuple(value, field, func) triples, 
        or an integer to go to"""
        if isinstance(criteria, int):
            if not -self._meta.header.record_count <= criteria < self._meta.header.record_count:
                raise IndexError("Record %d does not exist" % criteria)
            if criteria < 0:
                criteria += self._meta.header.record_count
            self._meta.current = criteria
            return self.current()
        criteria = _normalize_tuples(tuples=criteria, length=3, filler=[_nop])
        specs = tuple([(field, func) for value, field, func in criteria])
        match = tuple([value for value, field, func in criteria])
        current = self.current(index=True)
        matchlen = len(match)
        while not self.Eof():
            record = self.current()
            results = record(*specs)
            if results == match:
                return record
        return self.goto(current)
    def is_decimal(self, name):
        "returns True if name is a variable-length field type"
        return self._meta[name][TYPE] in self._decimal_fields
    def is_memotype(self, name):
        "returns True if name is a memo type field"
        return self._meta[name][TYPE] in self._memotypes
    def new(self, filename, field_specs=None, codepage=None):
        "returns a new table of the same type"
        if field_specs is None:
            field_specs = self.structure()
        if not (filename[0] == filename[-1] == ':'):
            path, name = os.path.split(filename)
            if path == "":
                filename = os.path.join(os.path.split(self.filename)[0], filename)
            elif name == "":
                filename = os.path.join(path, os.path.split(self.filename)[1])
        if codepage is None:
            codepage = self._meta.header.codepage()[0]
        return self.__class__(filename, field_specs, codepage=codepage)
    def next(self):
        "set record pointer to next (non-deleted) record, and return it"
        if self.eof(_move=True):
            raise Eof()
        return self.current()
    def nullable_field(self, field):
        "returns True if field allows Nulls"
        if field not in self.field_names:
            raise MissingField(field)
        return bool(self._meta[field][FLAGS] & NULLABLE)
    def open(self):
        "(re)opens disk table, (re)initializes data structures"
        if self.filename[0] == self.filename[-1] == ':':
            return
        meta = self._meta
        meta.inmemory = False
        meta.ondisk = True
        self._read_only = False
        self._meta_only = False
        if '_table' in dir(self):
            del self._table
        dfd = meta.dfd = open(meta.filename, 'r+b')
        dfd.seek(0)
        meta.header = self._TableHeader(dfd.read(32), self._pack_date, self._unpack_date)
        header = meta.header
        if not header.version in self._supported_tables:
            dfd.close()
            dfd = None
            raise DbfError("Unsupported dbf type: %s [%x]" % (version_map.get(header.version, 'Unknown: %s' % header.version), ord(header.version)))
        cp, sd, ld = _codepage_lookup(header.codepage())
        meta.decoder = codecs.getdecoder(sd) 
        meta.encoder = codecs.getencoder(sd)
        fieldblock = dfd.read(header.start - 32)
        for i in range(len(fieldblock)//32+1):
            fieldend = i * 32
            if fieldblock[fieldend] == '\x0d':
                break
        else:
            raise DbfError("corrupt field structure in header")
        if len(fieldblock[:fieldend]) % 32 != 0:
            raise DbfError("corrupt field structure in header")
        header.fields = fieldblock[:fieldend]
        header.extra = fieldblock[fieldend+1:]  # skip trailing \r
        self._meta.ignorememos = self._meta.original_ignorememos
        self._initialize_fields()
        self._check_memo_integrity()
        meta.current = -1
        if len(self) > 0:
            meta.current = 0
        dfd.seek(0)

    def pack(self):
        "physically removes all deleted records"
        for dbfindex in self._indexen:
            dbfindex.clear()
        newtable = []
        index = 0
        offset = 0 # +1 for each purged record
        for record in self._table:
            found = False
            if record.has_been_deleted:
                for dbflist in self._dbflists:
                    if dbflist._purge(record, record.record_number - offset, 1):
                        found = True
                record._recnum = -1
            else:
                record._recnum = index
                newtable.append(record)
                index += 1
            if found:
                offset += 1
                found = False
        self._table.clear()
        for record in newtable:
            self._table.append(record)
        self._meta.header.record_count = index
        self._current = -1
        self._update_disk()
        self.reindex()
    def prev(self):
        "set record pointer to previous (non-deleted) record, and return it"
        if self.bof(_move=True):
            raise Bof
        return self.current()
    def query(self, sql_command=None, python=None):
        "deprecated: use .find or .sql"
        if sql_command:
            return self.sql(sql_command)
        elif python:
            return self.find(python)
        raise DbfError("query: python parameter must be specified")
    def reindex(self):
        "reprocess all indices for this table"
        for dbfindex in self._indexen:
            dbfindex.reindex()
    def rename_field(self, oldname, newname):
        "renames an existing field"
        if self:
            self.create_backup()
        if not oldname in self._meta.user_fields:
            raise DbfError("field --%s-- does not exist -- cannot rename it." % oldname)
        if newname[0] == '_' or newname[0].isdigit() or not newname.replace('_','').isalnum():
            raise DbfError("field names cannot start with _ or digits, and can only contain the _, letters, and digits")
        newname = newname.lower()
        if newname in self._meta.fields:
            raise DbfError("field --%s-- already exists" % newname)
        if len(newname) > 10:
            raise DbfError("maximum field name length is 10.  '%s' is %d characters long." % (newname, len(newname)))
        self._meta[newname] = self._meta[oldname]
        self._meta.fields[self._meta.fields.index(oldname)] = newname
        self._build_header_fields()
        self._update_disk(headeronly=True)
    def resize_field(self, doomed, new_size):
        """resizes field (C only at this time)
        creates backup file, then modifies current structure"""
        if not 0 < new_size < 256:
            raise DbfError("new_size must be between 1 and 255 (use delete_fields to remove a field)")
        doomed = self._list_fields(doomed)
        for victim in doomed:
            if victim not in self._meta.user_fields:
                raise DbfError("field %s not in table -- resize aborted" % victim)
        all_records = [record for record in self]
        self.create_backup()
        for victim in doomed:
            specs = list(self._meta[victim])
            delta = new_size - specs[LENGTH]                #self._meta[victim]['length']
            start = specs[START]                            #self._meta[victim]['start']
            end = specs[END]                                #self._meta[victim]['end']
            eff_end = min(specs[LENGTH], new_size)          #min(self._meta[victim]['length'], new_size)
            specs[LENGTH] = new_size                        #self._meta[victim]['length'] = new_size
            specs[END] = start + new_size                   #self._meta[victim]['end'] = start + new_size
            self._meta[victim] = tuple(specs)
            blank = array('c', ' ' * new_size)
            for record in self:
                new_data = blank[:]
                new_data[:eff_end] = record._data[start:start+eff_end]
                record._data = record._data[:start] + new_data + record._data[end:]
            for field in self._meta.fields:
                if self._meta[field][START] == end:
                    specs = list(self._meta[field])
                    end = specs[END]                        #self._meta[field]['end']
                    specs[START] += delta                   #self._meta[field]['start'] += delta
                    specs[END] += delta                     #self._meta[field]['end'] += delta #+ self._meta[field]['length']
                    start = specs[END]                      #self._meta[field]['end']
                    self._meta[field] = tuple(specs)
        self._build_header_fields()
        self._update_disk()
    def size(self, field):
        "returns size of field as a tuple of (length, decimals)"
        if field in self:
            return (self._meta[field][LENGTH], self._meta[field][DECIMALS])
        raise DbfError("%s is not a field in %s" % (field, self.filename))
    def sql(self, command):
        "passes calls through to module level sql function"
        return sql(self, command)
    def structure(self, fields=None):
        """return list of fields suitable for creating same table layout
        @param fields: list of fields or None for all fields"""
        field_specs = []
        fields = self._list_fields(fields)
        try:
            for name in fields:
                field_specs.append(self._field_layout(self.field_names.index(name)))
        except ValueError:
            raise DbfError("field %s does not exist" % name)
        return field_specs
    def top(self, get_record=False):
        """sets record pointer to top of table; if get_record, seeks to and returns first (non-deleted) record
        DbfError if table is empty
        Eof if all records are deleted and use_deleted is False"""
        self._meta.current = -1
        if get_record:
            try:
                return self.next()
            except Eof:
                self._meta.current = -1
                raise Bof()
    def type(self, field):
        "returns (dbf type, class) of field"
        if field in self:
            return FieldType(self._meta[field][TYPE], self._meta[field][CLASS])
        raise DbfError("%s is not a field in %s" % (field, self.filename))
    def zap(self, areyousure=False):
        """removes all records from table -- this cannot be undone!
        areyousure must be True, else error is raised"""
        if areyousure:
            if self._meta.inmemory:
                self._table = []
            else:
                self._table.clear()
                if self._meta.memo:
                    self._meta.memo._zap()
            self._meta.header.record_count = 0
            self._current = -1
            self._update_disk()
        else:
            raise DbfError("You must say you are sure to wipe the table")
class Db3Table(DbfTable):
    """Provides an interface for working with dBase III tables."""
    _version = 'dBase III Plus'
    _versionabbr = 'db3'
    @MutableDefault
    def _field_types():
        return {
            'C' : {
                    'Type':'Character', 'Retrieve':retrieve_character, 'Update':update_character, 'Blank':str, 'Init':add_character,
                    'Class':unicode, 'Empty':unicode, 'flags':tuple(),
                    },
            'D' : {
                    'Type':'Date', 'Retrieve':retrieve_date, 'Update':update_date, 'Blank':Date, 'Init':add_date,
                    'Class':datetime.date, 'Empty':none, 'flags':tuple(),
                    },
            'F' : {
                    'Type':'Numeric', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':int, 'Init':add_numeric,
                    'Class':'default', 'Empty':none, 'flags':tuple(),
                    },
            'L' : {
                    'Type':'Logical', 'Retrieve':retrieve_logical, 'Update':update_logical, 'Blank':Logical, 'Init':add_logical,
                    'Class':bool, 'Empty':none, 'flags':tuple(),
                    },
            'M' : {
                    'Type':'Memo', 'Retrieve':retrieve_memo, 'Update':update_memo, 'Blank':str, 'Init':add_memo,
                    'Class':unicode, 'Empty':unicode, 'flags':tuple(),
                    },
            'N' : {
                    'Type':'Numeric', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':int, 'Init':add_numeric,
                    'Class':'default', 'Empty':none, 'flags':tuple(),
                    } }
    _memoext = '.dbt'
    _memotypes = ('M',)
    _memoClass = _Db3Memo
    _yesMemoMask = '\x80'
    _noMemoMask = '\x7f'
    _fixed_fields = ('D','L','M')
    _variable_fields = ('C','N')
    _binary_fields = ()
    _character_fields = ('C','M') 
    _decimal_fields = ('N',)
    _numeric_fields = ('N',)
    _currency_fields = tuple()
    _date_fields = ('D',)
    _datetime_fields = tuple()
    _logical_fields = ('L',)
    _dbfTableHeader = array('c', '\x00' * 32)
    _dbfTableHeader[0] = '\x03'         # version - dBase III w/o memo's
    _dbfTableHeader[8:10] = array('c', pack_short_int(33))
    _dbfTableHeader[10] = '\x01'        # record length -- one for delete flag
    _dbfTableHeader[29] = '\x03'        # code page -- 437 US-MS DOS
    _dbfTableHeader = _dbfTableHeader.tostring()
    _dbfTableHeaderExtra = ''
    _supported_tables = ['\x03', '\x83']
    _read_only = False
    _meta_only = False
    _use_deleted = True

    def _check_memo_integrity(self):
        "dBase III specific"
        if not self._meta.ignorememos:
            memo_fields = False
            for field in self._meta.fields:
                if self._meta[field][TYPE] in self._memotypes:
                    memo_fields = True
                    break
            if memo_fields and self._meta.header.version != '\x83':
                self._meta.dfd.close()
                self._meta.dfd = None
                raise DbfError("Table structure corrupt:  memo fields exist, header declares no memos")
            elif memo_fields and not os.path.exists(self._meta.memoname):
                self._meta.dfd.close()
                self._meta.dfd = None
                raise DbfError("Table structure corrupt:  memo fields exist without memo file")
            if memo_fields:
                try:
                    self._meta.memo = self._memoClass(self._meta)
                except Exception, exc:
                    self._meta.dfd.close()
                    self._meta.dfd = None
                    raise DbfError("Table structure corrupt:  unable to use memo file (%s)" % exc.args[-1])

    def _initialize_fields(self):
        "builds the FieldList of names, types, and descriptions"
        old_fields = defaultdict(dict)
        for name in self._meta.fields:
            old_fields[name]['type'] = self._meta[name][TYPE]
            old_fields[name]['empty'] = self._meta[name][EMPTY]
            old_fields[name]['class'] = self._meta[name][CLASS]
        self._meta.fields[:] = []
        offset = 1
        fieldsdef = self._meta.header.fields
        if len(fieldsdef) % 32 != 0:
            raise DbfError("field definition block corrupt: %d bytes in size" % len(fieldsdef))
        if len(fieldsdef) // 32 != self._meta.header.field_count:
            raise DbfError("Header shows %d fields, but field definition block has %d fields" % (self._meta.header.field_count, len(fieldsdef)//32))
        for i in range(self._meta.header.field_count):
            fieldblock = fieldsdef[i*32:(i+1)*32]
            name = unpack_str(fieldblock[:11])
            type = fieldblock[11]
            if not type in self._meta.fieldtypes:
                raise DbfError("Unknown field type: %s" % type)
            start = offset
            length = ord(fieldblock[16])
            offset += length
            end = start + length
            decimals = ord(fieldblock[17])
            flags = ord(fieldblock[18])
            if name in self._meta.fields:
                raise DbfError('Duplicate field name found: %s' % name)
            self._meta.fields.append(name)
            if name in old_fields and old_fields[name]['type'] == type:
                cls = old_fields[name]['class']
                empty = old_fields[name]['empty']
            else:
                cls = self._meta.fieldtypes[type]['Class']
                empty = self._meta.fieldtypes[type]['Empty']
            self._meta[name] = (
                    type,
                    start,
                    length,
                    end,
                    decimals,
                    flags,
                    cls,
                    empty,
                    )
        self._meta.user_fields = [f for f in self._meta.fields if not self._meta[f][FLAGS] & SYSTEM]
        self._meta.user_field_count = len(self._meta.user_fields)
class FpTable(DbfTable):
    'Provides an interface for working with FoxPro 2 tables'
    _version = 'Foxpro'
    _versionabbr = 'fp'
    @MutableDefault
    def _field_types():
        return {
            'C' : {
                    'Type':'Character', 'Retrieve':retrieve_character, 'Update':update_character, 'Blank':str, 'Init':add_character,
                    'Class':unicode, 'Empty':unicode, 'flags':('binary','nocptrans','null', ),
                    },
            'F' : {
                    'Type':'Float', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':float, 'Init':add_vfp_numeric,
                    'Class':'default', 'Empty':none, 'flags':('null', ),
                    },
            'N' : {
                    'Type':'Numeric', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':int, 'Init':add_vfp_numeric,
                    'Class':'default', 'Empty':none, 'flags':('null', ),
                    },
            'L' : {
                    'Type':'Logical', 'Retrieve':retrieve_logical, 'Update':update_logical, 'Blank':Logical, 'Init':add_logical,
                    'Class':bool, 'Empty':none, 'flags':('null', ),
                    },
            'D' : {
                    'Type':'Date', 'Retrieve':retrieve_date, 'Update':update_date, 'Blank':Date, 'Init':add_date,
                    'Class':datetime.date, 'Empty':none, 'flags':('null', ),
                    },
            'M' : {
                    'Type':'Memo', 'Retrieve':retrieve_memo, 'Update':update_memo, 'Blank':str, 'Init':add_vfp_memo,
                    'Class':unicode, 'Empty':unicode, 'flags':('binary','nocptrans','null', ),
                    },
            'G' : {
                    'Type':'General', 'Retrieve':retrieve_memo, 'Update':update_memo, 'Blank':str, 'Init':add_memo,
                    'Class':bytes, 'Empty':bytes, 'flags':('null', ),
                    },
            'P' : {
                    'Type':'Picture', 'Retrieve':retrieve_memo, 'Update':update_memo, 'Blank':str, 'Init':add_memo,
                    'Class':bytes, 'Empty':bytes, 'flags':('null', ),
                    },
            '0' : {
                    'Type':'_NullFlags', 'Retrieve':unsupported_type, 'Update':unsupported_type, 'Blank':int, 'Init':None,
                    'Class':none, 'Empty':none, 'flags':('binary','system', ),
                    } }
    _memoext = '.fpt'
    _memotypes = ('G','M','P')
    _memoClass = _VfpMemo
    _yesMemoMask = '\xf5'               # 1111 0101
    _noMemoMask = '\x03'                # 0000 0011
    _fixed_fields = ('B','D','G','I','L','M','P','T','Y')
    _variable_fields = ('C','F','N')
    _binary_fields = ('G','P')
    _character_fields = ('C','M')       # field representing character data
    _decimal_fields = ('F','N')
    _numeric_fields = ('F','N')
    _currency_fields = tuple()
    _date_fields = ('D',)
    _datetime_fields = tuple()
    _logical_fields = ('L',)
    _supported_tables = ('\x03', '\xf5')
    _dbfTableHeader = array('c', '\x00' * 32)
    _dbfTableHeader[0] = '\x30'         # version - Foxpro 6  0011 0000
    _dbfTableHeader[8:10] = array('c', pack_short_int(33+263))
    _dbfTableHeader[10] = '\x01'        # record length -- one for delete flag
    _dbfTableHeader[29] = '\x03'        # code page -- 437 US-MS DOS
    _dbfTableHeader = _dbfTableHeader.tostring()
    _dbfTableHeaderExtra = '\x00' * 263
    _use_deleted = True
    def _check_memo_integrity(self):
        if not self._meta.ignorememos:
            memo_fields = False
            for field in self._meta.fields:
                if self._meta[field][TYPE] in self._memotypes:
                    memo_fields = True
                    break
            if memo_fields and not os.path.exists(self._meta.memoname):
                self._meta.dfd.close()
                self._meta.dfd = None
                raise DbfError("Table structure corrupt:  memo fields exist without memo file")
            elif not memo_fields and os.path.exists(self._meta.memoname):
                self._meta.dfd.close()
                self._meta.dfd = None
                raise DbfError("Table structure corrupt:  no memo fields exist but memo file does")
            if memo_fields:
                try:
                    self._meta.memo = self._memoClass(self._meta)
                except Exception, exc:
                    self._meta.dfd.close()
                    self._meta.dfd = None
                    raise DbfError("Table structure corrupt:  unable to use memo file (%s)" % exc.args[-1])

    def _initialize_fields(self):
        "builds the FieldList of names, types, and descriptions"
        old_fields = defaultdict(dict)
        for name in self._meta.fields:
            old_fields[name]['type'] = self._meta[name][TYPE]
            old_fields[name]['class'] = self._meta[name][CLASS]
        self._meta.fields[:] = []
        offset = 1
        fieldsdef = self._meta.header.fields
        if len(fieldsdef) % 32 != 0:
            raise DbfError("field definition block corrupt: %d bytes in size" % len(fieldsdef))
        if len(fieldsdef) // 32 != self._meta.header.field_count:
            raise DbfError("Header shows %d fields, but field definition block has %d fields" % (self._meta.header.field_count, len(fieldsdef)//32))
        for i in range(self._meta.header.field_count):
            fieldblock = fieldsdef[i*32:(i+1)*32]
            name = unpack_str(fieldblock[:11])
            type = fieldblock[11]
            if not type in self._meta.fieldtypes:
                raise DbfError("Unknown field type: %s" % type)
            start = offset
            length = ord(fieldblock[16])
            offset += length
            end = start + length
            decimals = ord(fieldblock[17])
            flags = ord(fieldblock[18])
            if name in self._meta.fields:
                raise DbfError('Duplicate field name found: %s' % name)
            self._meta.fields.append(name)
            if name in old_fields and old_fields[name]['type'] == type:
                cls = old_fields[name]['class']
                empty = old_fields[name]['empty']
            else:
                cls = self._meta.fieldtypes[type]['Class']
                empty = self._meta.fieldtypes[type]['Empty']
            self._meta[name] = (
                    type,
                    start,
                    length,
                    end,
                    decimals,
                    flags,
                    cls,
                    empty,
                    )
        self._meta.user_fields = [f for f in self._meta.fields if not self._meta[f][FLAGS] & SYSTEM]
        self._meta.user_field_count = len(self._meta.user_fields)
    @staticmethod
    def _pack_date(date):
            "Returns a group of three bytes, in integer form, of the date"
            return "%c%c%c" % (date.year-2000, date.month, date.day)
    @staticmethod
    def _unpack_date(bytestr):
            "Returns a Date() of the packed three-byte date passed in"
            year, month, day = struct.unpack('<BBB', bytestr)
            year += 2000
            return Date(year, month, day)
            
class VfpTable(FpTable):
    'Provides an interface for working with Visual FoxPro 6 tables'
    _version = 'Visual Foxpro'
    _versionabbr = 'vfp'
    @MutableDefault
    def _field_types():
        return {
            'C' : {
                    'Type':'Character', 'Retrieve':retrieve_character, 'Update':update_character, 'Blank':str, 'Init':add_character,
                    'Class':unicode, 'Empty':unicode, 'flags':('binary','nocptrans','null', ), 
                    },
            'Y' : {
                    'Type':'Currency', 'Retrieve':retrieve_currency, 'Update':update_currency, 'Blank':Decimal, 'Init':add_vfp_currency,
                    'Class':Decimal, 'Empty':none, 'flags':('null', ),
                    },
            'B' : {
                    'Type':'Double', 'Retrieve':retrieve_double, 'Update':update_double, 'Blank':float, 'Init':add_vfp_double,
                    'Class':float, 'Empty':none, 'flags':('null', ),
                    },
            'F' : {
                    'Type':'Float', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':float, 'Init':add_vfp_numeric,
                    'Class':'default', 'Empty':none, 'flags':('null', ),
                    },
            'N' : {
                    'Type':'Numeric', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':int, 'Init':add_vfp_numeric,
                    'Class':'default', 'Empty':none, 'flags':('null', ),
                    },
            'I' : {
                    'Type':'Integer', 'Retrieve':retrieve_integer, 'Update':update_integer, 'Blank':int, 'Init':add_vfp_integer,
                    'Class':int, 'Empty':none, 'flags':('null', ),
                    },
            'L' : {
                    'Type':'Logical', 'Retrieve':retrieve_logical, 'Update':update_logical, 'Blank':Logical, 'Init':add_logical,
                    'Class':bool, 'Empty':none, 'flags':('null', ),
                    },
            'D' : {
                    'Type':'Date', 'Retrieve':retrieve_date, 'Update':update_date, 'Blank':Date, 'Init':add_date,
                    'Class':datetime.date, 'Empty':none, 'flags':('null', ),
                    },
            'T' : {
                    'Type':'DateTime', 'Retrieve':retrieve_vfp_datetime, 'Update':update_vfp_datetime, 'Blank':DateTime, 'Init':add_vfp_datetime,
                    'Class':datetime.datetime, 'Empty':none, 'flags':('null', ),
                    },
            'M' : {
                    'Type':'Memo', 'Retrieve':retrieve_vfp_memo, 'Update':update_vfp_memo, 'Blank':str, 'Init':add_vfp_memo,
                    'Class':unicode, 'Empty':unicode, 'flags':('binary','nocptrans','null', ), 
                    },
            'G' : {
                    'Type':'General', 'Retrieve':retrieve_vfp_memo, 'Update':update_vfp_memo, 'Blank':str, 'Init':add_vfp_memo,
                    'Class':bytes, 'Empty':bytes, 'flags':('null', ),
                    },
            'P' : {
                    'Type':'Picture', 'Retrieve':retrieve_vfp_memo, 'Update':update_vfp_memo, 'Blank':str, 'Init':add_vfp_memo,
                    'Class':bytes, 'Empty':bytes, 'flags':('null', ),
                    },
            '0' : {
                    'Type':'_NullFlags', 'Retrieve':unsupported_type, 'Update':unsupported_type, 'Blank':int, 'Init':int,
                    'Class':none, 'Empty':none, 'flags':('binary','system',),
                    } }
    _memoext = '.fpt'
    _memotypes = ('G','M','P')
    _memoClass = _VfpMemo
    _yesMemoMask = '\x30'               # 0011 0000
    _noMemoMask = '\x30'                # 0011 0000
    _fixed_fields = ('B','D','G','I','L','M','P','T','Y')
    _variable_fields = ('C','F','N')
    _binary_fields = ('G','P')
    _character_fields = ('C','M')       # field representing character data
    _currency_fields = ('Y',)
    _decimal_fields = ('F','N')
    _numeric_fields = ('B','F','I','N','Y')
    _date_fields = ('D',)
    _datetime_fields = ('T',)
    _logical_fields = ('L',)
    _supported_tables = ('\x30','\x31')
    _dbfTableHeader = array('c', '\x00' * 32)
    _dbfTableHeader[0] = '\x30'         # version - Foxpro 6  0011 0000
    _dbfTableHeader[8:10] = array('c', pack_short_int(33+263))
    _dbfTableHeader[10] = '\x01'        # record length -- one for delete flag
    _dbfTableHeader[29] = '\x03'        # code page -- 437 US-MS DOS
    _dbfTableHeader = _dbfTableHeader.tostring()
    _dbfTableHeaderExtra = '\x00' * 263
    _use_deleted = True
    def _initialize_fields(self):
        "builds the FieldList of names, types, and descriptions"
        old_fields = defaultdict(dict)
        for name in self._meta.fields:
            old_fields[name]['type'] = self._meta[name][TYPE]
            old_fields[name]['class'] = self._meta[name][CLASS]
            old_fields[name]['empty'] = self._meta[name][EMPTY]
        self._meta.fields[:] = []
        offset = 1
        fieldsdef = self._meta.header.fields
        self._meta.nullflags = None
        for i in range(self._meta.header.field_count):
            fieldblock = fieldsdef[i*32:(i+1)*32]
            name = unpack_str(fieldblock[:11])
            type = fieldblock[11]
            if not type in self._meta.fieldtypes:
                raise DbfError("Unknown field type: %s" % type)
            start = unpack_long_int(fieldblock[12:16])
            length = ord(fieldblock[16])
            offset += length
            end = start + length
            decimals = ord(fieldblock[17])
            flags = ord(fieldblock[18])
            if name in self._meta.fields:
                raise DbfError('Duplicate field name found: %s' % name)
            self._meta.fields.append(name)
            if name in old_fields and old_fields[name]['type'] == type:
                cls = old_fields[name]['class']
                empty = old_fields[name]['empty']
            else:
                cls = self._meta.fieldtypes[type]['Class']
                empty = self._meta.fieldtypes[type]['Empty']
            self._meta[name] = (
                    type,
                    start,
                    length,
                    end,
                    decimals,
                    flags,
                    cls,
                    empty,
                    )
        self._meta.user_fields = [f for f in self._meta.fields if not self._meta[f][FLAGS] & SYSTEM]
        self._meta.user_field_count = len(self._meta.user_fields)
    @staticmethod
    def _pack_date(date):
            "Returns a group of three bytes, in integer form, of the date"
            return "%c%c%c" % (date.year-2000, date.month, date.day)
    @staticmethod
    def _unpack_date(bytestr):
            "Returns a Date() of the packed three-byte date passed in"
            year, month, day = struct.unpack('<BBB', bytestr)
            year += 2000
            return Date(year, month, day)
                    
class List(object):
    "list of Dbf records, with set-like behavior"
    _desc = ''
    def __init__(self, new_records=None, desc=None, key=None, field_names=None):
        self.field_names = field_names
        self._list = []
        self._set = set()
        if key is not None:
            self.key = key
            if key.__doc__ is None:
                key.__doc__ = 'unknown'
        key = self.key
        self._current = -1
        if isinstance(new_records, self.__class__) and key is new_records.key:
                self._list = new_records._list[:]
                self._set = new_records._set.copy()
                self._current = 0
        elif new_records is not None:
            for record in new_records:
                value = key(record)
                item = (record.record_table, record.record_number, value)
                if value not in self._set:
                    self._set.add(value)
                    self._list.append(item)
            self._current = 0
        if desc is not None:
            self._desc = desc
    def __add__(self, other):
        key = self.key
        if isinstance(other, (DbfTable, list)):
            other = self.__class__(other, key=key)
        if isinstance(other, self.__class__):
            result = self.__class__()
            result._set = self._set.copy()
            result._list[:] = self._list[:]
            result.key = self.key
            if key is other.key:   # same key?  just compare key values
                for item in other._list:
                    if item[2] not in result._set:
                        result._set.add(item[2])
                        result._list.append(item)
            else:                   # different keys, use this list's key on other's records
                for rec in other:
                    value = key(rec)
                    if value not in result._set:
                        result._set.add(value)
                        result._list.append((rec.record_table, rec.record_number, value))
            result._current = (-1, 0)[bool(result)]
            return result
        return NotImplemented
    def __contains__(self, record):
        if not isinstance(record, _DbfRecord):
            raise ValueError("%r is not a record" % record)
        item = self.key(record)
        if not isinstance(item, tuple):
            item = (item, )
        return item in self._set and self._get_record(*item) is record
    def __delitem__(self, key):
        if isinstance(key, int):
            item = self._list.pop[key]
            self._set.remove(item[2])
        elif isinstance(key, slice):
            self._set.difference_update([item[2] for item in self._list[key]])
            self._list.__delitem__(key)
        elif isinstance(key, _DbfRecord):
            index = self.index(key)
            item = self._list.pop[index]
            self._set.remove(item[2])
        else:
            raise TypeError
    def __getitem__(self, key):
        if isinstance(key, int):
            count = len(self._list)
            if not -count <= key < count:
                raise IndexError("Record %d is not in list." % key)
            return self._get_record(*self._list[key])
        elif isinstance(key, slice):
            result = self.__class__()
            result._list[:] = self._list[key]
            result._set = set(result._list)
            result.key = self.key
            result._current = (-1, 0)[bool(result)]
            return result
        elif isinstance(key, _DbfRecord):
            index = self.index(key)
            return self._get_record(*self._list[index])
        else:
            raise TypeError('indices must be integers')
    def __iter__(self):
        return (table.get_record(recno) for table, recno, value in self._list)
    def __len__(self):
        return len(self._list)
    def __nonzero__(self):
        return len(self) > 0
    def __radd__(self, other):
        return self.__add__(other)
    def __repr__(self):
        if self._desc:
            return "%s(key=%s - %s - %d records)" % (self.__class__, self.key.__doc__, self._desc, len(self._list))
        else:
            return "%s(key=%s - %d records)" % (self.__class__, self.key.__doc__, len(self._list))
    def __rsub__(self, other):
        key = self.key
        if isinstance(other, (DbfTable, list)):
            other = self.__class__(other, key=key)
        if isinstance(other, self.__class__):
            result = self.__class__()
            result._list[:] = other._list[:]
            result._set = other._set.copy()
            result.key = key
            lost = set()
            if key is other.key:
                for item in self._list:
                    if item[2] in result._list:
                        result._set.remove(item[2])
                        lost.add(item)
            else:
                for rec in other:
                    value = key(rec)
                    if value in result._set:
                        result._set.remove(value)
                        lost.add((rec.record_table, rec.record_number, value))
            result._list = [item for item in result._list if item not in lost]
            result._current = (-1, 0)[bool(result)]
            return result
        return NotImplemented
    def __sub__(self, other):
        key = self.key
        if isinstance(other, (DbfTable, list)):
            other = self.__class__(other, key=key)
        if isinstance(other, self.__class__):
            result = self.__class__()
            result._list[:] = self._list[:]
            result._set = self._set.copy()
            result.key = key
            lost = set()
            if key is other.key:
                for item in other._list:
                    if item[2] in result._set:
                        result._set.remove(item[2])
                        lost.add(item[2])
            else:
                for rec in other:
                    value = key(rec)
                    if value in result._set:
                        result._set.remove(value)
                        lost.add(value)
            result._list = [item for item in result._list if item[2] not in lost]
            result._current = (-1, 0)[bool(result)]
            return result
        return NotImplemented
    def _maybe_add(self, item):
        if item[2] not in self._set:
            self._set.add(item[2])
            self._list.append(item)
    def _get_record(self, table=None, rec_no=None, value=None):
        if table is rec_no is None:
            table, rec_no, value = self._list[self._current]
        return table.get_record(rec_no)
    def _purge(self, record, old_record_number, offset):
        partial = record.record_table, old_record_number
        records = sorted(self._list, key=lambda item: (item[0], item[1]))
        for item in records:
            if partial == item[:2]:
                found = True
                break
            elif partial[0] is item[0] and partial[1] < item[1]:
                found = False
                break
        else:
            found = False
        if found:
            self._list.pop(self._list.index(item))
            self._set.remove(item[2])
        start = records.index(item) + found
        for item in records[start:]:
            if item[0] is not partial[0]:       # into other table's records
                break
            i = self._list.index(item)
            self._set.remove(item[2])
            item = item[0], (item[1] - offset), item[2]
            self._list[i] = item
            self._set.add(item[2])
        return found
    def append(self, new_record):
        self._maybe_add((new_record.record_table, new_record.record_number, self.key(new_record)))
        if self._current == -1 and self._list:
            self._current = 0
    def bottom(self):
        if self._list:
            self._current = len(self._list) - 1
            return self._get_record()
        raise DbfError("dbf.List is empty")
    def clear(self):
        self._list = []
        self._set = set()
        self._current = -1
    def current(self):
        if self._current < 0:
            raise Bof()
        elif self._current == len(self._list):
            raise Eof()
        return self._get_record()
    def extend(self, new_records):
        key = self.key
        if isinstance(new_records, self.__class__):
            if key is new_records.key:   # same key?  just compare key values
                for item in new_records._list:
                    self._maybe_add(item)
            else:                   # different keys, use this list's key on other's records
                for rec in new_records:
                    value = key(rec)
                    self._maybe_add((rec.record_table, rec.record_number, value))
        else:
            for rec in new_records:
                value = key(rec)
                self._maybe_add((rec.record_table, rec.record_number, value))
        if self._current == -1 and self._list:
            self._current = 0
    def goto(self, index_number):
        if self._list:
            if 0 <= index_number <= len(self._list):
                self._current = index_number
                return self._get_record()
            raise DbfError("index %d not in dbf.List of %d records" % (index_number, len(self._list)))
        raise DbfError("dbf.List is empty")

    def index(self, record, start=None, stop=None):
        item = record.record_table, record.record_number, self.key(record)
        key = self.key(record)
        if start is None:
            start = 0
        if stop is None:
            stop = len(self._list)
        for i in range(start, stop):
            if self._list[i][2] == key:
                return i
        else:
            raise ValueError("dbf.List.index(x): <x=%r> not in list" % (key,))
    def insert(self, i, record):
        item = record.record_table, record.record_number, self.key(record)
        if item not in self._set:
            self._set.add(item[2])
            self._list.insert(i, item)
    def key(self, record):
        "table_name, record_number"
        return record.record_table, record.record_number
    def next(self):
        if self._current < len(self._list):
            self._current += 1
            if self._current < len(self._list):
                return self._get_record()
        raise Eof()
    def pop(self, index=None):
        if index is None:
            table, recno, value = self._list.pop()
        else:
            table, recno, value = self._list.pop(index)
        self._set.remove(value)
        return self._get_record(table, recno, value)
    def prev(self):
        if self._current >= 0:
            self._current -= 1
            if self._current > -1:
                return self._get_record()
        raise Bof()
    def remove(self, record):
        item = record.record_table, record.record_number, self.key(record)
        self._list.remove(item)
        self._set.remove(item[2])
    def reverse(self):
        return self._list.reverse()
    def top(self):
        if self._list:
            self._current = 0
            return self._get_record()
        raise DbfError("dbf.List is empty")
    def sort(self, key=None, reverse=False):
        if key is None:
            return self._list.sort(reverse=reverse)
        return self._list.sort(key=lambda item: key(item[0].get_record(item[1])), reverse=reverse)

class Index(object):
    class IndexIterator(object):
        "returns records using this index"
        def __init__(self, table, records):
            self.table = table
            self.records = records[:]
            self.index = 0
        def __iter__(self):
            return self
        def next(self):
            while self.index < len(self.records):
                record = self.table.get_record(self.records[self.index])
                self.index += 1
                if not self.table.use_deleted and record.has_been_deleted:
                    continue
                return record
            else:
                raise StopIteration
    def __init__(self, table, key, field_names=None):
        self._table = table
        self._values = []             # ordered list of values
        self._rec_by_val = []         # matching record numbers
        self._records = {}            # record numbers:values
        self.__doc__ = key.__doc__ or 'unknown'
        self.key = key
        self.field_names = field_names or table.field_names
        for record in table:
            value = key(record)
            if value is DoNotIndex:
                continue
            rec_num = record.record_number
            if not isinstance(value, tuple):
                value = (value, )
            vindex = bisect_right(self._values, value)
            self._values.insert(vindex, value)
            self._rec_by_val.insert(vindex, rec_num)
            self._records[rec_num] = value
        table._indexen.add(self)
    def __call__(self, record):
        rec_num = record.record_number
        key = self.key(record)
        if not isinstance(key, tuple):
            key = (key, )
        if rec_num in self._records:
            if self._records[rec_num] == key:
                return
            key = self._records[rec_num]
            vindex = bisect_left(self._values, key)
            self._values.pop(vindex)
            self._rec_by_val.pop(vindex)
        key = self.key(record)
        if key is DoNotIndex:
            return
        if not isinstance(key, tuple):
            key = (key, )
        vindex = bisect_right(self._values, key)
        self._values.insert(vindex, key)
        self._rec_by_val.insert(vindex, rec_num)
        self._records[rec_num] = key
    def __contains__(self, record):
        if not isinstance(record, _DbfRecord):
            raise ValueError("%r is not a record" % record)
        if record.record_table is self._table:
            return record.record_number in self._records
        raise ValueError("record is from table %r, not %r" % (record.table, self._table))
    def __getitem__(self, key):
        if isinstance(key, int):
            count = len(self._values)
            if not -count <= key < count:
                raise IndexError("Record %d is not in list." % key)
            rec_num = self._rec_by_val[key]
            return self._table.get_record(rec_num)
        elif isinstance(key, slice):
            result = List(field_names=self._table.field_names)
            self._table._dbflists.add(result)
            start, stop, step = key.start, key.stop, key.step
            if start is None: start = 0
            if stop is None: stop = len(self._rec_by_val)
            if step is None: step = 1
            if step < 0:
                start, stop = stop - 1, -(stop - start + 1)
            for loc in range(start, stop, step):
                record = self._table.get_record(self._rec_by_val[loc])
                result._maybe_add(item=(self._table, self._rec_by_val[loc], result.key(record)))
            result._current = (-1, 0)[bool(result)]
            return result
        elif isinstance (key, (str, unicode, tuple, _DbfRecord)):
            if isinstance(key, _DbfRecord):
                key = self.key(key)
            elif not isinstance(key, tuple):
                key = (key, )
            loc = self.find(key)
            if loc == -1:
                raise KeyError(key)
            return self._table.get_record(self._rec_by_val[loc])
        else:
            raise TypeError('indices must be integers, match objects must by strings or tuples')
    def __enter__(self):
        return self
    def __exit__(self, *exc_info):
        self._table.close()
        self._values[:] = []
        self._rec_by_val[:] = []
        self._records.clear()
        return False
    def __iter__(self):
        return self.IndexIterator(self._table, self._rec_by_val)
    def __len__(self):
        return len(self._records)
    def _partial_match(self, target, match):
        target = target[:len(match)]
        if isinstance(match[-1], (str, unicode)):
            target = list(target)
            target[-1] = target[-1][:len(match[-1])]
            target = tuple(target)
        return target == match
    def _purge(self, rec_num):
        value = self._records.get(rec_num)
        if value is not None:
            vindex = bisect_left(self._values, value)
            del self._records[rec_num]
            self._values.pop(vindex)
            self._rec_by_val.pop(vindex)
    def _search(self, match, lo=0, hi=None):
        if hi is None:
            hi = len(self._values)
        return bisect_left(self._values, match, lo, hi)
    def clear(self):
        "removes all entries from index"
        self._values[:] = []
        self._rec_by_val[:] = []
        self._records.clear()
    def close(self):
        self._table.close()
    def find(self, match, partial=False):
        "returns numeric index of (partial) match, or -1"
        if isinstance(match, _DbfRecord):
            if match.record_number in self._records:
                return self._values.index(self._records[match.record_number])
            else:
                return -1
        if not isinstance(match, tuple):
            match = (match, )
        loc = self._search(match)
        while loc < len(self._values) and self._values[loc] == match:
            if not self._table.use_deleted and self._table.get_record(self._rec_by_val[loc]).has_been_deleted:
                loc += 1
                continue
            return loc
        if partial:
            while loc < len(self._values) and self._partial_match(self._values[loc], match):
                if not self._table.use_deleted and self._table.get_record(self._rec_by_val[loc]).has_been_deleted:
                    loc += 1
                    continue
                return loc
        return -1
    def find_index(self, match):
        "returns numeric index of either (partial) match, or position of where match would be"
        if isinstance(match, _DbfRecord):
            if match.record_number in self._records:
                return self._values.index(self._records[match.record_number])
            else:
                match = self.key(match)
        if not isinstance(match, tuple):
            match = (match, )
        loc = self._search(match)
        return loc
    def index(self, match, partial=False):
        "returns numeric index of (partial) match, or raises IndexError"
        loc = self.find(match, partial)
        if loc == -1:
            if isinstance(match, _DbfRecord):
                raise IndexError("table <%s> record [%d] not in index <%s>" % (self._table.filename, match.record_number, self.__doc__))
            else:
                raise IndexError("match criteria <%s> not in index" % (match, ))
        return loc
    def reindex(self):
        "reindexes all records"
        for record in self._table:
            self(record)
    def query(self, sql_command=None, python=None):
        """recognized sql commands are SELECT, UPDATE, REPLACE, INSERT, DELETE, and RECALL"""
        if sql_command:
            return sql(self, sql_command)
        elif python is None:
            raise DbfError("query: python parameter must be specified")
        possible = List(desc="%s -->  %s" % (self._table.filename, python), field_names=self._table.field_names)
        self._table._dbflists.add(possible)
        query_result = {}
        select = 'query_result["keep"] = %s' % python
        g = {}
        for record in self:
            query_result['keep'] = False
            g['query_result'] = query_result
            exec select in g, record
            if query_result['keep']:
                possible.append(record)
            record.write_record()
        return possible
    def search(self, match, partial=False):
        "returns dbf.List of all (partially) matching records"
        result = List(field_names=self._table.field_names)
        self._table._dbflists.add(result)
        if not isinstance(match, tuple):
            match = (match, )
        loc = self._search(match)
        if loc == len(self._values):
            return result
        while loc < len(self._values) and self._values[loc] == match:
            record = self._table.get_record(self._rec_by_val[loc])
            if not self._table.use_deleted and record.has_been_deleted:
                loc += 1
                continue
            result._maybe_add(item=(self._table, self._rec_by_val[loc], result.key(record)))
            loc += 1
        if partial:
            while loc < len(self._values) and self._partial_match(self._values[loc], match):
                record = self._table.get_record(self._rec_by_val[loc])
                if not self._table.use_deleted and record.has_been_deleted:
                    loc += 1
                    continue
                result._maybe_add(item=(self._table, self._rec_by_val[loc], result.key(record)))
                loc += 1
        return result

# table meta
table_types = {
    'db3' : Db3Table,
    'fp'  : FpTable,
    'vfp' : VfpTable,
    'dbf' : DbfTable,
    }

version_map = {
        '\x02' : 'FoxBASE',
        '\x03' : 'dBase III Plus',
        '\x04' : 'dBase IV',
        '\x05' : 'dBase V',
        '\x30' : 'Visual FoxPro',
        '\x31' : 'Visual FoxPro (auto increment field)',
        '\x32' : 'Visual FoxPro (VarChar, VarBinary, or BLOB enabled)',
        '\x43' : 'dBase IV SQL table files',
        '\x63' : 'dBase IV SQL system files',
        '\x83' : 'dBase III Plus w/memos',
        '\x8b' : 'dBase IV w/memos',
        '\x8e' : 'dBase IV w/SQL table',
        '\xf5' : 'FoxPro w/memos'}

code_pages = {
        '\x00' : ('ascii', "plain ol' ascii"),
        '\x01' : ('cp437', 'U.S. MS-DOS'),
        '\x02' : ('cp850', 'International MS-DOS'),
        '\x03' : ('cp1252', 'Windows ANSI'),
        '\x04' : ('mac_roman', 'Standard Macintosh'),
        '\x08' : ('cp865', 'Danish OEM'),
        '\x09' : ('cp437', 'Dutch OEM'),
        '\x0A' : ('cp850', 'Dutch OEM (secondary)'),
        '\x0B' : ('cp437', 'Finnish OEM'),
        '\x0D' : ('cp437', 'French OEM'),
        '\x0E' : ('cp850', 'French OEM (secondary)'),
        '\x0F' : ('cp437', 'German OEM'),
        '\x10' : ('cp850', 'German OEM (secondary)'),
        '\x11' : ('cp437', 'Italian OEM'),
        '\x12' : ('cp850', 'Italian OEM (secondary)'),
        '\x13' : ('cp932', 'Japanese Shift-JIS'),
        '\x14' : ('cp850', 'Spanish OEM (secondary)'),
        '\x15' : ('cp437', 'Swedish OEM'),
        '\x16' : ('cp850', 'Swedish OEM (secondary)'),
        '\x17' : ('cp865', 'Norwegian OEM'),
        '\x18' : ('cp437', 'Spanish OEM'),
        '\x19' : ('cp437', 'English OEM (Britain)'),
        '\x1A' : ('cp850', 'English OEM (Britain) (secondary)'),
        '\x1B' : ('cp437', 'English OEM (U.S.)'),
        '\x1C' : ('cp863', 'French OEM (Canada)'),
        '\x1D' : ('cp850', 'French OEM (secondary)'),
        '\x1F' : ('cp852', 'Czech OEM'),
        '\x22' : ('cp852', 'Hungarian OEM'),
        '\x23' : ('cp852', 'Polish OEM'),
        '\x24' : ('cp860', 'Portugese OEM'),
        '\x25' : ('cp850', 'Potugese OEM (secondary)'),
        '\x26' : ('cp866', 'Russian OEM'),
        '\x37' : ('cp850', 'English OEM (U.S.) (secondary)'),
        '\x40' : ('cp852', 'Romanian OEM'),
        '\x4D' : ('cp936', 'Chinese GBK (PRC)'),
        '\x4E' : ('cp949', 'Korean (ANSI/OEM)'),
        '\x4F' : ('cp950', 'Chinese Big 5 (Taiwan)'),
        '\x50' : ('cp874', 'Thai (ANSI/OEM)'),
        '\x57' : ('cp1252', 'ANSI'),
        '\x58' : ('cp1252', 'Western European ANSI'),
        '\x59' : ('cp1252', 'Spanish ANSI'),
        '\x64' : ('cp852', 'Eastern European MS-DOS'),
        '\x65' : ('cp866', 'Russian MS-DOS'),
        '\x66' : ('cp865', 'Nordic MS-DOS'),
        '\x67' : ('cp861', 'Icelandic MS-DOS'),
        '\x68' : (None, 'Kamenicky (Czech) MS-DOS'),
        '\x69' : (None, 'Mazovia (Polish) MS-DOS'),
        '\x6a' : ('cp737', 'Greek MS-DOS (437G)'),
        '\x6b' : ('cp857', 'Turkish MS-DOS'),
        '\x78' : ('cp950', 'Traditional Chinese (Hong Kong SAR, Taiwan) Windows'),
        '\x79' : ('cp949', 'Korean Windows'),
        '\x7a' : ('cp936', 'Chinese Simplified (PRC, Singapore) Windows'),
        '\x7b' : ('cp932', 'Japanese Windows'),
        '\x7c' : ('cp874', 'Thai Windows'),
        '\x7d' : ('cp1255', 'Hebrew Windows'),
        '\x7e' : ('cp1256', 'Arabic Windows'),
        '\xc8' : ('cp1250', 'Eastern European Windows'),
        '\xc9' : ('cp1251', 'Russian Windows'),
        '\xca' : ('cp1254', 'Turkish Windows'),
        '\xcb' : ('cp1253', 'Greek Windows'),
        '\x96' : ('mac_cyrillic', 'Russian Macintosh'),
        '\x97' : ('mac_latin2', 'Macintosh EE'),
        '\x98' : ('mac_greek', 'Greek Macintosh') }

default_codepage = code_pages.get(default_codepage, code_pages.get('ascii'))

# SQL functions
def sql_select(records, chosen_fields, condition, field_names):
    if chosen_fields != '*':
        field_names = chosen_fields.replace(' ','').split(',')
    result = condition(records)
    result.modified = 0, 'record' + ('','s')[len(result)>1]
    result.field_names = field_names
    return result

def sql_update(records, command, condition, field_names):
    possible = condition(records)
    modified = sql_cmd(command, field_names)(possible)
    possible.modified = modified, 'record' + ('','s')[modified>1]
    return possible

def sql_delete(records, dead_fields, condition, field_names):
    deleted = condition(records)
    deleted.modified = len(deleted), 'record' + ('','s')[len(deleted)>1]
    deleted.field_names = field_names
    if dead_fields == '*':
        for record in deleted:
            record.delete_record()
            record.write_record()
    else:
        keep = [f for f in field_names if f not in dead_fields.replace(' ','').split(',')]
        for record in deleted:
            record.reset_record(keep_fields=keep)
            record.write_record()
    return deleted

def sql_recall(records, all_fields, condition, field_names):
    if all_fields != '*':
        raise DbfError('SQL RECALL: fields must be * (only able to recover at the record level)')
    revivified = List()
    tables = set()
    for record in records:
        tables.add(record_table)
    old_setting = dict()
    for table in tables:
        old_setting[table] = table.use_deleted
        table.use_deleted = True
    for record in condition(records):
        if record.has_been_deleted:
            revivified.append(record)
            record.undelete_record()
            record.write_record()
    for table in tables:
        table.use_deleted = old_setting[table]
    revivified.modfied = len(revivified), 'record' + ('','s')[len(revivified)>1]
    revivified.field_names = field_names
    return revivified

def sql_add(records, new_fields, condition, field_names):
    tables = set()
    possible = condition(records)
    for record in possible:
        tables.add(record.record_table)
    for table in tables:
        table.add_fields(new_fields)
    possible.modified = len(tables), 'table' + ('','s')[len(tables)>1]
    possible.field_names = field_names
    return possible

def sql_drop(records, dead_fields, condition, field_names):
    tables = set()
    possible = condition(records)
    for record in possible:
        tables.add(record.record_table)
    for table in tables:
        table.delete_fields(dead_fields)
    possible.modified = len(tables), 'table' + ('','s')[len(tables)>1]
    possible.field_names = field_names
    return possible

def sql_pack(records, command, condition, field_names):
    tables = set()
    possible = condition(records)
    for record in possible:
        tables.add(record.record_table)
    for table in tables:
        table.pack()
    possible.modified = len(tables), 'table' + ('','s')[len(tables)>1]
    possible.field_names = field_names
    return possible

def sql_resize(records, fieldname_newsize, condition, field_names):
    tables = set()
    possible = condition(records)
    for record in possible:
        tables.add(record.record_table)
    fieldname, newsize = fieldname_newsize.split()
    newsize = int(newsize)
    for table in tables:
        table.resize_field(fieldname, newsize)
    possible.modified = len(tables), 'table' + ('','s')[len(tables)>1]
    possible.field_names = field_names
    return possible

def sql_criteria(records, criteria):
    "creates a function matching the sql criteria"
    function = """def func(records):
    \"\"\"%s
    \"\"\"
    matched = List(field_names=records[0].field_names)
    for rec in records:
        record_number = rec.record_number
        %s

        if %s:
            matched.append(rec)
    return matched"""
    fields = []
    for field in records[0].field_names:
        if field in criteria:
            fields.append(field)
    fields = '\n        '.join(['%s = rec.%s' % (field, field) for field in fields])
    g = sql_user_functions.copy()
    g['List'] = List
    function %= (criteria, fields, criteria)
    exec function in g
    return g['func']

def sql_cmd(command, field_names):
    "creates a function matching to apply command to each record in records"
    function = """def func(records):
    \"\"\"%s
    \"\"\"
    changed = 0
    for rec in records:
        record_number = rec.record_number
        %s

        %s

        %s
        changed += rec.write_record()
    return changed"""
    fields = []
    for field in field_names:
        if field in command:
            fields.append(field)
    pre_fields = '\n        '.join(['%s = rec.%s' % (field, field) for field in fields])
    post_fields = '\n        '.join(['rec.%s = %s' % (field, field) for field in fields])
    g = sql_user_functions.copy()
    if ' with ' in command.lower():
        offset = command.lower().index(' with ')
        command = command[:offset] + ' = ' + command[offset+6:]
    function %= (command, pre_fields, command, post_fields)
    exec function in g
    return g['func']

def sql(records, command):
    """recognized sql commands are SELECT, UPDATE | REPLACE, DELETE, RECALL, ADD, DROP"""
    close_table = False
    if isinstance(records, (str, unicode)):
        records = Table(records)
        close_table = True
    try:
        sql_command = command
        if ' where ' in command:
            command, condition = command.split(' where ', 1)
            condition = sql_criteria(records, condition)
        else:
            def condition(records):
                return records[:]
        name, command = command.split(' ', 1)
        command = command.strip()
        name = name.lower()
        field_names = records[0].field_names
        if sql_functions.get(name) is None:
            raise DbfError('unknown SQL command: %s' % name.upper())
        result = sql_functions[name](records, command, condition, field_names)
        tables = set()
        for record in result:
            tables.add(record.record_table)
        for list_table in tables:
            list_table._dbflists.add(result)
    finally:
        if close_table:
            records.close()
    return result

sql_functions = {
        'select' : sql_select,
        'update' : sql_update,
        'replace': sql_update,
        'insert' : None,
        'delete' : sql_delete,
        'recall' : sql_recall,
        'add'    : sql_add,
        'drop'   : sql_drop,
        'count'  : None,
        'pack'   : sql_pack,
        'resize' : sql_resize,
        }


def _nop(value):
    "returns parameter unchanged"
    return value
def _normalize_tuples(tuples, length, filler):
    "ensures each tuple is the same length, using filler[-missing] for the gaps"
    final = []
    for t in tuples:
        if len(t) < length:
            final.append( tuple([item for item in t] + filler[len(t)-length:]) )
        else:
            final.append(t)
    return tuple(final)
def _codepage_lookup(cp):
    if cp not in code_pages:
        for code_page in sorted(code_pages.keys()):
            sd, ld = code_pages[code_page]
            if cp == sd or cp == ld:
                if sd is None:
                    raise DbfError("Unsupported codepage: %s" % ld)
                cp = code_page
                break
        else:
            raise DbfError("Unsupported codepage: %s" % cp)
    sd, ld = code_pages[cp]
    return cp, sd, ld
# miscellany

def codepage(cp=None):
    "get/set default codepage for any new tables"
    global default_codepage
    cp, sd, ld = _codepage_lookup(cp or default_codepage)
    default_codepage = sd
    return "%s (LDID: 0x%02x - %s)" % (sd, ord(cp), ld)
def encoding(cp=None):
    "get/set default encoding for non-unicode strings passed into a table"
    global input_decoding
    cp, sd, ld = _codepage_lookup(cp or input_decoding)
    default_codepage = sd
    return "%s (LDID: 0x%02x - %s)" % (sd, ord(cp), ld)
class _Db4Table(DbfTable):
    version = 'dBase IV w/memos (non-functional)'
    _versionabbr = 'db4'
    @MutableDefault
    def _field_types():
        return {
            'C' : {'Type':'Character', 'Retrieve':retrieve_character, 'Update':update_character, 'Blank':str, 'Init':add_character},
            'Y' : {'Type':'Currency', 'Retrieve':retrieve_currency, 'Update':update_currency, 'Blank':Decimal, 'Init':add_vfp_currency},
            'B' : {'Type':'Double', 'Retrieve':retrieve_double, 'Update':update_double, 'Blank':float, 'Init':add_vfp_double},
            'F' : {'Type':'Float', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':float, 'Init':add_vfp_numeric},
            'N' : {'Type':'Numeric', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':int, 'Init':add_vfp_numeric},
            'I' : {'Type':'Integer', 'Retrieve':retrieve_integer, 'Update':update_integer, 'Blank':int, 'Init':add_vfp_integer},
            'L' : {'Type':'Logical', 'Retrieve':retrieve_logical, 'Update':update_logical, 'Blank':Logical, 'Init':add_logical},
            'D' : {'Type':'Date', 'Retrieve':retrieve_date, 'Update':update_date, 'Blank':Date, 'Init':add_date},
            'T' : {'Type':'DateTime', 'Retrieve':retrieve_vfp_datetime, 'Update':update_vfp_datetime, 'Blank':DateTime, 'Init':add_vfp_datetime},
            'M' : {'Type':'Memo', 'Retrieve':retrieve_memo, 'Update':update_memo, 'Blank':str, 'Init':add_memo},
            'G' : {'Type':'General', 'Retrieve':retrieve_memo, 'Update':update_memo, 'Blank':str, 'Init':add_memo},
            'P' : {'Type':'Picture', 'Retrieve':retrieve_memo, 'Update':update_memo, 'Blank':str, 'Init':add_memo},
            '0' : {'Type':'_NullFlags', 'Retrieve':unsupported_type, 'Update':unsupported_type, 'Blank':int, 'Init':None} }
    _memoext = '.dbt'
    _memotypes = ('G','M','P')
    _memoClass = _VfpMemo
    _yesMemoMask = '\x8b'               # 0011 0000
    _noMemoMask = '\x04'                # 0011 0000
    _fixed_fields = ('B','D','G','I','L','M','P','T','Y')
    _variable_fields = ('C','F','N')
    _binary_fields = ('G','P')
    _character_fields = ('C','M')       # field representing character data
    _decimal_fields = ('F','N')
    _numeric_fields = ('B','F','I','N','Y')
    _currency_fields = ('Y',)
    _supported_tables = ('\x04', '\x8b')
    _dbfTableHeader = ['\x00'] * 32
    _dbfTableHeader[0] = '\x8b'         # version - Foxpro 6  0011 0000
    _dbfTableHeader[10] = '\x01'        # record length -- one for delete flag
    _dbfTableHeader[29] = '\x03'        # code page -- 437 US-MS DOS
    _dbfTableHeader = ''.join(_dbfTableHeader)
    _dbfTableHeaderExtra = ''
    _use_deleted = True
    def _check_memo_integrity(self):
        "dBase III specific"
        if self._meta.header.version == '\x8b':
            try:
                self._meta.memo = self._memoClass(self._meta)
            except:
                self._meta.dfd.close()
                self._meta.dfd = None
                raise
        if not self._meta.ignorememos:
            for field in self._meta.fields:
                if self._meta[field][TYPE] in self._memotypes:
                    if self._meta.header.version != '\x8b':
                        self._meta.dfd.close()
                        self._meta.dfd = None
                        raise DbfError("Table structure corrupt:  memo fields exist, header declares no memos")
                    elif not os.path.exists(self._meta.memoname):
                        self._meta.dfd.close()
                        self._meta.dfd = None
                        raise DbfError("Table structure corrupt:  memo fields exist without memo file")
                    break

# utility functions

def Write(records):
    "commits each record to disk before returning the next one"
    for record in records:
        yield record
        record.write_record()
def Table(
        filename, 
        field_specs='', 
        memo_size=128, 
        ignore_memos=False,
        read_only=False, 
        keep_memos=False, 
        meta_only=False, 
        dbf_type=None, 
        codepage=None,
        default_data_types={},
        field_data_types={},
        ):
    "returns an open table of the correct dbf_type, or creates it if field_specs is given"
    if dbf_type is None and isinstance(filename, DbfTable):
        return filename
    if field_specs and dbf_type is None:
        dbf_type = default_type
    if dbf_type is not None:
        dbf_type = dbf_type.lower()
        table = table_types.get(dbf_type)
        if table is None:
            raise DbfError("Unknown table type: %s" % dbf_type)
        return table(filename, field_specs, memo_size, ignore_memos, read_only, keep_memos,
                meta_only, codepage, default_data_types, field_data_types)
    else:
        possibles = guess_table_type(filename)
        if len(possibles) == 1:
            return possibles[0][2](filename, field_specs, memo_size, ignore_memos,
                    read_only, keep_memos, meta_only, codepage, default_data_types, field_data_types)
        else:
            for type, desc, cls in possibles:
                if type == default_type:
                    return cls(filename, field_specs, memo_size, ignore_memos,
                            read_only, keep_memos, meta_only, codepage, default_data_types, field_data_types)
            else:
                types = ', '.join(["%s" % item[1] for item in possibles])
                abbrs = '[' + ' | '.join(["%s" % item[0] for item in possibles]) + ']'
                raise DbfError("Table could be any of %s.  Please specify %s when opening" % (types, abbrs))
def index(sequence):
    "returns integers 0 - len(sequence)"
    for i in xrange(len(sequence)):
        yield i    
def guess_table_type(filename):
    reported = table_type(filename)
    possibles = []
    version = reported[0]
    for tabletype in (Db3Table, FpTable, VfpTable):
        if version in tabletype._supported_tables:
            possibles.append((tabletype._versionabbr, tabletype._version, tabletype))
    if not possibles:
        raise DbfError("Tables of type %s not supported" % str(reported))
    return possibles
def table_type(filename):
    "returns text representation of a table's dbf version"
    base, ext = os.path.splitext(filename)
    if ext == '':
        filename = base + '.dbf'
    if not os.path.exists(filename):
        raise DbfError('File %s not found' % filename)
    fd = open(filename)
    version = fd.read(1)
    fd.close()
    fd = None
    if not version in version_map:
        raise DbfError("Unknown dbf type: %s (%x)" % (version, ord(version)))
    return version, version_map[version]

def add_fields(table_name, field_specs):
    "adds fields to an existing table"
    table = Table(table_name)
    try:
        table.add_fields(field_specs)
    finally:
        table.close()
def delete_fields(table_name, field_names):
    "deletes fields from an existing table"
    table = Table(table_name)
    try:
        table.delete_fields(field_names)
    finally:
        table.close()
def export(table_name, filename='', fields='', format='csv', header=True):
    "creates a csv or tab-delimited file from an existing table"
    if fields is None:
        fields = []
    table = Table(table_name)
    try:
        table.export(filename=filename, field_specs=fields, format=format, header=header)
    finally:
        table.close()
def first_record(table_name):
    "prints the first record of a table"
    table = Table(table_name)
    try:
        print str(table[0])
    finally:
        table.close()
def from_csv(csvfile, to_disk=False, filename=None, field_names=None, extra_fields=None, dbf_type='db3', memo_size=64, min_field_size=1):
    """creates a Character table from a csv file
    to_disk will create a table with the same name
    filename will be used if provided
    field_names default to f0, f1, f2, etc, unless specified (list)
    extra_fields can be used to add additional fields -- should be normal field specifiers (list)"""
    reader = csv.reader(open(csvfile))
    if field_names:
        if ' ' not in field_names[0]:
            field_names = ['%s M' % fn for fn in field_names]
    else:
        field_names = ['f0 M']
    mtable = Table(':memory:', [field_names[0]], dbf_type=dbf_type, memo_size=memo_size)
    fields_so_far = 1
    for row in reader:
        while fields_so_far < len(row):
            if fields_so_far == len(field_names):
                field_names.append('f%d M' % fields_so_far)
            mtable.add_fields(field_names[fields_so_far])
            fields_so_far += 1
        mtable.append(tuple(row))
    if filename:
        to_disk = True
    if not to_disk:
        if extra_fields:
            mtable.add_fields(extra_fields)
    else:
        if not filename:
            filename = os.path.splitext(csvfile)[0]
        length = [min_field_size] * len(field_names)
        for record in mtable:
            for i in index(record.field_names):
                length[i] = max(length[i], len(record[i]))
        fields = mtable.field_names
        fielddef = []
        for i in index(length):
            if length[i] < 255:
                fielddef.append('%s C(%d)' % (fields[i], length[i]))
            else:
                fielddef.append('%s M' % (fields[i]))
        if extra_fields:
            fielddef.extend(extra_fields)
        csvtable = Table(filename, fielddef, dbf_type=dbf_type)
        for record in mtable:
            csvtable.append(record.scatter_fields())
        return csvtable
    return mtable
def get_fields(table_name):
    "returns the list of field names of a table"
    table = Table(table_name)
    return table.field_names
def info(table_name):
    "prints table info"
    table = Table(table_name)
    print str(table)
def rename_field(table_name, oldfield, newfield):
    "renames a field in a table"
    table = Table(table_name)
    try:
        table.rename_field(oldfield, newfield)
    finally:
        table.close()
def structure(table_name, field=None):
    "returns the definition of a field (or all fields)"
    table = Table(table_name)
    return table.structure(field)
def hex_dump(records):
    "just what it says ;)"
    for index,dummy in enumerate(records):
        chars = dummy._data
        print "%2d: " % index,
        for char in chars[1:]:
            print " %2x " % ord(char),
        print

# from dbf.api import *
class fake_module(object):
    def __init__(self, name, *args):
        self.name = name
        self.__all__ = []
        all_objects = globals()
        for name in args:
            self.__dict__[name] = all_objects[name]
            self.__all__.append(name)
    def register(self):
        sys.modules["%s.%s" % (__name__, self.name)] = self

fake_module('api',
    'Table', 'List', 'Null', 'Char', 'Date', 'DateTime', 'Time', 'Logical', 'Quantum',
    'NullDate', 'NullDateTime', 'NullTime', 'NoneType', 'NullType', 'Decimal', 
    'Truth', 'Falsth', 'Unknown', 'On', 'Off', 'Other',
    'DbfError', 'DataOverflow', 'FieldMissing', 'NonUnicode',
    'DbfWarning', 'Eof', 'Bof', 'DoNotIndex',
    'Write',
    ).register()

