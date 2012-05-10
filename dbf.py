"""
=========
Copyright
=========
    - Copyright: 2008-2012 Ad-Mail, Inc -- All rights reserved.
    - Author: Ethan Furman
    - Contact: ethanf@admailinc.com
    - Organization: Ad-Mail, Inc.
    - Version: 0.92.00 as of 10 May 2012

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
version = (0, 92, 1)

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
import traceback
import unicodedata
import weakref

from array import array
from bisect import bisect_left, bisect_right
import decimal 
from decimal import Decimal
from math import floor
from shutil import copyfileobj
from stat import ST_SIZE
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
    def __init__(yo, message, data=None):
        DbfError.__init__(yo, message)
        yo.data = data
class BadData(DbfError):
    "bad data in table"
    def __init__(yo, message, data=None):
        DbfError.__init__(yo, message)
        yo.data = data
        yo.message = message
class FieldMissing(KeyError, DbfError):
    "Field does not exist in table"
    def __init__(yo, fieldname):
        KeyError.__init__(yo, '%s:  no such field in table' % fieldname)
        DbfError.__init__(yo, '%s:  no such field in table' % fieldname)
        yo.data = fieldname
class NonUnicode(DbfError):
    "Data for table not in unicode"
    def __init__(yo, message=None):
        DbfError.__init__(yo, message)
class DbfWarning(Exception):
    "Normal operations elicit this response"
class Eof(DbfWarning, StopIteration):
    "End of file reached"
    message = 'End of file reached'
    def __init__(yo):
        StopIteration.__init__(yo, yo.message)
        DbfWarning.__init__(yo, yo.message)
class Bof(DbfWarning, StopIteration):
    "Beginning of file reached"
    message = 'Beginning of file reached'
    def __init__(yo):
        StopIteration.__init__(yo, yo.message)
        DbfWarning.__init__(yo, yo.message)
class DoNotIndex(DbfWarning):
    "Returned by indexing functions to suppress a record from becoming part of the index"
    message = 'Not indexing record'
    def __init__(yo):
        DbfWarning.__init__(yo, yo.message)
# wrappers around datetime and logical objects to allow null values
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
        def __hash__(yo):
            raise TypeError("unhashable type: 'Null'")

    def __new__(cls):
        return cls.null
    def __nonzero__(yo):
        return False
    def __repr__(yo):
        return '<null>'
    def __setattr__(yo, name, value):
        return None
    def __setitem___(yo, index, value):
        return None
    def __str__(yo):
        return ''
NullType.null = object.__new__(NullType)
Null = NullType()

class Char(str):
    "adds null capable str constructs"
    def __new__(cls, text=''):
        if text == None:
            return None
        if not isinstance(text, (str, unicode, cls)):
            raise ValueError("Unable to automatically coerce %r to Char" % text)
        return str.__new__(cls, text)
    def __eq__(yo, other):
        "ignores trailing whitespace"
        if not isinstance(other, (yo.__class__, str)):
            return NotImplemented
        return yo.rstrip() == other.rstrip()
    def __ge__(yo, other):
        "ignores trailing whitespace"
        if not isinstance(other, (yo.__class__, str)):
            return NotImplemented
        return yo.rstrip() >= other.rstrip()
    def __gt__(yo, other):
        "ignores trailing whitespace"
        if not isinstance(other, (yo.__class__, str)):
            return NotImplemented
        return yo.rstrip() > other.rstrip()
    def __le__(yo, other):
        "ignores trailing whitespace"
        if not isinstance(other, (yo.__class__, str)):
            return NotImplemented
        return yo.rstrip() <= other.rstrip()
    def __lt__(yo, other):
        "ignores trailing whitespace"
        if not isinstance(other, (yo.__class__, str)):
            return NotImplemented
        return yo.rstrip() < other.rstrip()
    def __ne__(yo, other):
        "ignores trailing whitespace"
        if not isinstance(other, (yo.__class__, str)):
            return NotImplemented
        return yo.rstrip() != other.rstrip()
    def __nonzero__(yo):
        "ignores trailing whitespace"
        return bool(yo.rstrip())
    def __str__(yo):
        return yo.rstrip()

class Date(object):
    "adds null capable datetime.date constructs"
    __slots__ = ['_date']
    def __new__(cls, year=None, month=0, day=0):
        """date should be either a datetime.date or date/month/day should all be appropriate integers"""
        if year is None:
            return cls._null_date
        nd = object.__new__(cls)
        if isinstance(year, (datetime.date)):
            nd._date = year
        elif isinstance(year, (Date)):
            nd._date = year._date
        else:
            nd._date = datetime.date(year, month, day)
        return nd
    def __add__(yo, other):
        if yo and isinstance(other, (datetime.timedelta)):
            return Date(yo._date + other)
        else:
            return NotImplemented
    def __eq__(yo, other):
        if isinstance(other, yo.__class__):
            return yo._date == other._date
        if isinstance(other, datetime.date):
            return yo._date == other
        if isinstance(other, type(None)):
            return yo._date is None
        return NotImplemented
    def __getattr__(yo, name):
        return yo._date.__getattribute__(name)
    def __ge__(yo, other):
        if isinstance(other, (datetime.date)):
            return yo._date >= other
        elif isinstance(other, (Date)):
            if other:
                return yo._date >= other._date
            return False
        return NotImplemented
    def __gt__(yo, other):
        if isinstance(other, (datetime.date)):
            return yo._date > other
        elif isinstance(other, (Date)):
            if other:
                return yo._date > other._date
            return True
        return NotImplemented
    def __hash__(yo):
        return hash(yo._date)
    def __le__(yo, other):
        if yo:
            if isinstance(other, (datetime.date)):
                return yo._date <= other
            elif isinstance(other, (Date)):
                if other:
                    return yo._date <= other._date
                return False
        else:
            if isinstance(other, (datetime.date)):
                return True
            elif isinstance(other, (Date)):
                if other:
                    return True
                return True
        return NotImplemented
    def __lt__(yo, other):
        if yo:
            if isinstance(other, (datetime.date)):
                return yo._date < other
            elif isinstance(other, (Date)):
                if other:
                    return yo._date < other._date
                return False
        else:
            if isinstance(other, (datetime.date)):
                return True
            elif isinstance(other, (Date)):
                if other:
                    return True
                return False
        return NotImplemented
    def __ne__(yo, other):
        if yo:
            if isinstance(other, (datetime.date)):
                return yo._date != other
            elif isinstance(other, (Date)):
                if other:
                    return yo._date != other._date
                return True
        else:
            if isinstance(other, (datetime.date)):
                return True
            elif isinstance(other, (Date)):
                if other:
                    return True
                return False
        return NotImplemented
    def __nonzero__(yo):
        return bool(yo._date)
    __radd__ = __add__
    def __rsub__(yo, other):
        if yo and isinstance(other, (datetime.date)):
            return other - yo._date
        elif yo and isinstance(other, (Date)):
            return other._date - yo._date
        elif yo and isinstance(other, (datetime.timedelta)):
            return Date(other - yo._date)
        else:
            return NotImplemented
    def __repr__(yo):
        if yo:
            return "Date(%d, %d, %d)" % yo.timetuple()[:3]
        else:
            return "Date()"
    def __str__(yo):
        if yo:
            return yo.isoformat()
        return ""
    def __sub__(yo, other):
        if yo and isinstance(other, (datetime.date)):
            return yo._date - other
        elif yo and isinstance(other, (Date)):
            return yo._date - other._date
        elif yo and isinstance(other, (datetime.timedelta)):
            return Date(yo._date - other)
        else:
            return NotImplemented
    def date(yo):
        if yo:
            return yo._date
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
    def strftime(yo, format):
        if yo:
            return yo._date.strftime(format)
        return ''
    @classmethod
    def today(cls):
        return cls(datetime.date.today())
    def ymd(yo):
        if yo:
            return "%04d%02d%02d" % yo.timetuple()[:3]
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
        if year is None:
            return cls._null_datetime
        ndt = object.__new__(cls)
        if isinstance(year, (datetime.datetime)):
            ndt._datetime = year
        elif isinstance(year, (DateTime)):
            ndt._datetime = year._datetime
        elif year is not None:
            ndt._datetime = datetime.datetime(year, month, day, hour, minute, second, microsec)
        return ndt
    def __add__(yo, other):
        if yo and isinstance(other, (datetime.timedelta)):
            return DateTime(yo._datetime + other)
        else:
            return NotImplemented
    def __eq__(yo, other):
        if isinstance(other, yo.__class__):
            return yo._datetime == other._datetime
        if isinstance(other, datetime.date):
            return yo._datetime == other
        if isinstance(other, type(None)):
            return yo._datetime is None
        return NotImplemented
    def __getattr__(yo, name):
        if yo:
            attribute = yo._datetime.__getattribute__(name)
            return attribute
        else:
            raise AttributeError('null DateTime object has no attribute %s' % name)
    def __ge__(yo, other):
        if yo:
            if isinstance(other, (datetime.datetime)):
                return yo._datetime >= other
            elif isinstance(other, (DateTime)):
                if other:
                    return yo._datetime >= other._datetime
                return False
        else:
            if isinstance(other, (datetime.datetime)):
                return False
            elif isinstance(other, (DateTime)):
                if other:
                    return False
                return True
        return NotImplemented
    def __gt__(yo, other):
        if yo:
            if isinstance(other, (datetime.datetime)):
                return yo._datetime > other
            elif isinstance(other, (DateTime)):
                if other:
                    return yo._datetime > other._datetime
                return True
        else:
            if isinstance(other, (datetime.datetime)):
                return False
            elif isinstance(other, (DateTime)):
                if other:
                    return False
                return False
        return NotImplemented
    def __hash__(yo):
        return yo._datetime.__hash__()
    def __le__(yo, other):
        if yo:
            if isinstance(other, (datetime.datetime)):
                return yo._datetime <= other
            elif isinstance(other, (DateTime)):
                if other:
                    return yo._datetime <= other._datetime
                return False
        else:
            if isinstance(other, (datetime.datetime)):
                return True
            elif isinstance(other, (DateTime)):
                if other:
                    return True
                return True
        return NotImplemented
    def __lt__(yo, other):
        if yo:
            if isinstance(other, (datetime.datetime)):
                return yo._datetime < other
            elif isinstance(other, (DateTime)):
                if other:
                    return yo._datetime < other._datetime
                return False
        else:
            if isinstance(other, (datetime.datetime)):
                return True
            elif isinstance(other, (DateTime)):
                if other:
                    return True
                return False
        return NotImplemented
    def __ne__(yo, other):
        if yo:
            if isinstance(other, (datetime.datetime)):
                return yo._datetime != other
            elif isinstance(other, (DateTime)):
                if other:
                    return yo._datetime != other._datetime
                return True
        else:
            if isinstance(other, (datetime.datetime)):
                return True
            elif isinstance(other, (DateTime)):
                if other:
                    return True
                return False
        return NotImplemented
    def __nonzero__(yo):
        return bool(yo._datetime)
    __radd__ = __add__
    def __rsub__(yo, other):
        if yo and isinstance(other, (datetime.datetime)):
            return other - yo._datetime
        elif yo and isinstance(other, (DateTime)):
            return other._datetime - yo._datetime
        elif yo and isinstance(other, (datetime.timedelta)):
            return DateTime(other - yo._datetime)
        else:
            return NotImplemented
    def __repr__(yo):
        if yo:
            return "DateTime(%d, %d, %d, %d, %d, %d, %d)" % (
                yo._datetime.timetuple()[:6] + (yo._datetime.microsecond, )
                )
        else:
            return "DateTime()"
    def __str__(yo):
        if yo:
            return yo.isoformat()
        return ""
    def __sub__(yo, other):
        if yo and isinstance(other, (datetime.datetime)):
            return yo._datetime - other
        elif yo and isinstance(other, (DateTime)):
            return yo._datetime - other._datetime
        elif yo and isinstance(other, (datetime.timedelta)):
            return DateTime(yo._datetime - other)
        else:
            return NotImplemented
    @classmethod
    def combine(cls, date, time):
        if Date(date) and Time(time):
            return cls(date.year, date.month, date.day, time.hour, time.minute, time.second, time.microsecond)
        return cls()
    def date(yo):
        if yo:
            return Date(yo.year, yo.month, yo.day)
        return Date()
    def datetime(yo):
        if yo:
            return yo._datetime
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
    def time(yo):
        if yo:
            return Time(yo.hour, yo.minute, yo.second, yo.microsecond)
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
        if hour is None:
            return cls._null_time
        nt = object.__new__(cls)
        if isinstance(hour, (datetime.time)):
            nt._time = hour
        elif isinstance(hour, (Time)):
            nt._time = hour._time
        elif hour is not None:
            nt._time = datetime.time(hour, minute, second, microsec)
        return nt
    def __add__(yo, other):
        if yo and isinstance(other, (datetime.timedelta)):
            t = yo._time
            t = datetime.datetime(2012, 6, 27, t.hour, t.minute, t.second, t.microsecond)
            t += other
            return Time(t.hour, t.minute, t.second, t.microsecond)
        else:
            return NotImplemented
    def __eq__(yo, other):
        if isinstance(other, yo.__class__):
            return yo._time == other._time
        if isinstance(other, datetime.time):
            return yo._time == other
        if isinstance(other, type(None)):
            return yo._time is None
        return NotImplemented
    def __getattr__(yo, name):
        if yo:
            attribute = yo._time.__getattribute__(name)
            return attribute
        else:
            raise AttributeError('null Time object has no attribute %s' % name)
    def __ge__(yo, other):
        if yo:
            if isinstance(other, (datetime.time)):
                return yo._time >= other
            elif isinstance(other, (Time)):
                if other:
                    return yo._time >= other._time
                return False
        else:
            if isinstance(other, (datetime.time)):
                return False
            elif isinstance(other, (Time)):
                if other:
                    return False
                return True
        return NotImplemented
    def __gt__(yo, other):
        if yo:
            if isinstance(other, (datetime.time)):
                return yo._time > other
            elif isinstance(other, (DateTime)):
                if other:
                    return yo._time > other._time
                return True
        else:
            if isinstance(other, (datetime.time)):
                return False
            elif isinstance(other, (Time)):
                if other:
                    return False
                return False
        return NotImplemented
    def __hash__(yo):
        return yo._datetime.__hash__()
    def __le__(yo, other):
        if yo:
            if isinstance(other, (datetime.time)):
                return yo._time <= other
            elif isinstance(other, (Time)):
                if other:
                    return yo._time <= other._time
                return False
        else:
            if isinstance(other, (datetime.time)):
                return True
            elif isinstance(other, (Time)):
                if other:
                    return True
                return True
        return NotImplemented
    def __lt__(yo, other):
        if yo:
            if isinstance(other, (datetime.time)):
                return yo._time < other
            elif isinstance(other, (Time)):
                if other:
                    return yo._time < other._time
                return False
        else:
            if isinstance(other, (datetime.time)):
                return True
            elif isinstance(other, (Time)):
                if other:
                    return True
                return False
        return NotImplemented
    def __ne__(yo, other):
        if yo:
            if isinstance(other, (datetime.time)):
                return yo._time != other
            elif isinstance(other, (Time)):
                if other:
                    return yo._time != other._time
                return True
        else:
            if isinstance(other, (datetime.time)):
                return True
            elif isinstance(other, (Time)):
                if other:
                    return True
                return False
        return NotImplemented
    def __nonzero__(yo):
        return bool(yo._time)
    __radd__ = __add__
    def __rsub__(yo, other):
        if yo and isinstance(other, (Time, datetime.time)):
            t = yo._time
            t = datetime.datetime(2012, 6, 27, t.hour, t.minute, t.second, t.microsecond)
            other = datetime.datetime(2012, 6, 27, other.hour, other.minute, other.second, other.microsecond)
            other -= t
            return other
        else:
            return NotImplemented
    def __repr__(yo):
        if yo:
            return "Time(%d, %d, %d, %d)" % (yo.hour, yo.minute, yo.second, yo.microsecond)
        else:
            return "Time()"
    def __str__(yo):
        if yo:
            return yo.isoformat()
        return ""
    def __sub__(yo, other):
        if yo and isinstance(other, (Time, datetime.time)):
            t = yo._time
            t = datetime.datetime(2012, 6, 27, t.hour, t.minute, t.second, t.microsecond)
            o = datetime.datetime(2012, 6, 27, other.hour, other.minute, other.second, other.microsecond)
            t -= other
            return Time(t.hour, t.minute, t.second, t.microsecond)
        elif yo and isinstance(other, (datetime.timedelta)):
            t = yo._time
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
    def __new__(cls, value=None):
        if value is None or value is Null:
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
    can take the values of True, False, or None/Null"""
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
    def __new__(cls, value=None):
        if value is None or value is Null:
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
    def _refresh_record(yo, header=None):
        "refresh record data from disk"
        header = yo._layout.header
        size = header.record_length
        location = yo._recnum * size + header.start
        yo._layout.dfd.seek(location)
        yo._data[:] = array('c', yo._layout.dfd.read(size))
        yo._dirty = False
        return yo
    def _retrieveFieldValue(yo, index, name):
        """calls appropriate routine to convert value stored in field from array
        @param record_data: the data portion of the record
        @type record_data: array of characters
        @param fielddef: description of the field definition
        @type fielddef: dictionary with keys 'type', 'start', 'length', 'end', 'decimals', and 'flags'
        @returns: python data stored in field"""
        fielddef = yo._layout[name]
        flags = fielddef[FLAGS]
        nullable = flags & NULLABLE
        binary = flags & BINARY
        if nullable:
            byte, bit = divmod(index, 8)
            null_def = yo._layout['_nullflags']
            null_data = yo._data[null_def[START]:null_def[END]]
            try:
                if ord(null_data[byte]) >> bit & 1:
                    return Null
            except IndexError:
                print null_data
                print index
                print byte, bit
                print len(yo._data), yo._data
                print null_def
                print null_data
                raise

        record_data = yo._data[fielddef[START]:fielddef[END]]
        field_type = fielddef[TYPE]
        retrieve = yo._layout.fieldtypes[field_type]['Retrieve']
        datum = retrieve(record_data, fielddef, yo._layout.memo, yo._layout.decoder)
        return datum
    def _updateFieldValue(yo, index, name, value):
        "calls appropriate routine to convert value to ascii bytes, and save it in record"
        fielddef = yo._layout[name]
        field_type = fielddef[TYPE]
        flags = fielddef[FLAGS]
        binary = flags & BINARY
        nullable = flags & NULLABLE
        update = yo._layout.fieldtypes[field_type]['Update']
        if nullable:
            byte, bit = divmod(index, 8)
            null_def = yo._layout['_nullflags']
            null_data = yo._data[null_def[START]:null_def[END]].tostring()
            null_data = [ord(c) for c in null_data]
            if value is Null:
                null_data[byte] |= 1 << bit
                value = None
            else:
                null_data[byte] &= 0xff ^ 1 << bit
            null_data = array('c', [chr(n) for n in null_data])
            yo._data[null_def[START]:null_def[END]] = null_data
        if value is not Null:
            bytes = array('c', update(value, fielddef, yo._layout.memo, yo._layout.input_decoder, yo._layout.encoder))
            size = fielddef[LENGTH]
            if len(bytes) > size:
                raise DataOverflow("tried to store %d bytes in %d byte field" % (len(bytes), size))
            blank = array('c', ' ' * size)
            start = fielddef[START]
            end = start + size
            blank[:len(bytes)] = bytes[:]
            yo._data[start:end] = blank[:]
        yo._dirty = True
    def _update_disk(yo, location='', data=None):
        layout = yo._layout
        if not layout.inmemory:
            header = layout.header
            if yo._recnum < 0:
                raise DbfError("Attempted to update record that has been packed")
            if location == '':
                location = yo._recnum * header.record_length + header.start
            if data is None:
                data = yo._data
            layout.dfd.seek(location)
            layout.dfd.write(data)
            yo._dirty = False
        if layout.table() is not None:  # is None when table is being destroyed
            for index in yo.record_table._indexen:
                index(yo)
    def __contains__(yo, key):
        return key in yo._layout.fields or key in ['record_number','delete_flag']
    def __iter__(yo):
        return (yo[field] for field in yo._layout.fields if not yo._layout[field][FLAGS] & SYSTEM)
    def __getattr__(yo, name):
        if name[0:2] == '__' and name[-2:] == '__':
            raise AttributeError, 'Method %s is not implemented.' % name
        elif name == 'record_number':
            return yo._recnum
        elif name == 'delete_flag':
            return yo._data[0] != ' '
        elif not name in yo._layout.fields:
            raise FieldMissing(name)
        try:
            index = yo._layout.fields.index(name)
            value = yo._retrieveFieldValue(index, name)
            return value
        except DbfError, error:
            error.message = "field --%s-- is %s -> %s" % (name, yo._layout.fieldtypes[fielddef['type']]['Type'], error.message)
            raise
    def __getitem__(yo, item):
        if isinstance(item, (int, long)):
            fields = [f for f in yo._layout.fields if not yo._layout[f][FLAGS] & SYSTEM]
            field_count = len(fields)
            if not -field_count <= item < field_count:
                raise IndexError("Field offset %d is not in record" % item)
            return yo[fields[item]]
        elif isinstance(item, slice):
            sequence = []
            for index in yo._layout.fields[item]:
                sequence.append(yo[index])
            return sequence
        elif isinstance(item, (str, unicode)):
            return yo.__getattr__(item)
        else:
            raise TypeError("%r is not a field name" % item)
    def __len__(yo):
        return yo._layout.header.field_count
    def __new__(cls, recnum, layout, kamikaze='', _fromdisk=False):
        """record = ascii array of entire record; layout=record specification; memo = memo object for table"""
        record = object.__new__(cls)
        record._dirty = False
        record._recnum = recnum
        record._layout = layout
        header = layout.header
        if layout.blankrecord is None and not _fromdisk:
            record._createBlankRecord()
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
    def __setattr__(yo, name, value):
        if name in yo.__slots__:
            object.__setattr__(yo, name, value)
            return
        elif not name in yo._layout.fields:
            raise FieldMissing(name)
        index = yo._layout.fields.index(name)
        try:
            yo._updateFieldValue(index, name, value)
        except DbfError, error:
            fielddef = yo._layout[name]
            message = "%s (%s) = %r --> %s" % (name, yo._layout.fieldtypes[fielddef[TYPE]]['Type'], value, error.message)
            data = name
            err_cls = error.__class__
            raise err_cls(message, data)
    def __setitem__(yo, name, value):
        if isinstance(name, (str, unicode)):
            yo.__setattr__(name, value)
        elif isinstance(name, (int, long)):
            yo.__setattr__(yo._layout.fields[name], value)
        elif isinstance(name, slice):
            sequence = []
            for field in yo._layout.fields[name]:
                sequence.append(field)
            if len(sequence) != len(value):
                raise DbfError("length of slices not equal")
            for field, val in zip(sequence, value):
                yo[field] = val
        else:
            raise TypeError("%s is not a field name" % name)
    def __str__(yo):
        result = []
        for seq, field in enumerate(yo.field_names):
            if yo._layout[field][FLAGS] & SYSTEM:
                continue
            result.append("%3d - %-10s: %r" % (seq, field, yo[field]))
        return '\n'.join(result)
    def __repr__(yo):
        return yo._data.tostring()
    #@classmethod
    def _createBlankRecord(yo):
        "creates a blank record data chunk"
        layout = yo._layout
        ondisk = layout.ondisk
        layout.ondisk = False
        yo._data = array('c', ' ' * layout.header.record_length)
        layout.memofields = []
        for index, name in enumerate(layout.fields):
            if name == '_nullflags':
                yo._data[layout['_nullflags'][START]:layout['_nullflags'][END]] = array('c', chr(0) * layout['_nullflags'][LENGTH])
        for index, name in enumerate(layout.fields):
            if name != '_nullflags':
                yo._updateFieldValue(index, name, None)
                if layout[name][TYPE] in layout.memotypes:
                    layout.memofields.append(name)
        layout.blankrecord = yo._data[:]
        layout.ondisk = ondisk
    def delete_record(yo):
        "marks record as deleted"
        yo._data[0] = '*'
        yo._dirty = True
        return yo
    @property
    def field_names(yo):
        "fields in table/record"
        return [f for f in yo._layout.fields if not yo._layout[f][FLAGS] & SYSTEM]
    def gather_fields(yo, dictionary, drop=False):
        """saves a dictionary into a record's fields
        keys with no matching field will raise a FieldMissing exception unless drop_missing = True"""
        old_data = yo._data[:]
        try:
            for key, value in dictionary.items():
                if not key in yo.field_names:
                    if drop:
                        continue
                    raise FieldMissing(key)
                yo.__setattr__(key, value)
        except:
            yo._data[:] = old_data
            raise
        return yo
    @property
    def has_been_deleted(yo):
        "marked for deletion?"
        return yo._data[0] == '*'
    @property
    def record_number(yo):
        "physical record number"
        return yo._recnum
    @property
    def record_table(yo):
        table = yo._layout.table()
        if table is None:
            raise DbfError("table is no longer available")
        return table
    def check_index(yo):
        for dbfindex in yo._layout.table()._indexen:
            dbfindex(yo)
    def reset_record(yo, keep_fields=None):
        "blanks record"
        if keep_fields is None:
            keep_fields = []
        keep = {}
        for field in keep_fields:
            keep[field] = yo[field]
        if yo._layout.blankrecord == None:
            yo._createBlankRecord()
        yo._data[:] = yo._layout.blankrecord[:]
        for field in keep_fields:
            yo[field] = keep[field]
        yo._dirty = True
        return yo
    def scatter_fields(yo, blank=False):
        "returns a dictionary of fieldnames and values which can be used with gather_fields().  if blank is True, values are empty."
        keys = [field for field in yo._layout.fields if not yo._layout[field][FLAGS] & SYSTEM]
        if blank:
            values = []
            layout = yo._layout
            for key in keys:
                fielddef = layout[key]
                fieldtype = fielddef[TYPE]
                empty = fielddef[EMPTY]
                values.append(empty())
        else:
            values = [yo[field] for field in keys]
        return dict(zip(keys, values))
    def undelete_record(yo):
        "marks record as active"
        yo._data[0] = ' '
        yo._dirty = True
        return yo
    def write_record(yo, _force=False, **kwargs):
        "write record data to disk"
        if kwargs:
            yo.gather_fields(kwargs)
        if yo._dirty or _force:
            yo._update_disk()
            return 1
        return 0
class _DbfMemo(object):
    """Provides access to memo fields as dictionaries
       must override _init, _get_memo, and _put_memo to
       store memo contents to disk"""
    def _init(yo):
        "initialize disk file usage"
    def _get_memo(yo, block):
        "retrieve memo contents from disk"
    def _put_memo(yo, data):
        "store memo contents to disk"
    def _zap(yo):
        "resets memo structure back to zero memos"
        yo.memory.clear()
        yo.nextmemo = 1
    def __init__(yo, meta):
        ""
        yo.meta = meta
        yo.memory = {}
        yo.nextmemo = 1
        yo._init()
        yo.meta.newmemofile = False
    def get_memo(yo, block):
        "gets the memo in block"
        if yo.meta.ignorememos or not block:
            return ''
        if yo.meta.ondisk:
            return yo._get_memo(block)
        else:
            return yo.memory[block]
    def put_memo(yo, data):
        "stores data in memo file, returns block number"
        if yo.meta.ignorememos or data == '':
            return 0
        if yo.meta.inmemory:
            thismemo = yo.nextmemo
            yo.nextmemo += 1
            yo.memory[thismemo] = data
        else:
            thismemo = yo._put_memo(data)
        return thismemo
class _Db3Memo(_DbfMemo):
    def _init(yo):
        "dBase III specific"
        yo.meta.memo_size= 512
        yo.record_header_length = 2
        if yo.meta.ondisk and not yo.meta.ignorememos:
            if yo.meta.newmemofile:
                yo.meta.mfd = open(yo.meta.memoname, 'w+b')
                yo.meta.mfd.write(packLongInt(1) + '\x00' * 508)
            else:
                try:
                    yo.meta.mfd = open(yo.meta.memoname, 'r+b')
                    yo.meta.mfd.seek(0)
                    next = yo.meta.mfd.read(4)
                    yo.nextmemo = unpackLongInt(next)
                except Exception:
                    traceback.print_exc()
                    raise DbfError("memo file appears to be corrupt")
    def _get_memo(yo, block):
        block = int(block)
        yo.meta.mfd.seek(block * yo.meta.memo_size)
        eom = -1
        data = ''
        while eom == -1:
            newdata = yo.meta.mfd.read(yo.meta.memo_size)
            if not newdata:
                return data
            data += newdata
            eom = data.find('\x1a\x1a')
        return data[:eom]
    def _put_memo(yo, data):
        data = data
        length = len(data) + yo.record_header_length  # room for two ^Z at end of memo
        blocks = length // yo.meta.memo_size
        if length % yo.meta.memo_size:
            blocks += 1
        thismemo = yo.nextmemo
        yo.nextmemo = thismemo + blocks
        yo.meta.mfd.seek(0)
        yo.meta.mfd.write(packLongInt(yo.nextmemo))
        yo.meta.mfd.seek(thismemo * yo.meta.memo_size)
        yo.meta.mfd.write(data)
        yo.meta.mfd.write('\x1a\x1a')
        double_check = yo._get_memo(thismemo)
        if len(double_check) != len(data):
            uhoh = open('dbf_memo_dump.err','wb')
            uhoh.write('thismemo: %d' % thismemo)
            uhoh.write('nextmemo: %d' % yo.nextmemo)
            uhoh.write('saved: %d bytes' % len(data))
            uhoh.write(data)
            uhoh.write('retrieved: %d bytes' % len(double_check))
            uhoh.write(double_check)
            uhoh.close()
            raise DbfError("unknown error: memo not saved")
        return thismemo
    def _zap(yo):
        if yo.meta.ondisk and not yo.meta.ignorememos:
            mfd = yo.meta.mfd
            mfd.seek(0)
            mfd.truncate(0)
            mfd.write(packLongInt(1) + '\x00' * 508)
            mfd.flush()

class _VfpMemo(_DbfMemo):
    def _init(yo):
        "Visual Foxpro 6 specific"
        if yo.meta.ondisk and not yo.meta.ignorememos:
            yo.record_header_length = 8
            if yo.meta.newmemofile:
                if yo.meta.memo_size == 0:
                    yo.meta.memo_size = 1
                elif 1 < yo.meta.memo_size < 33:
                    yo.meta.memo_size *= 512
                yo.meta.mfd = open(yo.meta.memoname, 'w+b')
                nextmemo = 512 // yo.meta.memo_size
                if nextmemo * yo.meta.memo_size < 512:
                    nextmemo += 1
                yo.nextmemo = nextmemo
                yo.meta.mfd.write(packLongInt(nextmemo, bigendian=True) + '\x00\x00' + \
                        packShortInt(yo.meta.memo_size, bigendian=True) + '\x00' * 504)
            else:
                try:
                    yo.meta.mfd = open(yo.meta.memoname, 'r+b')
                    yo.meta.mfd.seek(0)
                    header = yo.meta.mfd.read(512)
                    yo.nextmemo = unpackLongInt(header[:4], bigendian=True)
                    yo.meta.memo_size = unpackShortInt(header[6:8], bigendian=True)
                except:
                    traceback.print_exc()
                    raise DbfError("memo file appears to be corrupt")
    def _get_memo(yo, block):
        yo.meta.mfd.seek(block * yo.meta.memo_size)
        header = yo.meta.mfd.read(8)
        length = unpackLongInt(header[4:], bigendian=True)
        return yo.meta.mfd.read(length)
    def _put_memo(yo, data):
        data = data
        yo.meta.mfd.seek(0)
        thismemo = unpackLongInt(yo.meta.mfd.read(4), bigendian=True)
        yo.meta.mfd.seek(0)
        length = len(data) + yo.record_header_length
        blocks = length // yo.meta.memo_size
        if length % yo.meta.memo_size:
            blocks += 1
        yo.meta.mfd.write(packLongInt(thismemo+blocks, bigendian=True))
        yo.meta.mfd.seek(thismemo*yo.meta.memo_size)
        yo.meta.mfd.write('\x00\x00\x00\x01' + packLongInt(len(data), bigendian=True) + data)
        return thismemo
    def _zap(yo):
        if yo.meta.ondisk and not yo.meta.ignorememos:
            mfd = yo.meta.mfd
            mfd.seek(0)
            mfd.truncate(0)
            nextmemo = 512 // yo.meta.memo_size
            if nextmemo * yo.meta.memo_size < 512:
                nextmemo += 1
            yo.nextmemo = nextmemo
            mfd.write(packLongInt(nextmemo, bigendian=True) + '\x00\x00' + \
                    packShortInt(yo.meta.memo_size, bigendian=True) + '\x00' * 504)
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

def packShortInt(value, bigendian=False):
        "Returns a two-bye integer from the value, or raises DbfError"
        # 256 / 65,536
        if value > 65535:
            raise DataOverflow("Maximum Integer size exceeded.  Possible: 65535.  Attempted: %d" % value)
        if bigendian:
            return struct.pack('>H', value)
        else:
            return struct.pack('<H', value)
def packLongInt(value, bigendian=False):
        "Returns a four-bye integer from the value, or raises DbfError"
        # 256 / 65,536 / 16,777,216
        if value > 4294967295:
            raise DataOverflow("Maximum Integer size exceeded.  Possible: 4294967295.  Attempted: %d" % value)
        if bigendian:
            return struct.pack('>L', value)
        else:
            return struct.pack('<L', value)
def packStr(string):
        "Returns an 11 byte, upper-cased, null padded string suitable for field names; raises DbfError if the string is bigger than 10 bytes"
        if len(string) > 10:
            raise DbfError("Maximum string size is ten characters -- %s has %d characters" % (string, len(string)))
        return struct.pack('11s', string.upper())       
def unpackShortInt(bytes, bigendian=False):
        "Returns the value in the two-byte integer passed in"
        if bigendian:
            return struct.unpack('>H', bytes)[0]
        else:
            return struct.unpack('<H', bytes)[0]
def unpackLongInt(bytes, bigendian=False):
        "Returns the value in the four-byte integer passed in"
        if bigendian:
            return int(struct.unpack('>L', bytes)[0])
        else:
            return int(struct.unpack('<L', bytes)[0])
def unpackStr(chars):
        "Returns a normal, lower-cased string from a null-padded byte string"
        field = struct.unpack('%ds' % len(chars), chars)[0]
        name = []
        for ch in field:
            if ch == '\x00':
                break
            name.append(ch.lower())
        return ''.join(name)
def unsupportedType(something, *ignore):
    "called if a data type is not supported for that style of table"
    return something
    raise DbfError('field type is not supported.')
def retrieveCharacter(bytes, fielddef, memo, decoder):
    "Returns the string in bytes"
    data = bytes.tostring()
    if not data.strip():
        cls = fielddef[EMPTY]
        if cls is NoneType:
            return None
        return cls(data)
    if fielddef[FLAGS] & BINARY:
        return data
    return fielddef[CLASS](decoder(data)[0])
def updateCharacter(string, fielddef, memo, decoder, encoder):
    "returns the string as bytes (not unicode)"
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
def retrieveCurrency(bytes, fielddef, *ignore):
    value = struct.unpack('<q', bytes)[0]
    return fielddef[CLASS](("%de-4" % value).strip())
def updateCurrency(value, *ignore):
    if value == None:
        value = 0
    currency = int(value * 10000)
    if not -9223372036854775808 < currency < 9223372036854775808:
        raise DataOverflow("value %s is out of bounds" % value)
    return struct.pack('<q', currency)
def retrieveDate(bytes, fielddef, *ignore):
    "Returns the ascii coded date as a Date object"
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
def updateDate(moment, *ignore):
    "returns the Date or datetime.date object ascii-encoded (yyyymmdd)"
    if moment == None:
        return '        '
    return "%04d%02d%02d" % moment.timetuple()[:3]
def retrieveDouble(bytes, fielddef, *ignore):
    typ = fielddef[CLASS]
    if typ == 'default':
        typ = float
    return typ(struct.unpack('<d', bytes)[0])
def updateDouble(value, *ignore):
    if value == None:
        value = 0
    return struct.pack('<d', float(value))
def retrieveInteger(bytes, fielddef, *ignore):
    "Returns the binary number stored in bytes in little-endian format"
    typ = fielddef[CLASS]
    if typ == 'default':
        typ = int
    return typ(struct.unpack('<i', bytes)[0])
def updateInteger(value, *ignore):
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
def retrieveLogical(bytes, fielddef, *ignore):
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
def updateLogical(data, *ignore):
    "Returns 'T' if logical is True, 'F' if False, '?' otherwise"
    if data is Unknown or data is None or data is Null or data is Other:
        return '?'
    if data == True:
        return 'T'
    if data == False:
        return 'F'
    raise ValueError("unable to automatically coerce %r to Logical" % data)
def retrieveMemo(bytes, fielddef, memo, decoder):
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
def updateMemo(string, fielddef, memo, decoder, encoder):
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
def retrieveNumeric(bytes, fielddef, *ignore):
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
def updateNumeric(value, fielddef, *ignore):
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
def retrieveVfpDateTime(bytes, fielddef, *ignore):
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
    time = unpackLongInt(bytes[4:])
    microseconds = (time % 1000) * 1000
    time = time // 1000                      # int(round(time, -3)) // 1000 discard milliseconds
    hours = time // 3600
    mins = time % 3600 // 60
    secs = time % 3600 % 60
    time = datetime.time(hours, mins, secs, microseconds)
    possible = unpackLongInt(bytes[:4])
    possible -= VFPTIME
    possible = max(0, possible)
    try:
        date = datetime.date.fromordinal(possible)
    except:
        print
        print bytes
        print possible
    return cls(date.year, date.month, date.day, time.hour, time.minute, time.second, time.microsecond)
def updateVfpDateTime(moment, *ignore):
    """sets the date/time stored in moment
    moment must have fields year, month, day, hour, minute, second, microsecond"""
    bytes = ['\x00'] * 8
    if moment:
        hour = moment.hour
        minute = moment.minute
        second = moment.second
        millisecond = moment.microsecond // 1000       # convert from millionths to thousandths
        time = ((hour * 3600) + (minute * 60) + second) * 1000 + millisecond
        bytes[4:] = updateInteger(time)
        bytes[:4] = updateInteger(moment.toordinal() + VFPTIME)
    return ''.join(bytes)
def retrieveVfpMemo(bytes, fielddef, memo, decoder):
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
def updateVfpMemo(string, fielddef, memo, decoder, encoder):
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
def addCharacter(format, flags):
    if format[0][0] != '(' or format[0][-1] != ')' or any([f not in flags for f in format[1:]]):
        raise DbfError("Format for Character field creation is <C(n)%s>, not <C%s>" % fieldSpecErrorText(format, flags))
    length = int(format[0][1:-1])
    if not 0 < length < 255:
        raise ValueError
    decimals = 0
    flag = 0
    for f in format[1:]:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def addDate(format, flags):
    if any([f not in flags for f in format[1:]]):
        raise DbfError("Format for Date field creation is <D%s>, not <D%s>" % fieldSpecErrorText(format, flags))
    length = 8
    decimals = 0
    flag = 0
    flag = 0
    for f in format:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def addLogical(format, flags):
    if any([f not in flags for f in format[1:]]):
        raise DbfError("Format for Logical field creation is <L%s>, not <L%s>" % fieldSpecErrorText(format, flags))
    length = 1
    decimals = 0
    flag = 0
    for f in format:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def addMemo(format, flags):
    if any(f not in flags for f in format[1:]):
        raise DbfError("Format for Memo field creation is <M(n)%s>, not <M%s>" % fieldSpecErrorText(format, flags))
    length = 10
    decimals = 0
    flag = 0
    for f in format:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def addNumeric(format, flags):
    if len(format) > 1 or format[0][0] != '(' or format[0][-1] != ')' or any(f not in flags for f in format[1:]):
        raise DbfError("Format for Numeric field creation is <N(s,d)%s>, not <N%s>" % fieldSpecErrorText(format, flags))
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
def addVfpCurrency(format, flags):
    if any(f not in flags for f in format[1:]):
        raise DbfError("Format for Currency field creation is <Y%s>, not <Y%s>" % fieldSpecErrorText(format, flags))
    length = 8
    decimals = 0
    flag = 0
    for f in format:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def addVfpDateTime(format, flags):
    if any(f not in flags for f in format[1:]):
        raise DbfError("Format for DateTime field creation is <T%s>, not <T%s>" % fieldSpecErrorText(format, flags))
    length = 8
    decimals = 8
    flag = 0
    for f in format:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def addVfpDouble(format, flags):
    if any(f not in flags for f in format[1:]):
        raise DbfError("Format for Double field creation is <B%s>, not <B%s>" % fieldSpecErrorText(format, flags))
    length = 8
    decimals = 0
    flag = 0
    for f in format:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def addVfpInteger(format, flags):
    if any(f not in flags for f in format[1:]):
        raise DbfError("Format for Integer field creation is <I%s>, not <I%s>" % fieldSpecErrorText(format, flags))
    length = 4
    decimals = 0
    flag = 0
    for f in format:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def addVfpMemo(format, flags):
    if any(f not in flags for f in format[1:]):
        raise DbfError("Format for Memo field creation is <M%s>, not <M%s>" % fieldSpecErrorText(format, flags))
    length = 4
    decimals = 0
    flag = 0
    for f in format:
        flag |= FIELD_FLAGS[f]
    return length, decimals, flag
def addVfpNumeric(format, flags):
    if format[0][0] != '(' or format[0][-1] != ')' or any(f not in flags for f in format[1:]):
        raise DbfError("Format for Numeric field creation is <N(s,d)%s>, not <N%s>" % fieldSpecErrorText(format, flags))
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
def fieldSpecErrorText(format, flags):
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
    def _fieldtypes():
        return {
                'C' : {
                        'Type':'Character', 'Init':addCharacter, 'Blank':str, 'Retrieve':retrieveCharacter, 'Update':updateCharacter,
                        'Class':unicode, 'Empty':unicode, 'flags':tuple(),
                        },
                'D' : { 
                        'Type':'Date', 'Init':addDate, 'Blank':Date, 'Retrieve':retrieveDate, 'Update':updateDate,
                        'Class':datetime.date, 'Empty':none, 'flags':tuple(),
                        },
                'L' : { 
                        'Type':'Logical', 'Init':addLogical, 'Blank':Logical, 'Retrieve':retrieveLogical, 'Update':updateLogical,
                        'Class':bool, 'Empty':none, 'flags':tuple(),
                        },
                'M' : { 
                        'Type':'Memo', 'Init':addMemo, 'Blank':str, 'Retrieve':retrieveMemo, 'Update':updateMemo,
                        'Class':unicode, 'Empty':unicode, 'flags':tuple(),
                        },
                'N' : { 
                        'Type':'Numeric', 'Init':addNumeric, 'Blank':int, 'Retrieve':retrieveNumeric, 'Update':updateNumeric,
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
    _dbfTableHeader[8:10] = array('c', packShortInt(33))
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
        def __init__(yo):
            yo._lists = set()
        def __iter__(yo):
            yo._lists = set([s for s in yo._lists if s() is not None])    
            return (s() for s in yo._lists if s() is not None)
        def __len__(yo):
            yo._lists = set([s for s in yo._lists if s() is not None])
            return len(yo._lists)
        def add(yo, new_list):
            yo._lists.add(weakref.ref(new_list))
            yo._lists = set([s for s in yo._lists if s() is not None])
    class _Indexen(object):
        "implements the weakref structure for seperate indexes"
        def __init__(yo):
            yo._indexen = set()
        def __iter__(yo):
            yo._indexen = set([s for s in yo._indexen if s() is not None])    
            return (s() for s in yo._indexen if s() is not None)
        def __len__(yo):
            yo._indexen = set([s for s in yo._indexen if s() is not None])
            return len(yo._indexen)
        def add(yo, new_list):
            yo._indexen.add(weakref.ref(new_list))
            yo._indexen = set([s for s in yo._indexen if s() is not None])
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
    class _TableHeader(object):
        def __init__(yo, data, pack_date, unpack_date):
            if len(data) != 32:
                raise DbfError('table header should be 32 bytes, but is %d bytes' % len(data))
            yo.packDate = pack_date
            yo.unpackDate = unpack_date
            yo._data = array('c', data + '\x0d')
        def codepage(yo, cp=None):
            "get/set code page of table"
            if cp is None:
                return yo._data[29]
            else:
                cp, sd, ld = _codepage_lookup(cp)
                yo._data[29] = cp                    
                return cp
        @property
        def data(yo):
            "main data structure"
            date = yo.packDate(Date.today())
            yo._data[1:4] = array('c', date)
            return yo._data.tostring()
        @data.setter
        def data(yo, bytes):
            if len(bytes) < 32:
                raise DbfError("length for data of %d is less than 32" % len(bytes))
            yo._data[:] = array('c', bytes)
        @property
        def extra(yo):
            "extra dbf info (located after headers, before data records)"
            fieldblock = yo._data[32:]
            for i in range(len(fieldblock)//32+1):
                cr = i * 32
                if fieldblock[cr] == '\x0d':
                    break
            else:
                raise DbfError("corrupt field structure")
            cr += 33    # skip past CR
            return yo._data[cr:].tostring()
        @extra.setter
        def extra(yo, data):
            fieldblock = yo._data[32:]
            for i in range(len(fieldblock)//32+1):
                cr = i * 32
                if fieldblock[cr] == '\x0d':
                    break
            else:
                raise DbfError("corrupt field structure")
            cr += 33    # skip past CR
            yo._data[cr:] = array('c', data)                             # extra
            yo._data[8:10] = array('c', packShortInt(len(yo._data)))  # start
        @property
        def field_count(yo):
            "number of fields (read-only)"
            fieldblock = yo._data[32:]
            for i in range(len(fieldblock)//32+1):
                cr = i * 32
                if fieldblock[cr] == '\x0d':
                    break
            else:
                raise DbfError("corrupt field structure")
            return len(fieldblock[:cr]) // 32
        @property
        def fields(yo):
            "field block structure"
            fieldblock = yo._data[32:]
            for i in range(len(fieldblock)//32+1):
                cr = i * 32
                if fieldblock[cr] == '\x0d':
                    break
            else:
                raise DbfError("corrupt field structure")
            return fieldblock[:cr].tostring()
        @fields.setter
        def fields(yo, block):
            fieldblock = yo._data[32:]
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
            yo._data[32:cr] = array('c', block)                           # fields
            yo._data[8:10] = array('c', packShortInt(len(yo._data)))   # start
            fieldlen = fieldlen // 32
            recordlen = 1                                     # deleted flag
            for i in range(fieldlen):
                recordlen += ord(block[i*32+16])
            yo._data[10:12] = array('c', packShortInt(recordlen))
        @property
        def record_count(yo):
            "number of records (maximum 16,777,215)"
            return unpackLongInt(yo._data[4:8].tostring())
        @record_count.setter
        def record_count(yo, count):
            yo._data[4:8] = array('c', packLongInt(count))
        @property
        def record_length(yo):
            "length of a record (read_only) (max of 65,535)"
            return unpackShortInt(yo._data[10:12].tostring())
        @property
        def start(yo):
            "starting position of first record in file (must be within first 64K)"
            return unpackShortInt(yo._data[8:10].tostring())
        @start.setter
        def start(yo, pos):
            yo._data[8:10] = array('c', packShortInt(pos))
        @property
        def update(yo):
            "date of last table modification (read-only)"
            return yo.unpackDate(yo._data[1:4].tostring())
        @property
        def version(yo):
            "dbf version"
            return yo._data[0]
        @version.setter
        def version(yo, ver):
            yo._data[0] = ver
    class _Table(object):
        "implements the weakref table for records"
        def __init__(yo, count, meta):
            yo._meta = meta
            yo._weakref_list = [weakref.ref(lambda x: None)] * count
        def __getitem__(yo, index):
            maybe = yo._weakref_list[index]()
            if maybe is None:
                meta = yo._meta
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
                yo._weakref_list[index] = weakref.ref(maybe)
            return maybe
        def append(yo, record):
            yo._weakref_list.append(weakref.ref(record))
        def clear(yo):
            yo._weakref_list[:] = []
        def flush(yo):
            for maybe in yo._weakref_list:
                maybe = maybe()
                if maybe is not None and maybe._dirty:
                    maybe.write_record()            
        def pop(yo):
            return yo._weakref_list.pop()
    class DbfIterator(object):
        "returns records using current index"
        def __init__(yo, table, update=False):
            yo._table = table
            yo._index = -1
            yo._more_records = True
            yo._record = None
            yo._update = update
        def __iter__(yo):
            return yo
        def next(yo):
            while True:
                if yo._record is not None and yo._update:
                    yo._record.write_record()
                    yo._record = None
                if not yo._more_records:
                    break
                yo._index += 1
                if yo._index >= len(yo._table):
                    yo._more_records = False
                    break
                record = yo._record = yo._table[yo._index]
                if not yo._table.use_deleted and record.has_been_deleted:
                    continue
                return record
            raise StopIteration
    def _buildHeaderFields(yo):
        "constructs fieldblock for disk table"
        fieldblock = array('c', '')
        memo = False
        nulls = False
        meta = yo._meta
        header = meta.header
        header.version = chr(ord(header.version) & ord(yo._noMemoMask))
        meta.fields = [f for f in meta.fields if f != '_nullflags']
        for field in meta.fields:
            layout = meta[field]
            if meta.fields.count(field) > 1:
                raise DbfError("corrupted field structure (noticed in _buildHeaderFields)")
            fielddef = array('c', '\x00' * 32)
            fielddef[:11] = array('c', packStr(meta.encoder(field)[0]))
            try:
                fielddef[11] = layout[TYPE]
            except TypeError:
                print
                print repr(meta[field])
                print repr(layout[TYPE])
            fielddef[12:16] = array('c', packLongInt(layout[START]))
            fielddef[16] = chr(layout[LENGTH])
            fielddef[17] = chr(layout[DECIMALS])
            fielddef[18] = chr(layout[FLAGS])
            fieldblock.extend(fielddef)
            if layout[TYPE] in meta.memotypes:
                memo = True
            if layout[FLAGS] & NULLABLE:
                nulls = True
        if memo:
            header.version = chr(ord(header.version) | ord(yo._yesMemoMask))
            if meta.memo is None:
                meta.memo = yo._memoClass(meta)
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
            fielddef[:11] = array('c', packStr('_nullflags'))
            fielddef[11] = '0'
            fielddef[12:16] = array('c', packLongInt(start))
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

    def _checkMemoIntegrity(yo):
        "memory memos are simple dicts"
        pass
    def _initializeFields(yo):
        "builds the FieldList of names, types, and descriptions from the disk file"
        old_fields = defaultdict(dict)
        missing = object()
        meta = yo._meta
        for name in meta.fields:
            old_fields[name]['type'] = meta[name][TYPE]
            old_fields[name]['class'] = meta[name][CLASS]
            old_fields[name]['empty'] = meta[name][EMPTY]
        meta.fields[:] = []
        offset = 1
        fieldsdef = meta.header.fields
        if len(fieldsdef) % 32 != 0:
            raise DbfError("field definition block corrupt: %d bytes in size" % len(fieldsdef))
        if len(fieldsdef) // 32 != yo._meta.header.field_count:
            raise DbfError("Header shows %d fields, but field definition block has %d fields" % (yo._meta.header.field_count, len(fieldsdef)//32))
        for i in range(yo._meta.header.field_count):
            fieldblock = meta.decoder(fieldsdef[i*32:(i+1)*32])
            name = unpackStr(fieldblock[:11])
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
        yo._meta.user_fields = [f for f in yo._meta.fields if not yo._meta[f][FLAGS] & SYSTEM]
        yo._meta.user_field_count = len(yo._meta.user_fields)

    def _fieldLayout(yo, i):
        "Returns field information Name Type(Length[,Decimals])"
        name = yo._meta.fields[i]
        fielddef = yo._meta[name]
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
        if type in yo._decimal_fields:
            description = "%s %s(%d,%d)%s" % (name, type, length, decimals, flags)
        elif type in yo._fixed_fields:
            description = "%s %s%s" % (name, type, flags)
        else:
            description = "%s %s(%d)%s" % (name, type, length, flags)
        return description
    def _loadtable(yo):
        "loads the records from disk to memory"
        if yo._meta_only:
            raise DbfError("%s has been closed, records are unavailable" % yo.filename)
        dfd = yo._meta.dfd
        header = yo._meta.header
        dfd.seek(header.start)
        allrecords = dfd.read()                     # kludge to get around mysterious errno 0 problems
        dfd.seek(0)
        length = header.record_length
        for i in range(header.record_count):
            record_data = allrecords[length*i:length*i+length]
            yo._table.append(_DbfRecord(i, yo._meta, allrecords[length*i:length*i+length], _fromdisk=True))
        dfd.seek(0)
    def _list_fields(yo, specs, sep=','):
        if specs is None:
            specs = yo.field_names
        elif isinstance(specs, str):
            specs = specs.split(sep)
        else:
            specs = list(specs)
        specs = [s.strip() for s in specs]
        return specs
    @staticmethod
    def _packDate(date):
            "Returns a group of three bytes, in integer form, of the date"
            return "%c%c%c" % (date.year-1900, date.month, date.day)
    @staticmethod
    def _unpackDate(bytestr):
            "Returns a Date() of the packed three-byte date passed in"
            year, month, day = struct.unpack('<BBB', bytestr)
            year += 1900
            return Date(year, month, day)
    def _update_disk(yo, headeronly=False):
        "synchronizes the disk file with current data"
        if yo._meta.inmemory:
            return
        meta = yo._meta
        header = meta.header
        fd = meta.dfd
        fd.seek(0)
        fd.write(header.data)
        eof = header.start + header.record_count * header.record_length
        if not headeronly:
            for record in yo:
                if record._update_disk():
                    fd.flush()
            fd.truncate(eof)
        if 'db3' in yo._versionabbr:
            fd.seek(0, SEEK_END)
            fd.write('\x1a')        # required for dBase III
            fd.flush()
            fd.truncate(eof + 1)

    def __contains__(yo, key):
        return key in yo.field_names
                    
    def __enter__(yo):
        return yo
    def __exit__(yo, *exc_info):
        yo.close()
    def __getattr__(yo, name):
        if name in (
                'memotypes',
                'fixed_fields',
                'variable_fields',
                'character_fields',
                'numeric_fields',
                'decimal_fields',
                'currency_fields',
                ):
            return getattr(yo, '_'+name)
        if name in ('_table'):
                if yo._meta.ondisk:
                    yo._table = yo._Table(len(yo), yo._meta)
                else:
                    yo._table = []
                    yo._loadtable()
        return object.__getattribute__(yo, name)
    def __getitem__(yo, value):
        if type(value) == int:
            if not -yo._meta.header.record_count <= value < yo._meta.header.record_count: 
                raise IndexError("Record %d is not in table." % value)
            return yo._table[value]
        elif type(value) == slice:
            sequence = List(desc='%s -->  %s' % (yo.filename, value), field_names=yo.field_names)
            yo._dbflists.add(sequence)
            for index in range(len(yo))[value]:
                record = yo._table[index]
                if yo.use_deleted is True or not record.has_been_deleted:
                    sequence.append(record)
            return sequence
        else:
            raise TypeError('type <%s> not valid for indexing' % type(value))
    def __init__(yo, filename=':memory:', field_specs=None, memo_size=128, ignore_memos=False, 
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
        elif type(yo) is DbfTable:
            raise DbfError("only memory tables supported")
        yo._dbflists = yo._DbfLists()
        yo._indexen = yo._Indexen()
        yo._meta = meta = yo._MetaData()
        meta.max_fields = yo._max_fields
        meta.max_records = yo._max_records
        meta.table = weakref.ref(yo)
        meta.filename = filename
        meta.fields = []
        meta.user_fields = []
        meta.user_field_count = 0
        meta.fieldtypes = fieldtypes = yo._fieldtypes
        meta.fixed_fields = yo._fixed_fields
        meta.variable_fields = yo._variable_fields
        meta.character_fields = yo._character_fields
        meta.decimal_fields = yo._decimal_fields
        meta.numeric_fields = yo._numeric_fields
        meta.memotypes = yo._memotypes
        meta.ignorememos = meta.original_ignorememos = ignore_memos
        meta.memo_size = memo_size
        meta.input_decoder = codecs.getdecoder(input_decoding)      # from ascii to unicode
        meta.output_encoder = codecs.getencoder(input_decoding)     # and back to ascii
        meta.header = header = yo._TableHeader(yo._dbfTableHeader, yo._packDate, yo._unpackDate)
        header.extra = yo._dbfTableHeaderExtra
        header.data        #force update of date
        for field, types in default_data_types.items():
            if not isinstance(types, tuple):
                types = (types, )
            for result_name, result_type in ezip(('Class','Empty','Null'), types):
                fieldtypes[field][result_name] = result_type
        if filename[0] == filename[-1] == ':':
            yo._table = []
            meta.ondisk = False
            meta.inmemory = True
            meta.memoname = filename
        else:
            base, ext = os.path.splitext(filename)
            if ext == '':
                meta.filename =  base + '.dbf'
            meta.memoname = base + yo._memoext
            meta.ondisk = True
            meta.inmemory = False
        if codepage is not None:
            cp, sd, ld = _codepage_lookup(codepage)
            yo._meta.decoder = codecs.getdecoder(sd) 
            yo._meta.encoder = codecs.getencoder(sd)
        if field_specs:
            if meta.ondisk:
                meta.dfd = open(meta.filename, 'w+b')
                meta.newmemofile = True
            if codepage is None:
                header.codepage(default_codepage)
                cp, sd, ld = _codepage_lookup(header.codepage())
                meta.decoder = codecs.getdecoder(sd) 
                meta.encoder = codecs.getencoder(sd)
            yo.add_fields(field_specs)
        else:
            try:
                dfd = meta.dfd = open(meta.filename, 'r+b')
            except IOError, e:
                raise DbfError(str(e))
            dfd.seek(0)
            meta.header = header = yo._TableHeader(dfd.read(32), yo._packDate, yo._unpackDate)
            if not header.version in yo._supported_tables:
                dfd.close()
                dfd = None
                raise DbfError(
                    "%s does not support %s [%x]" % 
                    (yo._version,
                    version_map.get(header.version, 'Unknown: %s' % header.version),
                    ord(header.version)))
            if codepage is None:
                cp, sd, ld = _codepage_lookup(header.codepage())
                yo._meta.decoder = codecs.getdecoder(sd) 
                yo._meta.encoder = codecs.getencoder(sd)
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
            yo._initializeFields()
            yo._checkMemoIntegrity()
            meta.current = -1
            if len(yo) > 0:
                meta.current = 0
            dfd.seek(0)
            if meta_only:
                yo.close(keep_table=False, keep_memos=False)
            elif read_only:
                yo.close(keep_table=True, keep_memos=keep_memos)

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

        
    def __iter__(yo):
        return yo.DbfIterator(yo)           
    def __len__(yo):
        return yo._meta.header.record_count
    def __nonzero__(yo):
        return yo._meta.header.record_count != 0
    def __repr__(yo):
        if yo._read_only:
            return __name__ + ".Table('%s', read_only=True)" % yo._meta.filename
        elif yo._meta_only:
            return __name__ + ".Table('%s', meta_only=True)" % yo._meta.filename
        else:
            return __name__ + ".Table('%s')" % yo._meta.filename
    def __str__(yo):
        if yo._read_only:
            status = "read-only"
        elif yo._meta_only:
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
        Record length: %d """ % (yo.filename, version_map.get(yo._meta.header.version, 
            'unknown - ' + hex(ord(yo._meta.header.version))), yo.codepage, status, 
            yo.last_update, len(yo), yo.field_count, yo.record_length)
        str += "\n        --Fields--\n"
        for i in range(len(yo.field_names)):
            str += "%11d) %s\n" % (i, yo._fieldLayout(i))
        return str
    @property
    def codepage(yo):
        return "%s (%s)" % code_pages[yo._meta.header.codepage()]
    @codepage.setter
    def codepage(yo, cp):
        cp = code_pages[yo._meta.header.codepage(cp)][0]
        yo._meta.decoder = codecs.getdecoder(cp) 
        yo._meta.encoder = codecs.getencoder(cp)
        yo._update_disk(headeronly=True)
    @property
    def field_count(yo):
        "the number of user fields in the table"
        return yo._meta.user_field_count
    @property
    def field_names(yo):
        "a list of the user fields in the table"
        return yo._meta.user_fields[:]
    @property
    def filename(yo):
        "table's file name, including path (if specified on open)"
        return yo._meta.filename
    @property
    def last_update(yo):
        "date of last update"
        return yo._meta.header.update
    @property
    def memoname(yo):
        "table's memo name (if path included in filename on open)"
        return yo._meta.memoname
    @property
    def record_length(yo):
        "number of bytes in a record"
        return yo._meta.header.record_length
    @property
    def record_number(yo):
        "index number of the current record"
        return yo._meta.current
    @property
    def supported_tables(yo):
        "allowable table types"
        return yo._supported_tables
    @property
    def use_deleted(yo):
        "process or ignore deleted records"
        return yo._use_deleted
    @use_deleted.setter
    def use_deleted(yo, new_setting):
        yo._use_deleted = new_setting
    @property
    def version(yo):
        "returns the dbf type of the table"
        return yo._version
    def add_fields(yo, field_specs):
        """adds field(s) to the table layout; format is Name Type(Length,Decimals)[; Name Type(Length,Decimals)[...]]
        backup table is created with _backup appended to name
        then modifies current structure"""
        meta = yo._meta
        header = meta.header
        fields = yo.structure() + yo._list_fields(field_specs, sep=';')
        if len(fields) > meta.max_fields:
            raise DbfError(
                    "Adding %d more field%s would exceed the limit of %d" 
                    % (len(fields), ('','s')[len(fields)==1], meta.max_fields)
                    )
        old_table = None
        if yo:
            old_table = yo.create_backup()
            yo.zap(YES_I_AM_SURE)
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
            init = yo._meta.fieldtypes[field_type]['Init']
            flags = yo._meta.fieldtypes[field_type]['flags']
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
        yo._buildHeaderFields()
        yo._update_disk()
        if old_table is not None:
            old_table.open()
            for record in old_table:
                yo.append(record.scatter_fields())
            old_table.close()

    def allow_nulls(yo, fields):
        "set fields to allow null values"
        if yo._versionabbr in ('db3', ):
            raise DbfError("Nullable fields are not allowed in %s tables" % yo._version)
        meta = yo._meta
        header = meta.header
        fields = yo._list_fields(fields)
        missing = set(fields) - set(yo.field_names)
        if missing:
            raise FieldMissing(', '.join(missing))
        if len(yo.field_names) + 1 > meta.max_fields:
            raise DbfError(
                    "Adding the hidden _nullflags field would exceed the limit of %d fields for this table" 
                    % (meta.max_fields, )
                    )
        old_table = None
        if yo:
            old_table = yo.create_backup()
            yo.zap(YES_I_AM_SURE)
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
        yo._buildHeaderFields()
        yo._update_disk()
        if old_table is not None:
            old_table.open()
            for record in old_table:
                yo.append(record.scatter_fields())
            old_table.close()

    def append(yo, kamikaze='', drop=False, multiple=1):
        "adds <multiple> blank records, and fills fields with dict/tuple values if present"
        if not yo.field_count:
            raise DbfError("No fields defined, cannot append")
        empty_table = len(yo) == 0
        dictdata = False
        tupledata = False
        meta = yo._meta
        header = meta.header
        if not isinstance(kamikaze, _DbfRecord):
            if isinstance(kamikaze, dict):
                dictdata = kamikaze
                kamikaze = ''
            elif isinstance(kamikaze, tuple):
                tupledata = kamikaze
                kamikaze = ''
        newrecord = _DbfRecord(recnum=header.record_count, layout=meta, kamikaze=kamikaze)
        yo._table.append(newrecord)
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
                            (yo.filename, len(newrecord), len(tupledata)))
            elif kamikaze == str:
                for field in meta.memofields:
                    newrecord[field] = ''
            elif kamikaze:
                for field in meta.memofields:
                    newrecord[field] = kamikaze[field]
            newrecord.write_record()
        except Exception:
            yo._table.pop()     # discard failed record
            header.record_count = header.record_count - 1
            yo._update_disk()
            raise
        multiple -= 1
        if multiple:
            data = newrecord._data
            single = header.record_count
            total = single + multiple
            while single < total:
                multi_record = _DbfRecord(single, meta, kamikaze=data)
                yo._table.append(multi_record)
                for field in meta.memofields:
                    multi_record[field] = newrecord[field]
                single += 1
                multi_record.write_record()
            header.record_count = total   # += multiple
            meta.current = header.record_count - 1
            newrecord = multi_record
        yo._update_disk(headeronly=True)
        if empty_table:
            meta.current = 0
        return newrecord
    def bof(yo, _move=False):
        "moves record pointer to previous usable record; returns True if no more usable records"
        current = yo._meta.current
        try:
            while yo._meta.current > 0:
                yo._meta.current -= 1
                if yo.use_deleted or not yo.current().has_been_deleted:
                    break
            else:
                yo._meta.current = -1
                return True
            return False
        finally:
            if not _move:
                yo._meta.current = current
    def bottom(yo, get_record=False):
        """sets record pointer to bottom of table
        if get_record, seeks to and returns last (non-deleted) record
        DbfError if table is empty
        Bof if all records deleted and use_deleted is False"""
        yo._meta.current = yo._meta.header.record_count
        if get_record:
            try:
                return yo.prev()
            except Bof:
                yo._meta.current = yo._meta.header.record_count
                raise Eof()
    def close(yo, keep_table=False, keep_memos=False):
        """closes disk files, flushing record data to disk
        ensures table data is available if keep_table
        ensures memo data is available if keep_memos"""
        if not yo._meta.inmemory:
            yo._table.flush()
        yo._meta.inmemory = True
        if keep_table:
            replacement_table = []
            for record in yo._table:
                replacement_table.append(record)
            yo._table = replacement_table
        else:
            if yo._meta.ondisk:
                yo._meta_only = True
        if yo._meta.mfd is not None:
            if not keep_memos:
                yo._meta.ignorememos = True
            else:
                memo_fields = []
                for field in yo.field_names:
                    if yo.is_memotype(field):
                        memo_fields.append(field)
                for record in yo:
                    for field in memo_fields:
                        record[field] = record[field]
            yo._meta.mfd.close()
            yo._meta.mfd = None
        if yo._meta.ondisk:
            yo._meta.dfd.close()
            yo._meta.dfd = None
        if keep_table:
            yo._read_only = True
        yo._meta.ondisk = False
    def create_backup(yo, new_name=None):
        "creates a backup table"
        if yo.filename[0] == yo.filename[-1] == ':' and new_name is None:
            new_name = yo.filename[:-1] + '_backup:'
        if new_name is None:
            upper = yo.filename.isupper()
            name, ext = os.path.splitext(os.path.split(yo.filename)[1])
            extra = ('_backup', '_BACKUP')[upper]
            new_name = os.path.join(temp_dir, name + extra + ext)
        bkup = yo.__class__(new_name, yo.structure())
        for record in yo:
            bkup.append(record)
        bkup.close()
        yo.backup = new_name
        return bkup
    def create_index(yo, key):
        return Index(yo, key)
    def current(yo, index=False):
        "returns current logical record, or its index"
        if yo._meta.current < 0:
            raise Bof()
        elif yo._meta.current >= yo._meta.header.record_count:
            raise Eof()
        if index:
            return yo._meta.current
        return yo._table[yo._meta.current]
    def delete_fields(yo, doomed):
        """removes field(s) from the table
        creates backup files with _backup appended to the file name,
        then modifies current structure"""
        doomed = yo._list_fields(doomed)
        meta = yo._meta
        header = meta.header
        for victim in doomed:
            if victim not in meta.user_fields:
                raise DbfError("field %s not in table -- delete aborted" % victim)
        old_table = None
        if yo:
            old_table = yo.create_backup()
            yo.zap(YES_I_AM_SURE)
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
                    end = specs[END]                    #yo._meta[field][END]
                    specs[START] = start                #yo._meta[field][START] = start
                    specs[END] = start + specs[LENGTH]  #yo._meta[field][END] = start + yo._meta[field][LENGTH]
                    start = specs[END]                  #yo._meta[field][END]
                    meta[field] =  tuple(specs)
        yo._buildHeaderFields()
        yo._update_disk()
        for name in list(meta):
            if name not in meta.fields:
                del meta[name]
        if old_table is not None:
            old_table.open()
            for record in old_table:
                yo.append(record.scatter_fields(), drop=True)
            old_table.close()

    def disallow_nulls(yo, fields):
        "set fields to not allow null values"
        meta = yo._meta
        fields = yo._list_fields(fields)
        missing = set(fields) - set(yo.field_names)
        if missing:
            raise FieldMissing(', '.join(missing))
        old_table = None
        if yo:
            old_table = yo.create_backup()
            yo.zap(YES_I_AM_SURE)
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
        yo._buildHeaderFields()
        yo._update_disk()
        if old_table is not None:
            old_table.open()
            for record in old_table:
                yo.append(record.scatter_fields())
            old_table.close()

    def eof(yo, _move=False):
        "moves record pointer to next usable record; returns True if no more usable records"
        current = yo._meta.current
        try:
            while yo._meta.current < yo._meta.header.record_count - 1:
                yo._meta.current += 1
                if yo.use_deleted or not yo.current().has_been_deleted:
                    break
            else:
                yo._meta.current = yo._meta.header.record_count
                return True
            return False
        finally:
            if not _move:
                yo._meta.current = current
    def export(yo, records=None, filename=None, field_specs=None, format='csv', header=True):
        """writes the table using CSV or tab-delimited format, using the filename
        given if specified, otherwise the table name"""
        if filename is not None:
            path, filename = os.path.split(filename)
        else:
            path, filename = os.path.split(yo.filename)
        filename = os.path.join(path, filename)
        field_specs = yo._list_fields(field_specs)
        if records is None:
            records = yo
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
                            fields.append(yo._meta.encoder(data)[0])
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
                            fields.append(yo._meta.encoder(data)[0])
                        else:
                            fields.append(str(data))
                    fd.write('\t'.join(fields) + '\n')
            else: # format == 'fixed'
                header = open("%s_layout.txt" % os.path.splitext(filename)[0], 'w')
                header.write("%-15s  Size\n" % "Field Name")
                header.write("%-15s  ----\n" % ("-" * 15))
                sizes = []
                for field in field_specs:
                    size = yo.size(field)[0]
                    sizes.append(size)
                    header.write("%-15s  %3d\n" % (field, size))
                header.write('\nTotal Records in file: %d\n' % len(records))
                header.close()
                for record in records:
                    fields = []
                    for i, fieldname in enumerate(field_specs):
                        data = record[fieldname]
                        if isinstance(data, (str, unicode)):
                            fields.append("%-*s" % (sizes[i], yo._meta.encoder(data)[0]))
                        else:
                            fields.append("%-*s" % (sizes[i], data))
                    fd.write(''.join(fields) + '\n')
        finally:
            fd.close()
            fd = None
        return len(records)
    def find(yo, command):
        "uses exec to perform queries on the table"
        possible = List(desc="%s -->  %s" % (yo.filename, command), field_names=yo.field_names)
        yo._dbflists.add(possible)
        result = {}
        select = 'result["keep"] = %s' % command
        g = {}
        use_deleted = yo.use_deleted
        for record in yo:
            result['keep'] = False
            g['result'] = result
            exec select in g, record
            if result['keep']:
                possible.append(record)
            record.write_record()
        return possible
    def get_record(yo, recno):
        "returns record at physical_index[recno]"
        return yo._table[recno]
    def goto(yo, criteria):
        """changes the record pointer to the first matching (non-deleted) record
        criteria should be either a tuple of tuple(value, field, func) triples, 
        or an integer to go to"""
        if isinstance(criteria, int):
            if not -yo._meta.header.record_count <= criteria < yo._meta.header.record_count:
                raise IndexError("Record %d does not exist" % criteria)
            if criteria < 0:
                criteria += yo._meta.header.record_count
            yo._meta.current = criteria
            return yo.current()
        criteria = _normalize_tuples(tuples=criteria, length=3, filler=[_nop])
        specs = tuple([(field, func) for value, field, func in criteria])
        match = tuple([value for value, field, func in criteria])
        current = yo.current(index=True)
        matchlen = len(match)
        while not yo.Eof():
            record = yo.current()
            results = record(*specs)
            if results == match:
                return record
        return yo.goto(current)
    def is_decimal(yo, name):
        "returns True if name is a variable-length field type"
        return yo._meta[name][TYPE] in yo._decimal_fields
    def is_memotype(yo, name):
        "returns True if name is a memo type field"
        return yo._meta[name][TYPE] in yo._memotypes
    def new(yo, filename, field_specs=None, codepage=None):
        "returns a new table of the same type"
        if field_specs is None:
            field_specs = yo.structure()
        if not (filename[0] == filename[-1] == ':'):
            path, name = os.path.split(filename)
            if path == "":
                filename = os.path.join(os.path.split(yo.filename)[0], filename)
            elif name == "":
                filename = os.path.join(path, os.path.split(yo.filename)[1])
        if codepage is None:
            codepage = yo._meta.header.codepage()[0]
        return yo.__class__(filename, field_specs, codepage=codepage)
    def next(yo):
        "set record pointer to next (non-deleted) record, and return it"
        if yo.eof(_move=True):
            raise Eof()
        return yo.current()
    def nullable_field(yo, field):
        "returns True if field allows Nulls"
        if field not in yo.field_names:
            raise MissingField(field)
        return bool(yo._meta[field][FLAGS] & NULLABLE)
    def open(yo):
        if yo.filename[0] == yo.filename[-1] == ':':
            return
        meta = yo._meta
        meta.inmemory = False
        meta.ondisk = True
        yo._read_only = False
        yo._meta_only = False
        if '_table' in dir(yo):
            del yo._table
        dfd = meta.dfd = open(meta.filename, 'r+b')
        dfd.seek(0)
        meta.header = yo._TableHeader(dfd.read(32), yo._packDate, yo._unpackDate)
        header = meta.header
        if not header.version in yo._supported_tables:
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
        yo._meta.ignorememos = yo._meta.original_ignorememos
        yo._initializeFields()
        yo._checkMemoIntegrity()
        meta.current = -1
        if len(yo) > 0:
            meta.current = 0
        dfd.seek(0)

    def pack(yo):
        "physically removes all deleted records"
        for dbfindex in yo._indexen:
            dbfindex.clear()
        newtable = []
        index = 0
        offset = 0 # +1 for each purged record
        for record in yo._table:
            found = False
            if record.has_been_deleted:
                for dbflist in yo._dbflists:
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
        yo._table.clear()
        for record in newtable:
            yo._table.append(record)
        yo._meta.header.record_count = index
        yo._current = -1
        yo._update_disk()
        yo.reindex()
    def prev(yo):
        "set record pointer to previous (non-deleted) record, and return it"
        if yo.bof(_move=True):
            raise Bof
        return yo.current()
    def query(yo, sql_command=None, python=None):
        "deprecated: use .find or .sql"
        if sql_command:
            return yo.sql(sql_command)
        elif python:
            return yo.find(python)
        raise DbfError("query: python parameter must be specified")
    def reindex(yo):
        for dbfindex in yo._indexen:
            dbfindex.reindex()
    def rename_field(yo, oldname, newname):
        "renames an existing field"
        if yo:
            yo.create_backup()
        if not oldname in yo._meta.user_fields:
            raise DbfError("field --%s-- does not exist -- cannot rename it." % oldname)
        if newname[0] == '_' or newname[0].isdigit() or not newname.replace('_','').isalnum():
            raise DbfError("field names cannot start with _ or digits, and can only contain the _, letters, and digits")
        newname = newname.lower()
        if newname in yo._meta.fields:
            raise DbfError("field --%s-- already exists" % newname)
        if len(newname) > 10:
            raise DbfError("maximum field name length is 10.  '%s' is %d characters long." % (newname, len(newname)))
        yo._meta[newname] = yo._meta[oldname]
        yo._meta.fields[yo._meta.fields.index(oldname)] = newname
        yo._buildHeaderFields()
        yo._update_disk(headeronly=True)
    def resize_field(yo, doomed, new_size):
        """resizes field (C only at this time)
        creates backup file, then modifies current structure"""
        if not 0 < new_size < 256:
            raise DbfError("new_size must be between 1 and 255 (use delete_fields to remove a field)")
        doomed = yo._list_fields(doomed)
        for victim in doomed:
            if victim not in yo._meta.user_fields:
                raise DbfError("field %s not in table -- resize aborted" % victim)
        all_records = [record for record in yo]
        yo.create_backup()
        for victim in doomed:
            specs = list(yo._meta[victim])
            delta = new_size - specs[LENGTH]                #yo._meta[victim]['length']
            start = specs[START]                            #yo._meta[victim]['start']
            end = specs[END]                                #yo._meta[victim]['end']
            eff_end = min(specs[LENGTH], new_size)          #min(yo._meta[victim]['length'], new_size)
            specs[LENGTH] = new_size                        #yo._meta[victim]['length'] = new_size
            specs[END] = start + new_size                   #yo._meta[victim]['end'] = start + new_size
            yo._meta[victim] = tuple(specs)
            blank = array('c', ' ' * new_size)
            for record in yo:
                new_data = blank[:]
                new_data[:eff_end] = record._data[start:start+eff_end]
                record._data = record._data[:start] + new_data + record._data[end:]
            for field in yo._meta.fields:
                if yo._meta[field][START] == end:
                    specs = list(yo._meta[field])
                    end = specs[END]                        #yo._meta[field]['end']
                    specs[START] += delta                   #yo._meta[field]['start'] += delta
                    specs[END] += delta                     #yo._meta[field]['end'] += delta #+ yo._meta[field]['length']
                    start = specs[END]                      #yo._meta[field]['end']
                    yo._meta[field] = tuple(specs)
        yo._buildHeaderFields()
        yo._update_disk()
    def size(yo, field):
        "returns size of field as a tuple of (length, decimals)"
        if field in yo:
            return (yo._meta[field][LENGTH], yo._meta[field][DECIMALS])
        raise DbfError("%s is not a field in %s" % (field, yo.filename))
    def sql(yo, command):
        "passes calls through to module level sql function"
        return sql(yo, command)
    def structure(yo, fields=None):
        """return list of fields suitable for creating same table layout
        @param fields: list of fields or None for all fields"""
        field_specs = []
        fields = yo._list_fields(fields)
        try:
            for name in fields:
                field_specs.append(yo._fieldLayout(yo.field_names.index(name)))
        except ValueError:
            raise DbfError("field %s does not exist" % name)
        return field_specs
    def top(yo, get_record=False):
        """sets record pointer to top of table; if get_record, seeks to and returns first (non-deleted) record
        DbfError if table is empty
        Eof if all records are deleted and use_deleted is False"""
        yo._meta.current = -1
        if get_record:
            try:
                return yo.next()
            except Eof:
                yo._meta.current = -1
                raise Bof()
    def type(yo, field):
        "returns (dbf type, class) of field"
        if field in yo:
            return FieldType(yo._meta[field][TYPE], yo._meta[field][CLASS])
        raise DbfError("%s is not a field in %s" % (field, yo.filename))
    def zap(yo, areyousure=False):
        """removes all records from table -- this cannot be undone!
        areyousure must be True, else error is raised"""
        if areyousure:
            if yo._meta.inmemory:
                yo._table = []
            else:
                yo._table.clear()
                if yo._meta.memo:
                    yo._meta.memo._zap()
            yo._meta.header.record_count = 0
            yo._current = -1
            yo._update_disk()
        else:
            raise DbfError("You must say you are sure to wipe the table")
class Db3Table(DbfTable):
    """Provides an interface for working with dBase III tables."""
    _version = 'dBase III Plus'
    _versionabbr = 'db3'
    @MutableDefault
    def _fieldtypes():
        return {
            'C' : {
                    'Type':'Character', 'Retrieve':retrieveCharacter, 'Update':updateCharacter, 'Blank':str, 'Init':addCharacter,
                    'Class':unicode, 'Empty':unicode, 'flags':tuple(),
                    },
            'D' : {
                    'Type':'Date', 'Retrieve':retrieveDate, 'Update':updateDate, 'Blank':Date, 'Init':addDate,
                    'Class':datetime.date, 'Empty':none, 'flags':tuple(),
                    },
            'L' : {
                    'Type':'Logical', 'Retrieve':retrieveLogical, 'Update':updateLogical, 'Blank':Logical, 'Init':addLogical,
                    'Class':bool, 'Empty':none, 'flags':tuple(),
                    },
            'M' : {
                    'Type':'Memo', 'Retrieve':retrieveMemo, 'Update':updateMemo, 'Blank':str, 'Init':addMemo,
                    'Class':unicode, 'Empty':unicode, 'flags':tuple(),
                    },
            'N' : {
                    'Type':'Numeric', 'Retrieve':retrieveNumeric, 'Update':updateNumeric, 'Blank':int, 'Init':addNumeric,
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
    _dbfTableHeader[8:10] = array('c', packShortInt(33))
    _dbfTableHeader[10] = '\x01'        # record length -- one for delete flag
    _dbfTableHeader[29] = '\x03'        # code page -- 437 US-MS DOS
    _dbfTableHeader = _dbfTableHeader.tostring()
    _dbfTableHeaderExtra = ''
    _supported_tables = ['\x03', '\x83']
    _read_only = False
    _meta_only = False
    _use_deleted = True

    def _checkMemoIntegrity(yo):
        "dBase III specific"
        if not yo._meta.ignorememos:
            memo_fields = False
            for field in yo._meta.fields:
                if yo._meta[field][TYPE] in yo._memotypes:
                    memo_fields = True
                    break
            if memo_fields and yo._meta.header.version != '\x83':
                yo._meta.dfd.close()
                yo._meta.dfd = None
                raise DbfError("Table structure corrupt:  memo fields exist, header declares no memos")
            elif memo_fields and not os.path.exists(yo._meta.memoname):
                yo._meta.dfd.close()
                yo._meta.dfd = None
                raise DbfError("Table structure corrupt:  memo fields exist without memo file")
            if memo_fields:
                try:
                    yo._meta.memo = yo._memoClass(yo._meta)
                except Exception, exc:
                    yo._meta.dfd.close()
                    yo._meta.dfd = None
                    raise DbfError("Table structure corrupt:  unable to use memo file (%s)" % exc.args[-1])

    def _initializeFields(yo):
        "builds the FieldList of names, types, and descriptions"
        old_fields = defaultdict(dict)
        for name in yo._meta.fields:
            old_fields[name]['type'] = yo._meta[name][TYPE]
            old_fields[name]['empty'] = yo._meta[name][EMPTY]
            old_fields[name]['class'] = yo._meta[name][CLASS]
        yo._meta.fields[:] = []
        offset = 1
        fieldsdef = yo._meta.header.fields
        if len(fieldsdef) % 32 != 0:
            raise DbfError("field definition block corrupt: %d bytes in size" % len(fieldsdef))
        if len(fieldsdef) // 32 != yo._meta.header.field_count:
            raise DbfError("Header shows %d fields, but field definition block has %d fields" % (yo._meta.header.field_count, len(fieldsdef)//32))
        for i in range(yo._meta.header.field_count):
            fieldblock = fieldsdef[i*32:(i+1)*32]
            name = unpackStr(fieldblock[:11])
            type = fieldblock[11]
            if not type in yo._meta.fieldtypes:
                raise DbfError("Unknown field type: %s" % type)
            start = offset
            length = ord(fieldblock[16])
            offset += length
            end = start + length
            decimals = ord(fieldblock[17])
            flags = ord(fieldblock[18])
            if name in yo._meta.fields:
                raise DbfError('Duplicate field name found: %s' % name)
            yo._meta.fields.append(name)
            if name in old_fields and old_fields[name]['type'] == type:
                cls = old_fields[name]['class']
                empty = old_fields[name]['empty']
            else:
                cls = yo._meta.fieldtypes[type]['Class']
                empty = yo._meta.fieldtypes[type]['Empty']
            yo._meta[name] = (
                    type,
                    start,
                    length,
                    end,
                    decimals,
                    flags,
                    cls,
                    empty,
                    )
        yo._meta.user_fields = [f for f in yo._meta.fields if not yo._meta[f][FLAGS] & SYSTEM]
        yo._meta.user_field_count = len(yo._meta.user_fields)
class FpTable(DbfTable):
    'Provides an interface for working with FoxPro 2 tables'
    _version = 'Foxpro'
    _versionabbr = 'fp'
    @MutableDefault
    def _fieldtypes():
        return {
            'C' : {
                    'Type':'Character', 'Retrieve':retrieveCharacter, 'Update':updateCharacter, 'Blank':str, 'Init':addCharacter,
                    'Class':unicode, 'Empty':unicode, 'flags':('binary','nocptrans','null', ),
                    },
            'F' : {
                    'Type':'Float', 'Retrieve':retrieveNumeric, 'Update':updateNumeric, 'Blank':float, 'Init':addVfpNumeric,
                    'Class':'default', 'Empty':none, 'flags':('null', ),
                    },
            'N' : {
                    'Type':'Numeric', 'Retrieve':retrieveNumeric, 'Update':updateNumeric, 'Blank':int, 'Init':addVfpNumeric,
                    'Class':'default', 'Empty':none, 'flags':('null', ),
                    },
            'L' : {
                    'Type':'Logical', 'Retrieve':retrieveLogical, 'Update':updateLogical, 'Blank':Logical, 'Init':addLogical,
                    'Class':bool, 'Empty':none, 'flags':('null', ),
                    },
            'D' : {
                    'Type':'Date', 'Retrieve':retrieveDate, 'Update':updateDate, 'Blank':Date, 'Init':addDate,
                    'Class':datetime.date, 'Empty':none, 'flags':('null', ),
                    },
            'M' : {
                    'Type':'Memo', 'Retrieve':retrieveMemo, 'Update':updateMemo, 'Blank':str, 'Init':addVfpMemo,
                    'Class':unicode, 'Empty':unicode, 'flags':('binary','nocptrans','null', ),
                    },
            'G' : {
                    'Type':'General', 'Retrieve':retrieveMemo, 'Update':updateMemo, 'Blank':str, 'Init':addMemo,
                    'Class':bytes, 'Empty':bytes, 'flags':('null', ),
                    },
            'P' : {
                    'Type':'Picture', 'Retrieve':retrieveMemo, 'Update':updateMemo, 'Blank':str, 'Init':addMemo,
                    'Class':bytes, 'Empty':bytes, 'flags':('null', ),
                    },
            '0' : {
                    'Type':'_NullFlags', 'Retrieve':unsupportedType, 'Update':unsupportedType, 'Blank':int, 'Init':None,
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
    _dbfTableHeader[8:10] = array('c', packShortInt(33+263))
    _dbfTableHeader[10] = '\x01'        # record length -- one for delete flag
    _dbfTableHeader[29] = '\x03'        # code page -- 437 US-MS DOS
    _dbfTableHeader = _dbfTableHeader.tostring()
    _dbfTableHeaderExtra = '\x00' * 263
    _use_deleted = True
    def _checkMemoIntegrity(yo):
        if not yo._meta.ignorememos:
            memo_fields = False
            for field in yo._meta.fields:
                if yo._meta[field][TYPE] in yo._memotypes:
                    memo_fields = True
                    break
            if memo_fields and (not os.path.exists(yo._meta.memoname) or not os.stat(yo._meta.memoname)[ST_SIZE]):
                yo._meta.dfd.close()
                yo._meta.dfd = None
                raise DbfError("Table structure corrupt:  memo fields exist without memo file")
            elif not memo_fields and os.path.exists(yo._meta.memoname):
                yo._meta.dfd.close()
                yo._meta.dfd = None
                raise DbfError("Table structure corrupt:  no memo fields exist but memo file does")
            if memo_fields:
                try:
                    yo._meta.memo = yo._memoClass(yo._meta)
                except Exception, exc:
                    yo._meta.dfd.close()
                    yo._meta.dfd = None
                    raise DbfError("Table structure corrupt:  unable to use memo file (%s)" % exc.args[-1])

    def _initializeFields(yo):
        "builds the FieldList of names, types, and descriptions"
        old_fields = defaultdict(dict)
        for name in yo._meta.fields:
            old_fields[name]['type'] = yo._meta[name][TYPE]
            old_fields[name]['class'] = yo._meta[name][CLASS]
        yo._meta.fields[:] = []
        offset = 1
        fieldsdef = yo._meta.header.fields
        if len(fieldsdef) % 32 != 0:
            raise DbfError("field definition block corrupt: %d bytes in size" % len(fieldsdef))
        if len(fieldsdef) // 32 != yo._meta.header.field_count:
            raise DbfError("Header shows %d fields, but field definition block has %d fields" % (yo._meta.header.field_count, len(fieldsdef)//32))
        for i in range(yo._meta.header.field_count):
            fieldblock = fieldsdef[i*32:(i+1)*32]
            name = unpackStr(fieldblock[:11])
            type = fieldblock[11]
            if not type in yo._meta.fieldtypes:
                raise DbfError("Unknown field type: %s" % type)
            start = offset
            length = ord(fieldblock[16])
            offset += length
            end = start + length
            decimals = ord(fieldblock[17])
            flags = ord(fieldblock[18])
            if name in yo._meta.fields:
                raise DbfError('Duplicate field name found: %s' % name)
            yo._meta.fields.append(name)
            if name in old_fields and old_fields[name]['type'] == type:
                cls = old_fields[name]['class']
                empty = old_fields[name]['empty']
            else:
                cls = yo._meta.fieldtypes[type]['Class']
                empty = yo._meta.fieldtypes[type]['Empty']
            yo._meta[name] = (
                    type,
                    start,
                    length,
                    end,
                    decimals,
                    flags,
                    cls,
                    empty,
                    )
        yo._meta.user_fields = [f for f in yo._meta.fields if not yo._meta[f][FLAGS] & SYSTEM]
        yo._meta.user_field_count = len(yo._meta.user_fields)
    @staticmethod
    def _packDate(date):
            "Returns a group of three bytes, in integer form, of the date"
            return "%c%c%c" % (date.year-2000, date.month, date.day)
    @staticmethod
    def _unpackDate(bytestr):
            "Returns a Date() of the packed three-byte date passed in"
            year, month, day = struct.unpack('<BBB', bytestr)
            year += 2000
            return Date(year, month, day)
            
class VfpTable(FpTable):
    'Provides an interface for working with Visual FoxPro 6 tables'
    _version = 'Visual Foxpro'
    _versionabbr = 'vfp'
    @MutableDefault
    def _fieldtypes():
        return {
            'C' : {
                    'Type':'Character', 'Retrieve':retrieveCharacter, 'Update':updateCharacter, 'Blank':str, 'Init':addCharacter,
                    'Class':unicode, 'Empty':unicode, 'flags':('binary','nocptrans','null', ), 
                    },
            'Y' : {
                    'Type':'Currency', 'Retrieve':retrieveCurrency, 'Update':updateCurrency, 'Blank':Decimal, 'Init':addVfpCurrency,
                    'Class':Decimal, 'Empty':none, 'flags':('null', ),
                    },
            'B' : {
                    'Type':'Double', 'Retrieve':retrieveDouble, 'Update':updateDouble, 'Blank':float, 'Init':addVfpDouble,
                    'Class':float, 'Empty':none, 'flags':('null', ),
                    },
            'F' : {
                    'Type':'Float', 'Retrieve':retrieveNumeric, 'Update':updateNumeric, 'Blank':float, 'Init':addVfpNumeric,
                    'Class':'default', 'Empty':none, 'flags':('null', ),
                    },
            'N' : {
                    'Type':'Numeric', 'Retrieve':retrieveNumeric, 'Update':updateNumeric, 'Blank':int, 'Init':addVfpNumeric,
                    'Class':'default', 'Empty':none, 'flags':('null', ),
                    },
            'I' : {
                    'Type':'Integer', 'Retrieve':retrieveInteger, 'Update':updateInteger, 'Blank':int, 'Init':addVfpInteger,
                    'Class':int, 'Empty':none, 'flags':('null', ),
                    },
            'L' : {
                    'Type':'Logical', 'Retrieve':retrieveLogical, 'Update':updateLogical, 'Blank':Logical, 'Init':addLogical,
                    'Class':bool, 'Empty':none, 'flags':('null', ),
                    },
            'D' : {
                    'Type':'Date', 'Retrieve':retrieveDate, 'Update':updateDate, 'Blank':Date, 'Init':addDate,
                    'Class':datetime.date, 'Empty':none, 'flags':('null', ),
                    },
            'T' : {
                    'Type':'DateTime', 'Retrieve':retrieveVfpDateTime, 'Update':updateVfpDateTime, 'Blank':DateTime, 'Init':addVfpDateTime,
                    'Class':datetime.datetime, 'Empty':none, 'flags':('null', ),
                    },
            'M' : {
                    'Type':'Memo', 'Retrieve':retrieveVfpMemo, 'Update':updateVfpMemo, 'Blank':str, 'Init':addVfpMemo,
                    'Class':unicode, 'Empty':unicode, 'flags':('binary','nocptrans','null', ), 
                    },
            'G' : {
                    'Type':'General', 'Retrieve':retrieveVfpMemo, 'Update':updateVfpMemo, 'Blank':str, 'Init':addVfpMemo,
                    'Class':bytes, 'Empty':bytes, 'flags':('null', ),
                    },
            'P' : {
                    'Type':'Picture', 'Retrieve':retrieveVfpMemo, 'Update':updateVfpMemo, 'Blank':str, 'Init':addVfpMemo,
                    'Class':bytes, 'Empty':bytes, 'flags':('null', ),
                    },
            '0' : {
                    'Type':'_NullFlags', 'Retrieve':unsupportedType, 'Update':unsupportedType, 'Blank':int, 'Init':int,
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
    _dbfTableHeader[8:10] = array('c', packShortInt(33+263))
    _dbfTableHeader[10] = '\x01'        # record length -- one for delete flag
    _dbfTableHeader[29] = '\x03'        # code page -- 437 US-MS DOS
    _dbfTableHeader = _dbfTableHeader.tostring()
    _dbfTableHeaderExtra = '\x00' * 263
    _use_deleted = True
    def _initializeFields(yo):
        "builds the FieldList of names, types, and descriptions"
        old_fields = defaultdict(dict)
        for name in yo._meta.fields:
            old_fields[name]['type'] = yo._meta[name][TYPE]
            old_fields[name]['class'] = yo._meta[name][CLASS]
            old_fields[name]['empty'] = yo._meta[name][EMPTY]
        yo._meta.fields[:] = []
        offset = 1
        fieldsdef = yo._meta.header.fields
        yo._meta.nullflags = None
        for i in range(yo._meta.header.field_count):
            fieldblock = fieldsdef[i*32:(i+1)*32]
            name = unpackStr(fieldblock[:11])
            type = fieldblock[11]
            if not type in yo._meta.fieldtypes:
                raise DbfError("Unknown field type: %s" % type)
            start = unpackLongInt(fieldblock[12:16])
            length = ord(fieldblock[16])
            offset += length
            end = start + length
            decimals = ord(fieldblock[17])
            flags = ord(fieldblock[18])
            if name in yo._meta.fields:
                raise DbfError('Duplicate field name found: %s' % name)
            yo._meta.fields.append(name)
            if name in old_fields and old_fields[name]['type'] == type:
                cls = old_fields[name]['class']
                empty = old_fields[name]['empty']
            else:
                cls = yo._meta.fieldtypes[type]['Class']
                empty = yo._meta.fieldtypes[type]['Empty']
            yo._meta[name] = (
                    type,
                    start,
                    length,
                    end,
                    decimals,
                    flags,
                    cls,
                    empty,
                    )
        yo._meta.user_fields = [f for f in yo._meta.fields if not yo._meta[f][FLAGS] & SYSTEM]
        yo._meta.user_field_count = len(yo._meta.user_fields)
    @staticmethod
    def _packDate(date):
            "Returns a group of three bytes, in integer form, of the date"
            return "%c%c%c" % (date.year-2000, date.month, date.day)
    @staticmethod
    def _unpackDate(bytestr):
            "Returns a Date() of the packed three-byte date passed in"
            year, month, day = struct.unpack('<BBB', bytestr)
            year += 2000
            return Date(year, month, day)
                    
class List(object):
    "list of Dbf records, with set-like behavior"
    _desc = ''
    def __init__(yo, new_records=None, desc=None, key=None, field_names=None):
        yo.field_names = field_names
        yo._list = []
        yo._set = set()
        if key is not None:
            yo.key = key
            if key.__doc__ is None:
                key.__doc__ = 'unknown'
        key = yo.key
        yo._current = -1
        if isinstance(new_records, yo.__class__) and key is new_records.key:
                yo._list = new_records._list[:]
                yo._set = new_records._set.copy()
                yo._current = 0
        elif new_records is not None:
            for record in new_records:
                value = key(record)
                item = (record.record_table, record.record_number, value)
                if value not in yo._set:
                    yo._set.add(value)
                    yo._list.append(item)
            yo._current = 0
        if desc is not None:
            yo._desc = desc
    def __add__(yo, other):
        key = yo.key
        if isinstance(other, (DbfTable, list)):
            other = yo.__class__(other, key=key)
        if isinstance(other, yo.__class__):
            result = yo.__class__()
            result._set = yo._set.copy()
            result._list[:] = yo._list[:]
            result.key = yo.key
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
    def __contains__(yo, record):
        if not isinstance(record, _DbfRecord):
            raise ValueError("%r is not a record" % record)
        item = yo.key(record)
        if not isinstance(item, tuple):
            item = (item, )
        return item in yo._set and yo._get_record(*item) is record
    def __delitem__(yo, key):
        if isinstance(key, int):
            item = yo._list.pop[key]
            yo._set.remove(item[2])
        elif isinstance(key, slice):
            yo._set.difference_update([item[2] for item in yo._list[key]])
            yo._list.__delitem__(key)
        elif isinstance(key, _DbfRecord):
            index = yo.index(key)
            item = yo._list.pop[index]
            yo._set.remove(item[2])
        else:
            raise TypeError
    def __getitem__(yo, key):
        if isinstance(key, int):
            count = len(yo._list)
            if not -count <= key < count:
                raise IndexError("Record %d is not in list." % key)
            return yo._get_record(*yo._list[key])
        elif isinstance(key, slice):
            result = yo.__class__()
            result._list[:] = yo._list[key]
            result._set = set(result._list)
            result.key = yo.key
            result._current = (-1, 0)[bool(result)]
            return result
        elif isinstance(key, _DbfRecord):
            index = yo.index(key)
            return yo._get_record(*yo._list[index])
        else:
            raise TypeError('indices must be integers')
    def __iter__(yo):
        return (table.get_record(recno) for table, recno, value in yo._list)
    def __len__(yo):
        return len(yo._list)
    def __nonzero__(yo):
        return len(yo) > 0
    def __radd__(yo, other):
        return yo.__add__(other)
    def __repr__(yo):
        if yo._desc:
            return "%s(key=%s - %s - %d records)" % (yo.__class__, yo.key.__doc__, yo._desc, len(yo._list))
        else:
            return "%s(key=%s - %d records)" % (yo.__class__, yo.key.__doc__, len(yo._list))
    def __rsub__(yo, other):
        key = yo.key
        if isinstance(other, (DbfTable, list)):
            other = yo.__class__(other, key=key)
        if isinstance(other, yo.__class__):
            result = yo.__class__()
            result._list[:] = other._list[:]
            result._set = other._set.copy()
            result.key = key
            lost = set()
            if key is other.key:
                for item in yo._list:
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
    def __sub__(yo, other):
        key = yo.key
        if isinstance(other, (DbfTable, list)):
            other = yo.__class__(other, key=key)
        if isinstance(other, yo.__class__):
            result = yo.__class__()
            result._list[:] = yo._list[:]
            result._set = yo._set.copy()
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
    def _maybe_add(yo, item):
        if item[2] not in yo._set:
            yo._set.add(item[2])
            yo._list.append(item)
    def _get_record(yo, table=None, rec_no=None, value=None):
        if table is rec_no is None:
            table, rec_no, value = yo._list[yo._current]
        return table.get_record(rec_no)
    def _purge(yo, record, old_record_number, offset):
        partial = record.record_table, old_record_number
        records = sorted(yo._list, key=lambda item: (item[0], item[1]))
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
            yo._list.pop(yo._list.index(item))
            yo._set.remove(item[2])
        start = records.index(item) + found
        for item in records[start:]:
            if item[0] is not partial[0]:       # into other table's records
                break
            i = yo._list.index(item)
            yo._set.remove(item[2])
            item = item[0], (item[1] - offset), item[2]
            yo._list[i] = item
            yo._set.add(item[2])
        return found
    def append(yo, new_record):
        yo._maybe_add((new_record.record_table, new_record.record_number, yo.key(new_record)))
        if yo._current == -1 and yo._list:
            yo._current = 0
    def bottom(yo):
        if yo._list:
            yo._current = len(yo._list) - 1
            return yo._get_record()
        raise DbfError("dbf.List is empty")
    def clear(yo):
        yo._list = []
        yo._set = set()
        yo._current = -1
    def current(yo):
        if yo._current < 0:
            raise Bof()
        elif yo._current == len(yo._list):
            raise Eof()
        return yo._get_record()
    def extend(yo, new_records):
        key = yo.key
        if isinstance(new_records, yo.__class__):
            if key is new_records.key:   # same key?  just compare key values
                for item in new_records._list:
                    yo._maybe_add(item)
            else:                   # different keys, use this list's key on other's records
                for rec in new_records:
                    value = key(rec)
                    yo._maybe_add((rec.record_table, rec.record_number, value))
        else:
            for rec in new_records:
                value = key(rec)
                yo._maybe_add((rec.record_table, rec.record_number, value))
        if yo._current == -1 and yo._list:
            yo._current = 0
    def goto(yo, index_number):
        if yo._list:
            if 0 <= index_number <= len(yo._list):
                yo._current = index_number
                return yo._get_record()
            raise DbfError("index %d not in dbf.List of %d records" % (index_number, len(yo._list)))
        raise DbfError("dbf.List is empty")

    def index(yo, record, start=None, stop=None):
        item = record.record_table, record.record_number, yo.key(record)
        key = yo.key(record)
        if start is None:
            start = 0
        if stop is None:
            stop = len(yo._list)
        for i in range(start, stop):
            if yo._list[i][2] == key:
                return i
        else:
            raise ValueError("dbf.List.index(x): <x=%r> not in list" % (key,))
    def insert(yo, i, record):
        item = record.record_table, record.record_number, yo.key(record)
        if item not in yo._set:
            yo._set.add(item[2])
            yo._list.insert(i, item)
    def key(yo, record):
        "table_name, record_number"
        return record.record_table, record.record_number
    def next(yo):
        if yo._current < len(yo._list):
            yo._current += 1
            if yo._current < len(yo._list):
                return yo._get_record()
        raise Eof()
    def pop(yo, index=None):
        if index is None:
            table, recno, value = yo._list.pop()
        else:
            table, recno, value = yo._list.pop(index)
        yo._set.remove(value)
        return yo._get_record(table, recno, value)
    def prev(yo):
        if yo._current >= 0:
            yo._current -= 1
            if yo._current > -1:
                return yo._get_record()
        raise Bof()
    def remove(yo, record):
        item = record.record_table, record.record_number, yo.key(record)
        yo._list.remove(item)
        yo._set.remove(item[2])
    def reverse(yo):
        return yo._list.reverse()
    def top(yo):
        if yo._list:
            yo._current = 0
            return yo._get_record()
        raise DbfError("dbf.List is empty")
    def sort(yo, key=None, reverse=False):
        if key is None:
            return yo._list.sort(reverse=reverse)
        return yo._list.sort(key=lambda item: key(item[0].get_record(item[1])), reverse=reverse)

class Index(object):
    class IndexIterator(object):
        "returns records using this index"
        def __init__(yo, table, records):
            yo.table = table
            yo.records = records[:]
            yo.index = 0
        def __iter__(yo):
            return yo
        def next(yo):
            while yo.index < len(yo.records):
                record = yo.table.get_record(yo.records[yo.index])
                yo.index += 1
                if not yo.table.use_deleted and record.has_been_deleted:
                    continue
                return record
            else:
                raise StopIteration
    def __init__(yo, table, key, field_names=None):
        yo._table = table
        yo._values = []             # ordered list of values
        yo._rec_by_val = []         # matching record numbers
        yo._records = {}            # record numbers:values
        yo.__doc__ = key.__doc__ or 'unknown'
        yo.key = key
        yo.field_names = field_names or table.field_names
        for record in table:
            value = key(record)
            if value is DoNotIndex:
                continue
            rec_num = record.record_number
            if not isinstance(value, tuple):
                value = (value, )
            vindex = bisect_right(yo._values, value)
            yo._values.insert(vindex, value)
            yo._rec_by_val.insert(vindex, rec_num)
            yo._records[rec_num] = value
        table._indexen.add(yo)
    def __call__(yo, record):
        rec_num = record.record_number
        key = yo.key(record)
        if not isinstance(key, tuple):
            key = (key, )
        if rec_num in yo._records:
            if yo._records[rec_num] == key:
                return
            key = yo._records[rec_num]
            vindex = bisect_left(yo._values, key)
            yo._values.pop(vindex)
            yo._rec_by_val.pop(vindex)
        key = yo.key(record)
        if key is DoNotIndex:
            return
        if not isinstance(key, tuple):
            key = (key, )
        vindex = bisect_right(yo._values, key)
        yo._values.insert(vindex, key)
        yo._rec_by_val.insert(vindex, rec_num)
        yo._records[rec_num] = key
    def __contains__(yo, record):
        if not isinstance(record, _DbfRecord):
            raise ValueError("%r is not a record" % record)
        if record.record_table is yo._table:
            return record.record_number in yo._records
        raise ValueError("record is from table %r, not %r" % (record.table, yo._table))
    def __getitem__(yo, key):
        if isinstance(key, int):
            count = len(yo._values)
            if not -count <= key < count:
                raise IndexError("Record %d is not in list." % key)
            rec_num = yo._rec_by_val[key]
            return yo._table.get_record(rec_num)
        elif isinstance(key, slice):
            result = List(field_names=yo._table.field_names)
            yo._table._dbflists.add(result)
            start, stop, step = key.start, key.stop, key.step
            if start is None: start = 0
            if stop is None: stop = len(yo._rec_by_val)
            if step is None: step = 1
            if step < 0:
                start, stop = stop - 1, -(stop - start + 1)
            for loc in range(start, stop, step):
                record = yo._table.get_record(yo._rec_by_val[loc])
                result._maybe_add(item=(yo._table, yo._rec_by_val[loc], result.key(record)))
            result._current = (-1, 0)[bool(result)]
            return result
        elif isinstance (key, (str, unicode, tuple, _DbfRecord)):
            if isinstance(key, _DbfRecord):
                key = yo.key(key)
            elif not isinstance(key, tuple):
                key = (key, )
            loc = yo.find(key)
            if loc == -1:
                raise KeyError(key)
            return yo._table.get_record(yo._rec_by_val[loc])
        else:
            raise TypeError('indices must be integers, match objects must by strings or tuples')
    def __enter__(yo):
        return yo
    def __exit__(yo, *exc_info):
        yo._table.close()
        yo._values[:] = []
        yo._rec_by_val[:] = []
        yo._records.clear()
        return False
    def __iter__(yo):
        return yo.IndexIterator(yo._table, yo._rec_by_val)
    def __len__(yo):
        return len(yo._records)
    def _partial_match(yo, target, match):
        target = target[:len(match)]
        if isinstance(match[-1], (str, unicode)):
            target = list(target)
            target[-1] = target[-1][:len(match[-1])]
            target = tuple(target)
        return target == match
    def _purge(yo, rec_num):
        value = yo._records.get(rec_num)
        if value is not None:
            vindex = bisect_left(yo._values, value)
            del yo._records[rec_num]
            yo._values.pop(vindex)
            yo._rec_by_val.pop(vindex)
    def _search(yo, match, lo=0, hi=None):
        if hi is None:
            hi = len(yo._values)
        return bisect_left(yo._values, match, lo, hi)
    def clear(yo):
        "removes all entries from index"
        yo._values[:] = []
        yo._rec_by_val[:] = []
        yo._records.clear()
    def close(yo):
        yo._table.close()
    def find(yo, match, partial=False):
        "returns numeric index of (partial) match, or -1"
        if isinstance(match, _DbfRecord):
            if match.record_number in yo._records:
                return yo._values.index(yo._records[match.record_number])
            else:
                return -1
        if not isinstance(match, tuple):
            match = (match, )
        loc = yo._search(match)
        while loc < len(yo._values) and yo._values[loc] == match:
            if not yo._table.use_deleted and yo._table.get_record(yo._rec_by_val[loc]).has_been_deleted:
                loc += 1
                continue
            return loc
        if partial:
            while loc < len(yo._values) and yo._partial_match(yo._values[loc], match):
                if not yo._table.use_deleted and yo._table.get_record(yo._rec_by_val[loc]).has_been_deleted:
                    loc += 1
                    continue
                return loc
        return -1
    def find_index(yo, match):
        "returns numeric index of either (partial) match, or position of where match would be"
        if isinstance(match, _DbfRecord):
            if match.record_number in yo._records:
                return yo._values.index(yo._records[match.record_number])
            else:
                match = yo.key(match)
        if not isinstance(match, tuple):
            match = (match, )
        loc = yo._search(match)
        return loc
    def index(yo, match, partial=False):
        "returns numeric index of (partial) match, or raises IndexError"
        loc = yo.find(match, partial)
        if loc == -1:
            if isinstance(match, _DbfRecord):
                raise IndexError("table <%s> record [%d] not in index <%s>" % (yo._table.filename, match.record_number, yo.__doc__))
            else:
                raise IndexError("match criteria <%s> not in index" % (match, ))
        return loc
    def reindex(yo):
        "reindexes all records"
        for record in yo._table:
            yo(record)
    def query(yo, sql_command=None, python=None):
        """recognized sql commands are SELECT, UPDATE, REPLACE, INSERT, DELETE, and RECALL"""
        if sql_command:
            return sql(yo, sql_command)
        elif python is None:
            raise DbfError("query: python parameter must be specified")
        possible = List(desc="%s -->  %s" % (yo._table.filename, python), field_names=yo._table.field_names)
        yo._table._dbflists.add(possible)
        query_result = {}
        select = 'query_result["keep"] = %s' % python
        g = {}
        for record in yo:
            query_result['keep'] = False
            g['query_result'] = query_result
            exec select in g, record
            if query_result['keep']:
                possible.append(record)
            record.write_record()
        return possible
    def search(yo, match, partial=False):
        "returns dbf.List of all (partially) matching records"
        result = List(field_names=yo._table.field_names)
        yo._table._dbflists.add(result)
        if not isinstance(match, tuple):
            match = (match, )
        loc = yo._search(match)
        if loc == len(yo._values):
            return result
        while loc < len(yo._values) and yo._values[loc] == match:
            record = yo._table.get_record(yo._rec_by_val[loc])
            if not yo._table.use_deleted and record.has_been_deleted:
                loc += 1
                continue
            result._maybe_add(item=(yo._table, yo._rec_by_val[loc], result.key(record)))
            loc += 1
        if partial:
            while loc < len(yo._values) and yo._partial_match(yo._values[loc], match):
                record = yo._table.get_record(yo._rec_by_val[loc])
                if not yo._table.use_deleted and record.has_been_deleted:
                    loc += 1
                    continue
                result._maybe_add(item=(yo._table, yo._rec_by_val[loc], result.key(record)))
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
    def _fieldtypes():
        return {
            'C' : {'Type':'Character', 'Retrieve':retrieveCharacter, 'Update':updateCharacter, 'Blank':str, 'Init':addCharacter},
            'Y' : {'Type':'Currency', 'Retrieve':retrieveCurrency, 'Update':updateCurrency, 'Blank':Decimal, 'Init':addVfpCurrency},
            'B' : {'Type':'Double', 'Retrieve':retrieveDouble, 'Update':updateDouble, 'Blank':float, 'Init':addVfpDouble},
            'F' : {'Type':'Float', 'Retrieve':retrieveNumeric, 'Update':updateNumeric, 'Blank':float, 'Init':addVfpNumeric},
            'N' : {'Type':'Numeric', 'Retrieve':retrieveNumeric, 'Update':updateNumeric, 'Blank':int, 'Init':addVfpNumeric},
            'I' : {'Type':'Integer', 'Retrieve':retrieveInteger, 'Update':updateInteger, 'Blank':int, 'Init':addVfpInteger},
            'L' : {'Type':'Logical', 'Retrieve':retrieveLogical, 'Update':updateLogical, 'Blank':Logical, 'Init':addLogical},
            'D' : {'Type':'Date', 'Retrieve':retrieveDate, 'Update':updateDate, 'Blank':Date, 'Init':addDate},
            'T' : {'Type':'DateTime', 'Retrieve':retrieveVfpDateTime, 'Update':updateVfpDateTime, 'Blank':DateTime, 'Init':addVfpDateTime},
            'M' : {'Type':'Memo', 'Retrieve':retrieveMemo, 'Update':updateMemo, 'Blank':str, 'Init':addMemo},
            'G' : {'Type':'General', 'Retrieve':retrieveMemo, 'Update':updateMemo, 'Blank':str, 'Init':addMemo},
            'P' : {'Type':'Picture', 'Retrieve':retrieveMemo, 'Update':updateMemo, 'Blank':str, 'Init':addMemo},
            '0' : {'Type':'_NullFlags', 'Retrieve':unsupportedType, 'Update':unsupportedType, 'Blank':int, 'Init':None} }
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
    def _checkMemoIntegrity(yo):
        "dBase III specific"
        if yo._meta.header.version == '\x8b':
            try:
                yo._meta.memo = yo._memoClass(yo._meta)
            except:
                yo._meta.dfd.close()
                yo._meta.dfd = None
                raise
        if not yo._meta.ignorememos:
            for field in yo._meta.fields:
                if yo._meta[field][TYPE] in yo._memotypes:
                    if yo._meta.header.version != '\x8b':
                        yo._meta.dfd.close()
                        yo._meta.dfd = None
                        raise DbfError("Table structure corrupt:  memo fields exist, header declares no memos")
                    elif not os.path.exists(yo._meta.memoname):
                        yo._meta.dfd.close()
                        yo._meta.dfd = None
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

