"""
=========
Copyright
=========

    - Portions copyright: 2008-2012 Ad-Mail, Inc -- All rights reserved.
    - Portions copyright: 2012-2017 Ethan Furman -- All rights reserved.
    - Author: Ethan Furman
    - Contact: ethan@stoneleaf.us

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

THIS SOFTWARE IS PROVIDED ''AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
version = 0, 99, 10, 1
# Python 2 code may need to change these
default_codepage = None     # will be set by tables module (defaults to ascii)
default_type = 'db3'        # lowest common denominator
input_decoding = 'ascii'

# make dbf module importabl internally (i.e. from . import dbf)
import sys as _sys
dbf = _sys.modules[__package__]

## user-defined pql functions  (pql == primitive query language)
# it is not real sql and won't be for a long time (if ever)
pql_user_functions = dict()

## signature:_meta of template records
_Template_Records = dict()

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
        _sys.modules["%s.%s" % (__name__, self.name)] = self

from .bridge import Decimal
from .exceptions import DbfError, DataOverflowError, BadDataError
from .exceptions import FieldMissingError, FieldSpecError, NonUnicodeError
from .exceptions import NotFoundError, DbfWarning, Eof, Bof, DoNotIndex
from .exceptions import FieldNameWarning

from .constants import CLOSED, READ_ONLY, READ_WRITE, IN_MEMORY, ON_DISK

from .utils import create_template, delete, field_names, is_deleted, recno
from .utils import reset, source_table, undelete, write, Process, Templates
from .utils import gather, scatter, scan, ensure_unicode, table_type
from .utils import add_fields, delete_fields, export, from_csv

from .data_types import Char, Date, DateTime, Time, Logical, Quantum, Null
from .data_types import NullDate, NullDateTime, NullTime, NullType, NoneType
from .data_types import Vapor, Period, On, Off, Other, Truth, Falsth, Unknown
from .pql import pqlc

from .tables import Table, Record, List, Index, Relation, Iter, IndexLocation
from .tables import CodePage, FieldnameList, RecordTemplate
from .tables import Db3Table, ClpTable, FpTable, VfpTable
from .tables import RecordVaporWare

api = fake_module('api',
    'Table', 'Record', 'List', 'Index', 'Relation', 'Iter', 'Null', 'Char', 'Date', 'DateTime', 'Time',
    'Logical', 'Quantum', 'CodePage', 'create_template', 'delete', 'field_names', 'gather', 'is_deleted',
    'recno', 'source_table', 'reset', 'scatter', 'scan', 'undelete', 'write', 'export', 'pqlc', 'from_csv',
    'NullDate', 'NullDateTime', 'NullTime', 'NoneType', 'NullType', 'Decimal', 'Vapor', 'Period',
    'Truth', 'Falsth', 'Unknown', 'On', 'Off', 'Other', 'table_type',
    'DbfError', 'DataOverflowError', 'BadDataError', 'FieldMissingError',
    'FieldSpecError', 'NonUnicodeError', 'NotFoundError',
    'DbfWarning', 'Eof', 'Bof', 'DoNotIndex', 'FieldNameWarning', 'IndexLocation',
    'Process', 'Templates', 'CLOSED', 'READ_ONLY', 'READ_WRITE',
    )

api.register()


__all__ = (
        'Decimal', 'DbfError', 'DataOverflowError', 'BadDataError', 
        'FieldMissingError', 'FieldSpecError', 'NonUnicodeError', 
        'NotFoundError', 'DbfWarning', 'Eof', 'Bof', 'DoNotIndex', 
        'FieldNameWarning', 
        'CLOSED', 'READ_ONLY', 'READ_WRITE', 'IN_MEMORY', 'ON_DISK', 
        'create_template', 'delete', 'field_names', 'is_deleted', 'recno', 
        'reset', 'source_table', 'undelete', 'write', 'Process', 'Templates', 
        'gather', 'scatter', 'scan', 'ensure_unicode', 'table_type', 
        'add_fields', 'delete_fields', 'export', 'from_csv', 
        'Char', 'Date', 'DateTime', 'Time', 'Logical', 'Quantum', 'Null', 
        'NullDate', 'NullDateTime', 'NullTime', 'NullType', 'NoneType', 
        'Vapor', 'Period', 'On', 'Off', 'Other', 'Truth', 'Falsth', 'Unknown', 
        'pqlc', 
        'Table', 'Record', 'List', 'Index', 'Relation', 'Iter', 'IndexLocation', 
        'CodePage', 'FieldnameList', 'RecordTemplate', 
        'Db3Table', 'ClpTable', 'FpTable', 'VfpTable', 
        'RecordVaporWare', 
        )
