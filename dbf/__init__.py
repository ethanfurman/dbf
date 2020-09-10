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

from .bridge import Decimal
from .constant import CLOSED, READ_ONLY, READ_WRITE, IN_MEMORY, ON_DISK 
from .data_types import Char, Date, DateTime, Time, Logical, Quantum, Null
from .data_types import NullDate, NullDateTime, NullTime, NullType, NoneType
from .data_types import Vapor, Period, On, Off, Other, Truth, Falsth, Unknown
from .exceptions import DbfError, DataOverflowError, BadDataError
from .exceptions import FieldMissingError, FieldSpecError, NonUnicodeError
from .exceptions import NotFoundError, DbfWarning, Eof, Bof, DoNotIndex
from .exceptions import FieldNameWarning
from .tables import Table, Record, List, Index, Relation, Iter, IndexLocation
from .tables import CodePage
from .utils import create_template, delete, field_names, is_deleted, recno
from .utils import reset, source_table, undelete, write, Process, Templates
from .utils import gather, scatter, scan

version = 0, 99, 1, 1

module = globals()

## user-defined pql functions  (pql == primitive query language)
## it is not real sql and won't be for a long time (if ever)
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
        sys.modules["%s.%s" % (__name__, self.name)] = self

api = fake_module('api',
    'Table', 'Record', 'List', 'Index', 'Relation', 'Iter', 'Null', 'Char', 'Date', 'DateTime', 'Time',
    'Logical', 'Quantum', 'CodePage', 'create_template', 'delete', 'field_names', 'gather', 'is_deleted',
    'recno', 'source_table', 'reset', 'scatter', 'scan', 'undelete', 'write',
    'NullDate', 'NullDateTime', 'NullTime', 'NoneType', 'NullType', 'Decimal', 'Vapor', 'Period',
    'Truth', 'Falsth', 'Unknown', 'On', 'Off', 'Other',
    'DbfError', 'DataOverflowError', 'BadDataError', 'FieldMissingError',
    'FieldSpecError', 'NonUnicodeError', 'NotFoundError',
    'DbfWarning', 'Eof', 'Bof', 'DoNotIndex', 'FieldNameWarning', 'IndexLocation',
    'Process', 'Templates', 'CLOSED', 'READ_ONLY', 'READ_WRITE',
    )

api.register()
