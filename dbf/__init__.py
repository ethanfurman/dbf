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
from __future__ import with_statement, print_function

import codecs
import collections
import csv
import datetime
import decimal
import os
import struct
import sys
import time
import traceback
import warnings
import weakref

from array import array
from bisect import bisect_left, bisect_right
from collections import defaultdict, deque
from functools import partial
from aenum import Enum, IntEnum, IntFlag, export
from glob import glob
from math import floor
from os import SEEK_END
from textwrap import dedent

version = 0, 99, 1, 1

NoneType = type(None)

module = globals()

# configuration flags

## Flag for behavior if bad data is encountered in a logical field
## Return None if True, else raise BadDataError
LOGICAL_BAD_IS_NONE = True

## treat non-unicode data as ...
input_decoding = 'utf-8'

## if no codepage specified on dbf creation, use this
default_codepage = 'ascii'

## default format if none specified
default_type = 'db3'

temp_dir = os.environ.get("DBF_TEMP") or os.environ.get("TMP") or os.environ.get("TEMP") or ""

## user-defined pql functions  (pql == primitive query language)
## it is not real sql and won't be for a long time (if ever)
pql_user_functions = dict()

## signature:_meta of template records
_Template_Records = dict()

## dec jan feb mar apr may jun jul aug sep oct nov dec jan
days_per_month = [31, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 31]
days_per_leap_month = [31, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 31]

## keep pyflakes happy :(
SYSTEM = NULLABLE = BINARY = NOCPTRANS = None
SPACE = ASTERISK = TYPE = CR = NULL = None
START = LENGTH = END = DECIMALS = FLAGS = CLASS = EMPTY = NUL = None
IN_MEMORY = ON_DISK = CLOSED = READ_ONLY = READ_WRITE = None
_NULLFLAG = CHAR = CURRENCY = DATE = DATETIME = DOUBLE = FLOAT = None
GENERAL = INTEGER = LOGICAL = MEMO = NUMERIC = PICTURE = None



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
    'recno', 'source_table', 'reset', 'scatter', 'undelete',
    'NullDate', 'NullDateTime', 'NullTime', 'NoneType', 'NullType', 'Decimal', 'Vapor', 'Period',
    'Truth', 'Falsth', 'Unknown', 'On', 'Off', 'Other',
    'DbfError', 'DataOverflowError', 'BadDataError', 'FieldMissingError',
    'FieldSpecError', 'NonUnicodeError', 'NotFoundError',
    'DbfWarning', 'Eof', 'Bof', 'DoNotIndex', 'IndexLocation',
    'Process', 'Templates', 'CLOSED', 'READ_ONLY', 'READ_WRITE',
    )

api.register()
