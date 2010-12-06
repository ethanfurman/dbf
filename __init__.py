"""
Copyright
=========
    - Copyright: 2008-2009 Ad-Mail, Inc -- All rights reserved.
    - Author: Ethan Furman
    - Contact: ethanf@admailinc.com
    - Organization: Ad-Mail, Inc.
    - Version: 0.87.003 as of 03 Dec 2009

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

B{I{Summary}}

Python package for reading/writing dBase III and VFP 6 tables and memos

The entire table is read into memory, and all operations occur on the in-memory
table, with data changes being written to disk as they occur.

Goals:  programming style with databases
    - C{table = dbf.table('table name' [, fielddesc[, fielddesc[, ....]]])}
        - fielddesc examples:  C{name C(30); age N(3,0); wisdom M; marriage D}
    - C{record = [ table.current() | table[int] | table.append() | table.[next|prev|top|bottom|goto]() ]}
    - C{record.field | record['field']} accesses the field

NOTE:  Of the VFP data types, auto-increment and null settings are not implemented.
"""
import os
import csv

from dbf.dates import Date, DateTime, Time
from dbf.exceptions import DbfWarning, Bof, Eof, DbfError, DataOverflow, FieldMissing, DoNotIndex
from dbf.tables import DbfTable, Db3Table, VfpTable, FpTable, List, DbfCsv
from dbf.tables import sql, ascii, codepage, encoding, version_map

version = (0, 88, 16)

default_type = 'db3'    # default format if none specified
sql_user_functions = {}      # user-defined sql functions

__docformat__ = 'epytext'

def Table(filename, field_specs='', memo_size=128, ignore_memos=False, \
          read_only=False, keep_memos=False, meta_only=False, dbf_type=None, codepage=None):
    "returns an open table of the correct dbf_type, or creates it if field_specs is given"
    #- print "dbf.Table(%s)" % ', '.join(['%r' % arg for arg in (filename, field_specs, dbf_type, codepage)])
    if field_specs and dbf_type is None:
        dbf_type = default_type
    if dbf_type is not None:
        dbf_type = dbf_type.lower()
        if dbf_type == 'db3':
            return Db3Table(filename, field_specs, memo_size, ignore_memos, read_only, keep_memos, meta_only, codepage)
        elif dbf_type == 'fp':
            return FpTable(filename, field_specs, memo_size, ignore_memos, read_only, keep_memos, meta_only, codepage)
        elif dbf_type == 'vfp':
            return VfpTable(filename, field_specs, memo_size, ignore_memos, read_only, keep_memos, meta_only, codepage)
        elif dbf_type == 'dbf':
            return DbfTable(filename, field_specs, memo_size, ignore_memos, read_only, keep_memos, meta_only, codepage)
        else:
            raise DbfError("Unknown table type: %s" % dbf_type)
    else:
        possibles = guess_table_type(filename)
        if len(possibles) == 1:
            return possibles[0][2](filename, field_specs, memo_size, ignore_memos, \
                                 read_only, keep_memos, meta_only)
        else:
            for type, desc, cls in possibles:
                if type == default_type:
                    return cls(filename, field_specs, memo_size, ignore_memos, \
                                 read_only, keep_memos, meta_only)
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
            possibles.append((tabletype._versionabbv, tabletype._version, tabletype))
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

def add_fields(table, field_specs):
    "adds fields to an existing table"
    table = Table(table)
    try:
        table.add_fields(field_specs)
    finally:
        table.close()
def delete_fields(table, field_names):
    "deletes fields from an existing table"
    table = Table(table)
    try:
        table.delete_fields(field_names)
    finally:
        table.close()
def export(table, filename='', fields='', format='csv', header=True):
    "creates a csv or tab-delimited file from an existing table"
    if fields is None:
        fields = []
    table = Table(table)
    try:
        table.export(filename=filename, field_specs=fields, format=format, header=header)
    finally:
        table.close()
def first_record(table):
    "prints the first record of a table"
    table = Table(table)
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
def get_fields(table):
    "returns the list of field names of a table"
    table = Table(table)
    return table.field_names
def info(table):
    "prints table info"
    table = Table(table)
    print str(table)
def rename_field(table, oldfield, newfield):
    "renames a field in a table"
    table = Table(table)
    try:
        table.rename_field(oldfield, newfield)
    finally:
        table.close()
def structure(table, field=None):
    "returns the definition of a field (or all fields)"
    table = Table(table)
    return table.structure(field)
def hex_dump(records):
    "just what it says ;)"
    for index,dummy in enumerate(records):
        chars = dummy._data
        print "%2d: " % index,
        for char in chars[1:]:
            print " %2x " % ord(char),
        print

