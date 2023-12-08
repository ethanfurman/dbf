from __future__ import print_function

from glob import glob

import codecs
import collections
import csv
import os

from . import dbf
from .constants import *

# utility functions

def add_fields(table_name, field_specs):
    """
    adds fields to an existing table
    """
    table = dbf.Table(table_name)
    table.open(dbf.READ_WRITE)
    try:
        table.add_fields(field_specs)
    finally:
        table.close()

def create_template(table_or_record, defaults=None):
    if isinstance(table_or_record, dbf.Table):
        return dbf.RecordTemplate(table_or_record._meta, defaults)
    else:
        return dbf.RecordTemplate(table_or_record._meta, table_or_record, defaults)

def delete(record):
    """
    marks record as deleted
    """
    template = isinstance(record, dbf.RecordTemplate)
    if not template and record._meta.status == CLOSED:
        raise DbfError("%s is closed; cannot delete record" % record._meta.filename)
    record_in_flux = not record._write_to_disk
    if not template and not record_in_flux:
        record._start_flux()
    try:
        record._data[0] = ASTERISK
        if not template:
            record._dirty = True
    except:
        if not template and not record_in_flux:
            record._rollback_flux()
        raise
    if not template and not record_in_flux:
        record._commit_flux()

def delete_fields(table_name, field_names):
    """
    deletes fields from an existing table
    """
    table = dbf.Table(table_name)
    table.open(dbf.READ_WRITE)
    try:
        table.delete_fields(field_names)
    finally:
        table.close()

def ensure_unicode(value):
    if isinstance(value, bytes):
        if dbf.input_decoding is None:
            raise dbf.DbfError('value must be unicode, not bytes (or set input_decoding)')
        value = value.decode(dbf.input_decoding)
    return value

def export(table_or_records, filename=None, field_names=None, format='csv', header=True, dialect='dbf', encoding=None, ignore_errors=False, strip_nulls=False):
    """
    writes the records using CSV or tab-delimited format, using the filename
    given if specified, otherwise the table name
    if table_or_records is a collection of records (not an actual table) they
    should all be of the same format
    """
    table = source_table(table_or_records[0])
    if filename is None:
        filename = table.filename
    if field_names is None:
        field_names = table.field_names
    if isinstance(field_names, basestring):
        field_names = [f.strip() for f in field_names.split(',')]
    format = format.lower()
    if format not in ('csv', 'tab', 'fixed'):
        raise dbf.DbfError("export format: csv, tab, or fixed -- not %s" % format)
    if format == 'fixed':
        format = 'txt'
    if encoding is None:
        encoding = table._meta.codepage
    header_names = field_names
    base, ext = os.path.splitext(filename)
    if ext.lower() in ('', '.dbf'):
        filename = base + "." + format
    with codecs.open(filename, 'w', encoding=encoding) as fd:
        if format == 'csv':
            if header is True:
                fd.write(','.join(header_names))
                fd.write('\n')
            elif header:
                fd.write(','.join(header))
                fd.write('\n')
            for record in table_or_records:
                fields = []
                for fieldname in field_names:
                    try:
                        data = record[fieldname]
                    except Exception:
                        if not ignore_errors:
                            raise
                        data = None
                    if isinstance(data, basestring) and data:
                        data = '"%s"' % data.replace('"','""')
                    elif data is None:
                        data = ''
                    data = unicode(data)
                    if strip_nulls:
                        data = data.replace(NULL, '')
                    fields.append(data)
                fd.write(','.join(fields))
                fd.write('\n')
        elif format == 'tab':
            if header is True:
                fd.write('\t'.join(header_names) + '\n')
            elif header:
                fd.write(','.join(header))
            for record in table_or_records:
                fields = []
                for fieldname in field_names:
                    try:
                        data = record[fieldname]
                    except Exception:
                        if not ignore_errors:
                            raise
                        data = None
                    data = unicode(data)
                    if strip_nulls:
                        data = data.replace(NULL, '')
                    fields.append(data)
                fd.write('\t'.join(fields) + '\n')
        else: # format == 'fixed'
            if header is True:
                header = False  # don't need it
            elif header:
                # names to use as field names
                header = list(header)   # in case header is an iterator
            with codecs.open("%s_layout.txt" % os.path.splitext(filename)[0], 'w', encoding=encoding) as layout:
                layout.write("%-15s  Size  Comment\n" % "Field Name")
                layout.write("%-15s  ----  -------------------------\n" % ("-" * 15))
                sizes = []
                for i, field in enumerate(field_names):
                    info = table.field_info(field)
                    if info.field_type == ord('D'):
                        size = 10
                    elif info.field_type in (ord('T'), ord('@')):
                        size = 19
                    else:
                        size = info.length
                    sizes.append(size)
                    comment = ''
                    if header and i < len(header):
                        # use overridden field name as comment
                        comment = header[i]
                    layout.write("%-15s  %4d  %s\n" % (field, size, comment))
                layout.write('\nTotal Records in file: %d\n' % len(table_or_records))
            for record in table_or_records:
                fields = []
                for i, fieldname in enumerate(field_names):
                    try:
                        data = record[fieldname]
                    except Exception:
                        if not ignore_errors:
                            raise
                        data = None
                    data = unicode(data)
                    if strip_nulls:
                        data = data.replace(NULL, '')
                    fields.append("%-*s" % (sizes[i], data))
                fd.write(''.join(fields) + '\n')
    return len(table_or_records)

def field_names(thing):
    """
    fields in table/record, keys in dict

    if dict and dict keys are not unicode, returned keys will also
    not be unicode; either way, they will not be upper-cased
    """
    if isinstance(thing, dict):
        return list(thing.keys())
    elif isinstance(thing, (dbf.Table, dbf.Record, dbf.RecordTemplate)):
        return thing._meta.user_fields[:]
    elif isinstance(thing, dbf.Index):
        return thing._table._meta.user_fields[:]
    else:
        for record in thing:    # grab any record
            return record._meta.user_fields[:]

def first_record(table_name):
    """
    prints the first record of a table
    """
    table = dbf.Table(table_name)
    table.open()
    try:
        print(unicode(table[0]))
    finally:
        table.close()

def from_csv(csvfile, to_disk=False, filename=None, field_names=None, extra_fields=None,
        dbf_type='db3', memo_size=64, min_field_size=1,
        encoding=None, errors=None):
    """
    creates a Character table from a csv file
    to_disk will create a table with the same name
    filename will be used if provided
    field_names default to f0, f1, f2, etc, unless specified (list)
    extra_fields can be used to add additional fields -- should be normal field specifiers (list)
    """
    with codecs.open(csvfile, 'r', encoding='latin-1', errors=errors) as fd:
        reader = csv.reader(fd)
        if field_names:
            if isinstance(field_names, basestring):
                field_names = field_names.split()
            if ' ' not in field_names[0]:
                field_names = ['%s M' % fn for fn in field_names]
        else:
            field_names = ['f0 M']
        if filename:
            to_disk = True
        else:
            filename = os.path.splitext(csvfile)[0]
        if to_disk:
            csv_table = dbf.Table(filename, [field_names[0]], dbf_type=dbf_type, memo_size=memo_size, codepage=encoding)
        else:
            csv_table = dbf.Table(':memory:', [field_names[0]], dbf_type=dbf_type, memo_size=memo_size, codepage=encoding, on_disk=False)
        csv_table.open(dbf.READ_WRITE)
        fields_so_far = 1
        while reader:
            try:
                row = next(reader)
            except UnicodeEncodeError:
                row = ['']
            except StopIteration:
                break
            while fields_so_far < len(row):
                if fields_so_far == len(field_names):
                    field_names.append('f%d M' % fields_so_far)
                csv_table.add_fields(field_names[fields_so_far])
                fields_so_far += 1
            csv_table.append(tuple(row))
        if extra_fields:
            csv_table.add_fields(extra_fields)
        csv_table.close()
        return csv_table

def guess_table_type(filename):
    reported = table_type(filename)
    possibles = []
    version = reported[0]
    for tabletype in (dbf.Db3Table, dbf.ClpTable, dbf.FpTable, dbf.VfpTable):
        if version in tabletype._supported_tables:
            possibles.append((tabletype._versionabbr, tabletype._version, tabletype))
    if not possibles:
        raise dbf.DbfError("Tables of type %s not supported" % unicode(reported))
    return possibles

def get_fields(table_name):
    """
    returns the list of field names of a table
    """
    table = dbf.Table(table_name)
    return table.field_names

def hex_dump(records):
    """
    just what it says ;)
    """
    for index, dummy in enumerate(records):
        chars = dummy._data
        print("%2d: " % (index,))
        for char in chars[1:]:
            print(" %2x " % (char,))
        print()


def index(sequence):
    """
    returns integers 0 - len(sequence)
    """
    for i in xrange(len(sequence)):
        yield i

def info(table_name):
    """
    prints table info
    """
    table = dbf.Table(table_name)
    print(unicode(table))

def is_deleted(record):
    """
    marked for deletion?
    """
    return record._data[0] == ASTERISK

def is_leapyear(year):
    if year % 400 == 0:
        return True
    elif year % 100 == 0:
        return False
    elif year % 4 == 0:
        return True
    else:
        return False



def recno(record):
    """
    physical record number
    """
    return record._recnum

def rename_field(table_name, oldfield, newfield):
    """
    renames a field in a table
    """
    table = dbf.Table(table_name)
    try:
        table.rename_field(oldfield, newfield)
    finally:
        table.close()

def reset(record, keep_fields=None):
    """
    sets record's fields back to blank values, except for fields in keep_fields
    """
    template = record_in_flux = False
    if isinstance(record, dbf.RecordTemplate):
        template = True
    else:
        record_in_flux = not record._write_to_disk
        if record._meta.status == CLOSED:
            raise dbf.DbfError("%s is closed; cannot modify record" % record._meta.filename)
    if keep_fields is None:
        keep_fields = []
    keep = {}
    for field in keep_fields:
        keep[field] = record[field]
    record._data[:] = record._meta.blankrecord[:]
    for field in keep_fields:
        record[field] = keep[field]
    if not template:
        if record_in_flux:
            record._dirty = True
        else:
            record._write()

def source_table(thingie):
    """
    table associated with table | record | index
    """
    table = thingie._meta.table()
    if table is None:
        raise dbf.DbfError("table is no longer available")
    return table

def string(text):
    if isinstance(text, unicode):
        return text
    elif isinstance(text, bytes):
        return text.decode(dbf.input_decoding)

def structure(table_name, field=None):
    """
    returns the definition of a field (or all fields)
    """
    table = dbf.Table(table_name)
    return table.structure(field)

def table_type(filename):
    """
    returns text representation of a table's dbf version
    """
    actual_filename = None
    search_name = None
    base, ext = os.path.splitext(filename)
    if ext == '.':
        # use filename without the '.'
        search_name = base
        matches = glob(search_name)
    elif ext.lower() == '.dbf':
        # use filename as-is
        search_name = filename
        matches = glob(search_name)
    else:
        search_name = base + '.[Dd][Bb][Ff]'
        matches = glob(search_name)
        if not matches:
            # back to original name
            search_name = filename
            matches = glob(search_name)
    if len(matches) == 1:
        actual_filename = matches[0]
    elif matches:
        raise dbf.DbfError("please specify exactly which of %r you want" % (matches, ))
    else:
        raise dbf.DbfError('File %r not found' % search_name)
    fd = open(actual_filename, 'rb')
    version = ord(fd.read(1))
    fd.close()
    fd = None
    if not version in dbf.tables.version_map:
        raise dbf.DbfError("Unknown dbf type: %s (%x)" % (version, version))
    return version, dbf.tables.version_map[version]

def undelete(record):
    """
    marks record as active
    """
    template = isinstance(record, dbf.RecordTemplate)
    if not template and record._meta.status == CLOSED:
        raise dbf.DbfError("%s is closed; cannot undelete record" % record._meta.filename)
    record_in_flux = not record._write_to_disk
    if not template and not record_in_flux:
        record._start_flux()
    try:
        record._data[0] = SPACE
        if not template:
            record._dirty = True
    except:
        if not template and not record_in_flux:
            record._rollback_flux()
        raise
    if not template and not record_in_flux:
        record._commit_flux()
def write(record, **kwargs):
    """
    write record data to disk (updates indices)
    """
    if record._meta.status == CLOSED:
        raise dbf.DbfError("%s is closed; cannot update record" % record._meta.filename)
    elif not record._write_to_disk:
        raise dbf.DbfError("unable to use .write_record() while record is in flux")
    if kwargs:
        gather(record, kwargs)
    if record._dirty:
        record._write()

def Process(records, start=0, stop=None, filter=None):
    """commits each record to disk before returning the next one; undoes all changes to that record if exception raised
    if records is a table, it will be opened and closed if necessary
    filter function should return True to skip record, False to keep"""
    already_open = True
    if isinstance(records, dbf.Table):
        already_open = records.status != CLOSED
        if not already_open:
            records.open(dbf.READ_WRITE)
    try:
        if stop is None:
            stop = len(records)
        for record in records[start:stop]:
            if filter is not None and filter(record):
                continue
            try:
                record._start_flux()
                yield record
            except:
                record._rollback_flux()
                raise
            else:
                record._commit_flux()
    finally:
        if not already_open:
            records.close()

def Templates(records, start=0, stop=None, filter=None):
    """
    returns a template of each record instead of the record itself
    if records is a table, it will be opened and closed if necessary
    """
    already_open = True
    if isinstance(records, dbf.Table):
        already_open = records.status != CLOSED
        if not already_open:
            records.open()
    try:
        if stop is None:
            stop = len(records)
        for record in records[start:stop]:
            if filter is not None and filter(record):
                continue
            yield(create_template(record))
    finally:
        if not already_open:
            records.close()


# Foxpro functions

def gather(record, data, drop=False):
    """
    saves data into a record's fields; writes to disk if not in flux
    keys with no matching field will raise a FieldMissingError
    exception unless drop_missing == True;
    if an Exception occurs the record is restored before reraising
    """
    if isinstance(record, dbf.Record) and record._meta.status == CLOSED:
        raise dbf.DbfError("%s is closed; cannot modify record" % record._meta.filename)
    record_in_flux = not record._write_to_disk
    if not record_in_flux:
        record._start_flux()
    try:
        record_fields = field_names(record)
        for key in field_names(data):
            value = data[key]
            key = ensure_unicode(key).upper()
            if not key in record_fields:
                if drop:
                    continue
                raise dbf.FieldMissingError(key)
            record[key] = value
    except:
        if not record_in_flux:
            record._rollback_flux()
        raise
    if not record_in_flux:
        record._commit_flux()

def scan(table, direction='forward', filter=lambda rec: True):
    """
    moves record pointer forward 1; returns False if Eof/Bof reached
    table must be derived from _Navigation or have skip() method
    """
    if direction not in ('forward', 'reverse'):
        raise TypeError("direction should be 'forward' or 'reverse', not %r" % direction)
    if direction == 'forward':
        n = +1
        no_more_records = dbf.Eof
    else:
        n = -1
        no_more_records = dbf.Bof
    try:
        while True:
            table.skip(n)
            if filter(table.current_record):
                return True
    except no_more_records:
        return False

def scatter(record, as_type=create_template, _mappings=getattr(collections, 'Mapping', dict)):
    """
    returns as_type() of [fieldnames and] values.
    """
    if isinstance(as_type, type) and issubclass(as_type, _mappings):
        return as_type(zip(field_names(record), record))
    else:
        return as_type(record)

