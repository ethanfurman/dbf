from __future__ import print_function

from array import array
from aenum import NamedTuple
from bisect import bisect_left, bisect_right
from collections import defaultdict
from functools import partial
from glob import glob
from os import SEEK_END

import codecs
import csv
import datetime
import logging
import os
import struct
import sys
import traceback
import warnings
import weakref

from . import dbf
from . import pqlc
from .bridge import *
from .constants import *
from .constants import _NULLFLAG
from .data_types import *
from .exceptions import *
from .utils import ensure_unicode, field_names, gather, guess_table_type, recno, scatter, source_table
from .utils import is_deleted

class NullHandler(logging.Handler):
    """
    This handler does nothing. It's intended to be used to avoid the
    "No handlers could be found for logger XXX" one-off warning. This is
    important for library code, which may contain code to log events. If a user
    of the library does not configure logging, the one-off warning might be
    produced; to avoid this, the library developer simply needs to instantiate
    a NullHandler and add it to the top-level logger of the library module or
    package.

    Taken from 2.7 lib.
    """
    def handle(self, record):
        """Stub."""

    def emit(self, record):
        """Stub."""

    def createLock(self):
        self.lock = None

logger = logging.getLogger('dbf')
logger.addHandler(NullHandler())

temp_dir = os.environ.get("DBF_TEMP") or os.environ.get("TMP") or os.environ.get("TEMP") or ""

# other constructs

class LazyAttr(object):
    """
    doesn't create object until actually accessed
    """

    def __init__(self, func=None, doc=None):
        self.fget = func
        self.__doc__ = doc or func.__doc__

    def __call__(self, func):
        self.fget = func

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return self.fget(instance)


class MutableDefault(object):
    """
    Lives in the class, and on first access calls the supplied factory and
    maps the result into the instance it was called on
    """

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
    """
    because we can't do `NoneType(*args, **kwargs)`
    """
    return None

class FieldnameList(list):
    "storage for field names"

    def __new__(cls, items=()):
        for item in items:
            item = ensure_unicode(item)
            if not isinstance(item, unicode):
                raise TypeError('%r cannot be a field name' % (item, ))
        new_list = super(FieldnameList, cls).__new__(cls)
        return new_list

    def __init__(self, items=()):
        for item in items:
            self.append(item.upper())

    def __add__(self, other_list):
        if not isinstance(other_list, list):
            return NotImplemented
        new_list = self[:]
        for item in other_list:
            item = ensure_unicode(item)
            if not isinstance(item, unicode):
                raise TypeError('%r cannot be a field name' % (item, ))
            item = item.upper()
            new_list.append(item)
        return new_list

    def __contains__(self, item):
        item = ensure_unicode(item)
        if not isinstance(item, unicode):
            raise TypeError('%r cannot be a field name' % (item, ))
        item = item.upper()
        return item in list(self)

    def __getslice__(self, start, stop):
        return FieldnameList(super(FieldnameList, self).__getslice__(start, stop))

    def __getitem__(self, thing):
        res = super(FieldnameList, self).__getitem__(thing)
        if isinstance(res, list):
            res = FieldnameList(res)
        return res

    def __iadd__(self, other_list):
        if not isinstance(other_list, list):
            return NotImplemented
        for item in other_list:
            item = ensure_unicode(item)
            if not isinstance(item, unicode):
                raise TypeError('%r cannot be a field name' % (item, ))
            item = item.upper()
            super(FieldnameList, self).append(item)
        return self

    def __radd__(self, other_list):
        if not isinstance(other_list, list):
            return NotImplemented
        new_list = FieldnameList(other_list)
        new_list.extend(self)
        return new_list

    def __repr__(self):
        return 'FieldnameList(%s)' % super(FieldnameList, self).__repr__()

    def __setitem__(self, pos, item):
        if isinstance(item, list):
            if not isinstance(pos, slice):
                raise TypeError('%r cannot be a single field name' % item)
            try:
                new_things = []
                for thing in item:
                    thing = ensure_unicode(thing)
                    if not isinstance(thing, unicode):
                        raise TypeError('%r cannot be a field name' % (thing, ))
                    thing = thing.upper()
                    new_things.append(thing)
                item = new_things
            except TypeError:
                raise TypeError('%r cannot be a field name' % (thing, ))
        else:
            item = ensure_unicode(item)
            if not isinstance(item, unicode):
                raise TypeError('%r cannot be a field name' % (item, ))
            item = item.upper()
        return super(FieldnameList, self).__setitem__(pos, item)

    def __cmp__(self, other):
        if not isinstance(other, list):
            return NotImplemented
        for s, o in zip(self, other):
            o = ensure_unicode(o)
            if not isinstance(o, unicode):
                raise TypeError('%r cannot be a field name' % (o, ))
            o = o.upper()
            if s < o:
                return -1
            elif s > o:
                return +1
        # at least one list exhausted with all elements equal
        # now check lengths
        if len(s) < len(o):
            return -1
        if len(s) > len(o):
            return +1
        else:
            return 0

    def __eq__(self, other):
        res = self.__cmp__(other)
        if res is NotImplemented:
            return res
        else:
            return res == 0

    def __ne__(self, other):
        res = self.__cmp__(other)
        if res is NotImplemented:
            return res
        else:
            return res != 0

    def __le__(self, other):
        res = self.__cmp__(other)
        if res is NotImplemented:
            return res
        else:
            return res <= 0

    def __lt__(self, other):
        res = self.__cmp__(other)
        if res is NotImplemented:
            return res
        else:
            return res < 0

    def __gt__(self, other):
        res = self.__cmp__(other)
        if res is NotImplemented:
            return res
        else:
            return res > 0

    def __ge__(self, other):
        res = self.__cmp__(other)
        if res is NotImplemented:
            return res
        else:
            return res >= 0

    def append(self, item):
        item = ensure_unicode(item)
        if not isinstance(item, unicode):
            raise TypeError('%r cannot be a field name' % (item, ))
        item = item.upper()
        super(FieldnameList, self).append(item)

    def count(self, item):
        item = ensure_unicode(item)
        if not isinstance(item, unicode):
            raise TypeError('%r cannot be a field name' % (item, ))
        item = item.upper()
        return super(FieldnameList, self).count(item)

    def extend(self, other_list):
        for item in other_list:
            self.append(item)

    def index(self, item):
        item = ensure_unicode(item)
        if not isinstance(item, unicode):
            raise TypeError('%r cannot be a field name' % (item, ))
        item = item.upper()
        return super(FieldnameList, self).index(item)

    def insert(self, pos, item):
        item = ensure_unicode(item)
        if not isinstance(item, unicode):
            raise TypeError('%r cannot be a field name' % (item, ))
        item = item.upper()
        super(FieldnameList, self).insert(pos, item)

    def remove(self, item):
        item = ensure_unicode(item)
        if not isinstance(item, unicode):
            raise TypeError('%r cannot be a field name' % (item, ))
        item = item.upper()
        return super(FieldnameList, self).remove(item)


# Internal classes

class _Navigation(object):
    """
    Navigation base class that provides VPFish movement methods
    """

    _index = -1

    def _nav_check(self):
        """
        implemented by subclass; must return True if underlying structure meets need
        """
        raise NotImplementedError()

    def _get_index(self, direction, n=1, start=None):
        """
        returns index of next available record towards direction
        """
        if start is not None:
            index = start
        else:
            index = self._index
        if direction == 'reverse':
            move = -1 * n
            limit = 0
            index += move
            if index < limit:
                return -1
            else:
                return index
        elif direction == 'forward':
            move = +1 * n
            limit = len(self) - 1
            index += move
            if index > limit:
                return len(self)
            else:
                return index
        else:
            raise ValueError("direction should be 'forward' or 'reverse', not %r" % direction)

    @property
    def bof(self):
        """
        returns True if no more usable records towards the beginning of the table
        """
        self._nav_check()
        index = self._get_index('reverse')
        return index == -1

    def bottom(self):
        """
        sets record index to bottom of table (end of table)
        """
        self._nav_check()
        self._index = len(self)
        return self._index

    @property
    def current_record(self):
        """
        returns current record (deleted or not)
        """
        self._nav_check()
        index = self._index
        if index < 0:
            return RecordVaporWare('bof', self)
        elif index >= len(self):
            return RecordVaporWare('eof', self)
        return self[index]

    @property
    def current(self):
        """
        returns current index
        """
        self._nav_check()
        return self._index

    @property
    def eof(self):
        """
        returns True if no more usable records towards the end of the table
        """
        self._nav_check()
        index = self._get_index('forward')
        return index == len(self)

    @property
    def first_record(self):
        """
        returns first available record (does not move index)
        """
        self._nav_check()
        index = self._get_index('forward', start=-1)
        if -1 < index < len(self):
            return self[index]
        else:
            return RecordVaporWare('bof', self)

    def goto(self, where):
        """
        changes the record pointer to the first matching (deleted) record
        where should be either an integer, or 'top' or 'bottom'.
        top    -> before first record
        bottom -> after last record
        """
        self._nav_check()
        max = len(self)
        if isinstance(where, baseinteger):
            if not -max <= where < max:
                raise IndexError("Record %d does not exist" % where)
            if where < 0:
                where += max
            self._index = where
            return self._index
        move = getattr(self, where, None)
        if move is None:
            raise DbfError("unable to go to %r" % where)
        return move()

    @property
    def last_record(self):
        """
        returns last available record (does not move index)
        """
        self._nav_check()
        index = self._get_index('reverse', start=len(self))
        if -1 < index < len(self):
            return self[index]
        else:
            return RecordVaporWare('bof', self)

    @property
    def next_record(self):
        """
        returns next available record (does not move index)
        """
        self._nav_check()
        index = self._get_index('forward')
        if -1 < index < len(self):
            return self[index]
        else:
            return RecordVaporWare('eof', self)

    @property
    def prev_record(self):
        """
        returns previous available record (does not move index)
        """
        self._nav_check()
        index = self._get_index('reverse')
        if -1 < index < len(self):
            return self[index]
        else:
            return RecordVaporWare('bof', self)

    def skip(self, n=1):
        """
        move index to the next nth available record
        """
        self._nav_check()
        if n < 0:
            n *= -1
            direction = 'reverse'
        else:
            direction = 'forward'
        self._index = index = self._get_index(direction, n)
        if index < 0:
            raise Bof()
        elif index >= len(self):
            raise Eof()
        else:
            return index

    def top(self):
        """
        sets record index to top of table (beginning of table)
        """
        self._nav_check()
        self._index = -1
        return self._index


class Record(object):
    """
    Provides routines to extract and save data within the fields of a
    dbf record.
    """

    __slots__ = ('_recnum', '_meta', '_data', '_old_data', '_dirty',
                 '_memos', '_write_to_disk', '__weakref__')

    def __new__(cls, recnum, layout, kamikaze=b'', _fromdisk=False):
        """
        record = ascii array of entire record;
        layout=record specification;
        memo = memo object for table
        """
        record = object.__new__(cls)
        record._dirty = False
        record._recnum = recnum
        record._meta = layout
        record._memos = {}
        record._write_to_disk = True
        record._old_data = None
        record._data = layout.blankrecord[:]
        if kamikaze and len(record._data) != len(kamikaze):
            raise BadDataError("record data is not the correct length (should be %r, not %r)" %
                    (len(record._data), len(kamikaze)), data=kamikaze[:])
        if recnum == -1:                    # not a disk-backed record
            return record
        elif type(kamikaze) == array:
            record._data = kamikaze[:]
        elif type(kamikaze) == bytes:
            if kamikaze:
                record._data = array('B', kamikaze)
        else:
            raise BadDataError("%r recieved for record data" % kamikaze)
        if record._data[0] == NULL:
            record._data[0] = SPACE
        if record._data[0] not in (SPACE, ASTERISK):
            # TODO: log warning instead
            logger.error(
                    "record %d: invalid delete byte %h (should be SPACE or ASTERISK).  "
                    "Record will be considered active", record._data[0]
                    )
        if not _fromdisk and layout.location == ON_DISK:
            record._update_disk()
        return record

    def __contains__(self, value):
        for field in self._meta.user_fields:
            if self[field] == value:
                return True
        return False

    def __enter__(self):
        if not self._write_to_disk:
            raise DbfError("`with record` is not reentrant")
        self._start_flux()
        return self

    def __eq__(self, other):
        if not isinstance(other, (Record, RecordTemplate, dict, tuple)):
            return NotImplemented
        if isinstance(other, (Record, RecordTemplate)):
            if field_names(self) != field_names(other):
                return False
            for field in self._meta.user_fields:
                s_value, o_value = self[field], other[field]
                if s_value is not o_value and s_value != o_value:
                    return False
        elif isinstance(other, dict):
            other = dict((ensure_unicode(k).upper(), v) for k, v in other.items())
            if sorted(field_names(self)) != sorted(other.keys()):
                return False
            for field in self._meta.user_fields:
                s_value, o_value = self[field], other[field]
                if s_value is not o_value and s_value != o_value:
                    return False
        else: # tuple
            if len(self) != len(other):
                return False
            for s_value, o_value in zip(self, other):
                if s_value is not o_value and s_value != o_value:
                    return False
        return True

    def __exit__(self, *args):
        if args == (None, None, None):
            self._commit_flux()
        else:
            self._rollback_flux()

    def __iter__(self):
        return (self[field] for field in self._meta.user_fields)

    def __getattr__(self, name):
        if name[0:2] == '__' and name[-2:] == '__':
            raise AttributeError('Method %s is not implemented.' % name)
        name = name.upper()
        if not name in self._meta.fields:
            raise FieldMissingError(name)
        if name in self._memos:
            return self._memos[name]
        try:
            value = self._retrieve_field_value(name)
            return value
        except DbfError:
            error = sys.exc_info()[1]
            error.message = "error accessing field %r:  %s" % (name, error.message)
            raise

    def __getitem__(self, item):
        if isinstance(item, baseinteger):
            fields = self._meta.user_fields
            field_count = len(fields)
            if not -field_count <= item < field_count:
                raise NotFoundError("Field offset %d is not in record" % item)
            field = fields[item]
            if field in self._memos:
                return self._memos[field]
            return self[field]
        elif isinstance(item, slice):
            sequence = []
            if isinstance(item.start, basestring) or isinstance(item.stop, basestring):
                names = field_names(self)
                start, stop, step = ensure_unicode(item.start).upper(), ensure_unicode(item.stop).upper(), item.step
                if start not in names or stop not in names:
                    raise FieldMissingError("Either %r or %r (or both) are not valid field names" % (start, stop))
                if step is not None and not isinstance(step, baseinteger):
                    raise DbfError("step value must be an intger, not %r" % type(step))
                start = names.index(start)
                stop = names.index(stop) + 1
                item = slice(start, stop, step)
            for index in self._meta.fields[item]:
                sequence.append(self[index])
            return sequence
        elif isinstance(item, basestring):
            return self.__getattr__(item)
        else:
            raise TypeError("%r is not a field name" % item)

    def __len__(self):
        return self._meta.user_field_count

    def __ne__(self, other):
        if not isinstance(other, (Record, RecordTemplate, dict, tuple)):
            return NotImplemented
        return not self == other

    def __setattr__(self, name, value):
        if name in self.__slots__:
            object.__setattr__(self, name, value)
            return
        name = name.upper()
        if self._meta.status != READ_WRITE:
            raise DbfError("%s not in read/write mode" % self._meta.filename)
        elif self._write_to_disk:
            raise DbfError("unable to modify fields individually except in `with` or `Process()`")
        elif not name in self._meta.fields:
            raise FieldMissingError(name)
        if name in self._meta.memofields:
            self._memos[name] = value
            self._dirty = True
            return
        try:
            self._update_field_value(name, value)
        except DbfError:
            error = sys.exc_info()[1]
            message = "field %r: %s" % (name, error.args)
            data = name
            err_cls = error.__class__
            raise err_cls(message, data)

    def __setitem__(self, name, value):
        if self._meta.status != READ_WRITE:
            raise DbfError("%s not in read/write mode" % self._meta.filename)
        if self._write_to_disk:
            raise DbfError("unable to modify fields individually except in `with` or `Process()`")
        if isinstance(name, basestring):
            self.__setattr__(name.upper(), value)
        elif isinstance(name, baseinteger):
            self.__setattr__(self._meta.fields[name], value)
        elif isinstance(name, slice):
            sequence = []
            names = field_names(self)
            if isinstance(name.start, basestring) or isinstance(name.stop, basestring):
                start, stop, step = ensure_unicode(name.start).upper(), ensure_unicode(name.stop).upper(), name.step
                if start not in names or stop not in names:
                    raise FieldMissingError("Either %r or %r (or both) are not valid field names" % (start, stop))
                if step is not None and not isinstance(step, baseinteger):
                    raise DbfError("step value must be an integer, not %r" % type(step))
                start = names.index(start)
                stop = names.index(stop) + 1
                name = slice(start, stop, step)
            for field in self._meta.fields[name]:
                sequence.append(field)
            if len(sequence) != len(value):
                raise DbfError("length of slices not equal")
            for field, val in zip(sequence, value):
                self[field] = val
        else:
            raise TypeError("%s is not a field name" % name)

    def __str__(self):
        result = []
        for seq, field in enumerate(field_names(self)):
            result.append("%3d - %-10s: %r" % (seq, field, self[field]))
        return '\n'.join(result)

    def __repr__(self):
        return '%r' % to_bytes(self._data)

    def _commit_flux(self):
        """
        stores field updates to disk; if any errors restores previous contents and propogates exception
        """
        if self._write_to_disk:
            raise DbfError("record not in flux")
        try:
            self._write()
        except Exception:
            exc = sys.exc_info()[1]
            self._data[:] = self._old_data
            self._update_disk(data=self._old_data)
            raise DbfError("unable to write updates to disk, original data restored: %r" % (exc,)).from_exc(None)
        self._memos.clear()
        self._old_data = None
        self._write_to_disk = True
        self._reindex_record()

    @classmethod
    def _create_blank_data(cls, layout):
        """
        creates a blank record data chunk
        """
        record = object.__new__(cls)
        record._dirty = False
        record._recnum = -1
        record._meta = layout
        record._data = array('B', b' ' * layout.header.record_length)
        layout.memofields = []
        signature = [layout.table().codepage.name]
        for index, name in enumerate(layout.fields):
            if name == '_NULLFLAGS':
                record._data[layout['_NULLFLAGS'][START]:layout['_NULLFLAGS'][END]] = array('B', [0xff] * layout['_NULLFLAGS'][LENGTH])
        for index, name in enumerate(layout.fields):
            signature.append(name)
            if name != '_NULLFLAGS':
                type = FieldType(layout[name][TYPE])
                start = layout[name][START]
                size = layout[name][LENGTH]
                end = layout[name][END]
                blank = layout.fieldtypes[type]['Blank']
                record._data[start:end] = array('B', blank(size))
                if layout[name][TYPE] in layout.memo_types:
                    layout.memofields.append(name)
                decimals = layout[name][DECIMALS]
                signature[-1] = '_'.join([unicode(x) for x in (signature[-1], type.symbol, size, decimals)])
        layout.blankrecord = record._data[:]
        data_types = []
        for fieldtype, defs in sorted(layout.fieldtypes.items()):
            if fieldtype != _NULLFLAG:    # ignore the nullflags field
                data_types.append("%s_%s_%s" % (fieldtype.symbol, defs['Empty'], defs['Class']))
        layout.record_sig = ('___'.join(signature), '___'.join(data_types))

    def _reindex_record(self):
        """
        rerun all indices with this record
        """
        if self._meta.status == CLOSED:
            raise DbfError("%s is closed; cannot alter indices" % self._meta.filename)
        elif not self._write_to_disk:
            raise DbfError("unable to reindex record until it is written to disk")
        for dbfindex in self._meta.table()._indexen:
            dbfindex(self)

    def _retrieve_field_value(self, name):
        """
        calls appropriate routine to convert value stored in field from array
        """
        # check nullable here, binary is handled in the appropriate retrieve_* functions
        # index = self._meta.fields.index(name)
        fielddef = self._meta[name]
        flags = fielddef[FLAGS]
        nullable = flags & NULLABLE and '_NULLFLAGS' in self._meta
        if nullable:
            index = fielddef[NUL]
            byte, bit = divmod(index, 8)
            null_def = self._meta['_NULLFLAGS']
            null_data = self._data[null_def[START]:null_def[END]]
            try:
                if null_data[byte] >> bit & 1:
                    return Null
            except IndexError:
                print(null_data)
                print(index)
                print(byte, bit)
                print(len(self._data), self._data)
                print(null_def)
                print(null_data)
                raise
        record_data = self._data[fielddef[START]:fielddef[END]]
        field_type = fielddef[TYPE]
        retrieve = self._meta.fieldtypes[field_type]['Retrieve']
        datum = retrieve(record_data, fielddef, self._meta.memo, self._meta.decoder)
        return datum

    def _rollback_flux(self):
        """
        discards all changes since ._start_flux()
        """
        if self._write_to_disk:
            raise DbfError("record not in flux")
        self._data = self._old_data
        self._old_data = None
        self._memos.clear()
        self._write_to_disk = True
        self._write()

    def _start_flux(self):
        """
        Allows record.field_name = ... and record[...] = ...; must use ._commit_flux() to commit changes
        """
        if self._meta.status == CLOSED:
            raise DbfError("%s is closed; cannot modify record" % self._meta.filename)
        elif self._recnum < 0:
            raise DbfError("record has been packed; unable to update")
        elif not self._write_to_disk:
            raise DbfError("record already in a state of flux")
        self._old_data = self._data[:]
        self._write_to_disk = False

    def _update_field_value(self, name, value):
        """
        calls appropriate routine to convert value to bytes, and save it in record
        """
        # check nullabel here, binary is handled in the appropriate update_* functions
        fielddef = self._meta[name]
        index = fielddef[NUL]
        field_type = fielddef[TYPE]
        flags = fielddef[FLAGS]
        nullable = flags & NULLABLE and '_NULLFLAGS' in self._meta
        update = self._meta.fieldtypes[field_type]['Update']
        if nullable:
            byte, bit = divmod(index, 8)
            null_def = self._meta['_NULLFLAGS']
            null_data = self._data[null_def[START]:null_def[END]]
            if value is Null:
                null_data[byte] |= 1 << bit
                value = None
            else:
                null_data[byte] &= 0xff ^ 1 << bit
            self._data[null_def[START]:null_def[END]] = null_data
        if value is not Null:
            bytes = array('B', update(value, fielddef, self._meta.memo, self._meta.input_decoder, self._meta.encoder))
            size = fielddef[LENGTH]
            if len(bytes) > size:
                raise DataOverflowError("tried to store %d bytes in %d byte field" % (len(bytes), size))
            blank = array('B', b' ' * size)
            start = fielddef[START]
            end = start + size
            blank[:len(bytes)] = bytes[:]
            self._data[start:end] = blank[:]
        self._dirty = True

    def _update_disk(self, location='', data=None):
        layout = self._meta
        if self._recnum < 0:
            raise DbfError("cannot update a packed record")
        if layout.location == ON_DISK:
            header = layout.header
            if location == '':
                location = self._recnum * header.record_length + header.start
            if data is None:
                data = self._data
            layout.dfd.seek(location)
            layout.dfd.write(data)
            self._dirty = False
        table = layout.table()
        if table is not None:  # is None when table is being destroyed
            for index in table._indexen:
                index(self)

    def _write(self):
        for field, value in self._memos.items():
            self._update_field_value(field, value)
        self._update_disk()


class RecordTemplate(object):
    """
    Provides routines to mimic a dbf record.
    """

    __slots__ = ('_meta', '_data', '_old_data', '_memos', '_write_to_disk', '__weakref__')

    def _commit_flux(self):
        """
        Flushes field updates to disk
        If any errors restores previous contents and raises `DbfError`
        """
        if self._write_to_disk:
            raise DbfError("record not in flux")
        self._memos.clear()
        self._old_data = None
        self._write_to_disk = True

    def _retrieve_field_value(self, name):
        """
        Calls appropriate routine to convert value stored in field from
        array
        """
        # check nullable here, binary is handled in the appropriate retrieve_* functions
        fielddef = self._meta[name]
        flags = fielddef[FLAGS]
        nullable = flags & NULLABLE and '_NULLFLAGS' in self._meta
        if nullable:
            index = fielddef[NUL]
            byte, bit = divmod(index, 8)
            null_def = self._meta['_NULLFLAGS']
            null_data = self._data[null_def[START]:null_def[END]]
            if null_data[byte] >> bit & 1:
                return Null
        record_data = self._data[fielddef[START]:fielddef[END]]
        field_type = fielddef[TYPE]
        retrieve = self._meta.fieldtypes[field_type]['Retrieve']
        datum = retrieve(record_data, fielddef, self._meta.memo, self._meta.decoder)
        return datum

    def _rollback_flux(self):
        """
        discards all changes since ._start_flux()
        """
        if self._write_to_disk:
            raise DbfError("template not in flux")
        self._data = self._old_data
        self._old_data = None
        self._memos.clear()
        self._write_to_disk = True

    def _start_flux(self):
        """
        Allows record.field_name = ... and record[...] = ...; must use ._commit_flux() to commit changes
        """
        if not self._write_to_disk:
            raise DbfError("template already in a state of flux")
        self._old_data = self._data[:]
        self._write_to_disk = False

    def _update_field_value(self, name, value):
        """
        calls appropriate routine to convert value to ascii bytes, and save it in record
        """
        # check nullabel here, binary is handled in the appropriate update_* functions
        fielddef = self._meta[name]
        index = fielddef[NUL]
        field_type = fielddef[TYPE]
        flags = fielddef[FLAGS]
        nullable = flags & NULLABLE and '_NULLFLAGS' in self._meta
        update = self._meta.fieldtypes[field_type]['Update']
        if nullable:
            byte, bit = divmod(index, 8)
            null_def = self._meta['_NULLFLAGS']
            null_data = self._data[null_def[START]:null_def[END]] #.tostring()
            if value is Null:
                null_data[byte] |= 1 << bit
                value = None
            else:
                null_data[byte] &= 0xff ^ 1 << bit
            self._data[null_def[START]:null_def[END]] = null_data
        if value is not Null:
            bytes = array('B', update(value, fielddef, self._meta.memo, self._meta.input_decoder, self._meta.encoder))
            size = fielddef[LENGTH]
            if len(bytes) > size:
                raise DataOverflowError("tried to store %d bytes in %d byte field" % (len(bytes), size))
            blank = array('B', b' ' * size)
            start = fielddef[START]
            end = start + size
            blank[:len(bytes)] = bytes[:]
            self._data[start:end] = blank[:]

    def __new__(cls, layout, original_record=None, defaults=None):
        """
        record = ascii array of entire record; layout=record specification
        """
        sig = layout.record_sig
        if sig not in dbf._Template_Records:
            table = layout.table()
            dbf._Template_Records[sig] = table.new(
                    ':%s:' % layout.filename,
                    default_data_types=table._meta._default_data_types,
                    field_data_types=table._meta._field_data_types, on_disk=False
                    )._meta
        layout = dbf._Template_Records[sig]
        record = object.__new__(cls)
        record._write_to_disk = True
        record._meta = layout
        record._memos = {}
        for name in layout.memofields:
            field_type = layout[name][TYPE]
            record._memos[name] = layout.fieldtypes[field_type]['Empty']()
        if original_record is None:
            record._data = layout.blankrecord[:]
        else:
            record._data = original_record._data[:]
            for name in layout.memofields:
                record._memos[name] = original_record[name]
        for name in field_names(defaults or {}):
            record[name] = defaults[name]
        record._old_data = record._data[:]
        return record

    def __contains__(self, key):
        return key in self._meta.user_fields

    def __eq__(self, other):
        if not isinstance(other, (Record, RecordTemplate, dict, tuple)):
            return NotImplemented
        if isinstance(other, (Record, RecordTemplate)):
            if field_names(self) != field_names(other):
                return False
            for field in self._meta.user_fields:
                s_value, o_value = self[field], other[field]
                if s_value is not o_value and s_value != o_value:
                    return False
        elif isinstance(other, dict):
            other = dict((ensure_unicode(k).upper(), v) for k, v in other.items())
            if sorted(field_names(self)) != sorted(other.keys()):
                return False
            for field in self._meta.user_fields:
                s_value, o_value = self[field], other[field]
                if s_value is not o_value and s_value != o_value:
                    return False
        else: # tuple
            if len(self) != len(other):
                return False
            for s_value, o_value in zip(self, other):
                if s_value is not o_value and s_value != o_value:
                    return False
        return True

    def __iter__(self):
        return (self[field] for field in self._meta.user_fields)

    def __getattr__(self, name):
        if name[0:2] == '__' and name[-2:] == '__':
            raise AttributeError('Method %s is not implemented.' % name)
        name = name.upper()
        if not name in self._meta.fields:
            raise FieldMissingError(name)
        if name in self._memos:
            return self._memos[name]
        try:
            value = self._retrieve_field_value(name)
            return value
        except DbfError:
            fielddef = self._meta[name]
            error = sys.exc_info()[1]
            error.message = "field --%s-- is %s -> %s" % (name, self._meta.fieldtypes[fielddef['type']]['Type'], error.message)
            raise

    def __getitem__(self, item):
        fields = self._meta.user_fields
        if isinstance(item, baseinteger):
            field_count = len(fields)
            if not -field_count <= item < field_count:
                raise NotFoundError("Field offset %d is not in record" % item)
            field = fields[item]
            if field in self._memos:
                return self._memos[field]
            return self[field]
        elif isinstance(item, slice):
            sequence = []
            if isinstance(item.start, basestring) or isinstance(item.stop, basestring):
                start, stop, step = item.start.upper(), item.stop.upper(), item.step
                if start not in fields or stop not in fields:
                    raise FieldMissingError("Either %r or %r (or both) are not valid field names" % (start, stop))
                if step is not None and not isinstance(step, baseinteger):
                    raise DbfError("step value must be an integer, not %r" % type(step))
                start = fields.index(start)
                stop = fields.index(stop) + 1
                item = slice(start, stop, step)
            for index in self._meta.fields[item]:
                sequence.append(self[index])
            return sequence
        elif isinstance(item, basestring):
            return self.__getattr__(item.upper())
        else:
            raise TypeError("%r is not a field name" % item)

    def __len__(self):
        return self._meta.user_field_count

    def __ne__(self, other):
        if not isinstance(other, (Record, RecordTemplate, dict, tuple)):
            return NotImplemented
        return not self == other

    def __setattr__(self, name, value):
        if name in self.__slots__:
            object.__setattr__(self, name, value)
            return
        name = name.upper()
        if not name in self._meta.fields:
            raise FieldMissingError(name)
        if name in self._meta.memofields:
            self._memos[name] = value
            return
        try:
            self._update_field_value(name, value)
        except DbfError:
            error = sys.exc_info()[1]
            fielddef = self._meta[name]
            message = "%s (%s) = %r --> %s" % (name, self._meta.fieldtypes[fielddef[TYPE]]['Type'], value, error.message)
            data = name
            err_cls = error.__class__
            raise err_cls(message, data).from_exc(None)

    def __setitem__(self, name, value):
        if isinstance(name, basestring):
            self.__setattr__(name.upper(), value)
        elif isinstance(name, baseinteger):
            self.__setattr__(self._meta.fields[name], value)
        elif isinstance(name, slice):
            sequence = []
            names = field_names(self)
            if isinstance(name.start, basestring) or isinstance(name.stop, basestring):
                start, stop, step = name.start.upper(), name.stop.upper(), name.step
                if start not in names or stop not in names:
                    raise FieldMissingError("Either %r or %r (or both) are not valid field names" % (start, stop))
                if step is not None and not isinstance(step, baseinteger):
                    raise DbfError("step value must be an integer, not %r" % type(step))
                start = names.index(start)
                stop = names.index(stop) + 1
                name = slice(start, stop, step)
            for field in self._meta.fields[name]:
                sequence.append(field)
            if len(sequence) != len(value):
                raise DbfError("length of slices not equal")
            for field, val in zip(sequence, value):
                self[field] = val
        else:
            raise TypeError("%s is not a field name" % name)


    def __repr__(self):
        return '%r' % to_bytes(self._data)

    def __str__(self):
        result = []
        for seq, field in enumerate(field_names(self)):
            result.append("%3d - %-10s: %r" % (seq, field, self[field]))
        return '\n'.join(result)


class RecordVaporWare(object):
    """
    Provides routines to mimic a dbf record, but all values are non-existent.
    """

    __slots__ = ('_recno', '_sequence')

    def __new__(cls, position, sequence):
        """
        record = ascii array of entire record
        layout=record specification
        memo = memo object for table
        """
        if position not in ('bof', 'eof'):
            raise ValueError("position should be 'bof' or 'eof', not %r" % position)
        vapor = object.__new__(cls)
        vapor._recno = (-1, None)[position == 'eof']
        vapor._sequence = sequence
        return vapor

    def __contains__(self, key):
        return False

    def __eq__(self, other):
        if not isinstance(other, (Record, RecordTemplate, RecordVaporWare, dict, tuple)):
            return NotImplemented
        return False


    def __getattr__(self, name):
        if name[0:2] == '__' and name[-2:] == '__':
            raise AttributeError('Method %s is not implemented.' % name)
        else:
            return Vapor

    def __getitem__(self, item):
        if isinstance(item, baseinteger):
            return Vapor
        elif isinstance(item, slice):
            raise TypeError('slice notation not allowed on Vapor records')
        elif isinstance(item, basestring):
            return self.__getattr__(item)
        else:
            raise TypeError("%r is not a field name" % item)

    def __len__(self):
        raise TypeError("Vapor records have no length")

    def __ne__(self, other):
        if not isinstance(other, (Record, RecordTemplate, RecordVaporWare, dict, tuple)):
            return NotImplemented
        return True

    if py_ver < (3, 0):
        def __nonzero__(self):
            """
            Vapor records are always False
            """
            return False
    else:
        def __bool__(self):
            """
            Vapor records are always False
            """
            return False

    def __setattr__(self, name, value):
        if name in self.__slots__:
            object.__setattr__(self, name, value)
            return
        raise TypeError("cannot change Vapor record")

    def __setitem__(self, name, value):
        if isinstance(name, (basestring, baseinteger)):
            raise TypeError("cannot change Vapor record")
        elif isinstance(name, slice):
            raise TypeError("slice notation not allowed on Vapor records")
        else:
            raise TypeError("%s is not a field name" % name)

    def __repr__(self):
        return "RecordVaporWare(position=%r, sequence=%r)" % (('bof', 'eof')[recno(self) is None], self._sequence)

    def __str__(self):
        return 'VaporRecord(%r)' % recno(self)

    @property
    def _recnum(self):
        if self._recno is None:
            return len(self._sequence)
        else:
            return self._recno


class _DbfMemo(object):
    """
    Provides access to memo fields as dictionaries
    Must override _init, _get_memo, and _put_memo to
    store memo contents to disk
    """

    def _init(self):
        """
        Initialize disk file usage
        """

    def _get_memo(self, block):
        """
        Retrieve memo contents from disk
        """

    def _put_memo(self, data):
        """
        Store memo contents to disk
        """

    def _zap(self):
        """
        Resets memo structure back to zero memos
        """
        self.memory.clear()
        self.nextmemo = 1

    def __init__(self, meta):
        self.meta = meta
        self.memory = {}
        self.nextmemo = 1
        self._init()
        self.meta.newmemofile = False

    def get_memo(self, block):
        """
        Gets the memo in block
        """
        if self.meta.ignorememos or not block:
            return ''
        if self.meta.location == ON_DISK:
            return self._get_memo(block)
        else:
            return self.memory[block]

    def put_memo(self, data):
        """
        Stores data in memo file, returns block number
        """
        if self.meta.ignorememos or data == '':
            return 0
        if self.meta.location == IN_MEMORY:
            thismemo = self.nextmemo
            self.nextmemo += 1
            self.memory[thismemo] = data
        else:
            thismemo = self._put_memo(data)
        return thismemo


class _Db3Memo(_DbfMemo):
    """
    dBase III specific
    """

    def _init(self):
        self.meta.memo_size= 512
        self.record_header_length = 2
        if self.meta.location == ON_DISK and not self.meta.ignorememos:
            if self.meta.newmemofile:
                self.meta.mfd = open(self.meta.memoname, 'w+b')
                self.meta.mfd.write(pack_long_int(1) + b'\x00' * 508)
            else:
                mode = ('rb', 'r+b')[self.meta.status is READ_WRITE]
                try:
                    self.meta.mfd = open(self.meta.memoname, mode)
                    self.meta.mfd.seek(0)
                    next = self.meta.mfd.read(4)
                    self.nextmemo = unpack_long_int(next)
                except Exception:
                    exc = sys.exc_info()[1]
                    raise DbfError("memo file appears to be corrupt: %r" % exc.args).from_exc(None)

    def _get_memo(self, block):
        block = int(block)
        self.meta.mfd.seek(block * self.meta.memo_size)
        eom = -1
        data = b''
        while eom == -1:
            newdata = self.meta.mfd.read(self.meta.memo_size)
            if not newdata:
                return data
            data += newdata
            eom = data.find(b'\x1a\x1a')
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
        self.meta.mfd.write(b'\x1a\x1a')
        double_check = self._get_memo(thismemo)
        if len(double_check) != len(data):
            uhoh = open('dbf_memo_dump.err', 'wb')
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
        if self.meta.location == ON_DISK and not self.meta.ignorememos:
            mfd = self.meta.mfd
            mfd.seek(0)
            mfd.truncate(0)
            mfd.write(pack_long_int(1) + b'\x00' * 508)
            mfd.flush()

class _VfpMemo(_DbfMemo):
    """
    Visual Foxpro 6 specific
    """

    def _init(self):
        if self.meta.location == ON_DISK and not self.meta.ignorememos:
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
                self.meta.mfd.write(pack_long_int(nextmemo, bigendian=True) + b'\x00\x00' + \
                        pack_short_int(self.meta.memo_size, bigendian=True) + b'\x00' * 504)
            else:
                mode = ('rb', 'r+b')[self.meta.status is READ_WRITE]
                try:
                    self.meta.mfd = open(self.meta.memoname, mode)
                    self.meta.mfd.seek(0)
                    header = self.meta.mfd.read(512)
                    self.nextmemo = unpack_long_int(header[:4], bigendian=True)
                    self.meta.memo_size = unpack_short_int(header[6:8], bigendian=True)
                except Exception:
                    exc = sys.exc_info()[1]
                    raise DbfError("memo file appears to be corrupt: %r" % exc.args).from_exc(None)

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
        self.meta.mfd.write(pack_long_int(thismemo + blocks, bigendian=True))
        self.meta.mfd.seek(thismemo * self.meta.memo_size)
        self.meta.mfd.write(b'\x00\x00\x00\x01' + pack_long_int(len(data), bigendian=True) + data)
        return thismemo

    def _zap(self):
        if self.meta.location == ON_DISK and not self.meta.ignorememos:
            mfd = self.meta.mfd
            mfd.seek(0)
            mfd.truncate(0)
            nextmemo = 512 // self.meta.memo_size
            if nextmemo * self.meta.memo_size < 512:
                nextmemo += 1
            self.nextmemo = nextmemo
            mfd.write(pack_long_int(nextmemo, bigendian=True) + b'\x00\x00' + \
                    pack_short_int(self.meta.memo_size, bigendian=True) + b'\x00' * 504)
            mfd.flush()


class DbfCsv(csv.Dialect):
    """
    csv format for exporting tables
    """
    delimiter = ','
    doublequote = True
    escapechar = None
    lineterminator = '\n'
    quotechar = '"'
    skipinitialspace = True
    quoting = csv.QUOTE_NONNUMERIC
csv.register_dialect('dbf', DbfCsv)


class _DeadObject(object):
    """
    used because you cannot weakref None
    """
    if py_ver < (3, 0):
        def __nonzero__(self):
            return False
    else:
        def __bool__(self):
            return False
_DeadObject = _DeadObject()


# Routines for saving, retrieving, and creating fields

VFPTIME = 1721425

def pack_short_int(value, bigendian=False):
    """
    Returns a two-bye integer from the value, or raises DbfError
    """
    # 256 / 65,536
    if value > 65535:
        raise DataOverflowError("Maximum Integer size exceeded.  Possible: 65535.  Attempted: %d" % value)
    if bigendian:
        return struct.pack('>H', value)
    else:
        return struct.pack('<H', value)

def pack_long_int(value, bigendian=False):
    """
    Returns a four-bye integer from the value, or raises DbfError
    """
    # 256 / 65,536 / 16,777,216
    if value > 4294967295:
        raise DataOverflowError("Maximum Integer size exceeded.  Possible: 4294967295.  Attempted: %d" % value)
    if bigendian:
        return struct.pack('>L', value)
    else:
        return struct.pack('<L', value)

def pack_str(string):
    """
    Returns an 11 byte, upper-cased, null padded string suitable for field names;
    raises DbfError if the string is bigger than 10 bytes
    """
    if len(string) > 10:
        raise DbfError("Maximum string size is ten characters -- %s has %d characters" % (string, len(string)))
    return struct.pack('11s', string.upper())

def unpack_short_int(bytes, bigendian=False):
    """
    Returns the value in the two-byte integer passed in
    """
    if bigendian:
        return struct.unpack('>H', bytes)[0]
    else:
        return struct.unpack('<H', bytes)[0]

def unpack_long_int(bytes, bigendian=False):
    """
    Returns the value in the four-byte integer passed in
    """
    if bigendian:
        return int(struct.unpack('>L', bytes)[0])
    else:
        return int(struct.unpack('<L', bytes)[0])

def unpack_str(chars):
    """
    Returns a normal, upper-cased string from a null-padded byte string
    """
    field = array('B', struct.unpack('%ds' % len(chars), chars)[0])
    for i, ch in enumerate(field):
        if ch == NULL:
            break
    return to_bytes(field[:i]).upper()

def scinot(value, decimals):
    """
    return scientific notation with not more than decimals-1 decimal places
    """
    value = unicode(value)
    sign = ''
    if value[0] in ('+-'):
        sign = value[0]
        if sign == '+':
            sign = ''
        value = value[1:]
    if 'e' in value:    #7.8e-05
        e = value.find('e')
        if e - 1 <= decimals:
            return sign + value
        integer, mantissa, power = value[0], value[1:e], value[e+1:]
        mantissa = mantissa[:decimals]
        value = sign + integer + mantissa + 'e' + power
        return value
    integer, mantissa = value[0], value[1:]
    if integer == '0':
        for e, integer in enumerate(mantissa):
            if integer not in ('.0'):
                break
        mantissa = '.' + mantissa[e+1:]
        mantissa = mantissa[:decimals]
        value = sign + integer + mantissa + 'e-%03d' % e
        return value
    e = mantissa.find('.')
    mantissa = '.' + mantissa.replace('.','')
    mantissa = mantissa[:decimals]
    value = sign + integer + mantissa + 'e+%03d' % e
    return value

def unsupported_type(something, *ignore):
    """
    called if a data type is not supported for that style of table
    """
    return something

def retrieve_character(bytes, fielddef, memo, decoder):
    """
    Returns the string in bytes as fielddef[CLASS] or fielddef[EMPTY]
    """
    data = to_bytes(bytes)
    if fielddef[FLAGS] & BINARY:
        return data
    data = fielddef[CLASS](decoder(data)[0])
    if not data.strip():
        cls = fielddef[EMPTY]
        if cls is NoneType:
            return None
        return cls(data)
    return data

def update_character(string, fielddef, memo, decoder, encoder):
    """
    returns the string as bytes (not unicode) as fielddef[CLASS] or fielddef[EMPTY]
    """
    length = fielddef[LENGTH]
    if string == None:
        return length * b' '
    if fielddef[FLAGS] & BINARY:
        if not isinstance(string, bytes):
            raise ValueError('binary field: %r not in bytes format' % string)
        return string
    else:
        if not isinstance(string, basestring):
            raise ValueError("unable to coerce %r(%r) to string" % (type(string), string))
        string = encoder(string.strip())[0]
        if not string[length:].strip():     # trims trailing white space to fit in field
            string = string[:length]
        return string

def retrieve_currency(bytes, fielddef, *ignore):
    """
    Returns the currency value in bytes
    """
    value = struct.unpack('<q', bytes)[0]
    return fielddef[CLASS](("%de-4" % value).strip())

def update_currency(value, *ignore):
    """
    Returns the value to be stored in the record's disk data
    """
    if value == None:
        value = 0
    currency = int(round(value * 10000))
    if not -9223372036854775808 < currency < 9223372036854775808:
        raise DataOverflowError("value %s is out of bounds" % value)
    return struct.pack('<q', currency)

def retrieve_date(bytes, fielddef, *ignore):
    """
    Returns the ascii coded date as fielddef[CLASS] or fielddef[EMPTY]
    """
    text = to_bytes(bytes)
    if text in (b'        ', b'00000000'):
        cls = fielddef[EMPTY]
        if cls is NoneType:
            return None
        return cls()
    year = int(text[0:4])
    month = int(text[4:6])
    day = int(text[6:8])
    return fielddef[CLASS](year, month, day)

def update_date(moment, *ignore):
    """
    Returns the Date or datetime.date object ascii-encoded (yyyymmdd)
    """
    if moment == None:
        return b'        '
    return (u"%04d%02d%02d" % moment.timetuple()[:3]).encode('ascii')

def retrieve_double(bytes, fielddef, *ignore):
    """
    Returns the double in bytes as fielddef[CLASS] ('default' == float)
    """
    cls = fielddef[CLASS]
    if cls == 'default':
        cls = float
    return cls(struct.unpack('<d', bytes)[0])

def update_double(value, *ignore):
    """
    returns the value to be stored in the record's disk data
    """
    if value == None:
        value = 0
    return struct.pack('<d', float(value))

def retrieve_integer(bytes, fielddef, *ignore):
    """
    Returns the binary number stored in bytes in little-endian
    format as fielddef[CLASS]
    """
    cls = fielddef[CLASS]
    if cls == 'default':
        cls = int
    return cls(struct.unpack('<i', bytes)[0])

def update_integer(value, *ignore):
    """
    Returns value in little-endian binary format
    """
    if value == None:
        value = 0
    try:
        value = int(value)
    except Exception:
        raise DbfError("incompatible type: %s(%s)" % (type(value), value)).from_exc(None)
    if not -2147483648 < value < 2147483647:
        raise DataOverflowError("Integer size exceeded.  Possible: -2,147,483,648..+2,147,483,647.  Attempted: %d" % value)
    return struct.pack('<i', int(value))

def retrieve_logical(bytes, fielddef, *ignore):
    """
    Returns True if bytes is 't', 'T', 'y', or 'Y'
    None if '?'
    False otherwise
    """
    cls = fielddef[CLASS]
    empty = fielddef[EMPTY]
    bytes = to_bytes(bytes)
    if bytes in b'tTyY':
        return cls(True)
    elif bytes in b'fFnN':
        return cls(False)
    elif bytes in b'? ':
        if empty is NoneType:
            return None
        return empty()
    elif LOGICAL_BAD_IS_NONE:
        return None
    else:
        raise BadDataError('Logical field contained %r' % bytes)
    return cls(bytes)

def update_logical(data, *ignore):
    """
    Returns 'T' if logical is True, 'F' if False, '?' otherwise
    """
    if data is Unknown or data is None or data is Null or data is Other:
        return b'?'
    if data == True:
        return b'T'
    if data == False:
        return b'F'
    raise ValueError("unable to automatically coerce %r to Logical" % data)

def retrieve_memo(bytes, fielddef, memo, decoder):
    """
    Returns the block of data from a memo file
    """
    stringval = to_bytes(bytes).strip()
    if not stringval or memo is None:
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
    """
    Writes string as a memo, returns the block number it was saved into
    """
    if memo is None:
        raise DbfError('Memos are being ignored, unable to update')
    if string == None:
        string = b''
    if fielddef[FLAGS] & BINARY:
        if not isinstance(string, bytes):
            raise ValueError('binary field: %r not in bytes format' % string)
    else:
        if not isinstance(string, basestring):
            raise ValueError("unable to coerce %r(%r) to string" % (type(string), string))
        string = encoder(string)[0]
    block = memo.put_memo(string)
    if block == 0:
        block = b''
    return ("%*s" % (fielddef[LENGTH], block)).encode('ascii')

def retrieve_numeric(bytes, fielddef, *ignore):
    """
    Returns the number stored in bytes as integer if field spec for
    decimals is 0, float otherwise
    """
    string = to_bytes(bytes).replace(b'\x00', b'').strip()
    cls = fielddef[CLASS]
    if not string or string[0:1] == b'*':  # value too big to store (Visual FoxPro idiocy)
        cls = fielddef[EMPTY]
        if cls is NoneType:
            return None
        if cls != 'default':
            return cls()
    if cls == 'default':
        if fielddef[DECIMALS] == 0:
            return string and int(string) or 0
        else:
            return string and float(string) or 0.0
    else:
        return cls(string)

def update_numeric(value, fielddef, *ignore):
    """
    returns value as ascii representation, rounding decimal
    portion as necessary
    """
    if value == None:
        return fielddef[LENGTH] * b' '
    try:
        value = float(value)
    except Exception:
        raise DbfError("incompatible type: %s(%s)" % (type(value), value)).from_exc(None)
    decimalsize = fielddef[DECIMALS]
    totalsize = fielddef[LENGTH]
    if decimalsize:
        decimalsize += 1
    maxintegersize = totalsize - decimalsize
    integersize = len("%.0f" % floor(value))
    if integersize > maxintegersize:
        if integersize != 1:
            raise DataOverflowError('Integer portion too big')
        string = scinot(value, decimalsize)
        if len(string) > totalsize:
            raise DataOverflowError('Value representation too long for field')
    return ("%*.*f" % (fielddef[LENGTH], fielddef[DECIMALS], value)).encode('ascii')

def retrieve_vfp_datetime(bytes, fielddef, *ignore):
    """
    returns the date/time stored in bytes; dates <= 01/01/1981 00:00:00
    may not be accurate;  BC dates are nulled.
    """
    # two four-byte integers store the date and time.
    # millesecords are discarded from time
    if bytes == array('B', [0] * 8):
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
    date = datetime.date.fromordinal(possible)
    return cls(date.year, date.month, date.day, time.hour, time.minute, time.second, time.microsecond)

def update_vfp_datetime(moment, *ignore):
    """
    Sets the date/time stored in moment
    moment must have fields:
        year, month, day, hour, minute, second, microsecond
    """
    data = [0] * 8
    if moment:
        hour = moment.hour
        minute = moment.minute
        second = moment.second
        millisecond = moment.microsecond // 1000       # convert from millionths to thousandths
        time = ((hour * 3600) + (minute * 60) + second) * 1000 + millisecond
        data[4:] = update_integer(time)
        data[:4] = update_integer(moment.toordinal() + VFPTIME)
    return to_bytes(data)

def retrieve_clp_timestamp(bytes, fielddef, *ignore):
    """
    returns the timestamp stored in bytes
    """
    # First long repesents date and second long time.
    # Date is the number of days since January 1st, 4713 BC.
    # Time is hours * 3600000L + minutes * 60000L + seconds * 1000L
    # http://www.manmrk.net/tutorials/database/xbase/data_types.html
    if bytes == array('B', [0] * 8):
        cls = fielddef[EMPTY]
        if cls is NoneType:
            return None
        return cls()
    cls = fielddef[CLASS]
    days = unpack_long_int(bytes[:4])
    # how many days between -4713-01-01 and 0001-01-01 ?  going to guess 1,721,425
    BC = 1721425
    if days < BC:
        # bail
        cls = fielddef[EMPTY]
        if cls is NoneType:
            return None
        return cls()
    date = datetime.date.fromordinal(days-BC)
    time = unpack_long_int(bytes[4:])
    microseconds = (time % 1000) * 1000
    time = time // 1000                      # int(round(time, -3)) // 1000 discard milliseconds
    hours = time // 3600
    mins = time % 3600 // 60
    secs = time % 3600 % 60
    time = datetime.time(hours, mins, secs, microseconds)
    return cls(date.year, date.month, date.day, time.hour, time.minute, time.second, time.microsecond)

def update_clp_timestamp(moment, *ignore):
    """
    Sets the timestamp stored in moment
    moment must have fields:
        year, month, day, hour, minute, second, microsecond
    """
    data = [0] * 8
    if moment:
        BC = 1721425
        days = BC + moment.toordinal()
        hour = moment.hour
        minute = moment.minute
        second = moment.second
        millisecond = moment.microsecond // 1000       # convert from millionths to thousandths
        time = ((hour * 3600) + (minute * 60) + second) * 1000 + millisecond
        data[:4] = pack_long_int(days)
        data[4:] = pack_long_int(time)
    return to_bytes(data)

def retrieve_vfp_memo(bytes, fielddef, memo, decoder):
    """
    Returns the block of data from a memo file
    """
    if memo is None:
        block = 0
    else:
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
    """
    Writes string as a memo, returns the block number it was saved into
    """
    if memo is None:
        raise DbfError('Memos are being ignored, unable to update')
    if string == None:
        return struct.pack('<i', 0)
    if fielddef[FLAGS] & BINARY:
        if not isinstance(string, bytes):
            raise ValueError('binary field: %r not in bytes format' % string)
        string = to_bytes(string)
    else:
        if not isinstance(string, basestring):
            raise ValueError("unable to coerce %r(%r) to string" % (type(string), string))
        string = encoder(string)[0]
    block = memo.put_memo(string)
    return struct.pack('<i', block)

def add_character(format, flags):
    if format[0][0] != '(' or format[0][-1] != ')' or any([f not in flags for f in format[1:]]):
        raise FieldSpecError("Format for Character field creation is 'C(n)%s', not 'C%s'" % field_spec_error_text(format, flags))
    length = int(format[0][1:-1])
    if not 0 < length < 256:
        raise FieldSpecError("Character fields must be between 1 and 255, not %d" % length)
    decimals = 0
    flag = 0
    for f in format[1:]:
        flag |= FieldFlag.lookup(f)
    return length, decimals, flag

def add_date(format, flags):
    if any([f not in flags for f in format]):
        raise FieldSpecError("Format for Date field creation is 'D%s', not 'D%s'" % field_spec_error_text(format, flags))
    length = 8
    decimals = 0
    flag = 0
    for f in format:
        flag |= FieldFlag.lookup(f)
    return length, decimals, flag

def add_logical(format, flags):
    if any([f not in flags for f in format]):
        raise FieldSpecError("Format for Logical field creation is 'L%s', not 'L%s'" % field_spec_error_text(format, flags))
    length = 1
    decimals = 0
    flag = 0
    for f in format:
        flag |= FieldFlag.lookup(f)
    return length, decimals, flag

def add_memo(format, flags):
    if any(f not in flags for f in format):
        raise FieldSpecError("Format for Memo field creation is 'M%s', not 'M%s'" % field_spec_error_text(format, flags))
    length = 10
    decimals = 0
    flag = 0
    for f in format:
        flag |= FieldFlag.lookup(f)
    return length, decimals, flag

def add_binary_memo(format, flags):
    if any(f not in flags for f in format):
        raise FieldSpecError("Format for Memo field creation is 'M%s', not 'M%s'" % field_spec_error_text(format, flags))
    length = 10
    decimals = 0
    flag = 0
    for f in format:
        flag |= FieldFlag.lookup(f)
    flag |= FieldFlag.BINARY
    return length, decimals, flag

def add_numeric(format, flags):
    if len(format) > 1 or format[0][0] != '(' or format[0][-1] != ')' or any(f not in flags for f in format[1:]):
        raise FieldSpecError("Format for Numeric field creation is 'N(s,d)%s', not 'N%s'" % field_spec_error_text(format, flags))
    length, decimals = format[0][1:-1].split(',')
    length = int(length)
    decimals = int(decimals)
    flag = 0
    for f in format[1:]:
        flag |= FieldFlag.lookup(f)
    if not 0 < length <= 20:
        raise FieldSpecError("Numeric fields must be between 1 and 20 digits, not %d" % length)
    if decimals and not 0 < decimals <= length - 2:
        raise FieldSpecError("Decimals must be between 0 and Length-2 (Length: %d, Decimals: %d)" % (length, decimals))
    return length, decimals, flag

def add_clp_character(format, flags):
    if format[0][0] != '(' or format[0][-1] != ')' or any([f not in flags for f in format[1:]]):
        raise FieldSpecError("Format for Character field creation is 'C(n)%s', not 'C%s'" % field_spec_error_text(format, flags))
    length = int(format[0][1:-1])
    if not 0 < length < 65519:
        raise FieldSpecError("Character fields must be between 1 and 65,519")
    decimals = 0
    flag = 0
    for f in format[1:]:
        flag |= FieldFlag.lookup(f)
    return length, decimals, flag

def add_vfp_character(format, flags):
    if format[0][0] != '(' or format[0][-1] != ')' or any([f not in flags for f in format[1:]]):
        raise FieldSpecError("Format for Character field creation is 'C(n)%s', not 'C%s'" % field_spec_error_text(format, flags))
    length = int(format[0][1:-1])
    if not 0 < length < 255:
        raise FieldSpecError("Character fields must be between 1 and 255")
    decimals = 0
    flag = 0
    for f in format[1:]:
        flag |= FieldFlag.lookup(f)
    return length, decimals, flag

def add_vfp_currency(format, flags):
    if any(f not in flags for f in format):
        raise FieldSpecError("Format for Currency field creation is 'Y%s', not 'Y%s'" % field_spec_error_text(format, flags))
    length = 8
    decimals = 0
    flag = 0
    for f in format:
        flag |= FieldFlag.lookup(f)
    return length, decimals, flag

def add_vfp_datetime(format, flags):
    if any(f not in flags for f in format):
        raise FieldSpecError("Format for DateTime field creation is 'T%s', not 'T%s'" % field_spec_error_text(format, flags))
    length = 8
    decimals = 0
    flag = 0
    for f in format:
        flag |= FieldFlag.lookup(f)
    return length, decimals, flag

def add_vfp_double(format, flags):
    if any(f not in flags for f in format):
        raise FieldSpecError("Format for Double field creation is 'B%s', not 'B%s'" % field_spec_error_text(format, flags))
    length = 8
    decimals = 0
    flag = 0
    for f in format:
        flag |= FieldFlag.lookup(f)
    return length, decimals, flag

def add_vfp_integer(format, flags):
    if any(f not in flags for f in format):
        raise FieldSpecError("Format for Integer field creation is 'I%s', not 'I%s'" % field_spec_error_text(format, flags))
    length = 4
    decimals = 0
    flag = 0
    for f in format:
        flag |= FieldFlag.lookup(f)
    return length, decimals, flag

def add_vfp_memo(format, flags):
    if any(f not in flags for f in format):
        raise FieldSpecError("Format for Memo field creation is 'M%s', not 'M%s'" % field_spec_error_text(format, flags))
    length = 4
    decimals = 0
    flag = 0
    for f in format:
        flag |= FieldFlag.lookup(f)
    if 'BINARY' not in flags:   # general or picture -- binary is implied
        flag |= FieldFlag.BINARY
    return length, decimals, flag

def add_vfp_binary_memo(format, flags):
    if any(f not in flags for f in format):
        raise FieldSpecError("Format for Memo field creation is 'M%s', not 'M%s'" % field_spec_error_text(format, flags))
    length = 4
    decimals = 0
    flag = 0
    for f in format:
        flag |= FieldFlag.lookup(f)
    # general or picture -- binary is implied
    flag |= FieldFlag.BINARY
    return length, decimals, flag

def add_vfp_numeric(format, flags):
    if format[0][0] != '(' or format[0][-1] != ')' or any(f not in flags for f in format[1:]):
        raise FieldSpecError("Format for Numeric field creation is 'N(s,d)%s', not 'N%s'" % field_spec_error_text(format, flags))
    length, decimals = format[0][1:-1].split(',')
    length = int(length)
    decimals = int(decimals)
    flag = 0
    for f in format[1:]:
        flag |= FieldFlag.lookup(f)
    if not 0 < length < 21:
        raise FieldSpecError("Numeric fields must be between 1 and 20 digits, not %d" % length)
    if decimals and not 0 < decimals <= length - 2:
        raise FieldSpecError("Decimals must be between 0 and Length-2 (Length: %d, Decimals: %d)" % (length, decimals))
    return length, decimals, flag

def add_clp_timestamp(format, flags):
    if any(f not in flags for f in format):
        raise FieldSpecError("Format for TimeStamp field creation is '@%s', not '@%s'" % field_spec_error_text(format, flags))
    length = 8
    decimals = 0
    flag = 0
    for f in format[1:]:
        flag |= FieldFlag.lookup(f)
    return length, decimals, flag

def field_spec_error_text(format, flags):
    """
    generic routine for error text for the add...() functions
    """
    flg = ''
    if flags:
        flg = ' [ ' + ' | '.join(flags) + ' ]'
    frmt = ''
    if format:
        frmt = ' ' + ' '.join(format)
    return flg, frmt

def ezip(*iters):
    """
    extends all iters to longest one, using last value from each as necessary
    """
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

def unicode_error_handler(decoder, encoder, errors):
    if errors in ('ignore', 'replace'):
        decoder = partial(decoder, errors=errors)
        encoder = partial(encoder, errors=errors)
    elif errors in ('xmlcharrefreplace', 'backslashreplace'):
        decoder = partial(decoder, errors='replace')
        encoder = partial(encoder, errors=errors)
    return decoder, encoder

# Public classes

class Tables(object):
    """
    context manager for multiple tables and/or indices
    """
    def __init__(self, *tables):
        if len(tables) == 1 and not isinstance(tables[0], (Table, basestring)):
            tables = tables[0]
        self._tables = []
        self._entered = []
        for table in tables:
            if isinstance(table, basestring):
                table = Table(table)
            self._tables.append(table)
    def __enter__(self):
        for table in self._tables:
            table.__enter__()
            self._entered.append(table)
        return tuple(self._tables)
    def __exit__(self, *args):
        while self._entered:
            table = self._entered.pop()
            try:
                table.__exit__()
            except Exception:
                pass

class Index(_Navigation):
    """
    non-persistent index for a table
    """

    def __init__(self, table, key):
        self._table = table
        self._values = []             # ordered list of values
        self._rec_by_val = []         # matching record numbers
        self._records = {}            # record numbers:values
        self.__doc__ = key.__doc__ or 'unknown'
        self._key = key
        self._previous_status = []
        for record in table:
            value = key(record)
            if value is DoNotIndex:
                continue
            rec_num = recno(record)
            if not isinstance(value, tuple):
                value = (value, )
            vindex = bisect_right(self._values, value)
            self._values.insert(vindex, value)
            self._rec_by_val.insert(vindex, rec_num)
            self._records[rec_num] = value
        table._indexen.add(self)

    def __call__(self, record):
        rec_num = recno(record)
        key = self.key(record)
        if rec_num in self._records:
            if self._records[rec_num] == key:
                return
            old_key = self._records[rec_num]
            vindex = bisect_left(self._values, old_key)
            self._values.pop(vindex)
            self._rec_by_val.pop(vindex)
            del self._records[rec_num]
            assert rec_num not in self._records
        if key == (DoNotIndex, ):
            return
        vindex = bisect_right(self._values, key)
        self._values.insert(vindex, key)
        self._rec_by_val.insert(vindex, rec_num)
        self._records[rec_num] = key

    def __contains__(self, data):
        if not isinstance(data, (Record, RecordTemplate, tuple, dict)):
            raise TypeError("%r is not a record, templace, tuple, nor dict" % (data, ))
        try:
            value = self.key(data)
            return value in self._values
        except Exception:
            for record in self:
                if record == data:
                    return True
            return False

    def __getitem__(self, key):
        '''if key is an integer, returns the matching record;
        if key is a [slice | string | tuple | record] returns a List;
        raises NotFoundError on failure'''
        if isinstance(key, baseinteger):
            count = len(self._values)
            if not -count <= key < count:
                raise NotFoundError("Record %d is not in list." % key)
            rec_num = self._rec_by_val[key]
            return self._table[rec_num]
        elif isinstance(key, slice):
            result = List()
            start, stop, step = key.start, key.stop, key.step
            if start is None: start = 0
            if stop is None: stop = len(self._rec_by_val)
            if step is None: step = 1
            if step < 0:
                start, stop = stop - 1, -(stop - start + 1)
            for loc in range(start, stop, step):
                record = self._table[self._rec_by_val[loc]]
                result._maybe_add(item=(self._table, self._rec_by_val[loc], result.key(record)))
            return result
        elif isinstance (key, (basestring, tuple, Record, RecordTemplate)):
            if isinstance(key, (Record, RecordTemplate)):
                key = self.key(key)
            elif isinstance(key, basestring):
                key = (key, )
            lo = self._search(key, where='left')
            hi = self._search(key, where='right')
            if lo == hi:
                raise NotFoundError(key)
            result = List(desc='match = %r' % (key, ))
            for loc in range(lo, hi):
                record = self._table[self._rec_by_val[loc]]
                result._maybe_add(item=(self._table, self._rec_by_val[loc], result.key(record)))
            return result
        else:
            raise TypeError('indices must be integers, match objects must by strings or tuples')

    def __enter__(self):
        self._table.__enter__()
        return self

    def __exit__(self, *exc_info):
        self._table.__exit__()
        return False

    def __iter__(self):
        return Iter(self)

    def __len__(self):
        return len(self._records)

    def _clear(self):
        """
        removes all entries from index
        """
        self._values[:] = []
        self._rec_by_val[:] = []
        self._records.clear()

    def _key(self, record):
        """
        table_name, record_number
        """
        self._still_valid_check()
        return source_table(record), recno(record)

    def _nav_check(self):
        """
        raises error if table is closed
        """
        if self._table._meta.status == CLOSED:
            raise DbfError('indexed table %s is closed' % self.filename)

    def _partial_match(self, target, match):
        target = target[:len(match)]
        if isinstance(match[-1], basestring):
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

    def _reindex(self):
        """
        reindexes all records
        """
        for record in self._table:
            self(record)

    def _search(self, match, lo=0, hi=None, where=None):
        if hi is None:
            hi = len(self._values)
        if where == 'left':
            return bisect_left(self._values, match, lo, hi)
        elif where == 'right':
            return bisect_right(self._values, match, lo, hi)

    def index(self, record, start=None, stop=None):
        """
        returns the index of record between start and stop
        start and stop default to the first and last record
        """
        if not isinstance(record, (Record, RecordTemplate, dict, tuple)):
            raise TypeError("x should be a record, template, dict, or tuple, not %r" % type(record))
        self._nav_check()
        if start is None:
            start = 0
        if stop is None:
            stop = len(self)
        for i in range(start, stop):
            if record == (self[i]):
                return i
        else:
            raise NotFoundError("dbf.Index.index(x): x not in Index", data=record)

    def index_search(self, match, start=None, stop=None, nearest=False, partial=False):
        """
        returns the index of match between start and stop
        start and stop default to the first and last record.
        if nearest is true returns the location of where the match should be
        otherwise raises NotFoundError
        """
        self._nav_check()
        if not isinstance(match, tuple):
            match = (match, )
        if start is None:
            start = 0
        if stop is None:
            stop = len(self)
        loc = self._search(match, start, stop, where='left')
        if loc == len(self._values):
            if nearest:
                return IndexLocation(loc, False)
            raise NotFoundError("dbf.Index.index_search(x): x not in index", data=match)
        if self._values[loc] == match \
        or partial and self._partial_match(self._values[loc], match):
            return IndexLocation(loc, True)
        elif nearest:
            return IndexLocation(loc, False)
        else:
            raise NotFoundError("dbf.Index.index_search(x): x not in Index", data=match)

    def key(self, record):
        result = self._key(record)
        if not isinstance(result, tuple):
            result = (result, )
        return result

    def query(self, criteria):
        """
        criteria is a callback that returns a truthy value for matching record
        """
        self._nav_check()
        return pqlc(self, criteria)

    def search(self, match, partial=False):
        """
        returns dbf.List of all (partially) matching records
        """
        self._nav_check()
        result = List()
        if not isinstance(match, tuple):
            match = (match, )
        loc = self._search(match, where='left')
        if loc == len(self._values):
            return result
        while loc < len(self._values) and self._values[loc] == match:
            record = self._table[self._rec_by_val[loc]]
            result._maybe_add(item=(self._table, self._rec_by_val[loc], result.key(record)))
            loc += 1
        if partial:
            while loc < len(self._values) and self._partial_match(self._values[loc], match):
                record = self._table[self._rec_by_val[loc]]
                result._maybe_add(item=(self._table, self._rec_by_val[loc], result.key(record)))
                loc += 1
        return result


class Relation(object):
    """
    establishes a relation between two dbf tables (not persistent)
    """

    relations = {}

    def __new__(cls, src, tgt, src_names=None, tgt_names=None):
        if (len(src) != 2 or  len(tgt) != 2):
            raise DbfError("Relation should be called with ((src_table, src_field), (tgt_table, tgt_field))")
        if src_names and len(src_names) !=2 or tgt_names and len(tgt_names) != 2:
            raise DbfError('src_names and tgt_names, if specified, must be ("table","field")')
        src_table, src_field = src
        tgt_table, tgt_field = tgt
        try:
            if isinstance(src_field, baseinteger):
                table, field = src_table, src_field
                src_field = table.field_names[field]
            else:
                src_table.field_names.index(src_field)
            if isinstance(tgt_field, baseinteger):
                table, field = tgt_table, tgt_field
                tgt_field = table.field_names[field]
            else:
                tgt_table.field_names.index(tgt_field)
        except (IndexError, ValueError):
            raise DbfError('%r not in %r' % (field, table)).from_exc(None)
        if src_names:
            src_table_name, src_field_name = src_names
        else:
            src_table_name, src_field_name = src_table.filename, src_field
            if src_table_name[-4:].lower() == '.dbf':
                src_table_name = src_table_name[:-4]
        if tgt_names:
            tgt_table_name, tgt_field_name = tgt_names
        else:
            tgt_table_name, tgt_field_name = tgt_table.filename, tgt_field
            if tgt_table_name[-4:].lower() == '.dbf':
                tgt_table_name = tgt_table_name[:-4]
        relation = cls.relations.get(((src_table, src_field), (tgt_table, tgt_field)))
        if relation is not None:
            return relation
        obj = object.__new__(cls)
        obj._src_table, obj._src_field = src_table, src_field
        obj._tgt_table, obj._tgt_field = tgt_table, tgt_field
        obj._src_table_name, obj._src_field_name = src_table_name, src_field_name
        obj._tgt_table_name, obj._tgt_field_name = tgt_table_name, tgt_field_name
        obj._tables = dict()
        cls.relations[((src_table, src_field), (tgt_table, tgt_field))] = obj
        return obj

    def __eq__(self, other):
        if (self.src_table == other.src_table
        and self.src_field == other.src_field
        and self.tgt_table == other.tgt_table
        and self.tgt_field == other.tgt_field):
            return True
        return False

    def __getitem__(self, record):
        """
        record should be from the source table
        """
        key = (record[self.src_field], )
        try:
            return self.index[key]
        except NotFoundError:
            return List(desc='%s not found' % key)

    def __hash__(self):
        return hash((self.src_table, self.src_field, self.tgt_table, self.tgt_field))

    def __ne__(self, other):
        if (self.src_table != other.src_table
        or  self.src_field != other.src_field
        or  self.tgt_table != other.tgt_table
        or  self.tgt_field != other.tgt_field):
            return True
        return False

    def __repr__(self):
        return "Relation((%r, %r), (%r, %r))" % (self.src_table_name, self.src_field, self.tgt_table_name, self.tgt_field)

    def __str__(self):
        return "%s:%s --> %s:%s" % (self.src_table_name, self.src_field_name, self.tgt_table_name, self.tgt_field_name)

    @property
    def src_table(self):
        "name of source table"
        return self._src_table

    @property
    def src_field(self):
        "name of source field"
        return self._src_field

    @property
    def src_table_name(self):
        return self._src_table_name

    @property
    def src_field_name(self):
        return self._src_field_name

    @property
    def tgt_table(self):
        "name of target table"
        return self._tgt_table

    @property
    def tgt_field(self):
        "name of target field"
        return self._tgt_field

    @property
    def tgt_table_name(self):
        return self._tgt_table_name

    @property
    def tgt_field_name(self):
        return self._tgt_field_name

    @LazyAttr
    def index(self):
        def index(record, field=self._tgt_field):
            return record[field]
        index.__doc__ = "%s:%s --> %s:%s" % (self.src_table_name, self.src_field_name, self.tgt_table_name, self.tgt_field_name)
        self.index = self._tgt_table.create_index(index)
        source = List(self._src_table, key=lambda rec, field=self._src_field: rec[field])
        target = List(self._tgt_table, key=lambda rec, field=self._tgt_field: rec[field])
        if len(source) != len(self._src_table):
            self._tables[self._src_table] = 'many'
        else:
            self._tables[self._src_table] = 'one'
        if len(target) != len(self._tgt_table):
            self._tables[self._tgt_table] = 'many'
        else:
            self._tables[self._tgt_table] = 'one'
        return self.index

    def one_or_many(self, table):
        self.index    # make sure self._tables has been populated
        try:
            if isinstance(table, basestring):
                table = (self._src_table, self._tgt_table)[self._tgt_table_name == table]
            return self._tables[table]
        except IndexError:
            raise NotFoundError("table %s not in relation" % table).from_exc(None)

class IndexLocation(long):
    """
    Represents the index where the match criteria is if True,
    or would be if False

    Used by Index.index_search
    """

    def __new__(cls, value, found):
        "value is the number, found is True/False"
        result = long.__new__(cls, value)
        result.found = found
        return result

    if py_ver < (3, 0):
        def __nonzero__(self):
            return self.found
    else:
        def __bool__(self):
            return self.found


class FieldInfo(NamedTuple):
    """
    tuple with named attributes for representing a field's dbf type,
    length, decimal portion, and python class
    """
    field_type = 0, "dbf field type (C, N, D, etc.)"
    length = 1, "overall length of field"
    decimal = 2, "number of decimal places for numeric fields"
    py_type = 3, "Python class for this field (Char, Logical, default, etc.)"


class CodePage(NamedTuple):
    """
    tuple with named attributes for representing a tables codepage
    """
    def __new__(cls, name):
        "call with name of codepage (e.g. 'cp1252')"
        code, name, desc = _codepage_lookup(name)
        return tuple.__new__(cls, (name, desc, code))

    def __repr__(self):
        return "CodePage(%r, %r, %02x)" % self

    def __str__(self):
        return "%s (%s)" % (self[0], self[1])

    name = 0, "name of code page"
    desc = 1, "description of code page"
    code = 2, "numeric code of code page"


class Iter(_Navigation):
    """
    Provides iterable behavior for a table
    """

    def __init__(self, table, include_vapor=False):
        """
        Return a Vapor record as the last record in the iteration
        if include_vapor is True
        """
        self._table = table
        self._record = None
        self._include_vapor = include_vapor
        self._exhausted = False

    def __iter__(self):
        return self

    if py_ver < (3, 0):
        def next(self):
            while not self._exhausted:
                if self._index == len(self._table):
                    break
                if self._index >= (len(self._table) - 1):
                    self._index = max(self._index, len(self._table))
                    if self._include_vapor:
                        return RecordVaporWare('eof', self._table)
                    break
                self._index += 1
                record = self._table[self._index]
                return record
            self._exhausted = True
            raise StopIteration
    else:
        def __next__(self):
            while not self._exhausted:
                if self._index == len(self._table):
                    break
                if self._index >= (len(self._table) - 1):
                    self._index = max(self._index, len(self._table))
                    if self._include_vapor:
                        return RecordVaporWare('eof', self._table)
                    break
                self._index += 1
                record = self._table[self._index]
                return record
            self._exhausted = True
            raise StopIteration


class Table(_Navigation):
    """
    Base class for dbf style tables
    """

    _version = 'basic memory table'
    _versionabbr = 'dbf'
    _max_fields = 255
    _max_records = 4294967296

    @MutableDefault
    def _field_types():
        return {
                CHAR: {
                        'Type':'Character', 'Init':add_character, 'Blank':lambda x: b' ' * x, 'Retrieve':retrieve_character, 'Update':update_character,
                        'Class':unicode, 'Empty':unicode, 'flags':tuple(),
                        },
                DATE: {
                        'Type':'Date', 'Init':add_date, 'Blank':lambda x: b'        ', 'Retrieve':retrieve_date, 'Update':update_date,
                        'Class':datetime.date, 'Empty':none, 'flags':tuple(),
                        },
                NUMERIC: {
                        'Type':'Numeric', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':lambda x: b' ' * x, 'Init':add_numeric,
                        'Class':'default', 'Empty':none, 'flags':tuple(),
                        },
                LOGICAL: {
                        'Type':'Logical', 'Init':add_logical, 'Blank':lambda x: b'?', 'Retrieve':retrieve_logical, 'Update':update_logical,
                        'Class':bool, 'Empty':none, 'flags':tuple(),
                        },
                MEMO: {
                        'Type':'Memo', 'Init':add_memo, 'Blank':lambda x: b'          ', 'Retrieve':retrieve_memo, 'Update':update_memo,
                        'Class':unicode, 'Empty':unicode, 'flags':tuple(),
                        },
                FLOAT: {
                        'Type':'Numeric', 'Init':add_numeric, 'Blank':lambda x: b' ' * x, 'Retrieve':retrieve_numeric, 'Update':update_numeric,
                        'Class':'default', 'Empty':none, 'flags':tuple(),
                        },
                }
    @MutableDefault
    def _previous_status():
        return []
    _memoext = ''
    _memoClass = _DbfMemo
    _yesMemoMask = 0
    _noMemoMask = 0
    _binary_types = tuple()                # as in non-unicode character, or non-text number
    _character_types = (CHAR, DATE, FLOAT, LOGICAL, MEMO, NUMERIC)          # field represented by text data
    _currency_types = tuple()              # money!
    _date_types = (DATE, )                  # dates
    _datetime_types = tuple()              # dates w/times
    _decimal_types = (NUMERIC, FLOAT)            # text-based numeric fields
    _fixed_types = (MEMO, DATE, LOGICAL)           # always same length in table
    _logical_types = (LOGICAL, )               # logicals
    _memo_types = (MEMO, )
    _numeric_types = (NUMERIC, FLOAT)            # fields representing a number
    _variable_types = (CHAR, NUMERIC, FLOAT)      # variable length in table
    _dbfTableHeader = array('B', [0] * 32)
    _dbfTableHeader[0] = 0              # table type - none
    _dbfTableHeader[8:10] = array('B', pack_short_int(33))
    _dbfTableHeader[10] = 1             # record length -- one for delete flag
    _dbfTableHeader[29] = 0             # code page -- none, using plain ascii
    # _dbfTableHeader = to_bytes(_dbfTableHeader)
    _dbfTableHeaderExtra = b''
    _supported_tables = ()
    _pack_count = 0
    backup = None

    class _Indexen(object):
        """
        implements the weakref structure for seperate indexes
        """

        def __init__(self):
            self._indexen = set()

        def __iter__(self):
            self._indexen = set([s for s in self._indexen if s() is not None])
            return (s() for s in self._indexen if s() is not None)

        def __len__(self):
            self._indexen = set([s for s in self._indexen if s() is not None])
            return len(self._indexen)

        def add(self, new_index):
            self._indexen.add(weakref.ref(new_index))
            self._indexen = set([s for s in self._indexen if s() is not None])

    class _MetaData(dict):
        """
        Container class for storing per table metadata
        """
        blankrecord = None
        codepage = None           # code page being used (can be overridden when table is opened)
        dfd = None                # file handle
        fields = None             # field names
        field_count = 0           # number of fields
        field_types = None        # dictionary of dbf type field specs
        filename = None           # name of .dbf file
        ignorememos = False       # True when memos should be ignored
        memoname = None           # name of .dbt/.fpt file
        memo_size = None           # size of blocks in memo file
        mfd = None                # file handle
        memo = None               # memo object
        memofields = None         # field names of Memo type
        newmemofile = False       # True when memo file needs to be created
        nulls = None              # non-None when Nullable fields present
        user_fields = None        # not counting SYSTEM fields
        user_field_count = 0      # also not counting SYSTEM fields
        unicode_errors = 'strict' # default to strict unicode translations
        status = CLOSED           # until we open it

    class _TableHeader(object):
        """
        represents the data block that defines a tables type and layout
        """

        def __init__(self, data, pack_date, unpack_date):
            if len(data) != 32:
                raise BadDataError('table header should be 32 bytes, but is %d bytes' % len(data))
            self.packDate = pack_date
            self.unpackDate = unpack_date
            self._data = array('B', data + CR)

        def codepage(self, cp=None):
            """
            get/set code page of table
            """
            if cp is None:
                return self._data[29]
            else:
                cp, sd, ld = _codepage_lookup(cp)
                self._data[29] = cp
                return cp

        @property
        def data(self):
            """
            main data structure
            """
            date = self.packDate(Date.today())
            self._data[1:4] = array('B', date)
            return self._data
            # return to_bytes(self._data)

        @data.setter
        def data(self, bytes):
            if len(bytes) < 32:
                raise BadDataError("length for data of %d is less than 32" % len(bytes))
            self._data[:] = array('B', bytes)

        @property
        def extra(self):
            "extra dbf info (located after headers, before data records)"
            fieldblock = self._data[32:]
            for i in range(len(fieldblock) // 32 + 1):
                cr = i * 32
                if fieldblock[cr] == CR:
                    break
            else:
                raise BadDataError("corrupt field structure")
            cr += 33    # skip past CR
            return self._data[cr:]
            # return to_bytes(self._data[cr:])

        @extra.setter
        def extra(self, data):
            data = array('B', data)
            fieldblock = self._data[32:]
            for i in range(len(fieldblock) // 32 + 1):
                cr = i * 32
                if fieldblock[cr] == CR:
                    break
            else:
                raise BadDataError("corrupt field structure")
            cr += 33    # skip past CR
            self._data[cr:] = data
            self._data[8:10] = array('B', pack_short_int(len(self._data)))  # start

        @property
        def field_count(self):
            "number of fields (read-only)"
            fieldblock = self._data[32:]
            for i in range(len(fieldblock) // 32 + 1):
                cr = i * 32
                if fieldblock[cr] == CR:
                    break
            else:
                raise BadDataError("corrupt field structure")
            return len(fieldblock[:cr]) // 32

        @property
        def fields(self):
            """
            field block structure
            """
            fieldblock = self._data[32:]
            for i in range(len(fieldblock) // 32 + 1):
                cr = i * 32
                if fieldblock[cr] == CR:
                    break
            else:
                raise BadDataError("corrupt field structure")
            return fieldblock[:cr]
            # return to_bytes(fieldblock[:cr])

        @fields.setter
        def fields(self, block):
            if isinstance(block, bytes):
                block = array('B', block)
            fieldblock = self._data[32:]
            for i in range(len(fieldblock) // 32 + 1):
                cr = i * 32
                if fieldblock[cr] == CR:
                    break
            else:
                raise BadDataError("corrupt field structure")
            cr += 32    # convert to indexing main structure
            fieldlen = len(block)
            if fieldlen % 32 != 0:
                raise BadDataError("fields structure corrupt: %d is not a multiple of 32" % fieldlen)
            self._data[32:cr] = array('B', block)                           # fields
            self._data[8:10] = array('B', pack_short_int(len(self._data)))   # start
            fieldlen = fieldlen // 32
            recordlen = 1                                     # deleted flag
            for i in range(fieldlen):
                recordlen += block[i*32+16]
            self._data[10:12] = array('B', pack_short_int(recordlen))

        @property
        def record_count(self):
            """
            number of records (maximum 16,777,215)
            """
            return unpack_long_int(to_bytes(self._data[4:8]))

        @record_count.setter
        def record_count(self, count):
            self._data[4:8] = array('B', pack_long_int(count))

        @property
        def record_length(self):
            """
            length of a record (read_only) (max of 65,535)
            """
            return unpack_short_int(to_bytes(self._data[10:12]))

        @record_length.setter
        def record_length(self, length):
            """
            to support Clipper large Character fields
            """
            self._data[10:12] = array('B', pack_short_int(length))

        @property
        def start(self):
            """
            starting position of first record in file (must be within first 64K)
            """
            return unpack_short_int(to_bytes(self._data[8:10]))

        @start.setter
        def start(self, pos):
            self._data[8:10] = array('B', pack_short_int(pos))

        @property
        def update(self):
            """
            date of last table modification (read-only)
            """
            return self.unpackDate(to_bytes(self._data[1:4]))

        @property
        def version(self):
            """
            dbf version
            """
            return self._data[0]

        @version.setter
        def version(self, ver):
            self._data[0] = ver

    class _Table(object):
        """
        implements the weakref table for records
        """

        def __init__(self, count, meta):
            self._meta = meta
            self._max_count = count
            self._weakref_list = {}
            self._accesses = 0
            self._dead_check = 1024

        def __getitem__(self, index):
            # maybe = self._weakref_list[index]()
            if index < 0:
                if self._max_count + index < 0:
                    raise IndexError('index %d smaller than available records' % index)
                index = self._max_count + index
            if index >= self._max_count:
                raise IndexError('index %d greater than available records' % index)
            maybe = self._weakref_list.get(index)
            if maybe:
                maybe = maybe()
            self._accesses += 1
            if self._accesses >= self._dead_check:
                for key, value in list(self._weakref_list.items()):
                    if value() is None:
                        del self._weakref_list[key]
            if not maybe:
                meta = self._meta
                if meta.status == CLOSED:
                    raise DbfError("%s is closed; record %d is unavailable" % (meta.filename, index))
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
                maybe = Record(recnum=index, layout=meta, kamikaze=bytes, _fromdisk=True)
                self._weakref_list[index] = weakref.ref(maybe)
            return maybe

        def append(self, record):
            self._weakref_list[self._max_count] = weakref.ref(record)
            self._max_count += 1

        def clear(self):
            for key in list(self._weakref_list.keys()):
                del self._weakref_list[key]
            self._max_count = 0

        def flush(self):
            for maybe in self._weakref_list.values():
                maybe = maybe()
                if maybe and not maybe._write_to_disk:
                    raise DbfError("some records have not been written to disk")

        def pop(self):
            if not self._max_count:
                raise IndexError('no records exist')
            self._max_count -= 1
            record = self._weakref_list[self._max_count]
            del self._weakref_list[self._max_count]
            return record

    def _build_header_fields(self):
        """
        constructs fieldblock for disk table
        """
        fieldblock = array('B', b'')
        memo = False
        nulls = False
        meta = self._meta
        header = meta.header
        if self._yesMemoMask <= 0x80:
            header.version = header.version & self._noMemoMask
        else:
            header.version = self._noMemoMask
        meta.fields = [f for f in meta.fields if f != '_NULLFLAGS']
        for field in meta.fields:
            layout = meta[field]
            if meta.fields.count(field) > 1:
                raise BadDataError("corrupted field structure (noticed in _build_header_fields)")
            fielddef = array('B', [0] * 32)
            fielddef[:11] = array('B', pack_str(meta.encoder(field)[0]))
            fielddef[11] = layout[TYPE]
            fielddef[12:16] = array('B', pack_long_int(layout[START]))
            fielddef[16] = layout[LENGTH]
            fielddef[17] = layout[DECIMALS]
            fielddef[18] = layout[FLAGS]
            fieldblock.extend(fielddef)
            if layout[TYPE] in meta.memo_types:
                memo = True
            if layout[FLAGS] & NULLABLE:
                nulls += 1
        if memo:
            if self._yesMemoMask <= 0x80:
                header.version = header.version | self._yesMemoMask
            else:
                header.version = self._yesMemoMask
            if meta.memo is None:
                meta.memo = self._memoClass(meta)
        else:
            if os.path.exists(meta.memoname):
                if meta.mfd is not None:
                    meta.mfd.close()
                os.remove(meta.memoname)
            meta.memo = None
        if nulls:
            start = layout[START] + layout[LENGTH]
            length, one_more = divmod(nulls, 8)
            if one_more:
                length += 1
            fielddef = array('B', [0] * 32)
            fielddef[:11] = array('B', pack_str(b'_NULLFLAGS'))
            fielddef[11] = 0x30
            fielddef[12:16] = array('B', pack_long_int(start))
            fielddef[16] = length
            fielddef[17] = 0
            fielddef[18] = BINARY | SYSTEM
            fieldblock.extend(fielddef)
            meta.fields.append('_NULLFLAGS')
            nullflags = (
                    _NULLFLAG,          # type
                    start,              # start
                    length,             # length
                    start + length,     # end
                    0,                  # decimals
                    BINARY | SYSTEM,    # flags
                    none,               # class
                    none,               # empty
                    )
            meta['_NULLFLAGS'] = nullflags
        # header.fields = to_bytes(fieldblock)
        header.fields = fieldblock
        meta.user_fields = FieldnameList([f for f in meta.fields if not meta[f][FLAGS] & SYSTEM])
        meta.user_field_count = len(meta.user_fields)
        Record._create_blank_data(meta)

    def _check_memo_integrity(self):
        """
        checks memo file for problems
        """
        raise NotImplementedError("_check_memo_integrity must be implemented by subclass")

    def _initialize_fields(self):
        """
        builds the FieldList of names, types, and descriptions from the disk file
        """
        raise NotImplementedError("_initialize_fields must be implemented by subclass")

    def _field_layout(self, i):
        """
        Returns field information Name Type(Length[, Decimals])
        """
        name = self._meta.fields[i]
        fielddef = self._meta[name]
        type = FieldType(fielddef[TYPE])
        length = fielddef[LENGTH]
        decimals = fielddef[DECIMALS]
        set_flags = fielddef[FLAGS]
        flags = []
        if type in (GENERAL, PICTURE):
            printable_flags = NULLABLE, SYSTEM
        else:
            printable_flags = BINARY, NULLABLE, SYSTEM
        for flg in printable_flags:
            if flg & set_flags == flg:
                flags.append(FieldFlag(flg))
                set_flags &= 255 ^ flg
        if flags:
            flags = ' ' + ' '.join(f.text for f in flags)
        else:
            flags = ''
        if type in self._fixed_types:
            description = "%s %s%s" % (name, type.symbol, flags)
        elif type in self._numeric_types:
            description = "%s %s(%d,%d)%s" % (name, type.symbol, length, decimals, flags)
        else:
            description = "%s %s(%d)%s" % (name, type.symbol, length, flags)
        return description

    def _list_fields(self, specs, sep=','):
        """
        standardizes field specs
        """
        if specs is None:
            specs = self.field_names
        elif isinstance(specs, basestring):
            specs = specs.strip(sep).split(sep)
        else:
            specs = list(specs)
        specs = [s.strip() for s in specs]
        for i, s in enumerate(specs):
            if isinstance(s, bytes):
                specs[i] = s.decode(dbf.input_decoding)
        return FieldnameList(specs)

    def _nav_check(self):
        """
        Raises `DbfError` if table is closed
        """
        if self._meta.status == CLOSED:
            raise DbfError('table %s is closed' % self.filename)

    @staticmethod
    def _pack_date(date):
        """
        Returns a group of three bytes, in integer form, of the date
        """
        return array('B', [date.year - 1900, date.month, date.day])

    @staticmethod
    def _unpack_date(bytestr):
        """
        Returns a Date() of the packed three-byte date passed in
        """
        year, month, day = struct.unpack('<BBB', bytestr)
        year += 1900
        return Date(year, month, day)

    def _update_disk(self, headeronly=False):
        """
        synchronizes the disk file with current data
        """
        if self._meta.location == IN_MEMORY:
            return
        meta = self._meta
        header = meta.header
        fd = meta.dfd
        fd.seek(0)
        fd.write(header.data)
        eof = header.start + header.record_count * header.record_length
        if not headeronly:
            for record in self:
                record._update_disk()
            fd.flush()
            fd.truncate(eof)
        if self._versionabbr in ('db3', 'clp'):
            fd.seek(0, SEEK_END)
            fd.write(b'\x1a')        # required for dBase III compatibility
            fd.flush()
            fd.truncate(eof + 1)

    def __contains__(self, data):
        """
        data can be a record, template, dict, or tuple
        """
        if not isinstance(data, (Record, RecordTemplate, dict, tuple)):
            raise TypeError("x should be a record, template, dict, or tuple, not %r" % type(data))
        for record in Iter(self):
            if data == record:
                return True
        return False

    def __enter__(self):
        self._previous_status.append(self._meta.status)
        self.open(READ_WRITE)
        return self

    def __exit__(self, *exc_info):
        if self._previous_status.pop() == CLOSED:
            self.close()

    def __getattr__(self, name):
        if name in (
                'binary_types',
                'character_types',
                'currency_types',
                'date_types',
                'datetime_types',
                'decimal_types',
                'fixed_types',
                'logical_types',
                'memo_types',
                'numeric_types',
                'variable_types',
                ):
            return getattr(self, '_'+name)
        if name in ('_table', ):
                if self._meta.location == ON_DISK:
                    self._table = self._Table(len(self), self._meta)
                else:
                    self._table = []
        return object.__getattribute__(self, name)

    def __getitem__(self, value):
        if isinstance(value, baseinteger):
            if not -self._meta.header.record_count <= value < self._meta.header.record_count:
                raise NotFoundError("Record %d is not in table %s." % (value, self.filename))
            return self._table[value]
        elif type(value) == slice:
            sequence = List(desc='%s -->  %s' % (self.filename, value))
            for index in range(len(self))[value]:
                record = self._table[index]
                sequence.append(record)
            return sequence
        else:
            raise TypeError('type <%s> not valid for indexing' % type(value))

    def __init__(self, filename, field_specs=None, memo_size=128, ignore_memos=False,
                 codepage=None, default_data_types=None, field_data_types=None,    # e.g. 'name':str, 'age':float
                 dbf_type=None, on_disk=True, unicode_errors='strict'
                 ):
        """
        open/create dbf file
        filename should include path if needed
        field_specs can be either a ;-delimited string or a list of strings
        memo_size is always 512 for db3 memos
        ignore_memos is useful if the memo file is missing or corrupt
        read_only will load records into memory, then close the disk file
        keep_memos will also load any memo fields into memory
        meta_only will ignore all records, keeping only basic table information
        codepage will override whatever is set in the table itself
        """
        if not on_disk:
            if field_specs is None:
                raise DbfError("field list must be specified for memory tables")
        self._indexen = self._Indexen()
        self._meta = meta = self._MetaData()
        meta.max_fields = self._max_fields
        meta.max_records = self._max_records
        meta.table = weakref.ref(self)
        meta.filename = filename
        meta.fields = []
        meta.user_fields = FieldnameList()
        meta.user_field_count = 0
        meta.fieldtypes = fieldtypes = self._field_types
        meta.fixed_types = self._fixed_types
        meta.variable_types = self._variable_types
        meta.character_types = self._character_types
        meta.currency_types = self._currency_types
        meta.decimal_types = self._decimal_types
        meta.numeric_types = self._numeric_types
        meta.memo_types = self._memo_types
        meta.ignorememos = meta.original_ignorememos = ignore_memos
        meta.memo_size = memo_size
        meta.input_decoder = codecs.getdecoder(dbf.input_decoding)      # from ascii to unicode
        meta.output_encoder = codecs.getencoder(dbf.input_decoding)     # and back to ascii
        meta.unicode_errors = unicode_errors
        meta.header = header = self._TableHeader(self._dbfTableHeader, self._pack_date, self._unpack_date)
        header.extra = self._dbfTableHeaderExtra
        if default_data_types is None:
            default_data_types = dict()
        elif default_data_types == 'enhanced':
            default_data_types = {
                    'C' : Char,
                    'L' : Logical,
                    'D' : Date,
                    }
            if self._versionabbr in ('vfp', 'db4'):
                default_data_types['T'] = DateTime
        self._meta._default_data_types = default_data_types
        if field_data_types is None:
            field_data_types = dict()
        field_data_types = dict((k.upper(), v) for k, v in field_data_types.items())
        self._meta._field_data_types = field_data_types
        for field, types in default_data_types.items():
            field = FieldType(field)
            if not isinstance(types, tuple):
                types = (types, )
            for result_name, result_type in ezip(('Class', 'Empty', 'Null'), types):
                fieldtypes[field][result_name] = result_type
        if not on_disk:
            self._table = []
            meta.location = IN_MEMORY
            meta.memoname = filename
            meta.header.data
        else:
            base, ext = os.path.splitext(filename)
            search_name = None
            if ext == '.':
                # use filename without the '.'
                search_name = search_memo = base
                matches = glob(search_name)
            elif ext.lower() == '.dbf':
                # use filename as-is
                matches = glob(filename)
                search_memo = base
            else:
                meta.filename = filename + '.dbf'
                search_name = filename + '.[Db][Bb][Ff]'
                search_memo = filename
                matches = glob(search_name)
                if not matches:
                    meta.filename = filename
                    search_name = filename
                    matches = glob(search_name)
            if len(matches) == 1:
                meta.filename = matches[0]
            elif matches:
                raise DbfError("please specify exactly which of %r you want" % (matches, ))
            case = [('l','u')[c.isupper()] for c in meta.filename[-4:]]
            meta.memoname = base + ''.join([c if case[i] == 'l' else c.upper() for i, c in enumerate(self._memoext)])
            if not os.path.exists(meta.memoname):
                # look for other case variations
                template = ''.join('[%s%s]' % (c, c.upper()) for c in self._memoext[1:])
                matches = glob('%s.%s' % (search_memo, template))
                if len(matches) == 1:
                    meta.memoname = matches[0]
                elif len(matches) > 1:
                    raise DbfError("too many possible memo files: %s" % ', '.join(matches))
            meta.location = ON_DISK
        if codepage is not None:
            header.codepage(codepage)
            cp, sd, ld = _codepage_lookup(codepage)
            self._meta.codepage = sd
            self._meta.decoder, self._meta.encoder = unicode_error_handler(codecs.getdecoder(sd), codecs.getencoder(sd), unicode_errors)
        if field_specs:
            meta.status = READ_WRITE
            if meta.location == ON_DISK:
                meta.dfd = open(meta.filename, 'w+b')
                meta.newmemofile = True
            if codepage is None:
                header.codepage(default_codepage)
                cp, sd, ld = _codepage_lookup(header.codepage())
                self._meta.codepage = sd
                self._meta.decoder, self._meta.encoder = unicode_error_handler(codecs.getdecoder(sd), codecs.getencoder(sd), unicode_errors)
            self.add_fields(field_specs)
        else:
            meta.status = READ_ONLY
            try:
                dfd = meta.dfd = open(meta.filename, 'rb')
            except IOError:
                e = sys.exc_info()[1]
                raise DbfError(unicode(e)).from_exc(None)
            dfd.seek(0)
            try:
                meta.header = header = self._TableHeader(dfd.read(32), self._pack_date, self._unpack_date)
                if not header.version in self._supported_tables:
                    dfd.close()
                    raise DbfError(
                        "%s does not support %s [%x]" %
                        (self._version,
                        version_map.get(header.version, 'Unknown: %s' % header.version),
                        header.version))
                if codepage is None:
                    cp, sd, ld = _codepage_lookup(header.codepage())
                    self._meta.codepage = sd
                    self._meta.decoder, self._meta.encoder = unicode_error_handler(codecs.getdecoder(sd), codecs.getencoder(sd), unicode_errors)
                fieldblock = array('B', dfd.read(header.start - 32))
                for i in range(len(fieldblock) // 32 + 1):
                    fieldend = i * 32
                    if fieldblock[fieldend] == CR:
                        break
                else:
                    raise BadDataError("corrupt field structure in header")
                if len(fieldblock[:fieldend]) % 32 != 0:
                    raise BadDataError("corrupt field structure in header")
                old_length = header.data[10:12]
                header.fields = fieldblock[:fieldend]
                header.data = header.data[:10] + old_length + header.data[12:]    # restore original for testing
                header.extra = fieldblock[fieldend + 1:]  # skip trailing \r
                self._initialize_fields()
                self._check_memo_integrity()
                dfd.seek(0)
            except DbfError:
                dfd.close()
                raise
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
                    ('class', 'empty'),
                    specific_field_type or default_field_type,
                    ):
                classes.append(result_type)
            meta[field] = meta[field][:Field.CLASS] + tuple(classes) + meta[field][Field.NUL:]
        self.close()

    def __iter__(self):
        """
        iterates over the table's records
        """
        return Iter(self)

    def __len__(self):
        """
        returns number of records in table
        """
        return self._meta.header.record_count

    def __new__(cls, filename, field_specs=None, memo_size=128, ignore_memos=False,
                 codepage=None, default_data_types=None, field_data_types=None,    # e.g. 'name':str, 'age':float
                 dbf_type=None, on_disk=True, unicode_errors='strict',
                 ):
        if dbf_type is None and isinstance(filename, Table):
            return filename
        if field_specs and dbf_type is None:
            dbf_type = dbf.default_type
        if dbf_type is not None:
            dbf_type = dbf_type.lower()
            table = table_types.get(dbf_type)
            if table is None:
                raise DbfError("Unknown table type: %s" % dbf_type)
            return object.__new__(table)
        else:
            possibles = guess_table_type(filename)
            if len(possibles) == 1:
                return object.__new__(possibles[0][2])
            else:
                for type, desc, cls in possibles:
                    if type == dbf.default_type:
                        return object.__new__(cls)
                else:
                    types = ', '.join(["%s" % item[1] for item in possibles])
                    abbrs = '[' + ' | '.join(["%s" % item[0] for item in possibles]) + ']'
                    raise DbfError("Table could be any of %s.  Please specify %s when opening" % (types, abbrs))

    if py_ver < (3, 0):
        def __nonzero__(self):
            """
            True if table has any records
            """
            return self._meta.header.record_count != 0
    else:
        def __bool__(self):
            """
            True if table has any records
            """
            return self._meta.header.record_count != 0

    def __repr__(self):
        return __name__ + ".Table(%r, status=%r)" % (self._meta.filename, self._meta.status)

    def __str__(self):
        encoder = self._meta.encoder
        status = self._meta.status
        version = version_map.get(self._meta.header.version)
        if version is not None:
            version = self._version
        else:
            version = 'unknown - ' + hex(self._meta.header.version)
        str =  """
        Table:           %s
        Type:            %s
        Listed Codepage: %s
        Used Codepage:   %s
        Status:          %s
        Last updated:    %s
        Record count:    %d
        Field count:     %d
        Record length:   %d """ % (self.filename, version,
            code_pages[self._meta.header.codepage()][1], self.codepage, status,
            self.last_update, len(self), self.field_count, self.record_length)
        str += "\n        --Fields--\n"
        for i in range(len(self.field_names)):
            str += "%11d) %s\n" % (i, (self._field_layout(i).encode(dbf.input_decoding, errors='backslashreplace')))
        return str

    @property
    def codepage(self):
        """
        code page used for text translation
        """
        return CodePage(self._meta.codepage)

    @codepage.setter
    def codepage(self, codepage):
        if not isinstance(codepage, CodePage):
            raise TypeError("codepage should be a CodePage, not a %r" % type(codepage))
        meta = self._meta
        if meta.status != READ_WRITE:
            raise DbfError('%s not in read/write mode, unable to change codepage' % meta.filename)
        cp, sd, ld = _codepage_lookup(meta.header.codepage(codepage.code))
        meta.decoder, meta.encoder = unicode_error_handler(codecs.getdecoder(sd), codecs.getencoder(sd), meta.unicode_errors)
        meta.codepage = sd
        self._update_disk(headeronly=True)

    @property
    def field_count(self):
        """
        the number of user fields in the table
        """
        return self._meta.user_field_count

    @property
    def field_names(self):
        """
        a list of the user fields in the table
        """
        return self._meta.user_fields[:]

    @property
    def filename(self):
        """
        table's file name, including path (if specified on open)
        """
        return self._meta.filename

    @property
    def last_update(self):
        """
        date of last update
        """
        return self._meta.header.update

    @property
    def memoname(self):
        """
        table's memo name (if path included in filename on open)
        """
        return self._meta.memoname

    @property
    def record_length(self):
        """
        number of bytes in a record (including deleted flag and null field size
        """
        return self._meta.header.record_length

    @property
    def supported_tables(self):
        """
        allowable table types
        """
        return self._supported_tables

    @property
    def status(self):
        """
        CLOSED, READ_ONLY, or READ_WRITE
        """
        return self._meta.status

    @property
    def version(self):
        """
        returns the dbf type of the table
        """
        return self._version

    def add_fields(self, field_specs):
        """
        adds field(s) to the table layout; format is Name Type(Length,Decimals)[; Name Type(Length,Decimals)[...]]
        backup table is created with _backup appended to name
        then zaps table, recreates current structure, and copies records back from the backup
        """
        # for python 2, convert field_specs from bytes to unicode if necessary
        if py_ver < (3, 0):
            if isinstance(field_specs, bytes):
                if dbf.input_decoding is None:
                    raise DbfError('field specifications must be unicode, not bytes (or set dbf.input_decoding)')
                field_specs = field_specs.decode(dbf.input_decoding)
            if isinstance(field_specs, list) and any(isinstance(t, bytes) for t in field_specs):
                if dbf.input_decoding is None:
                    raise DbfError('field specifications must be unicode, not bytes (or set dbf.input_decoding)')
                fs = []
                for text in field_specs:
                    if isinstance(text, bytes):
                        text = text.decode(dbf.input_decoding)
                    fs.append(text)
                field_specs = fs
        else:
            if (
                isinstance(field_specs, bytes) or
                isinstance(field_specs, list) and any(isinstance(t, bytes) for t in field_specs)
                ):
                raise DbfError('field specifications must be unicode, not bytes')
        meta = self._meta
        if meta.status != READ_WRITE:
            raise DbfError('%s not in read/write mode, unable to add fields (%s)' % (meta.filename, meta.status))
        fields = self.structure()
        original_fields = len(fields)
        fields += self._list_fields(field_specs, sep=u';')
        null_fields = any(['NULL' in f.upper() for f in fields])
        if (len(fields) + null_fields) > meta.max_fields:
            raise DbfError(
                    "Adding %d more field%s would exceed the limit of %d"
                    % (len(fields), ('','s')[len(fields)==1], meta.max_fields)
                    )
        old_table = None
        if self:
            old_table = self.create_backup()
            self.zap()
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
        null_index = -1
        for field_seq, field in enumerate(fields):
            if not field:
                continue
            field = field.upper()
            pieces = field.split()
            name = pieces.pop(0)
            try:
                if '(' in pieces[0]:
                    loc = pieces[0].index('(')
                    pieces.insert(0, pieces[0][:loc])
                    pieces[1] = pieces[1][loc:]
                format = FieldType(pieces.pop(0))
                if pieces and '(' in pieces[0]:
                    for i, p in enumerate(pieces):
                        if ')' in p:
                            pieces[0:i+1] = [''.join(pieces[0:i+1])]
                            break
            except IndexError:
                raise FieldSpecError('bad field spec: %r' % field)
            if field_seq >= original_fields and (name[0] == '_' or name[0].isdigit() or not name.replace('_', '').isalnum()):
                # find appropriate line to point warning to
                for i, frame in enumerate(reversed(traceback.extract_stack()), start=1):
                    if frame[0] == __file__ and frame[2] == 'resize_field':
                        # ignore
                        break
                    elif frame[0] != __file__ or frame[2] not in ('__init__','add_fields'):
                        warnings.warn('%r invalid:  field names should start with a letter, and only contain letters, digits, and _' % name, FieldNameWarning, stacklevel=i)
                        break
            if name in meta.fields:
                raise DbfError("Field '%s' already exists" % name)
            field_type = format
            if len(name) > 10:
                raise FieldSpecError("Maximum field name length is 10.  '%s' is %d characters long." % (name, len(name)))
            if not field_type in meta.fieldtypes.keys():
                raise FieldSpecError("Unknown field type:  %s" % field_type)
            init = self._meta.fieldtypes[field_type]['Init']
            flags = self._meta.fieldtypes[field_type]['flags']
            try:
                length, decimals, flags = init(pieces, flags)
            except FieldSpecError:
                exc = sys.exc_info()[1]
                raise FieldSpecError(exc.message + ' (%s:%s)' % (meta.filename, name)).from_exc(None)
            nullable = flags & NULLABLE
            if nullable:
                null_index += 1
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
                    nullable and null_index,
                    )
        self._build_header_fields()
        self._update_disk()
        if old_table is not None:
            old_table.open()
            for record in old_table:
                self.append(scatter(record))
            old_table.close()

    def allow_nulls(self, fields):
        """
        set fields to allow null values -- NO LONGER ALLOWED, MUST BE SET AT TABLE CREATION
        """
        raise DbfError('fields can only be set to allow NULLs at table creation')

    def append(self, data=b'', drop=False, multiple=1):
        """
        adds <multiple> blank records, and fills fields with dict/tuple values if present
        """
        meta = self._meta
        if meta.status != READ_WRITE:
            raise DbfError('%s not in read/write mode, unable to append records' % meta.filename)
        if not self.field_count:
            raise DbfError("No fields defined, cannot append")
        dictdata = False
        tupledata = False
        header = meta.header
        kamikaze = b''
        if header.record_count == meta.max_records:
            raise DbfError("table %r is full; unable to add any more records" % self)
        if isinstance(data, (Record, RecordTemplate)):
            if data._meta.record_sig[0] == self._meta.record_sig[0]:
                kamikaze = data._data
        else:
            if isinstance(data, dict):
                dictdata = dict((ensure_unicode(k).upper(), v) for k, v in data.items())
                data = b''
            elif isinstance(data, tuple):
                if len(data) > self.field_count:
                    raise DbfError("incoming data has too many values")
                tupledata = data
                data = b''
            elif data:
                raise TypeError("data to append must be a tuple, dict, record, or template; not a %r" % type(data))
        newrecord = Record(recnum=header.record_count, layout=meta, kamikaze=kamikaze)
        if kamikaze and meta.memofields:
            newrecord._start_flux()
            for field in meta.memofields:
                newrecord[field] = data[field]
            newrecord._commit_flux()

        self._table.append(newrecord)
        header.record_count += 1
        if not kamikaze:
            try:
                if dictdata:
                    gather(newrecord, dictdata, drop=drop)
                elif tupledata:
                    newrecord._start_flux()
                    for index, item in enumerate(tupledata):
                        item = ensure_unicode(item)
                        newrecord[index] = item
                    newrecord._commit_flux()
                elif data:
                    newrecord._start_flux()
                    data_fields = field_names(data)
                    my_fields = self.field_names
                    for field in data_fields:
                        if field not in my_fields:
                            if not drop:
                                raise DbfError("field %r not in table %r" % (field, self))
                        else:
                            newrecord[field] = data[field]
                    newrecord._commit_flux()
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
                multi_record = Record(single, meta, kamikaze=data)
                multi_record._start_flux()
                self._table.append(multi_record)
                for field in meta.memofields:
                    multi_record[field] = newrecord[field]
                single += 1
                multi_record._commit_flux()
            header.record_count = total   # += multiple
            newrecord = multi_record
        self._update_disk(headeronly=True)

    def close(self):
        """
        closes disk files, flushing record data to disk
        ensures table data is available if keep_table
        ensures memo data is available if keep_memos
        """
        if self._meta.location == ON_DISK and self._meta.status != CLOSED:
            self._table.flush()
            if self._meta.mfd is not None:
                self._meta.mfd.close()
                self._meta.mfd = None
            if self._meta.dfd is not None:
                self._meta.dfd.close()
                self._meta.dfd = None
        self._meta.status = CLOSED

    def create_backup(self, new_name=None, on_disk=None):
        """
        creates a backup table
        """
        meta = self._meta
        already_open = meta.status != CLOSED
        if not already_open:
            self.open()
        if on_disk is None:
            on_disk = meta.location
        if not on_disk and new_name is None:
            new_name = self.filename + '_backup'
        if new_name is None:
            upper = self.filename.isupper()
            directory, filename = os.path.split(self.filename)
            name, ext = os.path.splitext(filename)
            extra = ('_backup', '_BACKUP')[upper]
            new_name = os.path.join(temp_dir or directory, name + extra + ext)
        memo_size = meta.memo_size
        bkup = Table(
                new_name, self.structure(), memo_size,
                codepage=self.codepage.name,
                dbf_type=self._versionabbr,
                on_disk=on_disk,
                )
        # use same encoder/decoder as current table, which may have been overridden
        bkup._meta.encoder = self._meta.encoder
        bkup._meta.decoder = self._meta.decoder
        bkup.open(READ_WRITE)
        for record in self:
            bkup.append(record)
        bkup.close()
        self.backup = new_name
        if not already_open:
            self.close()
        return bkup

    def create_index(self, key):
        """
        creates an in-memory index using the function key
        """
        meta = self._meta
        if meta.status == CLOSED:
            raise DbfError('%s is closed' % meta.filename)
        return Index(self, key)

    def create_template(self, record=None, defaults=None):
        """
        returns a record template that can be used like a record
        """
        return RecordTemplate(self._meta, original_record=record, defaults=defaults)

    def delete_fields(self, doomed):
        """
        removes field(s) from the table
        creates backup files with _backup appended to the file name,
        then modifies current structure
        """
        meta = self._meta
        if meta.status != READ_WRITE:
            raise DbfError('%s not in read/write mode, unable to delete fields' % meta.filename)
        doomed = self._list_fields(doomed)
        for victim in doomed:
            if victim not in meta.user_fields:
                raise DbfError("field %s not in table -- delete aborted" % victim)
        old_table = None
        if self:
            old_table = self.create_backup()
            self.zap()
        if meta.mfd is not None and not meta.ignorememos:
            meta.mfd.close()
            meta.mfd = None
            meta.memo = None
        if not meta.ignorememos:
            meta.newmemofile = True
        if '_NULLFLAGS' in meta.fields:
            doomed.append('_NULLFLAGS')
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
                self.append(scatter(record), drop=True)
            old_table.close()

    def disallow_nulls(self, fields):
        """
        set fields to not allow null values
        """
        meta = self._meta
        if meta.status != READ_WRITE:
            raise DbfError('%s not in read/write mode, unable to change field types' % meta.filename)
        fields = self._list_fields(fields)
        missing = set(fields) - set(self.field_names)
        if missing:
            raise FieldMissingError(', '.join(missing))
        old_table = None
        if self:
            old_table = self.create_backup()
            self.zap()
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
                self.append(scatter(record))
            old_table.close()

    def field_info(self, field):
        """
        returns (field type, size, dec, class) of field
        """
        field = field.upper()
        if field in self.field_names:
            field = self._meta[field]
            return FieldInfo(field[TYPE], field[LENGTH], field[DECIMALS], field[CLASS])
        raise FieldMissingError("%s is not a field in %s" % (field, self.filename))

    def index(self, record, start=None, stop=None):
        """
        returns the index of record between start and stop
        start and stop default to the first and last record
        """
        if not isinstance(record, (Record, RecordTemplate, dict, tuple)):
            raise TypeError("x should be a record, template, dict, or tuple, not %r" % type(record))
        meta = self._meta
        if meta.status == CLOSED:
            raise DbfError('%s is closed' % meta.filename)
        if start is None:
            start = 0
        if stop is None:
            stop = len(self)
        for i in range(start, stop):
            if record == (self[i]):
                return i
        else:
            raise NotFoundError("dbf.Table.index(x): x not in table", data=record)

    def new(self, filename, field_specs=None, memo_size=None, ignore_memos=None, codepage=None, default_data_types=None, field_data_types=None, on_disk=True):
        """
        returns a new table of the same type
        """
        if field_specs is None:
            field_specs = self.structure()
        if on_disk:
            path, name = os.path.split(filename)
            if path == "":
                filename = os.path.join(os.path.split(self.filename)[0], filename)
            elif name == "":
                filename = os.path.join(path, os.path.split(self.filename)[1])
        if memo_size is None:
            memo_size = self._meta.memo_size
        if ignore_memos is None:
            ignore_memos = self._meta.ignorememos
        if codepage is None:
            codepage = self._meta.header.codepage()
        if default_data_types is None:
            default_data_types = self._meta._default_data_types
        if field_data_types is None:
            field_data_types = self._meta._field_data_types
        return Table(filename, field_specs, memo_size, ignore_memos, codepage, default_data_types, field_data_types, dbf_type=self._versionabbr, on_disk=on_disk)

    def nullable_field(self, field):
        """
        returns True if field allows Nulls
        """
        field = field.upper()
        if field not in self.field_names:
            raise FieldMissingError(field)
        return bool(self._meta[field][FLAGS] & NULLABLE)

    def open(self, mode=READ_ONLY):
        """
        (re)opens disk table, (re)initializes data structures
        """
        if mode not in (READ_WRITE, READ_ONLY):
            raise DbfError("mode for open must be dbf.READ_ONLY or dbf.READ_WRITE, not %r" % mode)
        meta = self._meta
        if meta.status == mode:
            return self     # no-op
        meta.status = mode
        if meta.location == IN_MEMORY:
            return self
        if '_table' in dir(self):
            del self._table
        mode = ('rb', 'r+b')[meta.status is READ_WRITE]
        dfd = meta.dfd = open(meta.filename, mode)
        dfd.seek(0)
        header = meta.header = self._TableHeader(dfd.read(32), self._pack_date, self._unpack_date)
        if not header.version in self._supported_tables:
            dfd.close()
            dfd = None
            raise DbfError("Unsupported dbf type: %s [%x]" % (version_map.get(header.version, 'Unknown: %s' % header.version), header.version))
        fieldblock = array('B', dfd.read(header.start - 32))
        for i in range(len(fieldblock) // 32 + 1):
            fieldend = i * 32
            if fieldblock[fieldend] == CR:
                break
        else:
            raise BadDataError("corrupt field structure in header")
        if len(fieldblock[:fieldend]) % 32 != 0:
            raise BadDataError("corrupt field structure in header")
        header.fields = fieldblock[:fieldend]
        header.extra = fieldblock[fieldend + 1:]  # skip trailing \r
        self._meta.ignorememos = self._meta.original_ignorememos
        self._initialize_fields()
        self._check_memo_integrity()
        self._index = -1
        dfd.seek(0)
        return self

    def pack(self):
        """
        physically removes all deleted records
        """
        meta = self._meta
        if meta.status != READ_WRITE:
            raise DbfError('%s not in read/write mode, unable to pack records' % meta.filename)
        for dbfindex in self._indexen:
            dbfindex._clear()
        newtable = []
        index = 0
        for record in self._table:
            if is_deleted(record):
                record._recnum = -1
            else:
                record._recnum = index
                newtable.append(record)
                index += 1
        if meta.location == ON_DISK:
            self._table.clear()
        else:
            self._table[:] = []
        for record in newtable:
            self._table.append(record)
        self._pack_count += 1
        self._meta.header.record_count = index
        self._index = -1
        self._update_disk()
        self.reindex()

    def query(self, criteria):
        """
        criteria is a string that will be converted into a function that returns
        a List of all matching records
        """
        meta = self._meta
        if meta.status == CLOSED:
            raise DbfError('%s is closed' % meta.filename)
        return pqlc(self, criteria)

    def reindex(self):
        """
        reprocess all indices for this table
        """
        meta = self._meta
        if meta.status == CLOSED:
            raise DbfError('%s is closed' % meta.filename)
        for dbfindex in self._indexen:
            dbfindex._reindex()

    def rename_field(self, oldname, newname):
        """
        renames an existing field
        """
        oldname = oldname.upper()
        newname = newname.upper()
        meta = self._meta
        if meta.status != READ_WRITE:
            raise DbfError('%s not in read/write mode, unable to change field names' % meta.filename)
        if self:
            self.create_backup()
        if not oldname in self._meta.user_fields:
            raise FieldMissingError("field --%s-- does not exist -- cannot rename it." % oldname)
        if newname[0] == '_' or newname[0].isdigit() or not newname.replace('_', '').isalnum():
            raise FieldSpecError("field names cannot start with _ or digits, and can only contain the _, letters, and digits")
        if newname in self._meta.fields:
            raise DbfError("field --%s-- already exists" % newname)
        if len(newname) > 10:
            raise FieldSpecError("maximum field name length is 10.  '%s' is %d characters long." % (newname, len(newname)))
        self._meta[newname] = self._meta[oldname]
        self._meta.fields[self._meta.fields.index(oldname)] = newname
        self._build_header_fields()
        self._update_disk(headeronly=True)

    def resize_field(self, chosen, new_size):
        """
        resizes field (C only at this time)
        creates backup file, then modifies current structure
        """
        meta = self._meta
        if meta.status != READ_WRITE:
            raise DbfError('%s not in read/write mode, unable to change field size' % meta.filename)
        if self._versionabbr == 'clp':
            max_size = 65535
        else:
            max_size = 255
        if not 0 < new_size <= max_size:
            raise DbfError("new_size must be between 1 and 255 (use delete_fields to remove a field)")
        chosen = self._list_fields(chosen)
        for candidate in chosen:
            if candidate not in self._meta.user_fields:
                raise DbfError("field %s not in table -- resize aborted" % candidate)
            elif self.field_info(candidate).field_type != FieldType.CHAR:
                raise DbfError("field %s is not Character -- resize aborted" % candidate)
        old_table = None
        if self:
            old_table = self.create_backup()
            self.zap()
        if meta.mfd is not None and not meta.ignorememos:
            meta.mfd.close()
            meta.mfd = None
            meta.memo = None
        if not meta.ignorememos:
            meta.newmemofile = True
        struct = self.structure()
        meta.user_fields[:] = []
        new_struct = []
        for field_spec in struct:
            name, spec = field_spec.split(' ', 1)
            if name in chosen:
                spec = "C(%d)" % new_size
            new_struct.append(' '.join([name, spec]))
        self.add_fields(';'.join(new_struct))
        if old_table is not None:
            old_table.open()
            for record in old_table:
                self.append(scatter(record), drop=True)
            old_table.close()

    def structure(self, fields=None):
        """
        return field specification list suitable for creating same table layout
        fields should be a list of fields or None for all fields in table
        """
        field_specs = FieldnameList([])
        fields = self._list_fields(fields)
        try:
            for name in fields:
                field_specs.append(self._field_layout(self.field_names.index(name)))
        except ValueError:
            raise DbfError("field %s does not exist" % name).from_exc(None)
        return field_specs

    def zap(self):
        """
        removes all records from table -- this cannot be undone!
        """
        meta = self._meta
        if meta.status != READ_WRITE:
            raise DbfError('%s not in read/write mode, unable to zap table' % meta.filename)
        if meta.location == IN_MEMORY:
            self._table[:] = []
        else:
            self._table.clear()
            if meta.memo:
                meta.memo._zap()
        meta.header.record_count = 0
        self._index = -1
        self._update_disk()


class Db3Table(Table):
    """
    Provides an interface for working with dBase III tables.
    """

    _version = 'dBase III Plus'
    _versionabbr = 'db3'

    @MutableDefault
    def _field_types():
        return {
            CHAR: {
                    'Type':'Character', 'Retrieve':retrieve_character, 'Update':update_character, 'Blank':lambda x: b' ' * x, 'Init':add_character,
                    'Class':unicode, 'Empty':unicode, 'flags':tuple(),
                    },
            DATE: {
                    'Type':'Date', 'Retrieve':retrieve_date, 'Update':update_date, 'Blank':lambda x: b'        ', 'Init':add_date,
                    'Class':datetime.date, 'Empty':none, 'flags':tuple(),
                    },
            NUMERIC: {
                    'Type':'Numeric', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':lambda x: b' ' * x, 'Init':add_numeric,
                    'Class':'default', 'Empty':none, 'flags':tuple(),
                    },
            LOGICAL: {
                    'Type':'Logical', 'Retrieve':retrieve_logical, 'Update':update_logical, 'Blank':lambda x: b'?', 'Init':add_logical,
                    'Class':bool, 'Empty':none, 'flags':tuple(),
                    },
            MEMO: {
                    'Type':'Memo', 'Retrieve':retrieve_memo, 'Update':update_memo, 'Blank':lambda x: b'          ', 'Init':add_memo,
                    'Class':unicode, 'Empty':unicode, 'flags':tuple(),
                    },
            FLOAT: {
                    'Type':'Numeric', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':lambda x: b' ' * x, 'Init':add_numeric,
                    'Class':'default', 'Empty':none, 'flags':tuple(),
                    },
            TIMESTAMP: {
                    'Type':'TimeStamp', 'Retrieve':retrieve_clp_timestamp, 'Update':update_clp_timestamp, 'Blank':lambda x: b'\x00' * 8, 'Init':add_clp_timestamp,
                    'Class':datetime.datetime, 'Empty':none, 'flags':tuple(),
                    },
            }


    _memoext = '.dbt'
    _memoClass = _Db3Memo
    _yesMemoMask = 0x80
    _noMemoMask = 0x7f
    _binary_types = (TIMESTAMP, )
    _character_types = (CHAR, MEMO)
    _currency_types = tuple()
    _date_types = (DATE, )
    _datetime_types = (TIMESTAMP, )
    _decimal_types = (NUMERIC, FLOAT)
    _fixed_types = (DATE, LOGICAL, MEMO, TIMESTAMP)
    _logical_types = (LOGICAL, )
    _memo_types = (MEMO, )
    _numeric_types = (NUMERIC, FLOAT)
    _variable_types = (CHAR, NUMERIC, FLOAT)
    _dbfTableHeader = array('B', [0] * 32)
    _dbfTableHeader[0] = 3         # version - dBase III w/o memo's
    _dbfTableHeader[8:10] = array('B', pack_short_int(33))
    _dbfTableHeader[10] = 1        # record length -- one for delete flag
    _dbfTableHeader[29] = 3        # code page -- 437 US-MS DOS
    # _dbfTableHeader = to_bytes(_dbfTableHeader)
    _dbfTableHeaderExtra = b''
    _supported_tables = (0x03, 0x83)

    def _check_memo_integrity(self):
        """
        dBase III and Clipper
        """
        if not self._meta.ignorememos:
            memo_fields = False
            for field in self._meta.fields:
                if self._meta[field][TYPE] in self._memo_types:
                    memo_fields = True
                    break
            if memo_fields and self._meta.header.version != 0x83:
                self._meta.dfd.close()
                self._meta.dfd = None
                raise BadDataError("Table structure corrupt:  memo fields exist, header declares no memos")
            elif memo_fields and not os.path.exists(self._meta.memoname):
                self._meta.dfd.close()
                self._meta.dfd = None
                raise BadDataError("Table structure corrupt:  memo fields exist without memo file")
            if memo_fields:
                try:
                    self._meta.memo = self._memoClass(self._meta)
                except Exception:
                    exc = sys.exc_info()[1]
                    self._meta.dfd.close()
                    self._meta.dfd = None
                    raise BadDataError("Table structure corrupt:  unable to use memo file (%s)" % exc.args[-1]).from_exc(None)

    def _initialize_fields(self):
        """
        builds the FieldList of names, types, and descriptions
        """
        old_fields = defaultdict(dict)
        meta = self._meta
        for name in meta.fields:
            old_fields[name]['type'] = meta[name][TYPE]
            old_fields[name]['empty'] = meta[name][EMPTY]
            old_fields[name]['class'] = meta[name][CLASS]
        meta.fields[:] = []
        offset = 1
        fieldsdef = meta.header.fields
        if len(fieldsdef) % 32 != 0:
            raise BadDataError("field definition block corrupt: %d bytes in size" % len(fieldsdef))
        if len(fieldsdef) // 32 != meta.header.field_count:
            raise BadDataError("Header shows %d fields, but field definition block has %d fields" % (meta.header.field_count, len(fieldsdef) // 32))
        total_length = meta.header.record_length
        # null fields not allowed in db3 tables
        null_index = None
        for i in range(meta.header.field_count):
            fieldblock = fieldsdef[i*32:(i+1)*32]
            name = self._meta.decoder(unpack_str(fieldblock[:11]))[0].upper()
            type = fieldblock[11]
            if not type in meta.fieldtypes:
                raise BadDataError("Unknown field type: %s" % type)
            start = offset
            length = fieldblock[16]
            offset += length
            end = start + length
            decimals = fieldblock[17]
            flags = fieldblock[18]
            if name in meta.fields:
                raise BadDataError('Duplicate field name found: %s' % name)
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
                    null_index
                    )
        if offset != total_length:
            raise BadDataError("Header shows record length of %d, but calculated record length is %d" % (total_length, offset))
        meta.user_fields = FieldnameList([f for f in meta.fields if not meta[f][FLAGS] & SYSTEM])
        meta.user_field_count = len(meta.user_fields)
        Record._create_blank_data(meta)


class ClpTable(Db3Table):
    """
    Provides an interface for working with Clipper tables.
    """

    _version = 'Clipper 5'
    _versionabbr = 'clp'

    @MutableDefault
    def _field_types():
        return {
            CHAR: {
                    'Type':'Character', 'Retrieve':retrieve_character, 'Update':update_character, 'Blank':lambda x: b' ' * x, 'Init':add_clp_character,
                    'Class':unicode, 'Empty':unicode, 'flags':tuple(),
                    },
            DATE: {
                    'Type':'Date', 'Retrieve':retrieve_date, 'Update':update_date, 'Blank':lambda x: b'        ', 'Init':add_date,
                    'Class':datetime.date, 'Empty':none, 'flags':tuple(),
                    },
            NUMERIC: {
                    'Type':'Numeric', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':lambda x: b' ' * x, 'Init':add_numeric,
                    'Class':'default', 'Empty':none, 'flags':tuple(),
                    },
            LOGICAL: {
                    'Type':'Logical', 'Retrieve':retrieve_logical, 'Update':update_logical, 'Blank':lambda x: b'?', 'Init':add_logical,
                    'Class':bool, 'Empty':none, 'flags':tuple(),
                    },
            MEMO: {
                    'Type':'Memo', 'Retrieve':retrieve_memo, 'Update':update_memo, 'Blank':lambda x: b'          ', 'Init':add_memo,
                    'Class':unicode, 'Empty':unicode, 'flags':tuple(),
                    },
            FLOAT: {
                    'Type':'Numeric', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':lambda x: b' ' * x, 'Init':add_numeric,
                    'Class':'default', 'Empty':none, 'flags':tuple(),
                    },
            TIMESTAMP: {
                    'Type':'TimeStamp', 'Retrieve':retrieve_clp_timestamp, 'Update':update_clp_timestamp, 'Blank':lambda x: b'\x00' * 8, 'Init':add_clp_timestamp,
                    'Class':datetime.datetime, 'Empty':none, 'flags':tuple(),
                    },
            }
    _memoext = '.dbt'
    _memoClass = _Db3Memo
    _yesMemoMask = 0x80
    _noMemoMask = 0x7f
    _binary_types = ()
    _character_types = (CHAR, MEMO)
    _currency_types = tuple()
    _date_types = (DATE, TIMESTAMP)
    _datetime_types = (TIMESTAMP, )
    _decimal_types = (NUMERIC, FLOAT)
    _fixed_types = (DATE, LOGICAL, MEMO, TIMESTAMP)
    _logical_types = (LOGICAL, )
    _memo_types = (MEMO, )
    _numeric_types = (NUMERIC, FLOAT)
    _variable_types = (CHAR, NUMERIC, FLOAT)
    _dbfTableHeader = array('B', [0] * 32)
    _dbfTableHeader[0] = 3          # version - dBase III w/o memo's
    _dbfTableHeader[8:10] = array('B', pack_short_int(33))
    _dbfTableHeader[10] = 1         # record length -- one for delete flag
    _dbfTableHeader[29] = 3         # code page -- 437 US-MS DOS
    # _dbfTableHeader = to_bytes(_dbfTableHeader)
    _dbfTableHeaderExtra = b''
    _supported_tables = (0x03, 0x83)

    class _TableHeader(Table._TableHeader):
        """
        represents the data block that defines a tables type and layout
        """

        @property
        def fields(self):
            "field block structure"
            fieldblock = self._data[32:]
            for i in range(len(fieldblock)//32+1):
                cr = i * 32
                if fieldblock[cr] == CR:
                    break
            else:
                raise BadDataError("corrupt field structure")
            return fieldblock[:cr]
            # return to_bytes(fieldblock[:cr])

        @fields.setter
        def fields(self, block):
            fieldblock = self._data[32:]
            for i in range(len(fieldblock)//32+1):
                cr = i * 32
                if fieldblock[cr] == CR:
                    break
            else:
                raise BadDataError("corrupt field structure")
            cr += 32    # convert to indexing main structure
            fieldlen = len(block)
            if fieldlen % 32 != 0:
                raise BadDataError("fields structure corrupt: %d is not a multiple of 32" % fieldlen)
            self._data[32:cr] = array('B', block)                           # fields
            self._data[8:10] = array('B', pack_short_int(len(self._data)))   # start
            fieldlen = fieldlen // 32
            recordlen = 1                                     # deleted flag
            for i in range(fieldlen):
                recordlen += block[i*32+16]
                if block[i*32+11] == CHAR:
                    recordlen += block[i*32+17] * 256
            self._data[10:12] = array('B', pack_short_int(recordlen))


    def _build_header_fields(self):
        """
        constructs fieldblock for disk table
        """
        fieldblock = array('B', b'')
        memo = False
        nulls = 0
        meta = self._meta
        header = meta.header
        header.version = header.version & self._noMemoMask
        meta.fields = [f for f in meta.fields if f != '_NULLFLAGS']
        total_length = 1    # delete flag
        for field in meta.fields:
            layout = meta[field]
            if meta.fields.count(field) > 1:
                raise BadDataError("corrupted field structure (noticed in _build_header_fields)")
            fielddef = array('B', [0] * 32)
            fielddef[:11] = array('B', pack_str(meta.encoder(field)[0]))
            fielddef[11] = layout[TYPE]
            fielddef[12:16] = array('B', pack_long_int(layout[START]))
            total_length += layout[LENGTH]
            if layout[TYPE] == CHAR:     # long character field
                fielddef[16] = layout[LENGTH] % 256
                fielddef[17] = layout[LENGTH] // 256
            else:
                fielddef[16] = layout[LENGTH]
                fielddef[17] = layout[DECIMALS]
            fielddef[18] = layout[FLAGS]
            fieldblock.extend(fielddef)
            if layout[TYPE] in meta.memo_types:
                memo = True
            if layout[FLAGS] & NULLABLE:
                nulls += 1
        if memo:
            header.version = header.version | self._yesMemoMask
            if meta.memo is None:
                meta.memo = self._memoClass(meta)
        else:
            if os.path.exists(meta.memoname):
                if meta.mfd is not None:
                    meta.mfd.close()

                os.remove(meta.memoname)
            meta.memo = None
        if nulls:
            start = layout[START] + layout[LENGTH]
            length, one_more = divmod(nulls, 8)
            if one_more:
                length += 1
            fielddef = array('B', [0] * 32)
            fielddef[:11] = array('B', pack_str(b'_NullFlags'))
            fielddef[11] = FieldType._NULLFLAG
            fielddef[12:16] = array('B', pack_long_int(start))
            fielddef[16] = length
            fielddef[17] = 0
            fielddef[18] = BINARY | SYSTEM
            fieldblock.extend(fielddef)
            meta.fields.append('_NULLFLAGS')
            nullflags = (
                    _NULLFLAG,          # type
                    start,              # start
                    length,             # length
                    start + length,     # end
                    0,                  # decimals
                    BINARY | SYSTEM,    # flags
                    none,               # class
                    none,               # empty
                    )
            meta['_NULLFLAGS'] = nullflags
        header.fields = fieldblock
        # header.fields = to_bytes(fieldblock)
        header.record_length = total_length
        meta.user_fields = FieldnameList([f for f in meta.fields if not meta[f][FLAGS] & SYSTEM])
        meta.user_field_count = len(meta.user_fields)
        Record._create_blank_data(meta)

    def _initialize_fields(self):
        """
        builds the FieldList of names, types, and descriptions
        """
        meta = self._meta
        old_fields = defaultdict(dict)
        for name in meta.fields:
            old_fields[name]['type'] = meta[name][TYPE]
            old_fields[name]['empty'] = meta[name][EMPTY]
            old_fields[name]['class'] = meta[name][CLASS]
        fieldsdef = meta.header.fields
        if len(fieldsdef) % 32 != 0:
            raise BadDataError(
                    "field definition block corrupt: %d bytes in size"
                        % len(fieldsdef))
        if len(fieldsdef) // 32 != meta.header.field_count:
            raise BadDataError(
                    "Header shows %d fields, but field definition block has %d fields"
                        % (meta.header.field_count, len(fieldsdef) // 32))
        total_length = meta.header.record_length
        nulls_found = False
        starters = set()  # keep track of starting values in case header is poorly created
        for starter in ('header', 'offset'):
            meta.fields[:] = []
            offset = 1
            for i in range(meta.header.field_count):
                fieldblock = fieldsdef[i*32:(i+1)*32]
                name = self._meta.decoder(unpack_str(fieldblock[:11]))[0]
                type = fieldblock[11]
                if not type in meta.fieldtypes:
                    raise BadDataError("Unknown field type: %s" % type)
                if starter == 'header':
                    start = unpack_long_int(fieldblock[12:16])
                    if start in starters:
                        # poor header
                        break
                    starters.add(start)
                else:
                    start = offset
                length = fieldblock[16]
                decimals = fieldblock[17]
                if type == CHAR:
                    length += decimals * 256
                end = start + length
                offset += length
                flags = fieldblock[18]
                null = flags & NULLABLE
                if null:
                    nulls_found = True
                if name in meta.fields:
                    raise BadDataError('Duplicate field name found: %s' % name)
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
                        null
                        )
            else:
                # made it through all the fields
                break
        if offset != total_length:
            raise BadDataError(
                    "Header shows record length of %d, but calculated record length is %d"
                        % (total_length, offset))
        if nulls_found:
            nullable_fields = [f for f in meta if meta[f][NUL]]
            nullable_fields.sort(key=lambda f: f[START])
            for i, f in enumerate(nullable_fields):
                meta[f][NUL] = i
            null_bytes, plus_one = divmod(len(nullable_fields), 8)
            if plus_one:
                null_bytes += 1
            meta.empty_null = array('B', b'\x00' * null_bytes)
        meta.user_fields = FieldnameList([f for f in meta.fields if not meta[f][FLAGS] & SYSTEM])
        meta.user_field_count = len(meta.user_fields)
        Record._create_blank_data(meta)


class FpTable(Table):
    """
    Provides an interface for working with FoxPro 2 tables
    """

    _version = 'Foxpro'
    _versionabbr = 'fp'

    @MutableDefault
    def _field_types():
        return {
            CHAR: {
                    'Type':'Character', 'Retrieve':retrieve_character, 'Update':update_character, 'Blank':lambda x: b' ' * x, 'Init':add_vfp_character,
                    'Class':unicode, 'Empty':unicode, 'flags':('BINARY', 'NOCPTRANS', 'NULL', ),
                    },
            FLOAT: {
                    'Type':'Float', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':lambda x: b' ' * x, 'Init':add_vfp_numeric,
                    'Class':'default', 'Empty':none, 'flags':('NULL', ),
                    },
            NUMERIC: {
                    'Type':'Numeric', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':lambda x: b' ' * x, 'Init':add_vfp_numeric,
                    'Class':'default', 'Empty':none, 'flags':('NULL', ),
                    },
            LOGICAL: {
                    'Type':'Logical', 'Retrieve':retrieve_logical, 'Update':update_logical, 'Blank':lambda x: b'?', 'Init':add_logical,
                    'Class':bool, 'Empty':none, 'flags':('NULL', ),
                    },
            DATE: {
                    'Type':'Date', 'Retrieve':retrieve_date, 'Update':update_date, 'Blank':lambda x: b'        ', 'Init':add_date,
                    'Class':datetime.date, 'Empty':none, 'flags':('NULL', ),
                    },
            MEMO: {
                    'Type':'Memo', 'Retrieve':retrieve_memo, 'Update':update_memo, 'Blank':lambda x: b'          ', 'Init':add_memo,
                    'Class':unicode, 'Empty':unicode, 'flags':('BINARY', 'NOCPTRANS', 'NULL', ),
                    },
            GENERAL: {
                    'Type':'General', 'Retrieve':retrieve_memo, 'Update':update_memo, 'Blank':lambda x: b'          ', 'Init':add_binary_memo,
                    'Class':bytes, 'Empty':bytes, 'flags':('NULL', ),
                    },
            PICTURE: {
                    'Type':'Picture', 'Retrieve':retrieve_memo, 'Update':update_memo, 'Blank':lambda x: b'          ', 'Init':add_binary_memo,
                    'Class':bytes, 'Empty':bytes, 'flags':('NULL', ),
                    },
            _NULLFLAG: {
                    'Type':'_NullFlags', 'Retrieve':unsupported_type, 'Update':unsupported_type, 'Blank':lambda x: b'\x00' * x, 'Init':None,
                    'Class':none, 'Empty':none, 'flags':('BINARY', 'SYSTEM', ),
                    } }

    _memoext = '.fpt'
    _memoClass = _VfpMemo
    _yesMemoMask = 0xf5               # 1111 0101
    _noMemoMask = 0x02                # 0000 0010
    _binary_types = (GENERAL, MEMO, PICTURE)
    _character_types = (CHAR, DATE, FLOAT, LOGICAL, MEMO, NUMERIC)       # field representing character data
    _currency_types = tuple()
    _date_types = (DATE, )
    _datetime_types = tuple()
    _fixed_types = (DATE, GENERAL, LOGICAL, MEMO, PICTURE)
    _logical_types = (LOGICAL, )
    _memo_types = (GENERAL, MEMO, PICTURE)
    _numeric_types = (FLOAT, NUMERIC)
    _text_types = (CHAR, MEMO)
    _variable_types = (CHAR, FLOAT, NUMERIC)
    _supported_tables = (0x02, 0x03, 0xf5)
    _dbfTableHeader = array('B', [0] * 32)
    _dbfTableHeader[0] = 0x02          # version - Foxbase
    _dbfTableHeader[8:10] = array('B', pack_short_int(33 + 263))
    _dbfTableHeader[10] = 1         # record length -- one for delete flag
    _dbfTableHeader[29] = 3         # code page -- 437 US-MS DOS
    # _dbfTableHeader = to_bytes(_dbfTableHeader)
    _dbfTableHeaderExtra = b'\x00' * 263

    def _check_memo_integrity(self):
        if not self._meta.ignorememos:
            memo_fields = False
            for field in self._meta.fields:
                if self._meta[field][TYPE] in self._memo_types:
                    memo_fields = True
                    break
            if memo_fields and not os.path.exists(self._meta.memoname):
                self._meta.dfd.close()
                self._meta.dfd = None
                raise BadDataError("Table structure corrupt:  memo fields exist without memo file")
            elif not memo_fields and os.path.exists(self._meta.memoname):
                self._meta.dfd.close()
                self._meta.dfd = None
                raise BadDataError("Table structure corrupt:  no memo fields exist but memo file does")
            if memo_fields:
                try:
                    self._meta.memo = self._memoClass(self._meta)
                except Exception:
                    exc = sys.exc_info()[1]
                    self._meta.dfd.close()
                    self._meta.dfd = None
                    raise BadDataError("Table structure corrupt:  unable to use memo file (%s)" % exc.args[-1]).from_exc(None)

    def _initialize_fields(self):
        """
        builds the FieldList of names, types, and descriptions
        """
        meta = self._meta
        old_fields = defaultdict(dict)
        for name in meta.fields:
            old_fields[name]['type'] = meta[name][TYPE]
            old_fields[name]['class'] = meta[name][CLASS]
            old_fields[name]['empty'] = meta[name][EMPTY]
        meta.fields[:] = []
        offset = 1
        fieldsdef = meta.header.fields
        if len(fieldsdef) % 32 != 0:
            raise BadDataError("field definition block corrupt: %d bytes in size" % len(fieldsdef))
        if len(fieldsdef) // 32 != meta.header.field_count:
            raise BadDataError(
                    "Header shows %d fields, but field definition block has %d fields"
                        % (meta.header.field_count, len(fieldsdef) // 32))
        total_length = meta.header.record_length
        for i in range(meta.header.field_count):
            fieldblock = fieldsdef[i*32:(i+1)*32]
            name = self._meta.decoder(unpack_str(fieldblock[:11]))[0]
            type = fieldblock[11]
            if not type in meta.fieldtypes:
                raise BadDataError("Unknown field type: %s" % type)
            start = offset
            length = fieldblock[16]
            offset += length
            end = start + length
            decimals = fieldblock[17]
            flags = fieldblock[18]
            if name in meta.fields:
                raise BadDataError('Duplicate field name found: %s' % name)
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
                    0,
                    )
        if offset != total_length:
            raise BadDataError("Header shows record length of %d, but calculated record length is %d" % (total_length, offset))
        meta.user_fields = FieldnameList([f for f in meta.fields if not meta[f][FLAGS] & SYSTEM])
        meta.user_field_count = len(meta.user_fields)
        Record._create_blank_data(meta)

    @staticmethod
    def _pack_date(date):
        """
        Returns a group of three bytes, in integer form, of the date
        """
        # return "%c%c%c" % (date.year - 2000, date.month, date.day)
        return array('B', [date.year - 2000, date.month, date.day])

    @staticmethod
    def _unpack_date(bytestr):
        """
        Returns a Date() of the packed three-byte date passed in
        """
        year, month, day = struct.unpack('<BBB', bytestr)
        year += 2000
        return Date(year, month, day)

class VfpTable(FpTable):
    """
    Provides an interface for working with Visual FoxPro 6 tables
    """

    _version = 'Visual Foxpro'
    _versionabbr = 'vfp'

    @MutableDefault
    def _field_types():
        return {
            CHAR: {
                    'Type':'Character', 'Retrieve':retrieve_character, 'Update':update_character, 'Blank':lambda x: b' ' * x, 'Init':add_vfp_character,
                    'Class':unicode, 'Empty':unicode, 'flags':('BINARY', 'NOCPTRANS', 'NULL', ),
                    },
            CURRENCY: {
                    'Type':'Currency', 'Retrieve':retrieve_currency, 'Update':update_currency, 'Blank':lambda x: b'\x00' * 8, 'Init':add_vfp_currency,
                    'Class':Decimal, 'Empty':none, 'flags':('NULL', 'BINARY'),
                    },
            DOUBLE: {
                    'Type':'Double', 'Retrieve':retrieve_double, 'Update':update_double, 'Blank':lambda x: b'\x00' * 8, 'Init':add_vfp_double,
                    'Class':float, 'Empty':none, 'flags':('NULL', 'BINARY'),
                    },
            FLOAT: {
                    'Type':'Float', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':lambda x: b' ' * x, 'Init':add_vfp_numeric,
                    'Class':'default', 'Empty':none, 'flags':('NULL', ),
                    },
            NUMERIC: {
                    'Type':'Numeric', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':lambda x: b' ' * x, 'Init':add_vfp_numeric,
                    'Class':'default', 'Empty':none, 'flags':('NULL', ),
                    },
            INTEGER: {
                    'Type':'Integer', 'Retrieve':retrieve_integer, 'Update':update_integer, 'Blank':lambda x: b'\x00' * 4, 'Init':add_vfp_integer,
                    'Class':int, 'Empty':none, 'flags':('NULL', 'BINARY'),
                    },
            LOGICAL: {
                    'Type':'Logical', 'Retrieve':retrieve_logical, 'Update':update_logical, 'Blank':lambda x: b'?', 'Init':add_logical,
                    'Class':bool, 'Empty':none, 'flags':('NULL', ),
                    },
            DATE: {
                    'Type':'Date', 'Retrieve':retrieve_date, 'Update':update_date, 'Blank':lambda x: b'        ', 'Init':add_date,
                    'Class':datetime.date, 'Empty':none, 'flags':('NULL', ),
                    },
            DATETIME: {
                    'Type':'DateTime', 'Retrieve':retrieve_vfp_datetime, 'Update':update_vfp_datetime, 'Blank':lambda x: b'\x00' * 8, 'Init':add_vfp_datetime,
                    'Class':datetime.datetime, 'Empty':none, 'flags':('NULL', ),
                    },
            MEMO: {
                    'Type':'Memo', 'Retrieve':retrieve_vfp_memo, 'Update':update_vfp_memo, 'Blank':lambda x: b'\x00\x00\x00\x00', 'Init':add_vfp_memo,
                    'Class':unicode, 'Empty':unicode, 'flags':('BINARY', 'NOCPTRANS', 'NULL', ),
                    },
            GENERAL: {
                    'Type':'General', 'Retrieve':retrieve_vfp_memo, 'Update':update_vfp_memo, 'Blank':lambda x: b'\x00\x00\x00\x00', 'Init':add_vfp_binary_memo,
                    'Class':bytes, 'Empty':bytes, 'flags':('NULL', 'BINARY'),
                    },
            PICTURE: {
                    'Type':'Picture', 'Retrieve':retrieve_vfp_memo, 'Update':update_vfp_memo, 'Blank':lambda x: b'\x00\x00\x00\x00', 'Init':add_vfp_binary_memo,
                    'Class':bytes, 'Empty':bytes, 'flags':('NULL', 'BINARY'),
                    },
            _NULLFLAG: {
                    'Type':'_NullFlags', 'Retrieve':unsupported_type, 'Update':unsupported_type, 'Blank':lambda x: b'\x00' * x, 'Init':int,
                    'Class':none, 'Empty':none, 'flags':('BINARY', 'SYSTEM',),
                    } }

    _memoext = '.fpt'
    _memoClass = _VfpMemo
    _yesMemoMask = 0x30               # 0011 0000
    _noMemoMask = 0x30                # 0011 0000
    # _binary_types = ('B', 'G', 'I', 'P', 'T', 'Y')
    _binary_types = (DOUBLE, GENERAL, INTEGER, MEMO, PICTURE, DATETIME, CURRENCY)
    # _character_types = ('C', 'D', 'F', 'L', 'M', 'N')       # field representing character data
    _character_types = (CHAR, DATE, FLOAT, LOGICAL, MEMO, NUMERIC)
    _currency_types = (CURRENCY, )
    _date_types = (DATE, DATETIME)
    _datetime_types = (DATETIME, )
    # _fixed_types = ('B', 'D', 'G', 'I', 'L', 'M', 'P', 'T', 'Y')
    _fixed_types = (DOUBLE, DATE, GENERAL, INTEGER, LOGICAL, MEMO, PICTURE, DATETIME, CURRENCY)
    _logical_types = (LOGICAL, )
    _memo_types = (GENERAL, MEMO, PICTURE)
    # _numeric_types = ('B', 'F', 'I', 'N', 'Y')
    _numeric_types = (DOUBLE, FLOAT, INTEGER, NUMERIC, CURRENCY)
    _variable_types = (CHAR, FLOAT, NUMERIC)
    _supported_tables = (0x30, 0x31)
    _dbfTableHeader = array('B', [0] * 32)
    _dbfTableHeader[0] = 0x30          # version - Foxpro 6  0011 0000
    _dbfTableHeader[8:10] = array('B', pack_short_int(33 + 263))
    _dbfTableHeader[10] = 1         # record length -- one for delete flag
    _dbfTableHeader[29] = 3         # code page -- 437 US-MS DOS
    # _dbfTableHeader = to_bytes(_dbfTableHeader)
    _dbfTableHeaderExtra = b'\x00' * 263

    def _initialize_fields(self):
        """
        builds the FieldList of names, types, and descriptions
        """
        meta = self._meta
        old_fields = defaultdict(dict)
        for name in meta.fields:
            old_fields[name]['type'] = meta[name][TYPE]
            old_fields[name]['class'] = meta[name][CLASS]
            old_fields[name]['empty'] = meta[name][EMPTY]
        meta.fields[:] = []
        offset = 1
        fieldsdef = meta.header.fields
        nulls_found = False
        total_length = meta.header.record_length
        starters = set()  # keep track of starting values in case header is poorly created
        for starter in ('header', 'offset'):
            meta.fields[:] = []
            offset = 1
            for i in range(meta.header.field_count):
                fieldblock = fieldsdef[i*32:(i+1)*32]
                name = self._meta.decoder(unpack_str(fieldblock[:11]))[0]
                type = fieldblock[11]
                if not type in meta.fieldtypes:
                    raise BadDataError("Unknown field type: %s" % type)
                if starter == 'header':
                    start = unpack_long_int(fieldblock[12:16])
                    if start in starters:
                        # poor header
                        break
                    starters.add(start)
                else:
                    start = offset
                length = fieldblock[16]
                decimals = fieldblock[17]
                end = start + length
                offset += length
                flags = fieldblock[18]
                null = flags & NULLABLE
                if null:
                    nulls_found = True
                if name in meta.fields:
                    raise BadDataError('Duplicate field name found: %s' % name)
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
                        null
                        )
            else:
                # made it through all the fields
                break
        if offset != total_length:
            raise BadDataError("Header shows record length of %d, but calculated record length is %d" % (total_length, offset))
        if nulls_found:
            nullable_fields = [f for f in meta if meta[f][NUL]]
            nullable_fields.sort(key=lambda f: meta[f][START])
            for i, f in enumerate(nullable_fields):
                meta[f] = meta[f][:-1] + (i, )
            null_bytes, plus_one = divmod(len(nullable_fields), 8)
            if plus_one:
                null_bytes += 1
            meta.empty_null = array('B', b'\x00' * null_bytes)
        meta.user_fields = FieldnameList([f for f in meta.fields if not meta[f][FLAGS] & SYSTEM])
        meta.user_field_count = len(meta.user_fields)
        Record._create_blank_data(meta)


class List(_Navigation):
    """
    list of Dbf records, with set-like behavior
    """

    _desc = ''

    def __init__(self, records=None, desc=None, key=None):
        self._list = []
        self._set = set()
        self._tables = dict()
        if key is not None:
            self.key = key
            if key.__doc__ is None:
                key.__doc__ = 'unknown'
        key = self.key
        self._current = -1
        if isinstance(records, self.__class__) and key is records.key:
                self._list = records._list[:]
                self._set = records._set.copy()
                self._current = 0
        elif records is not None:
            for record in records:
                value = key(record)
                item = (source_table(record), recno(record), value)
                if value not in self._set:
                    self._set.add(value)
                    self._list.append(item)
            self._current = 0
        if desc is not None:
            self._desc = desc

    def __add__(self, other):
        self._still_valid_check()
        key = self.key
        if isinstance(other, (Table, list)):
            other = self.__class__(other, key=key)
        if isinstance(other, self.__class__):
            other._still_valid_check()
            result = self.__class__()
            result._set = self._set.copy()
            result._list[:] = self._list[:]
            result._tables = {}
            result._tables.update(self._tables)
            result.key = self.key
            if key is other.key:   # same key?  just compare key values
                for item in other._list:
                    result._maybe_add(item)
            else:                   # different keys, use this list's key on other's records
                for rec in other:
                    result._maybe_add((source_table(rec), recno(rec), key(rec)))
            return result
        return NotImplemented

    def __contains__(self, data):
        self._still_valid_check()
        if not isinstance(data, (Record, RecordTemplate, tuple, dict)):
            raise TypeError("%r is not a record, templace, tuple, nor dict" % (data, ))
        try:    # attempt quick method
            item = self.key(data)
            if not isinstance(item, tuple):
                item = (item, )
            return item in self._set
        except Exception:       # argh, try brute force method
            for record in self:
                if record == data:
                    return True
            return False

    def __delitem__(self, key):
        self._still_valid_check()
        if isinstance(key, baseinteger):
            item = self._list.pop[key]
            self._set.remove(item[2])
        elif isinstance(key, slice):
            self._set.difference_update([item[2] for item in self._list[key]])
            self._list.__delitem__(key)
        elif isinstance(key, (Record, RecordTemplate, dict, tuple)):
            index = self.index(key)
            item = self._list.pop[index]
            self._set.remove(item[2])
        else:
            raise TypeError('%r should be an int, slice, record, template, tuple, or dict -- not a %r' % (key, type(key)))

    def __getitem__(self, key):
        self._still_valid_check()
        if isinstance(key, baseinteger):
            count = len(self._list)
            if not -count <= key < count:
                raise NotFoundError("Record %d is not in list." % key)
            return self._get_record(*self._list[key])
        elif isinstance(key, slice):
            result = self.__class__()
            result._list[:] = self._list[key]
            result._set = set(result._list)
            result.key = self.key
            return result
        elif isinstance(key, (Record, RecordTemplate, dict, tuple)):
            index = self.index(key)
            return self._get_record(*self._list[index])
        else:
            raise TypeError('%r should be an int, slice, record, record template, tuple, or dict -- not a %r' % (key, type(key)))

    def __iter__(self):
        self._still_valid_check()
        return Iter(self)

    def __len__(self):
        self._still_valid_check()
        return len(self._list)

    if py_ver < (3, 0):
        def __nonzero__(self):
            self._still_valid_check()
            return len(self) > 0
    else:
        def __bool__(self):
            self._still_valid_check()
            return len(self) > 0

    def __radd__(self, other):
        self._still_valid_check()
        key = self.key
        if isinstance(other, (Table, list)):
            other = self.__class__(other, key=key)
        if isinstance(other, self.__class__):
            other._still_valid_check()
            result = other.__class__()
            result._set = other._set.copy()
            result._list[:] = other._list[:]
            result._tables = {}
            result._tables.update(self._tables)
            result.key = other.key
            if key is other.key:   # same key?  just compare key values
                for item in self._list:
                    result._maybe_add(item)
            else:                   # different keys, use this list's key on other's records
                for rec in self:
                    result._maybe_add((source_table(rec), recno(rec), key(rec)))
            return result
        return NotImplemented

    def __repr__(self):
        self._still_valid_check()
        if self._desc:
            return "%s(key=(%s), desc=%s)" % (self.__class__, self.key.__doc__, self._desc)
        else:
            return "%s(key=(%s))" % (self.__class__, self.key.__doc__)

    def __rsub__(self, other):
        self._still_valid_check()
        key = self.key
        if isinstance(other, (Table, list)):
            other = self.__class__(other, key=key)
        if isinstance(other, self.__class__):
            other._still_valid_check()
            result = other.__class__()
            result._list[:] = other._list[:]
            result._set = other._set.copy()
            result._tables = {}
            result._tables.update(other._tables)
            result.key = key
            lost = set()
            if key is other.key:
                for item in self._list:
                    if item[2] in result._list:
                        result._set.remove(item[2])
                        lost.add(item)
            else:
                for rec in self:
                    value = key(rec)
                    if value in result._set:
                        result._set.remove(value)
                        lost.add((source_table(rec), recno(rec), value))
            result._list = [item for item in result._list if item not in lost]
            lost = set(result._tables.keys())
            for table, _1, _2 in result._list:
                if table in result._tables:
                    lost.remove(table)
                    if not lost:
                        break
            for table in lost:
                del result._tables[table]
            return result
        return NotImplemented

    def __sub__(self, other):
        self._still_valid_check()
        key = self.key
        if isinstance(other, (Table, list)):
            other = self.__class__(other, key=key)
        if isinstance(other, self.__class__):
            other._still_valid_check()
            result = self.__class__()
            result._list[:] = self._list[:]
            result._set = self._set.copy()
            result._tables = {}
            result._tables.update(self._tables)
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
            lost = set(result._tables.keys())
            for table, _1, _2 in result._list:
                if table in result._tables:
                    lost.remove(table)
                    if not lost:
                        break
            for table in lost:
                del result._tables[table]
            return result
        return NotImplemented

    def _maybe_add(self, item):
        self._still_valid_check()
        table, recno, key = item
        self._tables[table] = table._pack_count   # TODO: check that _pack_count is the same if already in table
        if key not in self._set:
            self._set.add(key)
            self._list.append(item)

    def _get_record(self, table=None, rec_no=None, value=None):
        if table is rec_no is None:
            table, rec_no, value = self._list[self._index]
        return table[rec_no]

    def _purge(self, record, old_record_number, offset):
        partial = source_table(record), old_record_number
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

    def _still_valid_check(self):
        for table, last_pack in self._tables.items():
            if last_pack != getattr(table, '_pack_count'):
                raise DbfError("table has been packed; list is invalid")

    _nav_check = _still_valid_check

    def append(self, record):
        self._still_valid_check()
        self._maybe_add((source_table(record), recno(record), self.key(record)))

    def clear(self):
        self._list = []
        self._set = set()
        self._index = -1
        self._tables.clear()

    def extend(self, records):
        self._still_valid_check()
        key = self.key
        if isinstance(records, self.__class__):
            if key is records.key:   # same key?  just compare key values
                for item in records._list:
                    self._maybe_add(item)
            else:                   # different keys, use this list's key on other's records
                for rec in records:
                    value = key(rec)
                    self._maybe_add((source_table(rec), recno(rec), value))
        else:
            for rec in records:
                value = key(rec)
                self._maybe_add((source_table(rec), recno(rec), value))

    def index(self, record, start=None, stop=None):
        """
        returns the index of record between start and stop
        start and stop default to the first and last record
        """
        if not isinstance(record, (Record, RecordTemplate, dict, tuple)):
            raise TypeError("x should be a record, template, dict, or tuple, not %r" % type(record))
        self._still_valid_check()
        if start is None:
            start = 0
        if stop is None:
            stop = len(self)
        for i in range(start, stop):
            if record == (self[i]):
                return i
        else:
            raise NotFoundError("dbf.List.index(x): x not in List", data=record)

    def insert(self, i, record):
        self._still_valid_check()
        item = source_table(record), recno(record), self.key(record)
        if item not in self._set:
            self._set.add(item[2])
            self._list.insert(i, item)

    def key(self, record):
        """
        table_name, record_number
        """
        self._still_valid_check()
        return source_table(record), recno(record)

    def pop(self, index=None):
        self._still_valid_check()
        if index is None:
            table, recno, value = self._list.pop()
        else:
            table, recno, value = self._list.pop(index)
        self._set.remove(value)
        return self._get_record(table, recno, value)

    def query(self, criteria):
        """
        criteria is a callback that returns a truthy value for matching record
        """
        return pqlc(self, criteria)

    def remove(self, data):
        self._still_valid_check()
        if not isinstance(data, (Record, RecordTemplate, dict, tuple)):
            raise TypeError("%r(%r) is not a record, template, tuple, nor dict" % (type(data), data))
        index = self.index(data)
        record = self[index]
        item = source_table(record), recno(record), self.key(record)
        self._list.remove(item)
        self._set.remove(item[2])

    def reverse(self):
        self._still_valid_check()
        return self._list.reverse()

    def sort(self, key=None, reverse=False):
        self._still_valid_check()
        if key is None:
            return self._list.sort(reverse=reverse)
        return self._list.sort(key=lambda item: key(item[0][item[1]]), reverse=reverse)


# table meta

table_types = {
    'db3' : Db3Table,
    'clp' : ClpTable,
    'fp'  : FpTable,
    'vfp' : VfpTable,
    }

# https://social.msdn.microsoft.com/Forums/en-US/315c582a-651f-4a2e-b51c-92aadef8bddf/opening-vfp-tables-with-fox26-dos?forum=visualfoxprogeneral
# File type:
# 0x02   FoxBASE
# 0x03   FoxBASE+/Dbase III plus, no memo
# 0x30   Visual FoxPro
# 0x31   Visual FoxPro, autoincrement enabled
#
# 0x32 Visual FoxPro, Varchar, Varbinary, or Blob-enabled
# 0x43   dBASE IV SQL table files, no memo
# 0x63   dBASE IV SQL system files, no memo
# 0x83   FoxBASE+/dBASE III PLUS, with memo
# 0x8B   dBASE IV with memo
# 0xCB   dBASE IV SQL table files, with memo
# 0xF5   FoxPro 2.x (or earlier) with memo
# 0xFB   FoxBASE
#

version_map = {
        0x02 : 'FoxBASE',
        0x03 : 'dBase III Plus',
        0x04 : 'dBase IV',
        0x05 : 'dBase V',
        0x30 : 'Visual FoxPro',
        0x31 : 'Visual FoxPro (auto increment field)',
        0x32 : 'Visual FoxPro (VarChar, VarBinary, or BLOB enabled)',
        0x43 : 'dBase IV SQL table files',
        0x63 : 'dBase IV SQL system files',
        0x83 : 'dBase III Plus w/memos',
        0x8b : 'dBase IV w/memos',
        0x8e : 'dBase IV w/SQL table',
        0xf5 : 'FoxPro w/memos'}

code_pages = {
        0x00 : ('ascii', "plain ol' ascii"),
        0x01 : ('cp437', 'U.S. MS-DOS'),
        0x02 : ('cp850', 'International MS-DOS'),
        0x03 : ('cp1252', 'Windows ANSI'),
        0x04 : ('mac_roman', 'Standard Macintosh'),
        0x08 : ('cp865', 'Danish OEM'),
        0x09 : ('cp437', 'Dutch OEM'),
        0x0A : ('cp850', 'Dutch OEM (secondary)'),
        0x0B : ('cp437', 'Finnish OEM'),
        0x0D : ('cp437', 'French OEM'),
        0x0E : ('cp850', 'French OEM (secondary)'),
        0x0F : ('cp437', 'German OEM'),
        0x10 : ('cp850', 'German OEM (secondary)'),
        0x11 : ('cp437', 'Italian OEM'),
        0x12 : ('cp850', 'Italian OEM (secondary)'),
        0x13 : ('cp932', 'Japanese Shift-JIS'),
        0x14 : ('cp850', 'Spanish OEM (secondary)'),
        0x15 : ('cp437', 'Swedish OEM'),
        0x16 : ('cp850', 'Swedish OEM (secondary)'),
        0x17 : ('cp865', 'Norwegian OEM'),
        0x18 : ('cp437', 'Spanish OEM'),
        0x19 : ('cp437', 'English OEM (Britain)'),
        0x1A : ('cp850', 'English OEM (Britain) (secondary)'),
        0x1B : ('cp437', 'English OEM (U.S.)'),
        0x1C : ('cp863', 'French OEM (Canada)'),
        0x1D : ('cp850', 'French OEM (secondary)'),
        0x1F : ('cp852', 'Czech OEM'),
        0x22 : ('cp852', 'Hungarian OEM'),
        0x23 : ('cp852', 'Polish OEM'),
        0x24 : ('cp860', 'Portugese OEM'),
        0x25 : ('cp850', 'Potugese OEM (secondary)'),
        0x26 : ('cp866', 'Russian OEM'),
        0x37 : ('cp850', 'English OEM (U.S.) (secondary)'),
        0x40 : ('cp852', 'Romanian OEM'),
        0x4D : ('cp936', 'Chinese GBK (PRC)'),
        0x4E : ('cp949', 'Korean (ANSI/OEM)'),
        0x4F : ('cp950', 'Chinese Big 5 (Taiwan)'),
        0x50 : ('cp874', 'Thai (ANSI/OEM)'),
        0x57 : ('cp1252', 'ANSI'),
        0x58 : ('cp1252', 'Western European ANSI'),
        0x59 : ('cp1252', 'Spanish ANSI'),
        0x64 : ('cp852', 'Eastern European MS-DOS'),
        0x65 : ('cp866', 'Russian MS-DOS'),
        0x66 : ('cp865', 'Nordic MS-DOS'),
        0x67 : ('cp861', 'Icelandic MS-DOS'),
        0x68 : (None, 'Kamenicky (Czech) MS-DOS'),
        0x69 : (None, 'Mazovia (Polish) MS-DOS'),
        0x6a : ('cp737', 'Greek MS-DOS (437G)'),
        0x6b : ('cp857', 'Turkish MS-DOS'),
        0x78 : ('cp950', 'Traditional Chinese (Hong Kong SAR, Taiwan) Windows'),
        0x79 : ('cp949', 'Korean Windows'),
        0x7a : ('cp936', 'Chinese Simplified (PRC, Singapore) Windows'),
        0x7b : ('cp932', 'Japanese Windows'),
        0x7c : ('cp874', 'Thai Windows'),
        0x7d : ('cp1255', 'Hebrew Windows'),
        0x7e : ('cp1256', 'Arabic Windows'),
        0x87 : ('cp852', 'Slovenian OEM'),
        0xc8 : ('cp1250', 'Eastern European Windows'),
        0xc9 : ('cp1251', 'Russian Windows'),
        0xca : ('cp1254', 'Turkish Windows'),
        0xcb : ('cp1253', 'Greek Windows'),
        0xcc : ('cp1257', 'Baltic Windows'),
        0x96 : ('mac_cyrillic', 'Russian Macintosh'),
        0x97 : ('mac_latin2', 'Macintosh EE'),
        0x98 : ('mac_greek', 'Greek Macintosh'),
        0xf0 : ('utf8', '8-bit unicode'),
        }


dbf.default_codepage = default_codepage =  code_pages.get(0x00)[0]

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

class _Db4Table(Table):
    """
    under development
    """

    version = 'dBase IV w/memos (non-functional)'
    _versionabbr = 'db4'

    @MutableDefault
    def _field_types():
        return {
            CHAR: {'Type':'Character', 'Retrieve':retrieve_character, 'Update':update_character, 'Blank':lambda x: b' ' * x, 'Init':add_vfp_character},
            CURRENCY: {'Type':'Currency', 'Retrieve':retrieve_currency, 'Update':update_currency, 'Blank':Decimal, 'Init':add_vfp_currency},
            DOUBLE: {'Type':'Double', 'Retrieve':retrieve_double, 'Update':update_double, 'Blank':float, 'Init':add_vfp_double},
            FLOAT: {'Type':'Float', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':float, 'Init':add_vfp_numeric},
            NUMERIC: {'Type':'Numeric', 'Retrieve':retrieve_numeric, 'Update':update_numeric, 'Blank':int, 'Init':add_vfp_numeric},
            INTEGER: {'Type':'Integer', 'Retrieve':retrieve_integer, 'Update':update_integer, 'Blank':int, 'Init':add_vfp_integer},
            LOGICAL: {'Type':'Logical', 'Retrieve':retrieve_logical, 'Update':update_logical, 'Blank':Logical, 'Init':add_logical},
            DATE: {'Type':'Date', 'Retrieve':retrieve_date, 'Update':update_date, 'Blank':Date, 'Init':add_date},
            DATETIME: {'Type':'DateTime', 'Retrieve':retrieve_vfp_datetime, 'Update':update_vfp_datetime, 'Blank':DateTime, 'Init':add_vfp_datetime},
            MEMO: {'Type':'Memo', 'Retrieve':retrieve_memo, 'Update':update_memo, 'Blank':lambda x: b' ' * x, 'Init':add_memo},
            GENERAL: {'Type':'General', 'Retrieve':retrieve_memo, 'Update':update_memo, 'Blank':lambda x: b' ' * x, 'Init':add_memo},
            PICTURE: {'Type':'Picture', 'Retrieve':retrieve_memo, 'Update':update_memo, 'Blank':lambda x: b' ' * x, 'Init':add_memo},
            _NULLFLAG: {'Type':'_NullFlags', 'Retrieve':unsupported_type, 'Update':unsupported_type, 'Blank':int, 'Init':None} }

    _memoext = '.dbt'
    _memotypes = ('G', 'M', 'P')
    _memoClass = _VfpMemo
    _yesMemoMask = 0x8b               # 1000 1011
    _noMemoMask = 0x04                # 0000 0100
    _fixed_fields = ('B', 'D', 'G', 'I', 'L', 'M', 'P', 'T', 'Y')
    _variable_fields = ('C', 'F', 'N')
    _binary_fields = ('G', 'P')
    _character_fields = ('C', 'M')       # field representing character data
    _decimal_fields = ('F', 'N')
    _numeric_fields = ('B', 'F', 'I', 'N', 'Y')
    _currency_fields = ('Y',)
    _supported_tables = (0x04, 0x8b)
    _dbfTableHeader = [0] * 32
    _dbfTableHeader[0] = 0x8b         # version - Foxpro 6  0011 0000
    _dbfTableHeader[10] = 0x01        # record length -- one for delete flag
    _dbfTableHeader[29] = 0x03        # code page -- 437 US-MS DOS
    # _dbfTableHeader = bytes(_dbfTableHeader)
    _dbfTableHeaderExtra = b''

    def _check_memo_integrity(self):
        """
        dBase IV specific
        """
        if self._meta.header.version == 0x8b:
            try:
                self._meta.memo = self._memoClass(self._meta)
            except:
                self._meta.dfd.close()
                self._meta.dfd = None
                raise
        if not self._meta.ignorememos:
            for field in self._meta.fields:
                if self._meta[field][TYPE] in self._memotypes:
                    if self._meta.header.version != 0x8b:
                        self._meta.dfd.close()
                        self._meta.dfd = None
                        raise BadDataError("Table structure corrupt:  memo fields exist, header declares no memos")
                    elif not os.path.exists(self._meta.memoname):
                        self._meta.dfd.close()
                        self._meta.dfd = None
                        raise BadDataError("Table structure corrupt:  memo fields exist without memo file")
                    break
