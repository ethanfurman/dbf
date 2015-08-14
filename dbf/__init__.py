import sys as _sys

version = (0, 96, 5)

py_ver = _sys.version_info[:2]
if py_ver >= (3, 3):
    from dbf import ver_33 as _dbf
elif py_ver[:2] == (3, 2):
    from dbf import ver_32 as _dbf
elif (2, 5) <= py_ver[:2] < (3, 0):
    from dbf import ver_2 as _dbf
else:
    raise ImportError('dbf does not support Python %d.%d' % py_ver[:2])

del py_ver

__all__ = (
        'Table', 'Record', 'List', 'Index', 'Relation', 'Iter', 'Date', 'DateTime', 'Time',
        'CodePage', 'create_template', 'delete', 'field_names', 'gather', 'is_deleted',
        'recno', 'source_table', 'reset', 'scatter', 'undelete',
        'DbfError', 'DataOverflowError', 'BadDataError', 'FieldMissingError',
        'FieldSpecError', 'NonUnicodeError', 'NotFoundError',
        'DbfWarning', 'Eof', 'Bof', 'DoNotIndex',
        'Null', 'Char', 'Date', 'DateTime', 'Time', 'Logical', 'Quantum',
        'NullDate', 'NullDateTime', 'NullTime', 'Vapor', 'Period',
        'Process', 'Templates',
        'Truth', 'Falsth', 'Unknown', 'NoneType', 'Decimal', 'IndexLocation',
        'guess_table_type', 'table_type',
        'add_fields', 'delete_fields', 'get_fields', 'rename_field',
        'export', 'first_record', 'from_csv', 'info', 'structure',
        )

module = globals()

for name in dir(_dbf):
    if name.startswith('__') or name == 'module':
        continue
    module[name] = getattr(_dbf, name)


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

fake_module('api',
    'Table', 'Record', 'List', 'Index', 'Relation', 'Iter', 'Null', 'Char', 'Date', 'DateTime', 'Time',
    'Logical', 'Quantum', 'CodePage', 'create_template', 'delete', 'field_names', 'gather', 'is_deleted',
    'recno', 'source_table', 'reset', 'scatter', 'undelete',
    'NullDate', 'NullDateTime', 'NullTime', 'NoneType', 'NullType', 'Decimal', 'Vapor', 'Period',
    'Truth', 'Falsth', 'Unknown', 'On', 'Off', 'Other',
    'DbfError', 'DataOverflowError', 'BadDataError', 'FieldMissingError',
    'FieldSpecError', 'NonUnicodeError', 'NotFoundError',
    'DbfWarning', 'Eof', 'Bof', 'DoNotIndex', 'IndexLocation',
    'Process', 'Templates',
    ).register()

dbf = fake_module('dbf', *__all__)
setattr(_dbf, 'dbf', dbf)
del dbf
del _dbf

