from . import dbf
from .bridge import *
from .utils import ensure_unicode, field_names, source_table
from .utils import delete, undelete, is_deleted, reset

# SQL functions

def pql_select(records, chosen_fields, condition, field_names):
    if chosen_fields != '*':
        field_names = chosen_fields.replace(' ', '').split(',')
    result = condition(records)
    result.modified = 0, 'record' + ('', 's')[len(result)>1]
    result.field_names = field_names
    return result

def pql_update(records, command, condition, field_names):
    possible = condition(records)
    modified = pql_cmd(command, field_names)(possible)
    possible.modified = modified, 'record' + ('', 's')[modified>1]
    return possible

def pql_delete(records, dead_fields, condition, field_names):
    deleted = condition(records)
    deleted.modified = len(deleted), 'record' + ('', 's')[len(deleted)>1]
    deleted.field_names = field_names
    if dead_fields == '*':
        for record in deleted:
            delete(record)
    else:
        keep = [f for f in field_names if f not in dead_fields.replace(' ', '').split(',')]
        for record in deleted:
            reset(record, keep_fields=keep)
    return deleted

def pql_recall(records, all_fields, condition, field_names):
    if all_fields != '*':
        raise DbfError('SQL RECALL: fields must be * (only able to recover at the record level)')
    revivified = List()
    for record in condition(records):
        if is_deleted(record):
            revivified.append(record)
            undelete(record)
    revivified.modfied = len(revivified), 'record' + ('', 's')[len(revivified)>1]
    return revivified

def pql_add(records, new_fields, condition, field_names):
    tables = set()
    possible = condition(records)
    for record in possible:
        tables.add(source_table(record))
    for table in tables:
        table.add_fields(new_fields)
    possible.modified = len(tables), 'table' + ('', 's')[len(tables)>1]
    possible.field_names = field_names
    return possible

def pql_drop(records, dead_fields, condition, field_names):
    tables = set()
    possible = condition(records)
    for record in possible:
        tables.add(source_table(record))
    for table in tables:
        table.delete_fields(dead_fields)
    possible.modified = len(tables), 'table' + ('', 's')[len(tables)>1]
    possible.field_names = field_names
    return possible

def pql_pack(records, command, condition, field_names):
    tables = set()
    possible = condition(records)
    for record in possible:
        tables.add(source_table(record))
    for table in tables:
        table.pack()
    possible.modified = len(tables), 'table' + ('', 's')[len(tables)>1]
    possible.field_names = field_names
    return possible

def pql_resize(records, fieldname_newsize, condition, field_names):
    tables = set()
    possible = condition(records)
    for record in possible:
        tables.add(source_table(record))
    fieldname, newsize = fieldname_newsize.split()
    newsize = int(newsize)
    for table in tables:
        table.resize_field(fieldname, newsize)
    possible.modified = len(tables), 'table' + ('', 's')[len(tables)>1]
    possible.field_names = field_names
    return possible

def pql_criteria(records, criteria):
    """
    creates a function matching the pql criteria
    """
    function = """def func(records):
    '''%s
    '''
    _matched = dbf.List()
    for _rec in records:
        %s

        if %s:
            _matched.append(_rec)
    return _matched"""
    fields = []
    criteria = ensure_unicode(criteria)
    uc_criteria = criteria.upper()
    for field in field_names(records):
        if field in uc_criteria:
            fields.append(field)
    criteria = criteria.replace('recno()', 'recno(_rec)').replace('is_deleted()', 'is_deleted(_rec)')
    fields = '\n        '.join(['%s = _rec.%s' % (field.lower(), field) for field in fields])
    g = dict()
    g['dbf'] = dbf.api
    g.update(dbf.pql_user_functions)
    function %= (criteria, fields, criteria)
    execute(function, g)
    return g['func']

def pql_cmd(command, field_names):
    """
    creates a function matching to apply command to each record in records
    """
    function = """def func(records):
    '''%s
    '''
    _changed = 0
    for _rec in records:
        _tmp = dbf.create_template(_rec)
        %s

        %s

        %s
        if _tmp != _rec:
            dbf.gather(_rec, _tmp)
            _changed += 1
    return _changed"""
    fields = []
    for field in field_names:
        if field in command:
            fields.append(field)
    command = command.replace('recno()', 'recno(_rec)').replace('is_deleted()', 'is_deleted(_rec)')
    pre_fields = '\n        '.join(['%s = _tmp.%s' % (field.lower(), field) for field in fields])
    post_fields = '\n        '.join(['_tmp.%s = %s' % (field, field).lower() for field in fields])
    g = dbf.pql_user_functions.copy()
    g['dbf'] = dbf.api
    g['recno'] = recno
    g['create_template'] = create_template
    g['gather'] = gather
    if ' with ' in command.lower():
        offset = command.lower().index(' with ')
        command = command[:offset] + ' = ' + command[offset + 6:]
    function %= (command, pre_fields, command, post_fields)
    execute(function, g)
    return g['func']

def pqlc(records, command):
    """
    recognized pql commands are SELECT, UPDATE | REPLACE, DELETE, RECALL, ADD, DROP
    """
    close_table = False
    if isinstance(records, basestring):
        records = Table(records)
        close_table = True
    try:
        if not records:
            return List()
        command = ensure_unicode(command)
        pql_command = command
        uc_command = command.upper()
        if u' WHERE ' in uc_command:
            index = uc_command.find(u' WHERE ')
            condition = command[index+7:]
            command = command[:index]
            # command, condition = command.split(' where ', 1)
            condition = pql_criteria(records, condition)
        else:
            def condition(records):
                return records[:]
        name, command = command.split(' ', 1)
        command = command.strip()
        name = name.upper()
        fields = field_names(records)
        if pql_functions.get(name) is None:
            raise DbfError('unknown SQL command %r in %r' % (name.upper(), pql_command))
        result = pql_functions[name](records, command, condition, fields)
        tables = set()
        for record in result:
            tables.add(source_table(record))
    finally:
        if close_table:
            records.close()
    return result

pql_functions = {
        u'SELECT' : pql_select,
        u'UPDATE' : pql_update,
        u'REPLACE': pql_update,
        u'INSERT' : None,
        u'DELETE' : pql_delete,
        u'RECALL' : pql_recall,
        u'ADD'    : pql_add,
        u'DROP'   : pql_drop,
        u'COUNT'  : None,
        u'PACK'   : pql_pack,
        u'RESIZE' : pql_resize,
        }


def _nop(value):
    """
    returns parameter unchanged
    """
    return value

def _normalize_tuples(tuples, length, filler):
    """
    ensures each tuple is the same length, using filler[-missing] for the gaps
    """
    final = []
    for t in tuples:
        if len(t) < length:
            final.append( tuple([item for item in t] + filler[len(t)-length:]) )
        else:
            final.append(t)
    return tuple(final)


