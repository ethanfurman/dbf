# warnings and errors

class _undef(object):
    def __repr__(self):
        return 'not defined'
_undef = _undef()

def exception(exc, cause=_undef, context=_undef, traceback=_undef):
    if cause is not _undef:
        exc.__cause__ = cause
    if context is not _undef:
        exc.__context__ = context
    if traceback is not _undef:
        exc.__traceback__ = traceback
    return exc

class DbfError(Exception):
    """
    Fatal errors elicit this response.
    """
    def __init__(self, message, *args):
        Exception.__init__(self, message, *args)
        self.message = message
    def from_exc(self, exc):
        self.__cause__ = exc
        return self
    def with_traceback(self, tb):
        self.__traceback__ = tb
        return self

class DataOverflowError(DbfError):
    """
    Data too large for field
    """
    def __init__(self, message, data=None):
        DbfError.__init__(self, message)
        self.data = data


class BadDataError(DbfError):
    """
    bad data in table
    """
    def __init__(self, message, data=None):
        DbfError.__init__(self, message)
        self.data = data


class FieldMissingError(KeyError, DbfError):
    """
    Field does not exist in table
    """
    def __init__(self, fieldname):
        KeyError.__init__(self, '%s:  no such field in table' % fieldname)
        DbfError.__init__(self, '%s:  no such field in table' % fieldname)
        self.data = fieldname


class FieldSpecError(DbfError, ValueError):
    """
    invalid field specification
    """
    def __init__(self, message):
        ValueError.__init__(self, message)
        DbfError.__init__(self, message)


class NonUnicodeError(DbfError):
    """
    Data for table not in unicode
    """
    def __init__(self, message=None):
        DbfError.__init__(self, message)


class NotFoundError(DbfError, ValueError, KeyError, IndexError):
    """
    record criteria not met
    """
    def __init__(self, message=None, data=None):
        ValueError.__init__(self, message)
        KeyError.__init__(self, message)
        IndexError.__init__(self, message)
        DbfError.__init__(self, message)
        self.data = data


class DbfWarning(UserWarning):
    """
    Normal operations elicit this response
    """


class Eof(DbfWarning, StopIteration):
    """
    End of file reached
    """
    message = 'End of file reached'

    def __init__(self):
        StopIteration.__init__(self, self.message)
        DbfWarning.__init__(self, self.message)


class Bof(DbfWarning, StopIteration):
    """
    Beginning of file reached
    """
    message = 'Beginning of file reached'

    def __init__(self):
        StopIteration.__init__(self, self.message)
        DbfWarning.__init__(self, self.message)


class DoNotIndex(DbfWarning):
    """
    Returned by indexing functions to suppress a record from becoming part of the index
    """
    message = 'Not indexing record'

    def __init__(self):
        DbfWarning.__init__(self, self.message)


class FieldNameWarning(UserWarning):
    message = 'non-standard characters in field name'


