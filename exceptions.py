"warnings and errors"

class DbfError(Exception):
    "Fatal errors elicit this response."
    pass
class DataOverflow(DbfError):
    "Data too large for field"
    def __init__(yo, message, data=None):
        super(DataOverflow, yo).__init__(message)
        yo.data = data
class FieldMissing(KeyError, DbfError):
    "Field does not exist in table"
    def __init__(yo, fieldname):
        super(FieldMissing, yo).__init__('%s:  no such field in table' % fieldname)
        yo.data = fieldname
class NonUnicode(DbfError):
    "Data for table not in unicode"
    def __init__(yo, message=None):
        super(NonUnicode, yo).__init__(message)
class DbfWarning(Exception):
    "Normal operations elicit this response"
class Eof(DbfWarning, StopIteration):
    "End of file reached"
    message = 'End of file reached'
    def __init__(yo):
        super(Eof, yo).__init__(yo.message)
class Bof(DbfWarning, StopIteration):
    "Beginning of file reached"
    message = 'Beginning of file reached'
    def __init__(yo):
        super(Bof, yo).__init__(yo.message)
class DoNotIndex(DbfWarning):
    "Returned by indexing functions to suppress a record from becoming part of the index"
    message = 'Not indexing record'
    def __init__(yo):
        super(DoNotIndex, yo).__init__(yo.message)
