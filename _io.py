"""Routines for saving, retrieving, and creating fields"""

import struct
from decimal import Decimal
from dbf.exceptions import DbfError, DataOverflow
from dbf.dates import Date, DateTime, Time


# Constants
VFPTIME = 1721425

def packShortInt(value, bigendian=False):
        "Returns a two-bye integer from the value, or raises DbfError"
        # 256 / 65,536
        if value > 65535:
            raise DateOverflow("Maximum Integer size exceeded.  Possible: 65535.  Attempted: %d" % value)
        if bigendian:
            return struct.pack('>H', value)
        else:
            return struct.pack('<H', value)
def packLongInt(value, bigendian=False):
        "Returns a four-bye integer from the value, or raises DbfError"
        # 256 / 65,536 / 16,777,216
        if value > 4294967295:
            raise DateOverflow("Maximum Integer size exceeded.  Possible: 4294967295.  Attempted: %d" % value)
        if bigendian:
            return struct.pack('>L', value)
        else:
            return struct.pack('<L', value)
def packDate(date):
        "Returns a group of three bytes, in integer form, of the date"
        return "%c%c%c" % (date.year-1900, date.month, date.day)
def packStr(string):
        "Returns an 11 byte, upper-cased, null padded string suitable for field names; raises DbfError if the string is bigger than 10 bytes"
        if len(string) > 10:
            raise DbfError("Maximum string size is ten characters -- %s has %d characters" % (string, len(string)))
        return struct.pack('11s', string.upper())       
def unpackShortInt(bytes, bigendian=False):
        "Returns the value in the two-byte integer passed in"
        if bigendian:
            return struct.unpack('>H', bytes)[0]
        else:
            return struct.unpack('<H', bytes)[0]
def unpackLongInt(bytes, bigendian=False):
        "Returns the value in the four-byte integer passed in"
        if bigendian:
            return int(struct.unpack('>L', bytes)[0])
        else:
            return int(struct.unpack('<L', bytes)[0])
def unpackDate(bytestr):
        "Returns a Date() of the packed three-byte date passed in"
        year, month, day = struct.unpack('<BBB', bytestr)
        year += 1900
        return Date(year, month, day)
def unpackStr(chars):
        "Returns a normal, lower-cased string from a null-padded byte string"
        return struct.unpack('%ds' % len(chars), chars)[0].replace('\x00','').lower()
def convertToBool(value):
    """Returns boolean true or false; normal rules apply to non-string values; string values
    must be 'y','t', 'yes', or 'true' (case insensitive) to be True"""
    if type(value) == str:
        return bool(value.lower() in ['t', 'y', 'true', 'yes'])
    else:
        return bool(value)
def unsupportedType(something, field, memo=None):
    "called if a data type is not supported for that style of table"
    raise DbfError('field type is not supported.')
def retrieveCharacter(bytes, fielddef={}, memo=None):
    "Returns the string in bytes with trailing white space removed"
    return bytes.tostring().rstrip()
def updateCharacter(string, fielddef, memo=None):
    "returns the string, truncating if string is longer than it's field"
    if type(string) != str:
        raise DbfError("incompatible type: %s" % type(string))
    return string.rstrip()
def retrieveCurrency(bytes, fielddef={}, memo=None):
    value = struct.unpack('<q', bytes)[0]
    return Decimal("%de-4" % value)
def updateCurrency(value, fielddef={}, memo=None):
    currency = int(value * 10000)
    if not -9223372036854775808 < currency < 9223372036854775808:
        raise DataOverflow("value %s is out of bounds" % value)
    return struct.pack('<q', currency)
def retrieveDate(bytes, fielddef={}, memo=None):
    "Returns the ascii coded date as a Date object"
    return Date.fromymd(bytes.tostring())
def updateDate(moment, fielddef={}, memo=None):
    "returns the Date or datetime.date object ascii-encoded (yyyymmdd)"
    if moment:
        return "%04d%02d%02d" % moment.timetuple()[:3]
    return '        '
def retrieveDouble(bytes, fielddef={}, memo=None):
    return struct.unpack('<d', bytes)[0]
def updateDouble(value, fielddef={}, memo=None):
    if not (type(value) in (int, long, float)):
        raise DbfError("incompatible type: %s" % type(value))
    return struct.pack('<d', value)
def retrieveInteger(bytes, fielddef={}, memo=None):
    "Returns the binary number stored in bytes in little-endian format"
    return struct.unpack('<i', bytes)[0]
def updateInteger(value, fielddef={}, memo=None):
    "returns value in little-endian binary format"
    if not (type(value) in (int, long)):
        raise DbfError("incompatible type: %s" % type(value))
    if not -2147483648 < value < 2147483647:
        raise DataOverflow("Integer size exceeded.  Possible: -2,147,483,648..+2,147,483,647.  Attempted: %d" % value)
    return struct.pack('<i', value)
def retrieveLogical(bytes, fielddef={}, memo=None):
    "Returns True if bytes is 't', 'T', 'y', or 'Y', None if '?', and False otherwise"
    bytes = bytes.tostring()
    if bytes == '?':
        return None
    return bytes in ['t','T','y','Y']
def updateLogical(logical, fielddef={}, memo=None):
    "Returs 'T' if logical is True, 'F' otherwise"
    if type(logical) != bool:
        logical = convertToBool(logical)
    if type(logical) <> bool:
        raise DbfError('Value %s is not logical.' % logical)
    return logical and 'T' or 'F'
def retrieveMemo(bytes, fielddef, memo):
    "Returns the block of data from a memo file"
    stringval = bytes.tostring()
    if stringval.strip():
        block = int(stringval.strip())
    else:
        block = 0
    return memo.get_memo(block, fielddef)
def updateMemo(string, fielddef, memo):
    "Writes string as a memo, returns the block number it was saved into"
    block = memo.put_memo(string)
    if block == 0:
        block = ''
    return "%*s" % (fielddef['length'], block)
def retrieveNumeric(bytes, fielddef, memo=None):
    "Returns the number stored in bytes as integer if field spec for decimals is 0, float otherwise"
    string = bytes.tostring()
    if string[0:1] == '*':  # value too big to store (Visual FoxPro idiocy)
        return None
    if not string.strip():
        string = '0'
    if fielddef['decimals'] == 0:
        return int(string)
    else:
        return float(string)
def updateNumeric(value, fielddef, memo=None):
    "returns value as ascii representation, rounding decimal portion as necessary"
    if not (type(value) in (int, long, float)):
        raise DbfError("incompatible type: %s" % type(value))
    decimalsize = fielddef['decimals']
    if decimalsize:
        decimalsize += 1
    maxintegersize = fielddef['length']-decimalsize
    integersize = len("%.0f" % value)
    if integersize > maxintegersize:
        raise DataOverflow('Integer portion too big')
    return "%*.*f" % (fielddef['length'], fielddef['decimals'], value)
def retrieveVfpDateTime(bytes, fielddef={}, memo=None):
    """returns the date/time stored in bytes; dates <= 01/01/1981 00:00:00
    may not be accurate;  BC dates are nulled."""
    # two four-byte integers store the date and time.
    # millesecords are discarded from time
    time = retrieveInteger(bytes[4:])
    microseconds = (time % 1000) * 1000
    time = time // 1000                      # int(round(time, -3)) // 1000 discard milliseconds
    hours = time // 3600
    mins = time % 3600 // 60
    secs = time % 3600 % 60
    time = Time(hours, mins, secs, microseconds)
    possible = retrieveInteger(bytes[:4])
    possible -= VFPTIME
    possible = max(0, possible)
    date = Date.fromordinal(possible)
    return DateTime.combine(date, time)
def updateVfpDateTime(moment, fielddef={}, memo=None):
    """sets the date/time stored in moment
    moment must have fields year, month, day, hour, minute, second, microsecond"""
    bytes = [0] * 8
    hour = moment.hour
    minute = moment.minute
    second = moment.second
    millisecond = moment.microsecond // 1000       # convert from millionths to thousandths
    time = ((hour * 3600) + (minute * 60) + second) * 1000 + millisecond
    bytes[4:] = updateInteger(time)
    bytes[:4] = updateInteger(moment.toordinal() + VFPTIME)
    return ''.join(bytes)
def retrieveVfpMemo(bytes, fielddef, memo):
    "Returns the block of data from a memo file"
    block = struct.unpack('<i', bytes)[0]
    return memo.get_memo(block, fielddef)
def updateVfpMemo(string, fielddef, memo):
    "Writes string as a memo, returns the block number it was saved into"
    block = memo.put_memo(string)
    return struct.pack('<i', block)
def addCharacter(format):
    if format[1] != '(' or format[-1] != ')':
        raise DbfError("Format for Character field creation is C(n), not %s" % format)
    length = int(format[2:-1])
    if not 0 < length < 255:
        raise ValueError
    decimals = 0
    return length, decimals
def addDate(format):
    length = 8
    decimals = 0
    return length, decimals
def addLogical(format):
    length = 1
    decimals = 0
    return length, decimals
def addMemo(format):
    length = 10
    decimals = 0
    return length, decimals
def addNumeric(format):
    if format[1] != '(' or format[-1] != ')':
        raise DbfError("Format for Numeric field creation is N(n,n), not %s" % format)
    length, decimals = format[2:-1].split(',')
    length = int(length)
    decimals = int(decimals)
    if not (0 < length < 18 and 0 <= decimals <= length - 2):
        raise ValueError
    return length, decimals
def addVfpCurrency(format):
    length = 8
    decimals = 0
    return length, decimals
def addVfpDateTime(format):
    length = 8
    decimals = 8
    return length, decimals
def addVfpDouble(format):
    length = 8
    decimals = 0
    return length, decimals
def addVfpInteger(format):
    length = 4
    decimals = 0
    return length, decimals
def addVfpMemo(format):
    length = 4
    decimals = 0
    return length, decimals
def addVfpNumeric(format):
    if format[1] != '(' or format[-1] != ')':
        raise DbfError("Format for Numeric field creation is N(n,n), not %s" % format)
    length, decimals = format[2:-1].split(',')
    length = int(length)
    decimals = int(decimals)
    if not (0 < length < 21 and 0 <= decimals <= length - 2):
        raise ValueError
    return length, decimals
