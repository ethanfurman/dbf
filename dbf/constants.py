from aenum import Enum, IntEnum, IntFlag, export
from array import array

class HexEnum(IntEnum):
    "repr is in hex"
    def __repr__(self):
        return '<%s.%s: %#02x>' % (
                self.__class__.__name__,
                self._name_,
                self._value_,
                )

class AutoEnum(IntEnum):
    """
    Automatically numbers enum members starting from __number__ (defaults to 0).

    Includes support for a custom docstring per member.
    """
    __number__ = 0

    def __new__(cls, *args):
        """Ignores arguments (will be handled in __init__."""
        value = cls.__number__
        cls.__number__ += 1
        obj = int.__new__(cls, value)
        obj._value_ = value
        return obj

    def __init__(self, *args):
        """Can handle 0 or 1 argument; more requires a custom __init__.
        0  = auto-number w/o docstring
        1  = auto-number w/ docstring
        2+ = needs custom __init__

        """
        if len(args) == 1 and isinstance(args[0], basestring):
            self.__doc__ = string(args[0])
        elif args:
            raise TypeError('%s not dealt with -- need custom __init__' % (args,))


class IsoDay(IntEnum):
    MONDAY = 1
    TUESDAY = 2
    WEDNESDAY = 3
    THURSDAY = 4
    FRIDAY = 5
    SATURDAY = 6
    SUNDAY = 7

    def next_delta(self, day):
        """Return number of days needed to get from self to day."""
        if self == day:
            return 7
        delta = day - self
        if delta < 0:
            delta += 7
        return delta

    def last_delta(self, day):
        """Return number of days needed to get from self to day."""
        if self == day:
            return -7
        delta = day - self
        if delta > 0:
            delta -= 7
        return delta

@export(module)
class RelativeDay(Enum):
    LAST_SUNDAY = ()
    LAST_SATURDAY = ()
    LAST_FRIDAY = ()
    LAST_THURSDAY = ()
    LAST_WEDNESDAY = ()
    LAST_TUESDAY = ()
    LAST_MONDAY = ()
    NEXT_MONDAY = ()
    NEXT_TUESDAY = ()
    NEXT_WEDNESDAY = ()
    NEXT_THURSDAY = ()
    NEXT_FRIDAY = ()
    NEXT_SATURDAY = ()
    NEXT_SUNDAY = ()

    def __new__(cls):
        result = object.__new__(cls)
        result._value = len(cls.__members__) + 1
        return result

    def days_from(self, day):
        target = IsoDay[self.name[5:]]
        if self.name[:4] == 'LAST':
            return day.last_delta(target)
        return day.next_delta(target)

class IsoMonth(IntEnum):
    JANUARY = 1
    FEBRUARY = 2
    MARCH = 3
    APRIL = 4
    MAY = 5
    JUNE = 6
    JULY = 7
    AUGUST = 8
    SEPTEMBER = 9
    OCTOBER = 10
    NOVEMBER = 11
    DECEMBER = 12

    def next_delta(self, month):
        """Return number of months needed to get from self to month."""
        if self == month:
            return 12
        delta = month - self
        if delta < 0:
            delta += 12
        return delta

    def last_delta(self, month):
        """Return number of months needed to get from self to month."""
        if self == month:
            return -12
        delta = month - self
        if delta > 0:
            delta -= 12
        return delta

@export(module)
class RelativeMonth(Enum):
    LAST_DECEMBER = ()
    LAST_NOVEMBER = ()
    LAST_OCTOBER = ()
    LAST_SEPTEMBER = ()
    LAST_AUGUST = ()
    LAST_JULY = ()
    LAST_JUNE = ()
    LAST_MAY = ()
    LAST_APRIL = ()
    LAST_MARCH= ()
    LAST_FEBRUARY = ()
    LAST_JANUARY = ()
    NEXT_JANUARY = ()
    NEXT_FEBRUARY = ()
    NEXT_MARCH = ()
    NEXT_APRIL = ()
    NEXT_MAY = ()
    NEXT_JUNE = ()
    NEXT_JULY = ()
    NEXT_AUGUST = ()
    NEXT_SEPTEMBER = ()
    NEXT_OCTOBER = ()
    NEXT_NOVEMBER = ()
    NEXT_DECEMBER = ()

    def __new__(cls):
        result = object.__new__(cls)
        result._value = len(cls.__members__) + 1
        return result

    def months_from(self, month):
        target = IsoMonth[self.name[5:]]
        if self.name[:4] == 'LAST':
            return month.last_delta(target)
        return month.next_delta(target)


# Constants

@export(module)
class LatinByte(HexEnum):
    NULL = 0x00
    SOH = 0x01
    STX = 0x02
    ETX = 0x03
    EOT = 0x04
    ENQ = 0x05
    ACK = 0x06
    BEL = 0x07
    BS  = 0x08
    TAB = 0x09
    LF  = 0x0a
    VT  = 0x0b
    FF  = 0x0c
    CR  = 0x0d
    SO  = 0x0e
    SI  = 0x0f
    DLE = 0x10
    DC1 = 0x11
    DC2 = 0x12
    DC3 = 0x13
    DC4 = 0x14
    NAK = 0x15
    SYN = 0x16
    ETB = 0x17
    CAN = 0x18
    EM  = 0x19
    EOF = 0x1a
    SUB = 0x1a
    ESC = 0x1b
    FS  = 0x1c
    GS  = 0x1d
    RS  = 0x1e
    US  = 0x1f
    SPACE = 0x20
    ASTERISK = 0x2a

    def __new__(cls, byte):
        obj = int.__new__(cls, byte)
        obj._value_ = byte
        obj.byte = chr(byte).encode('latin-1')
        obj.array = array('B', [byte])
        return obj

    def __add__(self, other):
        if isinstance(other, bytes):
            return self.byte + other
        elif isinstance(other, array):
            return self.array + other
        else:
            return super(LatinByte, self).__add__(other)

    def __radd__(self, other):
        if isinstance(other, bytes):
            return other + self.byte
        elif isinstance(other, array):
            return other + self.array
        else:
            return super(LatinByte, self).__add__(other)


@export(module)
class FieldType(IntEnum):
    def __new__(cls, char):
        char = char.upper()
        uchar = char.decode('ascii')
        int_value = ord(char)
        obj = int.__new__(cls, int_value)
        obj._value_ = int_value
        obj.symbol = uchar
        for alias in (
                char.lower(),
                char.upper(),
            ):
            cls._value2member_map_[alias] = obj
            cls._value2member_map_[alias.decode('ascii')] = obj
        return obj
    def __repr__(self):
        return '<%s.%s: %r>' % (
                self.__class__.__name__,
                self._name_,
                to_bytes([self._value_]),
                )
    _NULLFLAG = b'0'
    CHAR = b'C'
    CURRENCY = b'Y'
    DATE = b'D'
    DATETIME = b'T'
    DOUBLE = b'B'
    FLOAT = b'F'
    GENERAL = b'G'
    INTEGER = b'I'
    LOGICAL = b'L'
    MEMO = b'M'
    NUMERIC = b'N'
    PICTURE = b'P'

@export(module)
class FieldFlag(IntFlag):
    @classmethod
    def lookup(cls, alias):
        alias = alias.lower()
        if alias in ('system', ):
            return cls.SYSTEM
        elif alias in ('null', 'nullable'):
            return cls.NULLABLE
        elif alias in ('binary', 'nocptrans'):
            return cls.BINARY
        else:
            raise ValueError('no FieldFlag %r' % alias)
    @property
    def text(self):
        if self is NULLABLE:
            return 'null'
        else:
            return self._name_.lower()
    SYSTEM = 0x01
    NULLABLE = 0x02
    BINARY = 0x04
    NOCPTRANS = 0x04
    #AUTOINC = 0x0c         # not currently supported (not vfp 6)

@export(module)
class Field(AutoEnum):
    __order__ = 'TYPE START LENGTH END DECIMALS FLAGS CLASS EMPTY NUL'
    TYPE = "Char, Date, Logical, etc."
    START = "Field offset in record"
    LENGTH = "Length of field in record"
    END = "End of field in record (exclusive)"
    DECIMALS = "Number of decimal places if numeric"
    FLAGS = "System, Binary, Nullable"
    CLASS = "python class type"
    EMPTY = "python function for empty field"
    NUL = "python function for null field"

@export(module)
class DbfLocation(AutoEnum):
    __order__ = 'IN_MEMORY ON_DISK'
    IN_MEMORY = "dbf is kept in memory (disappears at program end)"
    ON_DISK = "dbf is kept on disk"

@export(module)
class DbfStatus(AutoEnum):
    __order__ = 'CLOSED READ_ONLY READ_WRITE'
    CLOSED = 'closed (only meta information available)'
    READ_ONLY = 'read-only'
    READ_WRITE = 'read-write'


