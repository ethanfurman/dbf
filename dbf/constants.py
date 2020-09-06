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


