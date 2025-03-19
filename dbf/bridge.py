from array import array
from collections import deque
from textwrap import dedent
from decimal import Decimal
import sys

__all__ = [
        'bytes', 'str', 'unicode', 'basestring',
        'int', 'long', 'baseinteger', 'Decimal',
        'builtins', 'execute', 'ord', 'to_bytes',
        'py_ver',
        ]

py_ver = sys.version_info[:2]

if py_ver < (3, 0):
    bytes = str
    str = unicode
    unicode = unicode
    basestring = bytes, unicode
    int = int
    long = long
    baseinteger = int, long
    import __builtin__ as builtins
else:
    bytes = bytes
    str = str
    unicode = str
    basestring = unicode,
    int = int
    long = int
    baseinteger = int,
    xrange = range
    import builtins

bi_ord = builtins.ord

def ord(int_or_char):
    if isinstance(int_or_char, baseinteger):
        return int_or_char
    else:
        return bi_ord(int_or_char)

## keep pyflakes happy  :(
execute = None
if py_ver < (3, 0):
    exec(dedent("""\
        def execute(code, gbl=None, lcl=None):
            if lcl is not None:
                exec code in gbl, lcl
            elif gbl is not None:
                exec code in gbl
            else:
                exec code in globals()
            """))
    def to_bytes(data):
        try:
            if not data:
                return b''
            elif isinstance(data, bytes):
                return data
            elif isinstance(data, baseinteger):
                return chr(data).encode('ascii')
            elif isinstance(data[0], bytes):
                return b''.join(data)
            elif not isinstance(data, array):
                data = array('B', data)
            return data.tostring()
        except Exception:
            raise ValueError('unable to convert %r to bytes' % (data, ))
else:
    exec(dedent("""\
        def execute(code, gbl=None, lcl=None):
            exec(code, gbl, lcl)
            """))
    def to_bytes(data):
        if isinstance(data, baseinteger):
            return chr(data).encode('ascii')
        elif isinstance(data, array):
            return data.tobytes()
        else:
            return bytes(data)

