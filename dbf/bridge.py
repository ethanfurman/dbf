from array import array
from collections import deque
from textwrap import dedent
import decimal
import sys


bytes = bytes
unicode = str
basestring = unicode,
baseinteger = int,
long = int
xrange = range

from builtins import ord as bi_ord

def to_bytes(data):
    if isinstance(data, baseinteger):
        return chr(data).encode('ascii')
    elif isinstance(data, array):
        return data.tobytes()
    else:
        return bytes(data)

def ord(int_or_char):
    if isinstance(int_or_char, baseinteger):
        return int_or_char
    else:
        return bi_ord(int_or_char)

