import sys as _sys

_version_info = _sys.version[:2]
if _version_info >= (3, 3):
    from .ver_33 import *
elif _version_info[0] == 3:
    from .ver_3 import *
elif _version_info[0] == 2:
    from .ver_2 import *
else:
    raise ImportError('dbf does not support Python %d.%d' % version_info[:2])
