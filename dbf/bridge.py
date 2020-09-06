from array import array
from collections import deque
from textwrap import dedent
import decimal
import sys

# 2/3 constructs

py_ver = sys.version_info[:2]
if py_ver < (3, 0):
    bytes = str
    unicode = unicode
    basestring = bytes, unicode
    baseinteger = int, long
else:
    bytes = bytes
    unicode = str
    basestring = unicode,
    baseinteger = int,
    long = int
    xrange = range

## keep pyflakes happy  :(
execute = None
if py_ver < (3, 0):
    from __builtin__ import ord as bi_ord
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
    from builtins import ord as bi_ord
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

def ord(int_or_char):
    if isinstance(int_or_char, baseinteger):
        return int_or_char
    else:
        return bi_ord(int_or_char)

# 2.7 constructs
if py_ver == (2, 6):
    # Decimal does not accept float inputs until 2.7
    def Decimal(val=0):
        if isinstance(val, float):
            return decimal.Decimal(unicode(val))
        return decimal.Decimal(val)
else:
    Decimal = decimal.Decimal

## from contextlib2
## Inspired by discussions on http://bugs.python.org/issue13585
class ExitStack(object):
    """Context manager for dynamic management of a stack of exit callbacks

    For example:

        with ExitStack() as stack:
            files = [stack.enter_context(open(fname)) for fname in filenames]
            # All opened files will automatically be closed at the end of
            # the with statement, even if attempts to open files later
            # in the list raise an exception

    """
    def __init__(self):
        self._exit_callbacks = deque()

    def pop_all(self):
        """Preserve the context stack by transferring it to a new instance"""
        new_stack = type(self)()
        new_stack._exit_callbacks = self._exit_callbacks
        self._exit_callbacks = deque()
        return new_stack

    def _push_cm_exit(self, cm, cm_exit):
        """Helper to correctly register callbacks to __exit__ methods"""
        def _exit_wrapper(*exc_details):
            return cm_exit(cm, *exc_details)
        _exit_wrapper.__self__ = cm
        self.push(_exit_wrapper)

    def push(self, exit):
        """Registers a callback with the standard __exit__ method signature

        Can suppress exceptions the same way __exit__ methods can.

        Also accepts any object with an __exit__ method (registering a call
        to the method instead of the object itself)
        """
        # We use an unbound method rather than a bound method to follow
        # the standard lookup behaviour for special methods
        _cb_type = _get_type(exit)
        try:
            exit_method = _cb_type.__exit__
        except AttributeError:
            # Not a context manager, so assume its a callable
            self._exit_callbacks.append(exit)
        else:
            self._push_cm_exit(exit, exit_method)
        return exit # Allow use as a decorator

    def callback(self, callback, *args, **kwds):
        """Registers an arbitrary callback and arguments.

        Cannot suppress exceptions.
        """
        def _exit_wrapper(exc_type, exc, tb):
            callback(*args, **kwds)
        # We changed the signature, so using @wraps is not appropriate, but
        # setting __wrapped__ may still help with introspection
        _exit_wrapper.__wrapped__ = callback
        self.push(_exit_wrapper)
        return callback # Allow use as a decorator

    def enter_context(self, cm):
        """Enters the supplied context manager

        If successful, also pushes its __exit__ method as a callback and
        returns the result of the __enter__ method.
        """
        # We look up the special methods on the type to match the with statement
        _cm_type = _get_type(cm)
        _exit = _cm_type.__exit__
        result = _cm_type.__enter__(cm)
        self._push_cm_exit(cm, _exit)
        return result

    def close(self):
        """Immediately unwind the context stack"""
        self.__exit__(None, None, None)

    def __enter__(self):
        return self

    def __exit__(self, *exc_details):
        received_exc = exc_details[0] is not None

        # We manipulate the exception state so it behaves as though
        # we were actually nesting multiple with statements
        frame_exc = sys.exc_info()[1]
        _fix_exception_context = _make_context_fixer(frame_exc)

        # Callbacks are invoked in LIFO order to match the behaviour of
        # nested context managers
        suppressed_exc = False
        pending_raise = False
        while self._exit_callbacks:
            cb = self._exit_callbacks.pop()
            try:
                if cb(*exc_details):
                    suppressed_exc = True
                    pending_raise = False
                    exc_details = (None, None, None)
            except:
                new_exc_details = sys.exc_info()
                # simulate the stack of exceptions by setting the context
                _fix_exception_context(new_exc_details[1], exc_details[1])
                pending_raise = True
                exc_details = new_exc_details
        if pending_raise:
            _reraise_with_existing_context(exc_details)
        return received_exc and suppressed_exc

## Context manipulation is Python 3 only
_HAVE_EXCEPTION_CHAINING = sys.version_info[0] >= 3
if _HAVE_EXCEPTION_CHAINING:
    def _make_context_fixer(frame_exc):
        def _fix_exception_context(new_exc, old_exc):
            # Context may not be correct, so find the end of the chain
            while 1:
                exc_context = new_exc.__context__
                if exc_context is old_exc:
                    # Context is already set correctly (see issue 20317)
                    return
                if exc_context is None or exc_context is frame_exc:
                    break
                new_exc = exc_context
            # Change the end of the chain to point to the exception
            # we expect it to reference
            new_exc.__context__ = old_exc
        return _fix_exception_context

    def _reraise_with_existing_context(exc_details):
        try:
            # bare "raise exc_details[1]" replaces our carefully
            # set-up context
            fixed_ctx = exc_details[1].__context__
            raise exc_details[1]
        except BaseException:
            exc_details[1].__context__ = fixed_ctx
            raise
else:
    # No exception context in Python 2
    def _make_context_fixer(frame_exc):
        return lambda new_exc, old_exc: None

    # Use 3 argument raise in Python 2,
    # but use exec to avoid SyntaxError in Python 3
    def _reraise_with_existing_context(exc_details):
        exc_type, exc_value, exc_tb = exc_details
        exec ("raise exc_type, exc_value, exc_tb")

## Handle old-style classes if they exist
try:
    from types import InstanceType
except ImportError:
    # Python 3 doesn't have old-style classes
    _get_type = type
else:
    # Need to handle old-style context managers on Python 2
    def _get_type(obj):
        obj_type = type(obj)
        if obj_type is InstanceType:
            return obj.__class__ # Old-style class
        return obj_type # New-style class


