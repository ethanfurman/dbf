from math import floor

import datetime
import time

from .bridge import *
from .constants import *
from .utils import string, is_leapyear

try:
    import pytz
except ImportError:
    pytz = None

NoneType = type(None)

## dec jan feb mar apr may jun jul aug sep oct nov dec jan
days_per_month = [31, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 31]
days_per_leap_month = [31, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 31]

def days_in_month(year):
    return (days_per_month, days_per_leap_month)[is_leapyear(year)]

# gets replaced later by their final values
Unknown = Other = object()

class NullType(object):
    """
    Null object -- any interaction returns Null
    """

    def _null(self, *args, **kwargs):
        return self

    __eq__ = __ne__ = __ge__ = __gt__ = __le__ = __lt__ = _null
    __add__ = __iadd__ = __radd__ = _null
    __sub__ = __isub__ = __rsub__ = _null
    __mul__ = __imul__ = __rmul__ = _null
    __div__ = __idiv__ = __rdiv__ = _null
    __mod__ = __imod__ = __rmod__ = _null
    __pow__ = __ipow__ = __rpow__ = _null
    __and__ = __iand__ = __rand__ = _null
    __xor__ = __ixor__ = __rxor__ = _null
    __or__ = __ior__ = __ror__ = _null
    __truediv__ = __itruediv__ = __rtruediv__ = _null
    __floordiv__ = __ifloordiv__ = __rfloordiv__ = _null
    __lshift__ = __ilshift__ = __rlshift__ = _null
    __rshift__ = __irshift__ = __rrshift__ = _null
    __neg__ = __pos__ = __abs__ = __invert__ = _null
    __call__ = __getattr__ = _null

    def __divmod__(self, other):
        return self, self
    __rdivmod__ = __divmod__

    __hash__ = None

    def __new__(cls, *args):
        return cls.null

    if py_ver < (3, 0):
        def __nonzero__(self):
            return False
    else:
        def __bool__(self):
            return False


    def __repr__(self):
        return '<null>'

    def __setattr__(self, name, value):
        return None

    def __setitem___(self, index, value):
        return None

    def __str__(self):
        return ''

NullType.null = object.__new__(NullType)
Null = NullType()


class Vapor(object):
    """
    used in Vapor Records -- compares unequal with everything
    """

    def __eq__(self, other):
        return False

    def __ne__(self, other):
        return True

    if py_ver < (3, 0):
        def __nonzero__(self):
            """
            Vapor objects are always False
            """
            return False
    else:
        def __bool__(self):
            """
            Vapor objects are always False
            """
            return False
Vapor = Vapor()


class Char(unicode):
    """
    Strips trailing whitespace, and ignores trailing whitespace for comparisons
    """

    def __new__(cls, text=''):
        if not isinstance(text, (basestring, cls)):
            raise ValueError("Unable to automatically coerce %r to Char" % text)
        result = unicode.__new__(cls, text.rstrip())
        result.field_size = len(text)
        return result

    __hash__ = unicode.__hash__

    def __eq__(self, other):
        """
        ignores trailing whitespace
        """
        if not isinstance(other, (self.__class__, basestring)):
            return NotImplemented
        return unicode(self) == other.rstrip()

    def __ge__(self, other):
        """
        ignores trailing whitespace
        """
        if not isinstance(other, (self.__class__, basestring)):
            return NotImplemented
        return unicode(self) >= other.rstrip()

    def __gt__(self, other):
        """
        ignores trailing whitespace
        """
        if not isinstance(other, (self.__class__, basestring)):
            return NotImplemented
        return unicode(self) > other.rstrip()

    def __le__(self, other):
        """
        ignores trailing whitespace
        """
        if not isinstance(other, (self.__class__, basestring)):
            return NotImplemented
        return unicode(self) <= other.rstrip()

    def __lt__(self, other):
        """
        ignores trailing whitespace
        """
        if not isinstance(other, (self.__class__, basestring)):
            return NotImplemented
        return unicode(self) < other.rstrip()

    def __ne__(self, other):
        """
        ignores trailing whitespace
        """
        if not isinstance(other, (self.__class__, basestring)):
            return NotImplemented
        return unicode(self) != other.rstrip()

    if py_ver < (3, 0):
        def __nonzero__(self):
            """
            ignores trailing whitespace
            """
            return bool(unicode(self))
    else:
        def __bool__(self):
            """
            ignores trailing whitespace
            """
            return bool(unicode(self))

    def __add__(self, other):
        result = self.__class__(unicode(self) + other)
        result.field_size = self.field_size
        return result

from . import bridge
basestring = bridge.basestring = bridge.basestring + (Char, )
baseinteger = bridge.baseinteger

# wrappers around datetime and logical objects to allow null values

class Date(object):
    """
    adds null capable datetime.date constructs
    """

    __slots__ = ['_date']

    def __new__(cls, year=None, month=0, day=0):
        """
        date should be either a datetime.date or date/month/day should
        all be appropriate integers
        """
        if year is None or year is Null:
            return cls._null_date
        nd = object.__new__(cls)
        if isinstance(year, basestring):
            return Date.strptime(year)
        elif isinstance(year, (datetime.date)):
            nd._date = year
        elif isinstance(year, (Date)):
            nd._date = year._date
        else:
            nd._date = datetime.date(year, month, day)
        return nd

    def __add__(self, other):
        if self and isinstance(other, (datetime.timedelta)):
            return Date(self._date + other)
        else:
            return NotImplemented

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._date == other._date
        if isinstance(other, datetime.date):
            return self._date == other
        if isinstance(other, type(None)):
            return self._date is None
        return NotImplemented

    def __format__(self, spec):
        if self:
            return self._date.__format__(spec)
        return ''

    def __getattr__(self, name):
        if name == '_date':
            raise AttributeError('_date missing!')
        elif self:
            return getattr(self._date, name)
        else:
            raise AttributeError('NullDate object has no attribute %s' % name)

    def __ge__(self, other):
        if self:
            if isinstance(other, (datetime.date)):
                return self._date >= other
            elif isinstance(other, (Date)):
                if other:
                    return self._date >= other._date
                return True
        else:
            if isinstance(other, (datetime.date)):
                return False
            elif isinstance(other, (Date)):
                if other:
                    return False
                return True
        return NotImplemented

    def __gt__(self, other):
        if self:
            if isinstance(other, (datetime.date)):
                return self._date > other
            elif isinstance(other, (Date)):
                if other:
                    return self._date > other._date
                return True
        else:
            if isinstance(other, (datetime.date)):
                return False
            elif isinstance(other, (Date)):
                if other:
                    return False
                return False
        return NotImplemented

    def __hash__(self):
        return hash(self._date)

    def __le__(self, other):
        if self:
            if isinstance(other, (datetime.date)):
                return self._date <= other
            elif isinstance(other, (Date)):
                if other:
                    return self._date <= other._date
                return False
        else:
            if isinstance(other, (datetime.date)):
                return True
            elif isinstance(other, (Date)):
                if other:
                    return True
                return True
        return NotImplemented

    def __lt__(self, other):
        if self:
            if isinstance(other, (datetime.date)):
                return self._date < other
            elif isinstance(other, (Date)):
                if other:
                    return self._date < other._date
                return False
        else:
            if isinstance(other, (datetime.date)):
                return True
            elif isinstance(other, (Date)):
                if other:
                    return True
                return False
        return NotImplemented

    def __ne__(self, other):
        if self:
            if isinstance(other, (datetime.date)):
                return self._date != other
            elif isinstance(other, (Date)):
                if other:
                    return self._date != other._date
                return True
        else:
            if isinstance(other, (datetime.date)):
                return True
            elif isinstance(other, (Date)):
                if other:
                    return True
                return False
        return NotImplemented

    if py_ver < (3, 0):
        def __nonzero__(self):
            return self._date is not None
    else:
        def __bool__(self):
            return self._date is not None

    __radd__ = __add__

    def __rsub__(self, other):
        if self and isinstance(other, (datetime.date)):
            return other - self._date
        elif self and isinstance(other, (Date)):
            return other._date - self._date
        elif self and isinstance(other, (datetime.timedelta)):
            return Date(other - self._date)
        else:
            return NotImplemented

    def __repr__(self):
        if self:
            return "Date(%d, %d, %d)" % self.timetuple()[:3]
        else:
            return "Date()"

    def __str__(self):
        if self:
            return unicode(self._date)
        return ""

    def __sub__(self, other):
        if self and isinstance(other, (datetime.date)):
            return self._date - other
        elif self and isinstance(other, (Date)):
            return self._date - other._date
        elif self and isinstance(other, (datetime.timedelta)):
            return Date(self._date - other)
        else:
            return NotImplemented

    def date(self):
        if self:
            return self._date
        return None

    @classmethod
    def fromordinal(cls, number):
        if number:
            return cls(datetime.date.fromordinal(number))
        return cls()

    @classmethod
    def fromtimestamp(cls, timestamp):
        return cls(datetime.date.fromtimestamp(timestamp))

    @classmethod
    def fromymd(cls, yyyymmdd):
        if yyyymmdd in ('', '        ', 'no date', '00000000'):
            return cls()
        return cls(datetime.date(int(yyyymmdd[:4]), int(yyyymmdd[4:6]), int(yyyymmdd[6:])))

    def replace(self, year=None, month=None, day=None, delta_year=0, delta_month=0, delta_day=0):
        if not self:
            return self.__class__._null_date
        old_year, old_month, old_day = self.timetuple()[:3]
        if isinstance(month, RelativeMonth):
            this_month = IsoMonth(old_month)
            delta_month += month.months_from(this_month)
            month = None
        if isinstance(day, RelativeDay):
            this_day = IsoDay(self.isoweekday())
            delta_day += day.days_from(this_day)
            day = None
        year = (year or old_year) + delta_year
        month = (month or old_month) + delta_month
        day = (day or old_day) + delta_day
        days_in_month = (days_per_month, days_per_leap_month)[is_leapyear(year)]
        while not(0 < month < 13) or not (0 < day <= days_in_month[month]):
            while month < 1:
                year -= 1
                month = 12 + month
            while month > 12:
                year += 1
                month = month - 12
            days_in_month = (days_per_month, days_per_leap_month)[is_leapyear(year)]
            while day < 1:
                month -= 1
                day = days_in_month[month] + day
                if not 0 < month < 13:
                    break
            while day > days_in_month[month]:
                day = day - days_in_month[month]
                month += 1
                if not 0 < month < 13:
                    break
        return Date(year, month, day)

    def strftime(self, format):
        fmt_cls = type(format)
        if self:
            return fmt_cls(self._date.strftime(format))
        return fmt_cls('')

    @classmethod
    def strptime(cls, date_string, format=None):
        if format is not None:
            return cls(*(time.strptime(date_string, format)[0:3]))
        return cls(*(time.strptime(date_string, "%Y-%m-%d")[0:3]))

    def timetuple(self):
        return self._date.timetuple()

    @classmethod
    def today(cls):
        return cls(datetime.date.today())

    def ymd(self):
        if self:
            return "%04d%02d%02d" % self.timetuple()[:3]
        else:
            return '        '

Date.max = Date(datetime.date.max)
Date.min = Date(datetime.date.min)
Date._null_date = object.__new__(Date)
Date._null_date._date = None
NullDate = Date()


class DateTime(object):
    """
    adds null capable datetime.datetime constructs
    """

    __slots__ = ['_datetime']

    def __new__(cls, year=None, month=0, day=0, hour=0, minute=0, second=0, microsecond=0, tzinfo=Null):
        """year may be a datetime.datetime"""
        if year is None or year is Null:
            return cls._null_datetime
        ndt = object.__new__(cls)
        if isinstance(year, basestring):
            return DateTime.strptime(year)
        elif isinstance(year, DateTime):
            if tzinfo is not Null and year._datetime.tzinfo:
                raise ValueError('not naive datetime (tzinfo is already set)')
            elif tzinfo is Null:
                tzinfo = None
            ndt._datetime = year._datetime
        elif isinstance(year, datetime.datetime):
            if tzinfo is not Null and year.tzinfo:
                raise ValueError('not naive datetime (tzinfo is already set)')
            elif tzinfo is Null:
                tzinfo = year.tzinfo
            microsecond = year.microsecond // 1000 * 1000
            hour, minute, second = year.hour, year.minute, year.second
            year, month, day = year.year, year.month, year.day
            if pytz is None or tzinfo is None:
                ndt._datetime = datetime.datetime(year, month, day, hour, minute, second, microsecond, tzinfo)
            else:
                # if pytz and tzinfo, tzinfo must be added after creation
                _datetime = datetime.datetime(year, month, day, hour, minute, second, microsecond)
                ndt._datetime = tzinfo.normalize(tzinfo.localize(_datetime))
        elif year is not None:
            if tzinfo is Null:
                tzinfo = None
            microsecond = microsecond // 1000 * 1000
            if pytz is None or tzinfo is None:
                ndt._datetime = datetime.datetime(year, month, day, hour, minute, second, microsecond, tzinfo)
            else:
                # if pytz and tzinfo, tzinfo must be added after creation
                _datetime = datetime.datetime(year, month, day, hour, minute, second, microsecond)
                ndt._datetime = tzinfo.normalize(tzinfo.localize(_datetime))
        return ndt

    def __add__(self, other):
        if self and isinstance(other, (datetime.timedelta)):
            return DateTime(self._datetime + other)
        else:
            return NotImplemented

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._datetime == other._datetime
        if isinstance(other, datetime.date):
            me = self._datetime.timetuple()
            them = other.timetuple()
            return me[:6] == them[:6] and self.microsecond == (other.microsecond//1000*1000)
        if isinstance(other, type(None)):
            return self._datetime is None
        return NotImplemented

    def __format__(self, spec):
        if self:
            return self._datetime.__format__(spec)
        return ''

    def __getattr__(self, name):
        if name == '_datetime':
            raise AttributeError('_datetime missing!')
        elif self:
            return getattr(self._datetime, name)
        else:
            raise AttributeError('NullDateTime object has no attribute %s' % name)

    def __ge__(self, other):
        if self:
            if isinstance(other, (datetime.datetime)):
                return self._datetime >= other
            elif isinstance(other, (DateTime)):
                if other:
                    return self._datetime >= other._datetime
                return False
        else:
            if isinstance(other, (datetime.datetime)):
                return False
            elif isinstance(other, (DateTime)):
                if other:
                    return False
                return True
        return NotImplemented

    def __gt__(self, other):
        if self:
            if isinstance(other, (datetime.datetime)):
                return self._datetime > other
            elif isinstance(other, (DateTime)):
                if other:
                    return self._datetime > other._datetime
                return True
        else:
            if isinstance(other, (datetime.datetime)):
                return False
            elif isinstance(other, (DateTime)):
                if other:
                    return False
                return False
        return NotImplemented

    def __hash__(self):
        return self._datetime.__hash__()

    def __le__(self, other):
        if self:
            if isinstance(other, (datetime.datetime)):
                return self._datetime <= other
            elif isinstance(other, (DateTime)):
                if other:
                    return self._datetime <= other._datetime
                return False
        else:
            if isinstance(other, (datetime.datetime)):
                return True
            elif isinstance(other, (DateTime)):
                if other:
                    return True
                return True
        return NotImplemented

    def __lt__(self, other):
        if self:
            if isinstance(other, (datetime.datetime)):
                return self._datetime < other
            elif isinstance(other, (DateTime)):
                if other:
                    return self._datetime < other._datetime
                return False
        else:
            if isinstance(other, (datetime.datetime)):
                return True
            elif isinstance(other, (DateTime)):
                if other:
                    return True
                return False
        return NotImplemented

    def __ne__(self, other):
        if self:
            if isinstance(other, (datetime.datetime)):
                return self._datetime != other
            elif isinstance(other, (DateTime)):
                if other:
                    return self._datetime != other._datetime
                return True
        else:
            if isinstance(other, (datetime.datetime)):
                return True
            elif isinstance(other, (DateTime)):
                if other:
                    return True
                return False
        return NotImplemented

    if py_ver < (3, 0):
        def __nonzero__(self):
            return self._datetime is not None
    else:
        def __bool__(self):
            return self._datetime is not None

    __radd__ = __add__

    def __rsub__(self, other):
        if self and isinstance(other, (datetime.datetime)):
            return other - self._datetime
        elif self and isinstance(other, (DateTime)):
            return other._datetime - self._datetime
        elif self and isinstance(other, (datetime.timedelta)):
            return DateTime(other - self._datetime)
        else:
            return NotImplemented

    def __repr__(self):
        if self:
            if self.tzinfo is None:
                tz = ''
            else:
                diff = self._datetime.utcoffset()
                hours, minutes = divmod(diff.days * 86400 + diff.seconds, 3600)
                minus, hours = hours < 0, abs(hours)
                tz = ', tzinfo=<%s %s%02d%02d>' % (self._datetime.tzname(), ('','-')[minus], hours, minutes)
            return "DateTime(%d, %d, %d, %d, %d, %d, %d%s)" % (
                self._datetime.timetuple()[:6] + (self._datetime.microsecond, tz)
                )
        else:
            return "DateTime()"

    def __str__(self):
        if self:
            return unicode(self._datetime)
        return ""

    def __sub__(self, other):
        if self and isinstance(other, (datetime.datetime)):
            return self._datetime - other
        elif self and isinstance(other, (DateTime)):
            return self._datetime - other._datetime
        elif self and isinstance(other, (datetime.timedelta)):
            return DateTime(self._datetime - other)
        else:
            return NotImplemented

    @classmethod
    def combine(cls, date, time, tzinfo=Null):
        # if tzinfo is given, timezone is added/stripped
        if tzinfo is Null:
            tzinfo = time.tzinfo
        if Date(date) and Time(time):
            return cls(
                    date.year, date.month, date.day,
                    time.hour, time.minute, time.second, time.microsecond,
                    tzinfo=tzinfo,
                    )
        return cls()

    def date(self):
        if self:
            return Date(self.year, self.month, self.day)
        return Date()

    def datetime(self):
        if self:
            return self._datetime
        return None

    @classmethod
    def fromordinal(cls, number):
        if number:
            return cls(datetime.datetime.fromordinal(number))
        else:
            return cls()

    @classmethod
    def fromtimestamp(cls, timestamp):
        return DateTime(datetime.datetime.fromtimestamp(timestamp))

    @classmethod
    def now(cls, tzinfo=None):
        "only accurate to milliseconds"
        return cls(datetime.datetime.now(), tzinfo=tzinfo)

    def replace(self, year=None, month=None, day=None, hour=None, minute=None, second=None, microsecond=None, tzinfo=Null,
              delta_year=0, delta_month=0, delta_day=0, delta_hour=0, delta_minute=0, delta_second=0):
        if not self:
            return self.__class__._null_datetime
        old_year, old_month, old_day, old_hour, old_minute, old_second, old_micro = self.timetuple()[:7]
        if tzinfo is Null:
            tzinfo = self._datetime.tzinfo
        if isinstance(month, RelativeMonth):
            this_month = IsoMonth(old_month)
            delta_month += month.months_from(this_month)
            month = None
        if isinstance(day, RelativeDay):
            this_day = IsoDay(self.isoweekday())
            delta_day += day.days_from(this_day)
            day = None
        year = (year or old_year) + delta_year
        month = (month or old_month) + delta_month
        day = (day or old_day) + delta_day
        hour = (hour or old_hour) + delta_hour
        minute = (minute or old_minute) + delta_minute
        second = (second or old_second) + delta_second
        microsecond = microsecond or old_micro
        days_in_month = (days_per_month, days_per_leap_month)[is_leapyear(year)]
        while ( not (0 < month < 13)
        or      not (0 < day <= days_in_month[month])
        or      not (0 <= hour < 24)
        or      not (0 <= minute < 60)
        or      not (0 <= second < 60)
        ):
            while month < 1:
                year -= 1
                month = 12 + month
            while month > 12:
                year += 1
                month = month - 12
            days_in_month = (days_per_month, days_per_leap_month)[is_leapyear(year)]
            while day < 1:
                month -= 1
                day = days_in_month[month] + day
                if not 0 < month < 13:
                    break
            while day > days_in_month[month]:
                day = day - days_in_month[month]
                month += 1
                if not 0 < month < 13:
                    break
            while hour < 1:
                day -= 1
                hour = 24 + hour
            while hour > 23:
                day += 1
                hour = hour - 24
            while minute < 0:
                hour -= 1
                minute = 60 + minute
            while minute > 59:
                hour += 1
                minute = minute - 60
            while second < 0:
                minute -= 1
                second = 60 + second
            while second > 59:
                minute += 1
                second = second - 60
        return DateTime(year, month, day, hour, minute, second, microsecond, tzinfo)

    def strftime(self, format):
        fmt_cls = type(format)
        if self:
            return fmt_cls(self._datetime.strftime(format))
        return fmt_cls('')

    @classmethod
    def strptime(cls, datetime_string, format=None):
        if format is not None:
            return cls(datetime.datetime.strptime(datetime_string, format))
        for format in (
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%d %H:%M:%S",
                ):
            try:
                return cls(datetime.datetime.strptime(datetime_string, format))
            except ValueError:
                pass
        raise ValueError("Unable to convert %r" % datetime_string)

    def time(self):
        if self:
            return Time(self.hour, self.minute, self.second, self.microsecond)
        return Time()

    def timetuple(self):
        return self._datetime.timetuple()

    def timetz(self):
        if self:
            return Time(self._datetime.timetz())
        return Time()

    @classmethod
    def utcnow(cls):
        return cls(datetime.datetime.utcnow())

    @classmethod
    def today(cls):
        return cls(datetime.datetime.today())

DateTime.max = DateTime(datetime.datetime.max)
DateTime.min = DateTime(datetime.datetime.min)
DateTime._null_datetime = object.__new__(DateTime)
DateTime._null_datetime._datetime = None
NullDateTime = DateTime()


class Time(object):
    """
    adds null capable datetime.time constructs
    """

    __slots__ = ['_time']

    def __new__(cls, hour=None, minute=0, second=0, microsecond=0, tzinfo=Null):
        """
        hour may be a datetime.time or a str(Time)
        """
        if hour is None or hour is Null:
            return cls._null_time
        nt = object.__new__(cls)
        if isinstance(hour, basestring):
            hour = Time.strptime(hour)
        if isinstance(hour, Time):
            if tzinfo is not Null and hour._time.tzinfo:
                raise ValueError('not naive time (tzinfo is already set)')
            elif tzinfo is Null:
                tzinfo = None
            nt._time = hour._time.replace(tzinfo=tzinfo)
        elif isinstance(hour, (datetime.time)):
            if tzinfo is not Null and hour.tzinfo:
                raise ValueError('not naive time (tzinfo is already set)')
            if tzinfo is Null:
                tzinfo = hour.tzinfo
            microsecond = hour.microsecond // 1000 * 1000
            hour, minute, second = hour.hour, hour.minute, hour.second
            nt._time = datetime.time(hour, minute, second, microsecond, tzinfo)
        elif hour is not None:
            if tzinfo is Null:
                tzinfo = None
            microsecond = microsecond // 1000 * 1000
            nt._time = datetime.time(hour, minute, second, microsecond, tzinfo)
        return nt

    def __add__(self, other):
        if self and isinstance(other, (datetime.timedelta)):
            t = self._time
            t = datetime.datetime(2012, 6, 27, t.hour, t.minute, t.second, t.microsecond)
            t += other
            return Time(t.hour, t.minute, t.second, t.microsecond)
        else:
            return NotImplemented

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._time == other._time
        if isinstance(other, datetime.time):
            return (
                    self.hour == other.hour and
                    self.minute == other.minute and
                    self.second == other.second and
                    self.microsecond == (other.microsecond//1000*1000)
                    )
        if isinstance(other, type(None)):
            return self._time is None
        return NotImplemented

    def __format__(self, spec):
        if self:
            return self._time.__format__(spec)
        return ''

    def __getattr__(self, name):
        if name == '_time':
            raise AttributeError('_time missing!')
        elif self:
            return getattr(self._time, name)
        else:
            raise AttributeError('NullTime object has no attribute %s' % name)

    def __ge__(self, other):
        if self:
            if isinstance(other, (datetime.time)):
                return self._time >= other
            elif isinstance(other, (Time)):
                if other:
                    return self._time >= other._time
                return False
        else:
            if isinstance(other, (datetime.time)):
                return False
            elif isinstance(other, (Time)):
                if other:
                    return False
                return True
        return NotImplemented

    def __gt__(self, other):
        if self:
            if isinstance(other, (datetime.time)):
                return self._time > other
            elif isinstance(other, (DateTime)):
                if other:
                    return self._time > other._time
                return True
        else:
            if isinstance(other, (datetime.time)):
                return False
            elif isinstance(other, (Time)):
                if other:
                    return False
                return False
        return NotImplemented

    def __hash__(self):
        return self._datetime.__hash__()

    def __le__(self, other):
        if self:
            if isinstance(other, (datetime.time)):
                return self._time <= other
            elif isinstance(other, (Time)):
                if other:
                    return self._time <= other._time
                return False
        else:
            if isinstance(other, (datetime.time)):
                return True
            elif isinstance(other, (Time)):
                if other:
                    return True
                return True
        return NotImplemented

    def __lt__(self, other):
        if self:
            if isinstance(other, (datetime.time)):
                return self._time < other
            elif isinstance(other, (Time)):
                if other:
                    return self._time < other._time
                return False
        else:
            if isinstance(other, (datetime.time)):
                return True
            elif isinstance(other, (Time)):
                if other:
                    return True
                return False
        return NotImplemented

    def __ne__(self, other):
        if self:
            if isinstance(other, (datetime.time)):
                return self._time != other
            elif isinstance(other, (Time)):
                if other:
                    return self._time != other._time
                return True
        else:
            if isinstance(other, (datetime.time)):
                return True
            elif isinstance(other, (Time)):
                if other:
                    return True
                return False
        return NotImplemented

    if py_ver < (3, 0):
        def __nonzero__(self):
            return self._time is not None
    else:
        def __bool__(self):
            return self._time is not None

    __radd__ = __add__

    def __rsub__(self, other):
        if self and isinstance(other, (Time, datetime.time)):
            t = self._time
            t = datetime.datetime(2012, 6, 27, t.hour, t.minute, t.second, t.microsecond)
            other = datetime.datetime(2012, 6, 27, other.hour, other.minute, other.second, other.microsecond)
            other -= t
            return other
        else:
            return NotImplemented

    def __repr__(self):
        if self:
            if self.tzinfo is None:
                tz = ''
            else:
                diff = self._time.tzinfo.utcoffset(self._time)
                hours, minutes = divmod(diff.days * 86400 + diff.seconds, 3600)
                minus, hours = hours < 0, abs(hours)
                tz = ', tzinfo=<%s %s%02d%02d>' % (self._time.tzinfo.tzname(self._time), ('','-')[minus], hours, minutes)
            return "Time(%d, %d, %d, %d%s)" % (self.hour, self.minute, self.second, self.microsecond, tz)
        else:
            return "Time()"

    def __str__(self):
        if self:
            return unicode(self._time)
        return ""

    def __sub__(self, other):
        if self and isinstance(other, (Time, datetime.time)):
            t = self._time
            t = datetime.datetime(2012, 6, 27, t.hour, t.minute, t.second, t.microsecond)
            o = datetime.datetime(2012, 6, 27, other.hour, other.minute, other.second, other.microsecond)
            return t - o
        elif self and isinstance(other, (datetime.timedelta)):
            t = self._time
            t = datetime.datetime(2012, 6, 27, t.hour, t.minute, t.second, t.microsecond)
            t -= other
            return Time(t.hour, t.minute, t.second, t.microsecond)
        else:
            return NotImplemented

    @classmethod
    def fromfloat(cls, num):
        "2.5 == 2 hours, 30 minutes, 0 seconds, 0 microseconds"
        if num < 0:
            raise ValueError("positive value required (got %r)" % num)
        if num == 0:
            return Time(0)
        hours = int(num)
        if hours:
            num = num % hours
        minutes = int(num * 60)
        if minutes:
            num = num * 60 % minutes
        else:
            num = num * 60
        seconds = int(num * 60)
        if seconds:
            num = num * 60 % seconds
        else:
            num = num * 60
        microseconds = int(num * 1000)
        return Time(hours, minutes, seconds, microseconds)

    @staticmethod
    def now(tzinfo=None):
        "only accurate to milliseconds"
        return DateTime.now(tzinfo).timetz()

    def replace(self, hour=None, minute=None, second=None, microsecond=None, tzinfo=Null, delta_hour=0, delta_minute=0, delta_second=0):
        if not self:
            return self.__class__._null_time
        if tzinfo is Null:
            tzinfo = self._time.tzinfo
        old_hour, old_minute, old_second, old_micro = self.hour, self.minute, self.second, self.microsecond
        hour = (hour or old_hour) + delta_hour
        minute = (minute or old_minute) + delta_minute
        second = (second or old_second) + delta_second
        microsecond = microsecond or old_micro
        while not (0 <= hour < 24) or not (0 <= minute < 60) or not (0 <= second < 60):
            while second < 0:
                minute -= 1
                second = 60 + second
            while second > 59:
                minute += 1
                second = second - 60
            while minute < 0:
                hour -= 1
                minute = 60 + minute
            while minute > 59:
                hour += 1
                minute = minute - 60
            while hour < 1:
                hour = 24 + hour
            while hour > 23:
                hour = hour - 24
        return Time(hour, minute, second, microsecond, tzinfo)

    def strftime(self, format):
        fmt_cls = type(format)
        if self:
            return fmt_cls(self._time.strftime(format))
        return fmt_cls('')

    @classmethod
    def strptime(cls, time_string, format=None):
        if format is not None:
            return cls(*time.strptime(time_string, format)[3:6])
        for format in (
                "%H:%M:%S.%f",
                "%H:%M:%S",
                ):
            try:
                return cls(*time.strptime(time_string, format)[3:6])
            except ValueError:
                pass
        raise ValueError("Unable to convert %r" % time_string)

    def time(self):
        if self:
            return self._time
        return None

    def tofloat(self):
        "returns Time as a float"
        hour = self.hour
        minute = self.minute * (1.0 / 60)
        second = self.second * (1.0 / 3600)
        microsecond = self.microsecond * (1.0 / 3600000)
        return hour + minute + second + microsecond

Time.max = Time(datetime.time.max)
Time.min = Time(datetime.time.min)
Time._null_time = object.__new__(Time)
Time._null_time._time = None
NullTime = Time()


class Period(object):
    "for matching various time ranges"

    def __init__(self, year=None, month=None, day=None, hour=None, minute=None, second=None, microsecond=None):
        params = vars()
        self._mask = {}
        #
        if year:
            attrs = []
            if isinstance(year, (Date, datetime.date)):
                attrs = ['year','month','day']
            elif isinstance(year, (DateTime, datetime.datetime)):
                attrs = ['year','month','day','hour','minute','second']
            elif isinstance(year, (Time, datetime.time)):
                attrs = ['hour','minute','second']
            for attr in attrs:
                value = getattr(year, attr)
                self._mask[attr] = value
        #
        for attr in ('year', 'month', 'day', 'hour', 'minute', 'second', 'microsecond'):
            value = params[attr]
            if value is not None:
                self._mask[attr] = value

    def __contains__(self, other):
        if not self._mask:
            return True
        for attr, value in self._mask.items():
            other_value = getattr(other, attr, None)
            try:
                if other_value == value or other_value in value:
                    continue
            except TypeError:
                pass
            return False
        return True

    def __repr__(self):
        items = []
        for attr in ('year', 'month', 'day', 'hour', 'minute', 'second', 'microsecond'):
            if attr in self._mask:
                items.append('%s=%s' % (attr, self._mask[attr]))
        return "Period(%s)" % ', '.join(items)


class Logical(object):
    """
    Logical field return type.

    Accepts values of True, False, or None/Null.
    boolean value of Unknown is False (use Quantum if you want an exception instead.
    """

    def __new__(cls, value=None):
        if value is None or value is Null or value is Other or value is Unknown:
            return cls.unknown
        elif isinstance(value, basestring):
            if value.lower() in ('t', 'true', 'y', 'yes', 'on'):
                return cls.true
            elif value.lower() in ('f', 'false', 'n', 'no', 'off'):
                return cls.false
            elif value.lower() in ('?', 'unknown', 'null', 'none', ' ', ''):
                return cls.unknown
            else:
                raise ValueError('unknown value for Logical: %s' % value)
        else:
            return (cls.false, cls.true)[bool(value)]

    def __add__(x, y):
        if isinstance(y, type(None)) or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(y)
        except Exception:
            return NotImplemented
        return int(x) + i

    __radd__ = __iadd__ = __add__

    def __sub__(x, y):
        if isinstance(y, type(None)) or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(y)
        except Exception:
            return NotImplemented
        return int(x) - i

    __isub__ = __sub__

    def __rsub__(y, x):
        if isinstance(x, type(None)) or x is Unknown or y is Unknown:
            return Unknown
        try:
            i = int(x)
        except Exception:
            return NotImplemented
        return i - int(y)

    def __mul__(x, y):
        if x == 0 or y == 0:
            return 0
        elif isinstance(y, type(None)) or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(y)
        except Exception:
            return NotImplemented
        return int(x) * i

    __rmul__ = __imul__ = __mul__

    def __div__(x, y):
        if isinstance(y, type(None)) or y == 0 or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(y)
        except Exception:
            return NotImplemented
        return int(x).__div__(i)

    __idiv__ = __div__

    def __rdiv__(y, x):
        if isinstance(x, type(None)) or y == 0 or x is Unknown or y is Unknown:
            return Unknown
        try:
            i = int(x)
        except Exception:
            return NotImplemented
        return i.__div__(int(y))

    def __truediv__(x, y):
        if isinstance(y, type(None)) or y == 0 or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(y)
        except Exception:
            return NotImplemented
        return int(x).__truediv__(i)

    __itruediv__ = __truediv__

    def __rtruediv__(y, x):
        if isinstance(x, type(None)) or y == 0 or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(x)
        except Exception:
            return NotImplemented
        return i.__truediv__(int(y))

    def __floordiv__(x, y):
        if isinstance(y, type(None)) or y == 0 or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(y)
        except Exception:
            return NotImplemented
        return int(x).__floordiv__(i)

    __ifloordiv__ = __floordiv__

    def __rfloordiv__(y, x):
        if isinstance(x, type(None)) or y == 0 or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(x)
        except Exception:
            return NotImplemented
        return i.__floordiv__(int(y))

    def __divmod__(x, y):
        if isinstance(y, type(None)) or y == 0 or y is Unknown or x is Unknown:
            return (Unknown, Unknown)
        try:
            i = int(y)
        except Exception:
            return NotImplemented
        return divmod(int(x), i)

    def __rdivmod__(y, x):
        if isinstance(x, type(None)) or y == 0 or y is Unknown or x is Unknown:
            return (Unknown, Unknown)
        try:
            i = int(x)
        except Exception:
            return NotImplemented
        return divmod(i, int(y))

    def __mod__(x, y):
        if isinstance(y, type(None)) or y == 0 or y is Unknown or x is Unknown:
            return Unknown
        try:
            i = int(y)
        except Exception:
            return NotImplemented
        return int(x) % i

    __imod__ = __mod__

    def __rmod__(y, x):
        if isinstance(x, type(None)) or y == 0 or x is Unknown or y is Unknown:
            return Unknown
        try:
            i = int(x)
        except Exception:
            return NotImplemented
        return i % int(y)

    def __pow__(x, y):
        if not isinstance(y, (x.__class__, bool, type(None), baseinteger)):
            return NotImplemented
        if isinstance(y, type(None)) or y is Unknown:
            return Unknown
        i = int(y)
        if i == 0:
            return 1
        if x is Unknown:
            return Unknown
        return int(x) ** i

    __ipow__ = __pow__

    def __rpow__(y, x):
        if not isinstance(x, (y.__class__, bool, type(None), baseinteger)):
            return NotImplemented
        if y is Unknown:
            return Unknown
        i = int(y)
        if i == 0:
            return 1
        if x is Unknown or isinstance(x, type(None)):
            return Unknown
        return int(x) ** i

    def __lshift__(x, y):
        if isinstance(y, type(None)) or x is Unknown or y is Unknown:
            return Unknown
        return int(x.value) << int(y)

    __ilshift__ = __lshift__

    def __rlshift__(y, x):
        if isinstance(x, type(None)) or x is Unknown or y is Unknown:
            return Unknown
        return int(x) << int(y)

    def __rshift__(x, y):
        if isinstance(y, type(None)) or x is Unknown or y is Unknown:
            return Unknown
        return int(x.value) >> int(y)

    __irshift__ = __rshift__

    def __rrshift__(y, x):
        if isinstance(x, type(None)) or x is Unknown or y is Unknown:
            return Unknown
        return int(x) >> int(y)

    def __neg__(x):
        "NEG (negation)"
        if x in (Truth, Falsth):
            return -x.value
        return Unknown

    def __pos__(x):
        "POS (posation)"
        if x in (Truth, Falsth):
            return +x.value
        return Unknown

    def __abs__(x):
        if x in (Truth, Falsth):
            return abs(x.value)
        return Unknown

    def __invert__(x):
        if x in (Truth, Falsth):
            return (Truth, Falsth)[x.value]
        return Unknown

    def __complex__(x):
        if x.value is None:
            raise ValueError("unable to return complex() of %r" % x)
        return complex(x.value)

    def __int__(x):
        if x.value is None:
            raise ValueError("unable to return int() of %r" % x)
        return int(x.value)

    if py_ver < (3, 0):
        def __long__(x):
            if x.value is None:
                raise ValueError("unable to return long() of %r" % x)
            return long(x.value)

    def __float__(x):
        if x.value is None:
            raise ValueError("unable to return float() of %r" % x)
        return float(x.value)

    if py_ver < (3, 0):
        def __oct__(x):
            if x.value is None:
                raise ValueError("unable to return oct() of %r" % x)
            return oct(x.value)

        def __hex__(x):
            if x.value is None:
                raise ValueError("unable to return hex() of %r" % x)
            return hex(x.value)

    def __and__(x, y):
        """
        AND (conjunction) x & y:
        True iff both x, y are True
        False iff at least one of x, y is False
        Unknown otherwise
        """
        if (isinstance(x, baseinteger) and not isinstance(x, bool)) or (isinstance(y, baseinteger) and not isinstance(y, bool)):
            if x == 0 or y == 0:
                return 0
            elif x is Unknown or y is Unknown:
                return Unknown
            return int(x) & int(y)
        elif x in (False, Falsth) or y in (False, Falsth):
            return Falsth
        elif x in (True, Truth) and y in (True, Truth):
            return Truth
        elif isinstance(x, type(None)) or isinstance(y, type(None)) or y is Unknown or x is Unknown:
            return Unknown
        return NotImplemented

    __rand__ = __and__

    def __or__(x, y):
        "OR (disjunction): x | y => True iff at least one of x, y is True"
        if (isinstance(x, baseinteger) and not isinstance(x, bool)) or (isinstance(y, baseinteger) and not isinstance(y, bool)):
            if x is Unknown or y is Unknown:
                return Unknown
            return int(x) | int(y)
        elif x in (True, Truth) or y in (True, Truth):
            return Truth
        elif x in (False, Falsth) and y in (False, Falsth):
            return Falsth
        elif isinstance(x, type(None)) or isinstance(y, type(None)) or y is Unknown or x is Unknown:
            return Unknown
        return NotImplemented

    __ror__ = __or__

    def __xor__(x, y):
        "XOR (parity) x ^ y: True iff only one of x,y is True"
        if (isinstance(x, baseinteger) and not isinstance(x, bool)) or (isinstance(y, baseinteger) and not isinstance(y, bool)):
            if x is Unknown or y is Unknown:
                return Unknown
            return int(x) ^ int(y)
        elif x in (True, Truth, False, Falsth) and y in (True, Truth, False, Falsth):
            return {
                    (True, True)  : Falsth,
                    (True, False) : Truth,
                    (False, True) : Truth,
                    (False, False): Falsth,
                   }[(x, y)]
        elif isinstance(x, type(None)) or isinstance(y, type(None)) or y is Unknown or x is Unknown:
            return Unknown
        return NotImplemented

    __rxor__ = __xor__

    if py_ver < (3, 0):
        def __nonzero__(x):
            "boolean value of Unknown is assumed False"
            return x.value is True
    else:
        def __bool__(x):
            "boolean value of Unknown is assumed False"
            return x.value is True

    def __eq__(x, y):
        if isinstance(y, x.__class__):
            return x.value == y.value
        elif isinstance(y, (bool, type(None), baseinteger)):
            return x.value == y
        return NotImplemented

    def __ge__(x, y):
        if isinstance(y, type(None)) or x is Unknown or y is Unknown:
            return x.value == None
        elif isinstance(y, x.__class__):
            return x.value >= y.value
        elif isinstance(y, (bool, baseinteger)):
            return x.value >= y
        return NotImplemented

    def __gt__(x, y):
        if isinstance(y, type(None)) or x is Unknown or y is Unknown:
            return False
        elif isinstance(y, x.__class__):
            return x.value > y.value
        elif isinstance(y, (bool, baseinteger)):
            return x.value > y
        return NotImplemented

    def __le__(x, y):
        if isinstance(y, type(None)) or x is Unknown or y is Unknown:
            return x.value == None
        elif isinstance(y, x.__class__):
            return x.value <= y.value
        elif isinstance(y, (bool, baseinteger)):
            return x.value <= y
        return NotImplemented

    def __lt__(x, y):
        if isinstance(y, type(None)) or x is Unknown or y is Unknown:
            return False
        elif isinstance(y, x.__class__):
            return x.value < y.value
        elif isinstance(y, (bool, baseinteger)):
            return x.value < y
        return NotImplemented

    def __ne__(x, y):
        if isinstance(y, x.__class__):
            return x.value != y.value
        elif isinstance(y, (bool, type(None), baseinteger)):
            return x.value != y
        return NotImplemented

    def __hash__(x):
        return hash(x.value)

    def __index__(x):
        if x.value is None:
            raise ValueError("unable to return int() of %r" % x)
        return int(x.value)

    def __repr__(x):
        return "Logical(%r)" % x.string

    def __str__(x):
        return x.string

Logical.true = object.__new__(Logical)
Logical.true.value = True
Logical.true.string = 'T'
Logical.false = object.__new__(Logical)
Logical.false.value = False
Logical.false.string = 'F'
Logical.unknown = object.__new__(Logical)
Logical.unknown.value = None
Logical.unknown.string = '?'
Truth = Logical(True)
Falsth = Logical(False)
Unknown = Logical()


class Quantum(object):
    """
    Logical field return type that implements boolean algebra

    Accepts values of True/On, False/Off, or None/Null/Unknown/Other
    """

    def __new__(cls, value=None):
        if value is None or value is Null or value is Other or value is Unknown:
            return cls.unknown
        elif isinstance(value, basestring):
            if value.lower() in ('t', 'true', 'y', 'yes', 'on'):
                return cls.true
            elif value.lower() in ('f', 'false', 'n', 'no', 'off'):
                return cls.false
            elif value.lower() in ('?', 'unknown', 'null', 'none', ' ', ''):
                return cls.unknown
            else:
                raise ValueError('unknown value for Quantum: %s' % value)
        else:
            return (cls.false, cls.true)[bool(value)]

    def A(x, y):
        "OR (disjunction): x | y => True iff at least one of x, y is True"
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        if x.value is True or y is not Other and y == True:
            return x.true
        elif x.value is False and y is not Other and y == False:
            return x.false
        return Other

    def _C_material(x, y):
        "IMP (material implication) x >> y => False iff x == True and y == False"
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        if (x.value is False
            or (x.value is True and y is not Other and y == True)):
            return x.true
        elif x.value is True and y is not Other and y == False:
            return False
        return Other

    def _C_material_reversed(y, x):
        "IMP (material implication) x >> y => False iff x = True and y = False"
        if not isinstance(x, (y.__class__, bool, NullType, type(None))):
            return NotImplemented
        if (x is not Other and x == False
            or (x is not Other and x == True and y.value is True)):
            return y.true
        elif x is not Other and x == True and y.value is False:
            return y.false
        return Other

    def _C_relevant(x, y):
        "IMP (relevant implication) x >> y => True iff both x, y are True, False iff x == True and y == False, Other if x is False"
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        if x.value is True and y is not Other and y == True:
            return x.true
        if x.value is True and y is not Other and y == False:
            return x.false
        return Other

    def _C_relevant_reversed(y, x):
        "IMP (relevant implication) x >> y => True iff both x, y are True, False iff x == True and y == False, Other if y is False"
        if not isinstance(x, (y.__class__, bool, NullType, type(None))):
            return NotImplemented
        if x is not Other and x == True and y.value is True:
            return y.true
        if x is not Other and x == True and y.value is False:
            return y.false
        return Other

    def D(x, y):
        "NAND (negative AND) x.D(y): False iff x and y are both True"
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        if x.value is False or y is not Other and y == False:
            return x.true
        elif x.value is True and y is not Other and y == True:
            return x.false
        return Other

    def E(x, y):
        "EQV (equivalence) x.E(y): True iff x and y are the same"
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        elif (
            (x.value is True and y is not Other and y == True)
            or
            (x.value is False and y is not Other and y == False)
            ):
            return x.true
        elif (
            (x.value is True and y is not Other and y == False)
            or
            (x.value is False and y is not Other and y == True)
            ):
            return x.false
        return Other

    def J(x, y):
        "XOR (parity) x ^ y: True iff only one of x,y is True"
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        if (
            (x.value is True and y is not Other and y == False)
            or
            (x.value is False and y is not Other and y == True)
            ):
            return x.true
        if (
            (x.value is False and y is not Other and y == False)
            or
            (x.value is True and y is not Other and y == True)
            ):
            return x.false
        return Other

    def K(x, y):
        "AND (conjunction) x & y: True iff both x, y are True"
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        if x.value is True and y is not Other and y == True:
            return x.true
        elif x.value is False or y is not Other and y == False:
            return x.false
        return Other

    def N(x):
        "NEG (negation) -x: True iff x = False"
        if x is x.true:
            return x.false
        elif x is x.false:
            return x.true
        return Other

    @classmethod
    def set_implication(cls, method):
        "sets IMP to material or relevant"
        if not isinstance(method, basestring) or string(method).lower() not in ('material', 'relevant'):
            raise ValueError("method should be 'material' (for strict boolean) or 'relevant', not %r'" % method)
        if method.lower() == 'material':
            cls.C = cls._C_material
            cls.__rshift__ = cls._C_material
            cls.__rrshift__ = cls._C_material_reversed
        elif method.lower() == 'relevant':
            cls.C = cls._C_relevant
            cls.__rshift__ = cls._C_relevant
            cls.__rrshift__ = cls._C_relevant_reversed

    def __eq__(x, y):
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        if (
            (x.value is True and y is not Other and y == True)
            or
            (x.value is False and y is not Other and y == False)
            ):
            return x.true
        elif (
            (x.value is True and y is not Other and y == False)
            or
            (x.value is False and y is not Other and y == True)
            ):
            return x.false
        return Other

    def __hash__(x):
        return hash(x.value)

    def __ne__(x, y):
        if not isinstance(y, (x.__class__, bool, NullType, type(None))):
            return NotImplemented
        if (
            (x.value is True and y is not Other and y == False)
            or
            (x.value is False and y is not Other and y == True)
            ):
            return x.true
        elif (
            (x.value is True and y is not Other and y == True)
            or
            (x.value is False and y is not Other and y == False)
            ):
            return x.false
        return Other

    if py_ver < (3, 0):
        def __nonzero__(x):
            if x is Other:
                raise TypeError('True/False value of %r is unknown' % x)
            return x.value is True
    else:
        def __bool__(x):
            if x is Other:
                raise TypeError('True/False value of %r is unknown' % x)
            return x.value is True

    def __repr__(x):
        return "Quantum(%r)" % x.string

    def __str__(x):
        return x.string

    __add__ = A
    __and__ = K
    __mul__ = K
    __neg__ = N
    __or__ = A
    __radd__ = A
    __rand__ = K
    __rshift__ = None
    __rmul__ = K
    __ror__ = A
    __rrshift__ = None
    __rxor__ = J
    __xor__ = J

Quantum.true = object.__new__(Quantum)
Quantum.true.value = True
Quantum.true.string = 'Y'
Quantum.false = object.__new__(Quantum)
Quantum.false.value = False
Quantum.false.string = 'N'
Quantum.unknown = object.__new__(Quantum)
Quantum.unknown.value = None
Quantum.unknown.string = '?'
Quantum.set_implication('material')
On = Quantum(True)
Off = Quantum(False)
Other = Quantum()

# add xmlrpc support
if py_ver < (3, 0):
    from xmlrpclib import Marshaller
else:
    from xmlrpc.client import Marshaller

# Char is unicode
Marshaller.dispatch[Char] = Marshaller.dump_unicode
# Logical unknown becomes False
Marshaller.dispatch[Logical] = Marshaller.dump_bool
# DateTime is transmitted as UTC if aware, local if naive
Marshaller.dispatch[DateTime] = lambda s, dt, w: w(
        '<value><dateTime.iso8601>'
        '%04d%02d%02dT%02d:%02d:%02d'
        '</dateTime.iso8601></value>\n'
            % dt.utctimetuple()[:6])
del Marshaller

