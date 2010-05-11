"""wrappers around datetime objects to allow null values"""

import datetime
import time


class Date(object):
    "adds null capable datetime.date constructs"
    __slots__ = ['_date']
    def __new__(cls, year=None, month=0, day=0):
        """date should be either a datetime.date, a string in yyyymmdd format, 
        or date/month/day should all be appropriate integers"""
        nd = object.__new__(cls)
        nd._date = False
        if type(year) == datetime.date:
            nd._date = year
        elif type(year) == Date:
            nd._date = year._date
        elif year == 'no date':
            pass    # date object is already False
        elif year is not None:
            nd._date = datetime.date(year, month, day)
        return nd
    def __add__(yo, other):
        if yo and type(other) == datetime.timedelta:
            return Date(yo._date + other)
        else:
            return NotImplemented
    def __eq__(yo, other):
        if yo:
            if type(other) == datetime.date:
                return yo._date == other
            elif type(other) == Date:
                if other:
                    return yo._date == other._date
                return False
        else:
            if type(other) == datetime.date:
                return False
            elif type(other) == Date:
                if other:
                    return False
                return True
        return NotImplemented
    def __getattr__(yo, name):
        if yo:
            attribute = yo._date.__getattribute__(name)
            return attribute
        else:
            raise AttributeError('null Date object has no attribute %s' % name)
    def __ge__(yo, other):
        if yo:
            if type(other) == datetime.date:
                return yo._date >= other
            elif type(other) == Date:
                if other:
                    return yo._date >= other._date
                return False
        else:
            if type(other) == datetime.date:
                return False
            elif type(other) == Date:
                if other:
                    return False
                return True
        return NotImplemented
    def __gt__(yo, other):
        if yo:
            if type(other) == datetime.date:
                return yo._date > other
            elif type(other) == Date:
                if other:
                    return yo._date > other._date
                return True
        else:
            if type(other) == datetime.date:
                return False
            elif type(other) == Date:
                if other:
                    return False
                return False
        return NotImplemented
    def __hash__(yo):
        return yo._date.__hash__()
    def __le__(yo, other):
        if yo:
            if type(other) == datetime.date:
                return yo._date <= other
            elif type(other) == Date:
                if other:
                    return yo._date <= other._date
                return False
        else:
            if type(other) == datetime.date:
                return True
            elif type(other) == Date:
                if other:
                    return True
                return True
        return NotImplemented
    def __lt__(yo, other):
        if yo:
            if type(other) == datetime.date:
                return yo._date < other
            elif type(other) == Date:
                if other:
                    return yo._date < other._date
                return False
        else:
            if type(other) == datetime.date:
                return True
            elif type(other) == Date:
                if other:
                    return True
                return False
        return NotImplemented
    def __ne__(yo, other):
        if yo:
            if type(other) == datetime.date:
                return yo._date != other
            elif type(other) == Date:
                if other:
                    return yo._date != other._date
                return True
        else:
            if type(other) == datetime.date:
                return True
            elif type(other) == Date:
                if other:
                    return True
                return False
        return NotImplemented
    def __nonzero__(yo):
        if yo._date:
            return True
        return False
    __radd__ = __add__
    def __rsub__(yo, other):
        if yo and type(other) == datetime.date:
            return other - yo._date
        elif yo and type(other) == Date:
            return other._date - yo._date
        elif yo and type(other) == datetime.timedelta:
            return Date(other - yo._date)
        else:
            return NotImplemented
    def __repr__(yo):
        if yo:
            return "Date(%d, %d, %d)" % yo.timetuple()[:3]
        else:
            return "Date()"
    def __str__(yo):
        if yo:
            return yo.isoformat()
        return "no date"
    def __sub__(yo, other):
        if yo and type(other) == datetime.date:
            return yo._date - other
        elif yo and type(other) == Date:
            return yo._date - other._date
        elif yo and type(other) == datetime.timedelta:
            return Date(yo._date - other)
        else:
            return NotImplemented
    def date(yo):
        if yo:
            return yo._date
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
        if yyyymmdd in ('', '        ','no date'):
            return cls()
        return cls(datetime.date(int(yyyymmdd[:4]), int(yyyymmdd[4:6]), int(yyyymmdd[6:])))
    def strftime(yo, format):
        if yo:
            return yo._date.strftime(format)
        return '<no date>'
    @classmethod
    def today(cls):
        return cls(datetime.date.today())
    def ymd(yo):
        if yo:
            return "%04d%02d%02d" % yo.timetuple()[:3]
        else:
            return '        '
Date.max = Date(datetime.date.max)
Date.min = Date(datetime.date.min)
class DateTime(object):
    "adds null capable datetime.datetime constructs"
    __slots__ = ['_datetime']
    def __new__(cls, year=None, month=0, day=0, hour=0, minute=0, second=0, microsec=0):
        """year may be a datetime.datetime"""
        ndt = object.__new__(cls)
        ndt._datetime = False
        if type(year) == datetime.datetime:
            ndt._datetime = year
        elif type(year) == DateTime:
            ndt._datetime = year._datetime
        elif year is not None:
            ndt._datetime = datetime.datetime(year, month, day, hour, minute, second, microsec)
        return ndt
    def __add__(yo, other):
        if yo and type(other) == datetime.timedelta:
            return DateTime(yo._datetime + other)
        else:
            return NotImplemented
    def __eq__(yo, other):
        if yo:
            if type(other) == datetime.datetime:
                return yo._datetime == other
            elif type(other) == DateTime:
                if other:
                    return yo._datetime == other._datetime
                return False
        else:
            if type(other) == datetime.datetime:
                return False
            elif type(other) == DateTime:
                if other:
                    return False
                return True
        return NotImplemented
    def __getattr__(yo, name):
        if yo:
            attribute = yo._datetime.__getattribute__(name)
            return attribute
        else:
            raise AttributeError('null DateTime object has no attribute %s' % name)
    def __ge__(yo, other):
        if yo:
            if type(other) == datetime.datetime:
                return yo._datetime >= other
            elif type(other) == DateTime:
                if other:
                    return yo._datetime >= other._datetime
                return False
        else:
            if type(other) == datetime.datetime:
                return False
            elif type(other) == DateTime:
                if other:
                    return False
                return True
        return NotImplemented
    def __gt__(yo, other):
        if yo:
            if type(other) == datetime.datetime:
                return yo._datetime > other
            elif type(other) == DateTime:
                if other:
                    return yo._datetime > other._datetime
                return True
        else:
            if type(other) == datetime.datetime:
                return False
            elif type(other) == DateTime:
                if other:
                    return False
                return False
        return NotImplemented
    def __hash__(yo):
        return yo._datetime.__hash__()
    def __le__(yo, other):
        if yo:
            if type(other) == datetime.datetime:
                return yo._datetime <= other
            elif type(other) == DateTime:
                if other:
                    return yo._datetime <= other._datetime
                return False
        else:
            if type(other) == datetime.datetime:
                return True
            elif type(other) == DateTime:
                if other:
                    return True
                return True
        return NotImplemented
    def __lt__(yo, other):
        if yo:
            if type(other) == datetime.datetime:
                return yo._datetime < other
            elif type(other) == DateTime:
                if other:
                    return yo._datetime < other._datetime
                return False
        else:
            if type(other) == datetime.datetime:
                return True
            elif type(other) == DateTime:
                if other:
                    return True
                return False
        return NotImplemented
    def __ne__(yo, other):
        if yo:
            if type(other) == datetime.datetime:
                return yo._datetime != other
            elif type(other) == DateTime:
                if other:
                    return yo._datetime != other._datetime
                return True
        else:
            if type(other) == datetime.datetime:
                return True
            elif type(other) == DateTime:
                if other:
                    return True
                return False
        return NotImplemented
    def __nonzero__(yo):
        if yo._datetime is not False:
            return True
        return False
    __radd__ = __add__
    def __rsub__(yo, other):
        if yo and type(other) == datetime.datetime:
            return other - yo._datetime
        elif yo and type(other) == DateTime:
            return other._datetime - yo._datetime
        elif yo and type(other) == datetime.timedelta:
            return DateTime(other - yo._datetime)
        else:
            return NotImplemented
    def __repr__(yo):
        if yo:
            return "DateTime(%d, %d, %d, %d, %d, %d, %d, %d, %d)" % yo._datetime.timetuple()[:]
        else:
            return "DateTime()"
    def __str__(yo):
        if yo:
            return yo.isoformat()
        return "no datetime"
    def __sub__(yo, other):
        if yo and type(other) == datetime.datetime:
            return yo._datetime - other
        elif yo and type(other) == DateTime:
            return yo._datetime - other._datetime
        elif yo and type(other) == datetime.timedelta:
            return DateTime(yo._datetime - other)
        else:
            return NotImplemented
    @classmethod
    def combine(cls, date, time):
        if Date(date) and Time(time):
            return cls(date.year, date.month, date.day, time.hour, time.minute, time.second, time.microsecond)
        return cls()
    def date(yo):
        if yo:
            return Date(yo.year, yo.month, yo.day)
        return Date()
    def datetime(yo):
        if yo:
            return yo._datetime
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
    def now(cls):
        return cls(datetime.datetime.now())
    def time(yo):
        if yo:
            return Time(yo.hour, yo.minute, yo.second, yo.microsecond)
        return Time()
    @classmethod
    def utcnow(cls):
        return cls(datetime.datetime.utcnow())
    @classmethod
    def today(cls):
        return cls(datetime.datetime.today())
DateTime.max = DateTime(datetime.datetime.max)
DateTime.min = DateTime(datetime.datetime.min)
class Time(object):
    "adds null capable datetime.time constructs"
    __slots__ = ['_time']
    def __new__(cls, hour=None, minute=0, second=0, microsec=0):
        """hour may be a datetime.time"""
        nt = object.__new__(cls)
        nt._time = False
        if type(hour) == datetime.time:
            nt._time = hour
        elif type(hour) == Time:
            nt._time = hour._time
        elif hour is not None:
            nt._time = datetime.time(hour, minute, second, microsec)
        return nt
    def __add__(yo, other):
        if yo and type(other) == datetime.timedelta:
            return Time(yo._time + other)
        else:
            return NotImplemented
    def __eq__(yo, other):
        if yo:
            if type(other) == datetime.time:
                return yo._time == other
            elif type(other) == Time:
                if other:
                    return yo._time == other._time
                return False
        else:
            if type(other) == datetime.time:
                return False
            elif type(other) == Time:
                if other:
                    return False
                return True
        return NotImplemented
    def __getattr__(yo, name):
        if yo:
            attribute = yo._time.__getattribute__(name)
            return attribute
        else:
            raise AttributeError('null Time object has no attribute %s' % name)
    def __ge__(yo, other):
        if yo:
            if type(other) == datetime.time:
                return yo._time >= other
            elif type(other) == Time:
                if other:
                    return yo._time >= other._time
                return False
        else:
            if type(other) == datetime.time:
                return False
            elif type(other) == Time:
                if other:
                    return False
                return True
        return NotImplemented
    def __gt__(yo, other):
        if yo:
            if type(other) == datetime.time:
                return yo._time > other
            elif type(other) == DateTime:
                if other:
                    return yo._time > other._time
                return True
        else:
            if type(other) == datetime.time:
                return False
            elif type(other) == Time:
                if other:
                    return False
                return False
        return NotImplemented
    def __hash__(yo):
        return yo._datetime.__hash__()
    def __le__(yo, other):
        if yo:
            if type(other) == datetime.time:
                return yo._time <= other
            elif type(other) == Time:
                if other:
                    return yo._time <= other._time
                return False
        else:
            if type(other) == datetime.time:
                return True
            elif type(other) == Time:
                if other:
                    return True
                return True
        return NotImplemented
    def __lt__(yo, other):
        if yo:
            if type(other) == datetime.time:
                return yo._time < other
            elif type(other) == Time:
                if other:
                    return yo._time < other._time
                return False
        else:
            if type(other) == datetime.time:
                return True
            elif type(other) == Time:
                if other:
                    return True
                return False
        return NotImplemented
    def __ne__(yo, other):
        if yo:
            if type(other) == datetime.time:
                return yo._time != other
            elif type(other) == Time:
                if other:
                    return yo._time != other._time
                return True
        else:
            if type(other) == datetime.time:
                return True
            elif type(other) == Time:
                if other:
                    return True
                return False
        return NotImplemented
    def __nonzero__(yo):
        if yo._time is not False:
            return True
        return False
    __radd__ = __add__
    def __rsub__(yo, other):
        if yo and type(other) == datetime.time:
            return other - yo._time
        elif yo and type(other) == Time:
            return other._time - yo._time
        elif yo and type(other) == datetime.timedelta:
            return Time(other - yo._datetime)
        else:
            return NotImplemented
    def __repr__(yo):
        if yo:
            return "Time(%d, %d, %d, %d)" % (yo.hour, yo.minute, yo.second, yo.microsecond)
        else:
            return "Time()"
    def __str__(yo):
        if yo:
            return yo.isoformat()
        return "no time"
    def __sub__(yo, other):
        if yo and type(other) == datetime.time:
            return yo._time - other
        elif yo and type(other) == Time:
            return yo._time - other._time
        elif yo and type(other) == datetime.timedelta:
            return Time(yo._time - other)
        else:
            return NotImplemented
Time.max = Time(datetime.time.max)
Time.min = Time(datetime.time.min)
