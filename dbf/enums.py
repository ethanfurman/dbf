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


