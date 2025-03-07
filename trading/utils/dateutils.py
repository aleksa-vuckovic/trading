from __future__ import annotations
import calendar
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from .common import Interval, binary_search, BinarySearchEdge, equatable
from .jsonutils import serializable

"""
datetime stores the date components and an optional timezone (if unset, treated as the local timezone)
datetime.astimezone converts to a different timezone, without changing the intrinsic timestamp, while adapting date components
datetime.replace just sets the timezone, keeping the date components
"""
ET =  ZoneInfo('US/Eastern')
UTC = ZoneInfo('UTC')
CET = ZoneInfo('CET')

class WorkCalendar:
    """
    A class for working with interval timestamps and time.
    All methods that accept time arguments accept either unix timestamps as a float or int,
    OR a datetime, in which case the datetime must be in the calendar's timezone!
        A datetime not in the calendar's timezone can produce invalid results.
    """
    cache: dict[Interval, list[datetime]]
    def __init__(self, tz: ZoneInfo):
        self.tz = tz
        self.cache = {}
    #region Basics
    def str_to_datetime(self, time_string: str, format: str = "%Y-%m-%d %H:%M:%S") -> datetime:
        return datetime.strptime(time_string, format).replace(tzinfo=self.tz)
    def str_to_unix(self, time_string: str, format: str = "%Y-%m-%d %H:%M:%S") -> float:
        return self.str_to_datetime(time_string, format=format).timestamp()
    def unix_to_datetime(self, unix: float|int) -> datetime:
        return datetime.fromtimestamp(unix, tz=self.tz)
    def datetime_to_unix(self, time:datetime) -> float:
        return time.timestamp()
    def localize(self, time: datetime) -> datetime:
        return time.astimezone(self.tz)
    def to_zero(self, time: datetime|float|int) -> datetime|float|int:
        if not isinstance(time, datetime):
            return self.to_zero(self.unix_to_datetime(time)).timestamp()
        return time.replace(hour=0, minute=0, second=0, microsecond=0)
    def now(self) -> datetime:
        return datetime.now(tz=self.tz)
    #endregion

    #region Abstract
    # These are the methods that must be implemented in a derived class.
    def is_workday(self, time: datetime|float|int) -> bool:
        raise NotImplementedError()
    def is_worktime(self, time: datetime|float|int) -> bool:
        raise NotImplementedError()
    def set_open(self, time: datetime|float|int) -> datetime:
        raise NotImplementedError()
    def set_close(self, time: datetime|float|int) -> datetime:
        raise NotImplementedError()
    def is_timestamp(self, time: datetime|float|int, interval: Interval) -> bool:
        raise NotImplementedError()
    def get_next_timestamp(self, time: datetime|float|int, interval: Interval) -> datetime|float:
        raise NotImplementedError()
    #endregion

    #region Caching
    def _generate_timestamps(self, start_time: datetime, end_time: datetime, interval: Interval) -> list[datetime]:
        result = []
        cur = self.get_next_timestamp(start_time, interval)
        while cur <= end_time:
            result.append(cur)
            cur = self.get_next_timestamp(cur, interval)
        return result
    def _get_cache(self, interval: Interval) -> list[datetime]:
        if interval not in self.cache:
            self.cache[interval] = []
            span = timedelta(days=100)
            i = self.now() - span; j = self.now() + span
            while not self.cache[interval]:
                self.cache[interval] = self._generate_timestamps(i, j, interval)
                j = i; i -= span; span *= 2
        return self.cache[interval]
    def get_timestamps(self, start_time: datetime|float|int, end_time: datetime|float|int, interval: Interval) -> list[datetime]|list[float]:
        if not isinstance(start_time, datetime):
            return [it.timestamp() for it in self.get_timestamps(self.unix_to_datetime(start_time), self.unix_to_datetime(end_time), interval)]
        cache = self._get_cache(interval)
        # Expand to the left until a timestamp <= start_time is reached
        if cache[0] > start_time:
            span = timedelta(days=100)
            i = start_time - span; j = cache[0]
            while cache[0] > start_time:
                prepend = self._generate_timestamps(i, j, interval)
                if prepend[-1] == cache[0]: prepend.pop()
                cache[:0] = prepend
                j = i; i -= span; span *= 2
        if cache[-1] < end_time:
            span = timedelta(days=100)
            i = cache[-1]; j = i + span
            while cache[-1] < end_time:
                cache.extend(self._generate_timestamps(i, j, interval))
                i = j; j = i + span; span *= 2
        start_index = binary_search(cache, start_time, edge=BinarySearchEdge.LOW)
        end_index = binary_search(cache, end_time, edge=BinarySearchEdge.LOW)
        return cache[start_index+1:end_index+1]
    def add_intervals(self, time: datetime|float|int, interval: Interval, count: int) -> datetime|float|int:
        if not count: return time
        if not isinstance(time, datetime):
            return self.add_intervals(self.unix_to_datetime(time), interval, count).timestamp()
        cache = self._get_cache(interval)
        span = timedelta(days=100)
        while True:
            index = binary_search(cache, time, edge=BinarySearchEdge.LOW)
            if index < 0 or index+count<0:
                # Expand to the left
                j = cache[0]; i = j-span; span *= 2
                prepend = self._generate_timestamps(i, j, interval)
                if prepend and prepend[-1] == cache[0]: prepend.pop()
                cache[:0] =  prepend
            elif index>=len(cache)-1 or index+count>=len(cache):
                # Expand to the right
                i = cache[-1]; j = i+span; span *= 2
                cache.extend(self._generate_timestamps(i, j, interval))
            else:
                return cache[index+count]
    #endregion

class HolidaySchedule:
    """
    Stores data about holidays, as a set of non working and semi working days.
    Time zone independent.
    """
    off_days: set[str]
    semi_days: set[str]
    def __init__(self):
        self.off_days = set()
        self.semi_days = set()
    def _format(self, time: datetime) -> str:
        return time.strftime("%Y-%m-%d")
    def add_off_day(self, time: datetime|str):
        self.off_days.add(self._format(time) if isinstance(time, datetime) else time)
    def add_off_days(self, *times: datetime|str):
        for time in times: self.add_off_day(time)
    def add_semi_day(self, time: datetime|str):
        self.semi_days.add(self._format(time) if isinstance(time, datetime) else time)
    def add_semi_days(self, *times: datetime):
        for time in times: self.add_semi_day(time)
    def is_off(self, time: datetime) -> bool:
        return self._format(time) in self.off_days
    def is_semi(self, time: datetime) -> bool:
        return self._format(time) in self.semi_days

#region TimingConfig
@serializable(skip_keys=['interval', 'calendar'])
@equatable(skip_keys=['interval', 'calendar'])
class TimingConfig:
    """
    Represents a set of timing intervals or points during a single day.
    The configuration is timezone and date independent, but:
        To use get_next_time, the interval and calendar must be set.
        To use __contains__, the calendar must be set.
    When using datetimes in these methods, the datetime must match the calendar's timezone,
    otherwise the behavior is undefined.
    """
    interval: Interval|None
    calendar: WorkCalendar|None
    def __init__(self, components: list[float|list[float]]):
        self.components = components
        self.interval = None
        self.calendar = None
    class Builder:
        def __init__(self):
            self.components = []
        def at(self, hour: int = 9, minute: int = 30) -> TimingConfig.Builder:
            self.components.append(float(hour*3600 + minute*60))
            return self
        class _Interval:
            def __init__(self, builder: TimingConfig.Builder, start: float):
                self._builder = builder
                self._start = start
            def until(self, hour: int = 16, minute: int = 0) -> TimingConfig.Builder:
                self._builder.components.append([self._start, float(hour*3600+minute*60)])
                return self._builder
        def starting(self, hour: int = 9, minute: int = 30) -> TimingConfig.Builder._Interval:
            return TimingConfig.Builder._Interval(self, float(hour*3600+minute*60))
        def around(self, hour: int = 10, minute: int = 0, delta_minute: int = 10):
            if not delta_minute: return self.at(hour = hour, minute = minute)
            time = float(hour*3600 + minute*60)
            self.components.append([time-delta_minute*60,time+delta_minute*60])
            return self
        def build(self) -> TimingConfig:
            return TimingConfig(self.components)
    def with_interval(self, interval: Interval) -> TimingConfig:
        result = TimingConfig(self.components)
        result.interval = interval
        result.calendar = self.calendar
        return result
    def with_calendar(self, calendar: WorkCalendar) -> TimingConfig:
        result = TimingConfig(self.components)
        result.interval = self.interval
        result.calendar = calendar
        return result
    def get_next_time(self, time: datetime|float|int) -> datetime|float:
        if not self.interval or not self.calendar: raise Exception(f"Both the interval and calendar must be set before calling get_next_time.")
        if not isinstance(time, datetime): return self.get_next_time(self.calendar.unix_to_datetime(time)).timestamp()
        time = self.calendar.get_next_timestamp(time, self.interval)
        while time not in self: time = self.calendar.get_next_timestamp(time, self.interval)
        return time
    def __contains__(self, time: datetime|float|int) -> bool:
        if not self.calendar: raise Exception(f"Calendar must be set to call __contains__.")
        if not isinstance(time, datetime): return self.calendar.unix_to_datetime(time) in self
        if not self.calendar.is_worktime(time): return False
        daysecs = time.hour*3600+time.minute*60+time.second+time.microsecond
        for it in self.components:
            if isinstance(it, list):
                if daysecs > it[0] and daysecs <= it[1]: return True
            else:
                if daysecs == it: return True
        return False
#endregion
