#2
from __future__ import annotations
import calendar
from typing import overload, TypeVar, override
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
from base.classes import equatable
from base.caching import Persistor, cached_series, MemoryPersistor
from base import dates
from base.serialization import serializable
from trading.core import Interval


T = TypeVar('T', datetime, float)

class WorkCalendar:
    """
    A class for working with interval timestamps and time.
    All methods that accept time arguments accept either unix timestamps as a float or int,
    OR a datetime, in which case the datetime must be in the calendar's timezone!
        A datetime not in the calendar's timezone can produce invalid results.
    Because intervals are consistently considered open at the start and closed at the end, in all methods
    the time 00:00 is (and should be) considered to belong to the previous day, i.e. to be the last moment of the previous day.
    """
    cache: dict[tuple[Interval, float], list[datetime]]
    def __init__(self, tz: ZoneInfo):
        self.tz = tz
        self.cache = {}
    #region Basics
    def str_to_datetime(self, time_string: str, format: str = "%Y-%m-%d %H:%M:%S") -> datetime:
        return dates.str_to_datetime(time_string, format, tz=self.tz)
    def str_to_unix(self, time_string: str, format: str = "%Y-%m-%d %H:%M:%S") -> float:
        return dates.str_to_unix(time_string, format, tz=self.tz)
    def unix_to_datetime(self, unix: float) -> datetime:
        return dates.unix_to_datetime(unix, tz=self.tz)
    def datetime_to_unix(self, time:datetime) -> float:
        return dates.datetime_to_unix(time)
    def localize(self, time: datetime) -> datetime:
        return dates.localize(time, tz=self.tz)
    
    @overload
    def to_zero(self, time: datetime) -> datetime: ...
    @overload
    def to_zero(self, time: float) -> float: ...
    def to_zero(self, time) -> datetime|float:
        return dates.to_zero(time, tz=self.tz)
    def now(self) -> datetime:
        return dates.now(self.tz)
    def nudge(self, time: datetime) -> datetime:
        if time == dates.to_zero(time): return time - timedelta(microseconds=1)
        return time
    #endregion

    #region Abstract
    # These are the methods that should be implemented in a derived class.
    def _is_workday(self, time: datetime) -> bool:
        raise NotImplementedError()
    def _set_open(self, time: datetime) -> datetime:
        raise NotImplementedError()
    def _set_close(self, time: datetime) -> datetime:
        raise NotImplementedError()
    def _is_timestamp(self, time: datetime, interval: Interval) -> bool:
        raise NotImplementedError()
    def _get_next_timestamp(self, time: datetime, interval: Interval) -> datetime:
        raise NotImplementedError()
    #endregion

    #region Utilities
    def is_workday(self, time: T) -> bool:
        if isinstance(time, datetime): return self._is_workday(time)
        else: return self._is_workday(self.unix_to_datetime(time))
    def set_open(self, time: T) -> T:
        """
        Set to the opening hour of the given date.
        Raise exception if there's not open on the given date i.e. it is not a workday (is_workday returns false)
        """
        if isinstance(time, datetime): return self._set_open(time)
        else: return self._set_open(self.unix_to_datetime(time)).timestamp()
    def set_close(self, time: T) -> T:
        """
        Set to the closing hour of the given date.
        Raise exception if the given date is not a workday (is_workday returns false).
        """
        if isinstance(time, datetime): return self._set_close(time)
        else: return self._set_close(self.unix_to_datetime(time)).timestamp()
    def is_timestamp(self, time: T, interval: Interval) -> bool:
        """
        Returns true if the given time is a valid timestamp for the given interval.
        """
        if isinstance(time, datetime): return self._is_timestamp(time, interval)
        else: return self._is_timestamp(self.unix_to_datetime(time), interval)
    def get_next_timestamp(self, time: T, interval: Interval) -> T:
        """
        Get the next timestamp greater than the given time, for the given interval.
        """
        if isinstance(time, datetime): return self._get_next_timestamp(time, interval)
        else: return self._get_next_timestamp(self.unix_to_datetime(time), interval).timestamp()
    def is_worktime(self, time: T) -> bool:
        if not isinstance(time, datetime): return self.is_worktime(self.unix_to_datetime(time))
        if not self.is_workday(time): return False
        return time > self.set_open(time) and time <= self.set_close(time)
    def month_end(self, time: T) -> T:
        if not isinstance(time, datetime): return self.month_end(self.unix_to_datetime(time)).timestamp()
        if time.day == 1 and time == dates.to_zero(time): return time
        return dates.to_zero(time.replace(day = calendar.monthrange(time.year, time.month)[1]) + timedelta(days=1))
    def week_end(self, time: datetime) -> datetime:
        if time.weekday() == 0 and time == dates.to_zero(time): return time
        return dates.to_zero(time + timedelta(days=7-time.weekday()))
    #endregion

    #region Caching
    @staticmethod
    def _get_timestamps_timestamp_fn(it: datetime) -> float: return it.timestamp()
    def _get_timestamps_key_fn(self, interval: Interval) -> str: return interval.name
    def _get_timestamps_persistor_fn(self, interval: Interval) -> Persistor:
        KEY = "_basic_work_calendar_persistor_"
        if KEY not in self.__dict__: self.__dict__[KEY] = MemoryPersistor()
        return self.__dict__[KEY]
    def _get_timestamps_time_step_fn(self, interval: Interval) -> float:
        if interval >= Interval.D1: return 1000*interval.time()
        else: return 4000*interval.time()
    @cached_series(
        timestamp_fn=_get_timestamps_timestamp_fn,
        key_fn=_get_timestamps_key_fn,
        persistor_fn=_get_timestamps_persistor_fn,
        time_step_fn=_get_timestamps_time_step_fn,
        live_delay_fn=None
    )
    def _get_timestamps(self, unix_from: float, unix_to: float, interval: Interval) -> list[datetime]:
        result: list[datetime] = []
        start_time = self.unix_to_datetime(unix_from)
        end_time = self.unix_to_datetime(unix_to)
        cur = self.get_next_timestamp(start_time, interval)
        while cur <= end_time:
            result.append(cur)
            cur = self.get_next_timestamp(cur, interval)
        return result
    def get_timestamps(self, start_time: T, end_time: T, interval: Interval) -> list[T]:
        if not isinstance(start_time, datetime) or not isinstance(end_time, datetime):
            assert not isinstance(start_time, datetime)
            assert not isinstance(end_time, datetime)
            return [it.timestamp() for it in self.get_timestamps(self.unix_to_datetime(start_time), self.unix_to_datetime(end_time), interval)]
        return self._get_timestamps(start_time.timestamp(), end_time.timestamp(), interval)
    def add_intervals(self, time: T, interval: Interval, count: int) -> T:
        if not count: return time
        if not isinstance(time, datetime):
            return self.add_intervals(self.unix_to_datetime(time), interval, count).timestamp()
        #estimate necessary timespan
        span = timedelta(seconds=interval.time()*(abs(count)+5))
        t: list[datetime] = []
        if count > 0:
            while len(t) < count:
                count -= len(t)
                t = self.get_timestamps(time, time+span, interval)
                time += span
                span *= 2
            return t[count-1] # type: ignore
        else:
            count = -count+1
            while len(t) < count:
                count -= len(t)
                t = self.get_timestamps(time-span, time, interval)
                time -= span
                span *= 2
            return t[-count] # type: ignore
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
    def add_semi_days(self, *times: datetime|str):
        for time in times: self.add_semi_day(time)
    def is_off(self, time: datetime) -> bool:
        return self._format(time) in self.off_days
    def is_semi(self, time: datetime) -> bool:
        return self._format(time) in self.semi_days

class BasicWorkCalendar(WorkCalendar):
    def __init__(
        self,
        *,
        tz: ZoneInfo,
        open_hour: int,
        open_minute: int = 0,
        close_hour: int,
        close_minute: int = 0,
        semi_close_hour: int,
        semi_close_minute: int = 0,
        holidays: HolidaySchedule = HolidaySchedule()
    ):
        super().__init__(tz)
        self.open_hour = open_hour
        self.open_minute = open_minute
        self.close_hour = close_hour
        self.close_minute = close_minute
        self.semi_close_hour = semi_close_hour
        self.semi_close_minute = semi_close_minute
        self.holidays = holidays
    
    #region Overrides
    @override
    def _is_workday(self, time: datetime) -> bool:
        time = self.nudge(time)
        return time.weekday() < 5 and not self.holidays.is_off(time)
    @override
    def _set_open(self, time: datetime) -> datetime:
        assert self.is_workday(time)
        time = self.nudge(time)
        return time.replace(hour=self.open_hour, minute=self.open_minute, second=0, microsecond=0)
    @override
    def _set_close(self, time: datetime) -> datetime:
        assert self.is_workday(time)
        time = self.nudge(time)
        if self.holidays.is_semi(time):
            close_hour = self.semi_close_hour
            close_minute = self.semi_close_minute
        else:
            close_hour = self.close_hour
            close_minute = self.close_minute
        if close_hour == 0 and close_minute == 0: return dates.to_zero(time + timedelta(days=1))
        return time.replace(hour=close_hour, minute=close_minute, second=0, microsecond=0)
    @override
    def _is_timestamp(self, time: datetime, interval: Interval) -> bool:
        if interval == Interval.L1: return time == self.month_end(time)
        if interval == Interval.W1: return time == self.week_end(time)
        if interval > Interval.D1: raise Exception(f"Unknown interval {interval}.")
        if not self.is_workday(time): return False
        if interval == Interval.D1: return time == self.to_zero(time)
        if time == self.set_close(time): return True
        return (time.timestamp() - self.set_open(time).timestamp())%interval.time() == 0
    @override
    def _get_next_timestamp(self, time: datetime, interval: Interval) -> datetime:
        if interval == Interval.L1:
            timestamp = self.month_end(time)
            if timestamp > time: return timestamp
            else: return self.month_end(time+timedelta(days=1))
        if interval == Interval.W1:
            timestamp = self.week_end(time)
            if timestamp > time: return timestamp
            else: return self.week_end(time + timedelta(days=1))
        if interval == Interval.D1:
            timestamp = self.to_zero(time + timedelta(days=1))
            while not self.is_workday(timestamp): timestamp += timedelta(days=1)
            return timestamp
        if interval > Interval.D1: raise Exception(f"Unknown interval {interval}.")
        #Intraday intervals
        if self.is_worktime(time):
            open = self.set_open(time)
            close = self.set_close(time)
            cnt = (time.timestamp() - open.timestamp())//interval.time()
            timestamp = self.unix_to_datetime(open.timestamp() + (cnt+1)*interval.time())
            if timestamp <= close: return timestamp
            if close > time: return close
        if self.is_workday(time) and time > self.set_open(time): time += timedelta(days=1)
        while not self.is_workday(time): time += timedelta(days=1)
        time = self.set_open(time)
        timestamp = self.unix_to_datetime(self.set_open(time).timestamp() + interval.time())
        if timestamp <= self.set_close(time): return timestamp
        return self.set_close(time)
    #endregion

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
    def get_next_time(self, time: T) -> T:
        if not self.interval or not self.calendar: raise Exception(f"Both the interval and calendar must be set before calling get_next_time.")
        if not isinstance(time, datetime): return self.get_next_time(self.calendar.unix_to_datetime(time)).timestamp()
        time = self.calendar.get_next_timestamp(time, self.interval)
        while time not in self: time = self.calendar.get_next_timestamp(time, self.interval)
        return time
    def __contains__(self, time: T) -> bool:
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
