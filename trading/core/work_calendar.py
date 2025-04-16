#2
from __future__ import annotations
import math
import calendar
from typing import Self, overload, TypeVar, override
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
from base.caching import Persistor, cached_series, MemoryPersistor
from base import dates
from base.types import equatable
from trading.core import Interval


T = TypeVar('T', datetime, float)

def nudge(time: datetime) -> datetime:
    if time == dates.to_zero(time): return time - timedelta(microseconds=1)
    return time

@equatable()
class Hours:
    def __init__(self, open_hour: int|None=None, close_hour: int= 0, *, open_minute: int = 0, close_minute: int = 0):
        self.open_hour = open_hour or 0
        self.close_hour = close_hour
        self.open_minute = open_minute
        self.close_minute = close_minute
        for hour in [self.open_hour, self.close_hour]:
            if hour < 0 or hour > 23: raise Exception(f"Hours must be 0-23 inclusive.")
        for minute in [self.open_minute, self.close_minute]:
            if minute < 0 or minute > 59: raise Exception(f"Minutes must be 0-59 inclusive")
        if open_hour is None: self.open = None
        else: self.open = open_hour*3600+open_minute*60
        self.close = close_hour*3600+close_minute*60
    def __contains__(self, time: datetime) -> bool:
        if self.open is None: return False
        daysecs = time.hour*3600+time.minute*60+time.second+time.microsecond/1000
        if not daysecs: daysecs = 24*3600
        return daysecs > self.open and daysecs <= (self.close or 24*3600)
    def set_open(self, time: datetime) -> datetime:
        if self.open is None: raise Exception(f"Can't set to open on closed hours.")
        return nudge(time).replace(hour=self.open_hour, minute=self.open_minute)
    def set_close(self, time: datetime) -> datetime:
        if self.open is None: raise Exception(f"Can't set to close on closed hours.")
        time = nudge(time)
        if not self.close: return dates.to_zero(time + timedelta(days=1))
        return time.replace(hour=self.close_hour, minute=self.close_minute)
    def is_off(self) -> bool: return self.open is None

class WorkSchedule:
    def __init__(self, regular_hours: list[Hours], special_hours: dict[Hours, set[str]]):
        assert len(regular_hours) == 7
        self.regular_hours = regular_hours
        self.special_hours = special_hours
    def _format(self, time: datetime) -> str:
        return time.strftime("%Y-%m-%d")
    
    class Builder:
        special_hours: dict[Hours, set[str]]
        def __init__(self, weekday: Hours, weekend: Hours = Hours()) -> None:
            self.regular_hours = [*(weekday for _ in range(5)), *(weekend for _ in range(2))]
            self.special_hours = {}
        def weekly(self, day: int, hours: Hours) -> Self:
            self.regular_hours[day] = hours
            return self
        def saturday(self, hours: Hours) -> Self: return self.weekly(5, hours)
        def sunday(self, hours: Hours) -> Self: return self.weekly(6, hours)
        def special(self, hours: Hours, *days: str) -> Self:
            if hours not in self.special_hours: self.special_hours[hours] = set()
            self.special_hours[hours].update(days)
            return self
        def off(self, *days: str) -> Self: return self.special(Hours(), *days)
        def build(self) -> WorkSchedule: return WorkSchedule(self.regular_hours, self.special_hours)

    def hours(self, time: datetime) -> Hours:
        time = nudge(time)
        t = self._format(time)
        for hours, days in self.special_hours.items():
            if t in days: return hours
        return self.regular_hours[time.weekday()]
    def is_off(self, time: datetime) -> bool: return self.hours(time).is_off()
    def set_open(self, time: datetime) -> datetime: return self.hours(time).set_open(time)
    def set_close(self, time: datetime) -> datetime: return self.hours(time).set_close(time)

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
    #endregion

    #region Abstract
    # These are the methods that should be implemented in a derived class.
    def _is_off(self, time: datetime) -> bool:
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
    def is_off(self, time: T) -> bool:
        if isinstance(time, datetime): return self._is_off(time)
        else: return self._is_off(self.unix_to_datetime(time))
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
        if self.is_off(time): return False
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

    def __eq__(self, other) -> bool:
        return isinstance(other, type(self))

class BasicWorkCalendar(WorkCalendar):
    def __init__(
        self,
        *,
        tz: ZoneInfo,
        work_schedule: WorkSchedule
    ):
        super().__init__(tz)
        self.work_schedule = work_schedule
    
    #region Overrides
    @override
    def _is_off(self, time: datetime) -> bool: return self.work_schedule.is_off(time)
    @override
    def _set_open(self, time: datetime) -> datetime: return self.work_schedule.set_open(time)
    @override
    def _set_close(self, time: datetime) -> datetime: return self.work_schedule.set_close(time)
    @override
    def _is_timestamp(self, time: datetime, interval: Interval) -> bool:
        if interval == Interval.L1: return time == self.month_end(time)
        if interval == Interval.W1: return time == self.week_end(time)

        if interval not in {Interval.D1, Interval.H1, Interval.M30, Interval.M15, Interval.M5, Interval.M1}: raise Exception(f"Unknown interval {interval}.")
        if self.is_off(time): return False
        if interval == Interval.D1: return time == self.to_zero(time)
        start = self.to_zero(time).timestamp()
        it = math.floor((time.timestamp()-start)/interval.time())
        if start + it*interval.time() != time.timestamp(): return False
        first = math.floor((self.set_open(time).timestamp()-start)/interval.time())
        last = math.ceil((self.set_close(time).timestamp()-start)/interval.time())
        return it > first and it <= last
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
            while self.is_off(timestamp): timestamp += timedelta(days=1)
            return timestamp
        if interval not in {Interval.H1, Interval.M30, Interval.M15, Interval.M5, Interval.M1}: raise Exception(f"Unknown interval {interval}.")
        #Intraday intervals
        if not self.is_off(time):
            start = self.to_zero(time).timestamp()
            it = math.floor((time.timestamp()-start)/interval.time())+1
            first = math.floor((self.set_open(time).timestamp()-start)/interval.time())
            last = math.ceil((self.set_close(time).timestamp()-start)/interval.time())
            if it > first and it <= last: return self.unix_to_datetime(start + it*interval.time())
            if it > last: time += timedelta(days=1)
        while self.is_off(time): time += timedelta(days=1)
        start = self.to_zero(time).timestamp()
        first = math.floor((self.set_open(time).timestamp()-start)/interval.time())
        return self.unix_to_datetime(start + (first+1)*interval.time())
    #endregion
