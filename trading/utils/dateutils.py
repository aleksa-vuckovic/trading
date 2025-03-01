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
        raise NotImplemented
    def is_worktime(self, time: datetime|float|int) -> bool:
        raise NotImplemented
    def set_open(self, time: datetime|float|int) -> datetime:
        raise NotImplemented
    def set_close(self, time: datetime|float|int) -> datetime:
        raise NotImplemented
    def is_timestamp(self, time: datetime|float|int, interval: Interval) -> bool:
        raise NotImplemented
    def get_next_timestamp(self, time: datetime|float|int, interval: Interval) -> datetime|float:
        raise NotImplemented
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
        if count > 0:
            i = cache[-1]; j = i + span
            while True:
                index = binary_search(cache, time, edge=BinarySearchEdge.LOW)
                if index+count < len(cache): return cache[index+count]
                cache.extend(self._generate_timestamps(i, j, interval))
                i = j; j += span; span *= 2
        else:
            i = cache[0] - span; j = cache[0]
            while True:
                index = binary_search(cache, time, edge=BinarySearchEdge.LOW)
                if index+count >= 0: return cache[index+count]
                prepend = self._generate_timestamps(i, j, interval)
                if prepend and prepend[-1] == cache[0]: prepend.pop()
                cache[:0] =  prepend
                j = i; i = j - span; span *= 2
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

class NasdaqCalendar(WorkCalendar):
    def __init__(self):
        super().__init__(tz=ET)
        self.holidays = HolidaySchedule()
        self.holidays.add_off_days(
            '2021-01-01', '2021-01-18', '2021-02-15',
            '2021-04-02', '2021-05-31', '2021-07-05',
            '2021-09-06', '2021-11-25', '2021-12-24',
            '2022-01-17', '2022-02-21', '2022-04-15',
            '2022-05-30', '2022-06-20', '2022-07-04',
            '2022-09-05', '2022-11-24', '2022-12-26',
            '2023-01-02', '2023-01-16', '2023-02-20',
            '2023-04-07', '2023-05-29', '2023-06-19',
            '2023-07-04', '2023-09-04', '2023-11-23',
            '2023-12-25', '2024-01-01', '2024-01-15',
            '2024-02-19', '2024-03-29', '2024-05-27',
            '2024-06-19', '2024-07-04', '2024-09-02',
            '2024-11-28', '2024-12-25', '2025-01-01',
            '2025-01-20', '2025-02-17', '2025-04-18',
            '2025-05-26', '2025-06-19', '2025-07-04',
            '2025-09-01', '2025-11-27', '2025-12-25'
        )
        self.holidays.add_semi_days(
            '2021-11-26',
            '2022-07-03', '2023-11-24', '2024-07-03',
            '2024-11-29', '2024-12-24', '2025-07-03',
            '2025-11-28', '2025-12-24'
        )
    #region Overrides
    def is_workday(self, time: datetime|float|int) -> bool:
        if not isinstance(time, datetime): return self.is_workday(self.unix_to_datetime(time))
        time = self.to_zero(time)
        return time.weekday() < 5 and not self.holidays.is_off(time)
    def is_worktime(self, time: datetime|float|int) -> bool:
        if not isinstance(time, datetime): return self.is_worktime(self.unix_to_datetime(time))
        if not self.is_workday(time): return False
        return time > self.set_open(time) and time <= self.set_close(time)
    def set_open(self, time: datetime|float|int) -> datetime:
        if not isinstance(time, datetime): return self.set_open(self.unix_to_datetime(time)).timestamp()
        if not self.is_workday(time): raise Exception(f"Can't set open for non workday {time}.")
        return time.replace(hour=9, minute=30, second=0, microsecond=0)
    def set_close(self, time: datetime|float|int) -> datetime:
        if not isinstance(time, datetime): return self.set_close(self.unix_to_datetime(time)).timestamp()
        if not self.is_workday(time): raise Exception(f"Can't set close for non workday {time}.")
        return time.replace(hour=16 if not self.holidays.is_semi(time) else 13, minute=0, second=0, microsecond=0)
    def is_interval_timestamp(self, time: datetime|float|int, interval: Interval):
        if not isinstance(time, datetime): return self.is_interval_timestamp(self.unix_to_datetime(time), interval)
        if not self.is_workday(time): return False
        if time.second or time.microsecond: return False
        if time <= self.set_open(time) or time > self.set_close(time): return False
        if interval == Interval.L1:
            return time == self.get_next_timestamp(time.replace(day=1), interval)
        if interval == Interval.W1:
            return time == self.get_next_timestamp(time - timedelta(days=time.weekday()+1), interval)
        if interval == Interval.D1:
            return time == self.set_close(time)
        if interval == Interval.H1:
            return time == self.set_close(time) or time.minute==30
        if interval == Interval.M15:
            return not (time.minute % 15)
        if interval == Interval.M5:
            return not (time.minute % 5)
        raise Exception(f"Unknown interval {interval}")
    def _month_end(self, time: datetime) -> datetime:
        time = time.replace(day=calendar.monthrange(time.year, time.month)[1])
        while not self.is_workday(time): time -= timedelta(days=1)
        return self.set_close(time)
    def _week_end(self, time: datetime) -> datetime:
        time += timedelta(days=6-time.weekday())
        while not self.is_workday(time): time -= timedelta(days=1)
        return self.set_close(time)
    def get_next_timestamp(self, time: datetime|float|int, interval: Interval) -> datetime|float|int:
        if not isinstance(time, datetime):
            return self.get_next_timestamp(self.unix_to_datetime(time), interval).timestamp()
        if interval == Interval.L1:
            timestamp = self._month_end(time)
            if timestamp > time: return timestamp
            return self._month_end(timestamp+timedelta(days=15))
        if interval == Interval.W1:
            timestamp = self._week_end(time)
            if timestamp > time: return timestamp
            return self._week_end(timestamp+timedelta(days=7))
        if interval == Interval.D1:
            timestamp = time
            while not self.is_workday(timestamp): timestamp += timedelta(days=1)
            timestamp = self.set_close(timestamp)
            if timestamp > time: return timestamp
            timestamp += timedelta(days=1)
            while not self.is_workday(timestamp): timestamp += timedelta(days=1)
            return self.set_close(timestamp)
        if interval == Interval.H1:
            timestamp = time
            if self.is_workday(timestamp) and timestamp < self.set_close(timestamp):
                if timestamp < self.set_open(timestamp):
                    return timestamp.replace(hour=10,minute=30,second=0,microsecond=0)
                if timestamp + timedelta(minutes=30) >= self.set_close(timestamp):
                    return self.set_close(timestamp)
                if timestamp.minute < 30:
                    return timestamp.replace(minute=30,second=0,microsecond=0)
                return timestamp.replace(hour=timestamp.hour+1,minute=30,second=0,microsecond=0)
            timestamp += timedelta(days=1)
            while not self.is_workday(timestamp): timestamp += timedelta(days=1)
            return timestamp.replace(hour=10,minute=30,second=0,microsecond=0)
        if interval == Interval.M15:
            timestamp = time
            if self.is_workday(timestamp) and timestamp < self.set_close(timestamp):
                if timestamp < self.set_open(timestamp):
                    return timestamp.replace(hour=9, minute=45, second=0, microsecond=0)
                return timestamp.replace(minute = timestamp.minute//15*15, second=0, microsecond=0) + timedelta(minutes=15)
            timestamp += timedelta(days=1)
            while not self.is_workday(timestamp): timestamp += timedelta(days=1)
            return timestamp.replace(hour=9,minute=45,second=0,microsecond=0)
        if interval == Interval.M5:
            timestamp = time
            if self.is_workday(timestamp) and timestamp < self.set_close(timestamp):
                if timestamp < self.set_open(timestamp):
                    return timestamp.replace(hour=9, minute=35, second=0, microsecond=0)
                return timestamp.replace(minute = timestamp.minute//5*5, second=0, microsecond=0) + timedelta(minutes=5)
            timestamp += timedelta(days=1)
            while not self.is_workday(timestamp): timestamp += timedelta(days=1)
            return timestamp.replace(hour=9,minute=35,second=0,microsecond=0)
        raise Exception(f"Unknown interval {interval}")
    #endregion

XNAS = NasdaqCalendar()

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
    def get_next_time(self, time: datetime|float|int) -> datetime:
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
