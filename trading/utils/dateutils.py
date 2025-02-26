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

#region Basics
def str_to_datetime(time_string: str, format: str = "%Y-%m-%d %H:%M:%S", tz = ET) -> datetime:
    return datetime.strptime(time_string, format).replace(tzinfo=tz)
def str_to_unix(time_string: str, format: str = "%Y-%m-%d %H:%M:%S", tz = ET) -> float:
    return str_to_datetime(time_string, format=format, tz=tz).timestamp()
def unix_to_datetime(unix: float|int, tz = ET) -> datetime:
    return datetime.fromtimestamp(unix, tz = tz)
def now(tz = ET) -> datetime:
    return datetime.now(tz = tz)
#endregion

#region Utils
def get_last_workday_of_month(date: datetime) -> int:
    range = calendar.monthrange(date.year, date.month)
    last_day_weekday = (range[0].value + range[1] - 1)%7
    return range[1] - max(last_day_weekday-4, 0)
def set_open(date: datetime):
    return date.replace(hour=9, minute=30, second=0, microsecond=0)
def set_close(date: datetime):
    return date.replace(hour=16, minute=0, second=0, microsecond=0)
def slide_to_working(date: datetime, negative: bool) -> datetime:
    daysecs = datetime_to_daysecs(date)
    if negative:
        if daysecs >= 16*3600: date = set_open(date+timedelta(days=1))
        if daysecs < 9.5*3600: date = set_open(date)
        if date.weekday() < 5: return date
        return set_open(date + timedelta(days = 7-date.weekday()))
    else:
        if daysecs > 16*3600: date = set_close(date)
        elif daysecs <= 9.5*3600: date = set_close(date-timedelta(days=1))
        if date.weekday() < 5: return date
        return set_close(date - timedelta(days=max(date.weekday()-4, 0)))
def datetime_to_daysecs(date: datetime) -> float:
    return date.hour*3600 + date.minute*60 + date.second + date.microsecond/1_000_000
def str_to_daysecs(date: str) -> float:
    return datetime_to_daysecs(str_to_datetime(date))
def unix_to_daysecs(unix_time: float, tz=ET) -> float:
    date = unix_to_datetime(unix_time, tz=tz)
    return datetime_to_daysecs(date)
def set_datetime_daysecs(date: datetime, daysecs: float) -> datetime:
    hour = daysecs//3600
    minute = daysecs%3600//60
    second = daysecs%60//1
    microsecond = daysecs%1
    return date.replace(hour = round(hour), minute = round(minute), second = round(second), microsecond=round(microsecond))
def skip_weekend_datetime(date: datetime) -> datetime:
    if date.weekday() >= 5: return date.replace(hour=0,minute=0,second=0,microsecond=0) + timedelta(7-date.weekday())
    return date
def skip_weekend_unix(unix_time: float, tz = ET) -> float:
    date = unix_to_datetime(unix_time, tz=tz)
    return skip_weekend_datetime(date).timestamp()
#endregion

#region Intervals
def is_interval_time_datetime(date: datetime, interval: Interval):
    if date.weekday()>=5: return False
    if interval == Interval.L1:
        return date.day == get_last_workday_of_month(date) and set_close(date) == date
    if interval == Interval.W1:
        return date.weekday() == 4 and set_close(date) == date
    if interval == Interval.D1:
        return date.hour == 16 and date.minute == 0 and date.second == 0 and date.microsecond == 0
    daysecs = datetime_to_daysecs(date)
    if daysecs <= 9.5*3600 or daysecs > 16*3600: return False
    if interval == Interval.H1:
        return (daysecs < 16*3600 and daysecs%3600 == 1800) or daysecs == 16*3600
    if interval == Interval.M15:
        return not (date.minute % 15) and not date.second and not date.microsecond
    if interval == Interval.M5:
        return not (date.minute % 5) and not date.second and not date.microsecond
    raise Exception(f"Unknown interval {interval}")
def is_interval_time_unix(unix_time: float, interval: Interval, tz=ET):
    return is_interval_time_datetime(unix_to_datetime(unix_time, tz=tz), interval)
def add_intervals_datetime(date: datetime, interval: Interval, count: int) -> datetime:
    """
    Add count trading intervals to date.
    For example, 1 trading day means 7.5 hours of trading.
    1 hour of trading means 1 hour between 9:30 and 15:30,
    or 30 minutes between 15:30 and 16:00 - i.e. the last 30 minutes are counted double.
    The returned time *date2* is the minimum possible time such that *date2-date1*
    includes *count* trading intervals, or the maximum, in case of negative count.
    """
    if not count: return date
    negative = count < 0
    if interval == Interval.L1:
        original_date = date
        set_last = is_interval_time_datetime(original_date, Interval.L1)
        count += date.month-1
        date = date.replace(year=date.year+count//12,month=1,day=1)
        date = date.replace(month=1+count%12)
        if set_last: return set_close(date.replace(day = get_last_workday_of_month(date)))
        date = date.replace(day = min(get_last_workday_of_month(date), original_date.day))
        return slide_to_working(date, negative)
    if interval == Interval.W1:
        date += timedelta(days=7*count)
        if is_interval_time_datetime(date, Interval.W1): return date
        return slide_to_working(date, negative)
    if interval == Interval.D1:
        if date.weekday() >= 5: date = set_open(date + timedelta(days=7-date.weekday()))
        count += date.weekday()
        date -= timedelta(days = date.weekday())
        date += timedelta(days=7*(count//5)+count%5)
        return slide_to_working(date, negative)
    if interval == Interval.H1:
        if count//7: date = add_intervals_datetime(date, Interval.D1, count//7)
        else: date = slide_to_working(date, not negative)
        count %= 7
        # Get total work seconds from start of date to desired time
        total = datetime_to_daysecs(date)-9.5*3600
        total += max(total-6*3600,0)
        total += count*3600
        date = set_open(date)
        if total > 7*3600:
            date = date + timedelta(days = 1 if date.weekday() != 4 else 3)
            total -= 7*3600
        result = set_datetime_daysecs(date, 9.5*3600+total-max((total-6*3600)/2,0))
        return slide_to_working(result, negative)
    if interval == Interval.M15 or interval == Interval.M5:
        day_count = (6.5*3600)//interval.time()
        if count//day_count: date = add_intervals_datetime(date, Interval.D1, count//day_count)
        else: date = slide_to_working(date, not negative)
        count %= day_count
        total = datetime_to_daysecs(date) - 9.5*3600 + count*interval.time()
        date = set_open(date)
        if total > 6.5*3600:
            date = date + timedelta(days = 1 if date.weekday() != 4 else 3)
            total -= 6.5*3600
        return slide_to_working(set_datetime_daysecs(date, 9.5*3600+total), negative)
    raise Exception(f"Unknown interval {interval}")
def add_intervals_unix(unix_time: float, interval: Interval, count: int, tz=ET) -> float:
    return add_intervals_datetime(unix_to_datetime(unix_time, tz=tz), interval, count).timestamp()
#endregion

#region Timestamp arrays
def get_next_interval_time_datetime(date: datetime, interval: Interval) -> datetime:
    if interval == Interval.L1:
        last = get_last_workday_of_month(date)
        if (date.day == last and date.hour >= 16) or date.day > last:
            date += timedelta(days = 10)
            return set_close(date.replace(day=get_last_workday_of_month(date)))
        return set_close(date.replace(day=last))
    if interval == Interval.W1:
        if (date.weekday() == 4 and date.hour >= 16) or date.weekday() > 4:
            date += timedelta(days = 3)
        return set_close(date + timedelta(days = 4-date.weekday()))
    if interval == Interval.D1:
        if date.hour < 16: date = set_open(date)
        return add_intervals_datetime(date, Interval.D1, 1)
    if interval == Interval.H1:
        if date.minute>=30: date = date.replace(minute=30, second=0, microsecond=0)
        elif date.hour < 16: date = date.replace(hour=max(date.hour-1,0), minute=30, second=0, microsecond=0)
        return add_intervals_datetime(date, Interval.H1, 1)
    if interval == Interval.M15:
        date = date.replace(minute = date.minute//15*15, second=0, microsecond=0)
        return add_intervals_datetime(date, Interval.M15, 1)
    if interval == Interval.M5:
        date = date.replace(minute = date.minute//5*5, second=0, microsecond=0)
        return add_intervals_datetime(date, Interval.M5, 1)
    raise Exception(f"Unknown interval {interval}")
def get_next_interval_time_unix(unix_time: float, interval: Interval, tz = ET) -> float:
    return get_next_interval_time_datetime(unix_to_datetime(unix_time, tz = tz), interval).timestamp()
def get_interval_timestamps(unix_from: float, unix_to: float, interval: Interval, tz=ET) -> list[float]:
    cur = unix_to_datetime(unix_from, tz=tz)
    cur = get_next_interval_time_datetime(cur, interval)
    date_to = unix_to_datetime(unix_to, tz=tz)
    result = []
    while cur <= date_to:
        result.append(cur.timestamp())
        cur = get_next_interval_time_datetime(cur, interval)
    return result
#endregion

#region TimingConfig
@serializable(skip_keys=['interval'])
@equatable(skip_keys=['interval'])
class TimingConfig:
    interval: Interval|None
    def __init__(self, components: list[float|list[float]]):
        self.components = components
        self.interval = None
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
    def for_interval(self, interval: Interval) -> TimingConfig:
        result = TimingConfig(self.components)
        result.interval = interval
        return result
    def get_next_datetime(self, date: datetime, interval: Interval) -> datetime:
        interval = interval or self.interval or Interval.ascending[0]
        cur = get_next_interval_time_datetime(date, interval)
        while cur not in self: cur = get_next_interval_time_datetime(cur, interval)
        return cur
    def get_next_unix(self, unix_time: float, interval: Interval, tz = ET) -> float:
        return self.get_next_datetime(unix_to_datetime(unix_time, tz=tz), interval).timestamp()
    
    def __contains__(self, unix_or_date: float|datetime) -> bool:
        if isinstance(unix_or_date, datetime): date = unix_or_date
        else: date = unix_to_datetime(unix_or_date)
        if date.weekday() >= 5: return False
        time = datetime_to_daysecs(date)
        for it in self.components:
            if isinstance(it, list):
                if time > it[0] and time <= it[1]: return True
            else:
                if time == it: return True
        return False
    def contains(self, unix_time: float, tz=ET) -> bool:
        return unix_to_datetime(unix_time, tz=tz) in self
#endregion
