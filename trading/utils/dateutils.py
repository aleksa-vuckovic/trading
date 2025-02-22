from __future__ import annotations
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from .common import Interval, binary_search, BinarySearchEdge

"""
datetime stores the date components and an optional timezone (if unset, treated as the local timezone)
datetime.astimezone converts to a different timezone, without changing the intrinsic timestamp, while adapting date components
datetime.replace just sets the timezone, keeping the date components
"""
ET =  ZoneInfo('US/Eastern')
UTC = ZoneInfo('UTC')
CET = ZoneInfo('CET')

def str_to_datetime(time_string: str, format: str = "%Y-%m-%d %H:%M:%S", tz = ET) -> datetime:
    return datetime.strptime(time_string, format).replace(tzinfo=tz)
def str_to_unix(time_string: str, format: str = "%Y-%m-%d %H:%M:%S", tz = ET) -> float:
    return str_to_datetime(time_string, format=format, tz=tz).timestamp()
def unix_to_datetime(unix: float, tz = ET) -> datetime:
    return datetime.fromtimestamp(unix, tz = tz)
def now(tz = ET) -> datetime:
    return datetime.now(tz = tz)

def _set_open(date: datetime):
    return date.replace(hour=9, minute=30, second=0, microsecond=0)
def _set_close(date: datetime):
    return date.replace(hour=16, minute=0, second=0, microsecond=0)
def add_intervals_datetime(date: datetime, interval: Interval, count: int) -> datetime:
    """
    Add count trading intervals to date.
    For example, 1 trading day means 7.5 hours of trading.
    1 hour of trading means 1 hour between 9:30 and 15:30,
    or 30 minutes between 15:30 and 16:00 - i.e. the last 30 minutes are counted double.
    The returned time *date2* is the minimum possible time such that *date2-date1*
    includes *count* trading intervals.
    """
    if not count: return date
    if interval == Interval.D1:
        if date.weekday() >= 5: date = _set_close(date - timedelta(days=1 if date.weekday()==5 else 2))
        count += date.weekday()
        date -= timedelta(days = date.weekday())
        date+=timedelta(weeks=count//5, days=count%5)
        if date.hour >= 16: return _set_close(date)
        if datetime_to_daysecs(date) <= 9.5*3600: return _set_close(date - timedelta(days=1 if date.weekday() else 3))
        return date
    if interval == Interval.H1:
        if date.hour >= 16: date = _set_open(date+timedelta(days=1))
        elif date.hour < 9 or (date.hour == 9 and date.minute < 30): date = _set_open(date)
        if date.weekday() >= 5: date = _set_open(date + timedelta(days = 2 if date.weekday() == 5 else 1))
        # Get total work seconds from start of date to desired time
        total = datetime_to_daysecs(date)-9.5*3600
        total += max(total-6*3600,0)
        total += count*3600
        date = _set_open(date)
        days = total // (7*3600)
        total %= 7*3600
        date = add_intervals_datetime(date, Interval.D1, days)
        if total>0:
            if date.hour == 16: date = add_intervals_datetime(date, Interval.D1, 1)
            return set_datetime_daysecs(date, 9.5*3600+total-max((total-6*3600)/2, 0))
        return date        
    raise Exception(f"Unknown interval {interval}")
def add_intervals_unix(unix_time: float, interval: Interval, count: int, tz=ET) -> float:
    return add_intervals_datetime(unix_to_datetime(unix_time, tz=tz), interval, count).timestamp()

def get_next_interval_time_datetime(date: datetime, interval: Interval) -> datetime:
    if interval == Interval.D1:
        if date.hour < 16: date = _set_open(date)
        return add_intervals_datetime(date, Interval.D1, 1)
    if interval == Interval.H1:
        if date.hour >= 16 or (date.hour < 9 or date.hour==9 and date.minute<30) or date.weekday() >= 5:
            return add_intervals_datetime(date, Interval.H1, 1)
        if date.minute>=30: return add_intervals_datetime(date.replace(minute=30, second=0, microsecond=0), Interval.H1, 1)
        return add_intervals_datetime(date.replace(hour=date.hour-1, minute=30, second=0, microsecond=0), Interval.H1, 1)
    raise Exception(f"Unknown interval {interval}")
def get_next_interval_time_unix(unix_time: float, interval: Interval, tz = ET) -> float:
    return get_next_interval_time_datetime(unix_to_datetime(unix_time, tz = tz), interval).timestamp()

def is_interval_time_datetime(date: datetime, interval: Interval):
    if date.weekday()>=5: return False
    if interval == Interval.D1:
        return date.hour == 16 and date.minute == 0 and date.second == 0 and date.microsecond == 0
    if interval == Interval.H1:
        daysecs = datetime_to_daysecs(date)
        return (daysecs >= 10.5*3600 and daysecs <= 15.5*3600 and daysecs%3600 == 1800) or daysecs == 16*3600
    raise Exception(f"Unknown interval {interval}")
def is_interval_time_unix(unix_time: float, interval: Interval, tz=ET):
    return is_interval_time_datetime(unix_to_datetime(unix_time, tz=tz), interval)

def get_interval_timestamps(unix_from: float, unix_to: float, interval: Interval, tz=ET) -> list[float]:
    cur = unix_to_datetime(unix_from, tz=tz)
    cur = cur if is_interval_time_datetime(cur, interval) else get_next_interval_time_datetime(cur, interval)
    date_to = unix_to_datetime(unix_to, tz=tz)
    result = []
    while cur < date_to:
        result.append(cur.timestamp())
        cur = get_next_interval_time_datetime(cur, interval)
    return result

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
    microsecond =daysecs%1
    return date.replace(hour = round(hour), minute = round(minute), second = round(second), microsecond=round(microsecond))

def skip_weekend_datetime(date: datetime) -> datetime:
    if date.weekday() >= 5: return date.replace(hour=0,minute=0,second=0,microsecond=0) + timedelta(7-date.weekday())
    return date
def skip_weekend_unix(unix_time: float, tz = ET) -> float:
    date = unix_to_datetime(unix_time, tz=tz)
    return skip_weekend_datetime(date).timestamp()

class TimingConfig:
    _timestamps: dict[Interval, list[float]]
    def __init__(self, components: list[float|tuple[float,float]]):
        self.components = components
        self._timestamps = {}
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
                self._builder.components.append((self._start, float(hour*3600+minute*60)))
                return self._builder
        def starting(self, hour: int = 9, minute: int = 30) -> TimingConfig.Builder._Interval:
            return TimingConfig.Builder._Interval(self, float(hour*3600+minute*60))
        def around(self, hour: int = 10, minute: int = 0, delta_minute: int = 10):
            if not delta_minute: return self.at(hour = hour, minute = minute)
            time = float(hour*3600 + minute*60)
            self.components.append((time-delta_minute*60,time+delta_minute*60))
            return self
        def build(self) -> TimingConfig:
            return TimingConfig(self.components)
    def get_timestamps(self, step: float) -> list[float]:
        if step in self._timestamps: return self._timestamps[step]
        result = []
        for component in self.components:
            if isinstance(component, tuple):
                cur = component[1]
                start = component[0]
                while cur > start:
                    result.append(cur)
                    cur -= step
            else: result.append(component)
        result = sorted(result)
        self._timestamps[step] = result
        return result
    def get_next_datetime(self, date: datetime, step: float) -> datetime:
        date = skip_weekend_datetime(date)
        daysecs = datetime_to_daysecs(date)
        timestamps = self.get_timestamps(step)
        if not timestamps: raise Exception(f"No timestamps")
        index = binary_search(timestamps, daysecs, edge=BinarySearchEdge.LOW) + 1
        if index < len(timestamps): return set_datetime_daysecs(date, timestamps[index])
        date = skip_weekend_datetime(date+timedelta(days=1))
        return set_datetime_daysecs(date, timestamps[0])
    def get_next_unix(self, unix_time: float, step: float, tz = ET) -> datetime:
        return self.get_next_datetime(unix_to_datetime(unix_time, tz=tz), step).timestamp()
    
    def __contains__(self, unix_or_date: float|datetime) -> bool:
        if isinstance(unix_or_date, datetime): date = unix_or_date
        else: date = unix_to_datetime(unix_or_date)
        if date.weekday() >= 5: return False
        time = datetime_to_daysecs(date)
        for it in self.components:
            if isinstance(it, tuple) and time > it[0] and time <= it[1]: return True
            if isinstance(it, float) and time == it: return True
        return False
    def contains(self, unix_time: float, tz=ET) -> bool:
        return unix_to_datetime(unix_time, tz=tz) in self
    
    def to_list(self) -> list:
        return self.components
    @staticmethod
    def from_list(obj: list) -> TimingConfig:
        return TimingConfig([(it[0], it[1]) if isinstance(it, list) else float(it) for it in obj])
    
    def __eq__(self, other):
        if not isinstance(other, TimingConfig): return False
        return self.components == other.components