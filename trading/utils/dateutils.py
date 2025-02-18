import pytz
import re
from datetime import datetime, timedelta
from .common import Interval

"""
datetime stores the date components and an optional timezone (if unset, treated as the local timezone)
datetime.astimezone converts to a different timezone, without changing the intrinsic timestamp, while adapting date components
pytz.localize just sets the timezone, keeping the date components
"""
ET = pytz.timezone('US/Eastern')
UTC = pytz.timezone('UTC')
CET = pytz.timezone('CET')

def str_to_datetime(time_string: str, format: str = "%Y-%m-%d %H:%M:%S", tz = ET) -> datetime:
    return tz.localize(datetime.strptime(time_string, format))
def str_to_unix(time_string: str, format: str = "%Y-%m-%d %H:%M:%S", tz = ET) -> float:
    return str_to_datetime(time_string, format=format, tz=tz).timestamp()

def unix_to_datetime(unix: float, tz = ET) -> datetime:
    return datetime.fromtimestamp(unix, tz = tz)

def add_business_days_unix(unix: float, count: int, tz = ET) -> float:
    time = unix_to_datetime(unix, tz=tz)
    return add_business_days_datetime(time, count).timestamp()



def add_business_days_datetime(time: datetime, count: int) -> datetime:
    """
    Adds count business days to time and returns the result.
    A business day is 24 hours during weekdays.
    """
    if is_weekend_datetime(time):
        time = to_start_of_day_datetime(time)
        while is_weekend_datetime(time):
            time += timedelta(days=1)
    for i in range(count):
        time += timedelta(days=1)
        while is_weekend_datetime(time):
            time += timedelta(days = 1)
    return time

def is_weekend_datetime(time: datetime) -> bool:
    return time.weekday() >= 5

def to_start_of_day_datetime(time: datetime) -> datetime:
    return time.replace(hour=0, minute=0, second=0, microsecond=0)

def now(tz = ET) -> datetime:
    return datetime.now(tz = tz)

def market_open_unix(date: str):
    pattern = r'^20[012]\d-[01]\d-[0123]\d$'
    if not re.match(pattern, date):
        raise ValueError(f'Invalid date {date}. The pattern is {pattern}.')
    return str_to_unix(f"{date} 09:30:00", tz = ET)

def market_close_unix(date: str):
    pattern = r'^20[012]\d-[01]\d-[0123]\d$'
    if not re.match(pattern, date):
        raise ValueError(f'Invalid date {date}. The pattern is {pattern}.')
    return str_to_unix(f"{date} 16:00:00", tz = ET)

def get_next_working_time_datetime(date: datetime, hour: int | None = None) -> datetime:
    if date.minute or date.second or date.microsecond:
        date = date.replace(minute=0, second=0, microsecond=0)
    if is_weekend_datetime(date) or date.hour >= (hour or 16):
        date = date.replace(hour = hour or 10)
        date += timedelta(days=1)
        while is_weekend_datetime(date):
            date += timedelta(days=1)
        return date
    elif date.hour < (hour or 10):
        return date.replace(hour = hour or 10)
    else:
        return date.replace(hour = date.hour + 1)
def get_next_working_time_unix(unix_time: float, hour: int | None = None, tz = ET) -> float:
    time = unix_to_datetime(unix_time, tz = tz)
    return get_next_working_time_datetime(time, hour).timestamp()

def get_prev_working_time(unix_time: float, hour: int | None = None) -> float:
    time = unix_to_datetime(unix_time, tz = ET)
    if time.minute or time.second or time.microsecond:
        time = time.replace(minute = 0, second = 0, microsecond = 0)
        time = time + timedelta(hours = 1)
    if is_weekend_datetime(time) or time.hour <= (hour or 9):
        time = time.replace(hour = hour or 16)
        time -= timedelta(days=1)
        while is_weekend_datetime(time):
            time -= timedelta(days = 1)
    elif time.hour > (hour or 16):
        time = time.replace(hour = hour or 16)
    else:
        time = time.replace(hour = time.hour - 1)
    return time.timestamp()

def get_next_interval_time_unix(unix_time: float, interval: Interval, tz = ET) -> float:
    time = unix_to_datetime(unix_time, tz = tz)
    return get_next_interval_time_datetime(time, interval).timestamp()
def get_next_interval_time_datetime(date: datetime, interval: Interval) -> datetime:
    daysecs = datetime_to_daysecs(date)
    if interval == Interval.D1:
        if daysecs >= 16*3600: date += timedelta(days = 1)
        while is_weekend_datetime(date): date += timedelta(days = 1)
        return date.replace(hour=16, minute=0, second=0, microsecond=0)
    if interval == Interval.H1:
        if is_weekend_datetime(date) or daysecs >= 16*3600:
            date += timedelta(days=1)
            while is_weekend_datetime(date): date += timedelta(days=1)
            return date.replace(hour = 10, minute=30, second=0, microsecond=0)
        if daysecs >= 15.5*3600: return date.replace(hour = 16, minute = 0, second = 0, microsecond= 0)
        if daysecs < 10.5*3600: return date.replace(hour = 10, minute = 30, second=0, microsecond=0)
        if daysecs%3600 >= 30*60: return date.replace(hour = date.hour + 1, minute = 30, second = 0, microsecond= 0)
        return date.replace(minute = 30, second=0, microsecond=0)
    raise Exception(f"Unknown interval {interval}")
def is_interval_time_datetime(date: datetime, interval: Interval):
    if is_weekend_datetime(date): return False
    if interval == Interval.D1:
        return date.hour == 16 and date.minute == 0 and date.second == 0 and date.microsecond == 0
    if interval == Interval.H1:
        daysecs = datetime_to_daysecs(date)
        return (daysecs >= 10.5*3600 and daysecs <= 15.5*3600 and daysecs%3600 == 1800) or daysecs == 16*3600
    raise Exception(f"Unknown interval {interval}")
def is_interval_time_unix(unix_time: float, interval: Interval, tz=ET):
    return is_interval_time_datetime(unix_to_datetime(unix_time, tz=tz), interval)
"""def get_interval_timestamps(unix_from: float, unix_to: float, interval: Interval, tz=ET) -> list[float]:
    cur = unix_from if is_interval_time_unix(unix_from, interval) else get_next_interval_time_unix(unix_from, interval)
    result = []
    while cur < unix_to:
        result.append(cur)
        cur = get_next_interval_time_unix(cur, interval)
    return result"""
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