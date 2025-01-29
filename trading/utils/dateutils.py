from datetime import datetime, timedelta
import pytz
import re

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