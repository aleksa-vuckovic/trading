from datetime import datetime
import pytz
import re

"""
datetime stores the date components and an optional timezone (if unset, treated as the local timezone)
datetime.astimezone converts to a different timezone, without changing the intrinsic timestamp, while adapting date components
pytz.localize just sets the timezone, keeping the date components
"""
EST = pytz.timezone('US/Eastern')
UTC = pytz.timezone('UTC')
CET = pytz.timezone('CET')

def str_to_unix(time_string: str, format: str = "%Y-%m-%d %H:%M:%S", tz = EST) -> int:
    time = datetime.strptime(time_string, format)
    time = tz.localize(time)
    return int(time.timestamp())

def unix_to_datetime(unix: int, tz = EST) -> datetime:
    return datetime.fromtimestamp(unix, tz = tz)

def now(tz = EST) -> datetime:
    return datetime.now(tz = tz)

def market_open_unix(date: str):
    pattern = r'^20[012]\d-[01]\d-[0123]\d$'
    if not re.match(pattern, date):
        raise ValueError(f'Invalid date {date}. The pattern is {pattern}.')
    return str_to_unix(f"{date} 09:30:00", tz = EST)

def market_close_unix(date: str):
    pattern = r'^20[012]\d-[01]\d-[0123]\d$'
    if not re.match(pattern, date):
        raise ValueError(f'Invalid date {date}. The pattern is {pattern}.')
    return str_to_unix(f"{date} 16:00:00", tz = EST)