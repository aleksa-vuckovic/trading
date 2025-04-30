#1
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import overload


"""
datetime stores the date components and an optional timezone (if unset, treated as the local timezone)
datetime.astimezone converts to a different timezone, without changing the intrinsic timestamp, while adapting date components
datetime.replace just sets the timezone, keeping the date components
"""
ET =  ZoneInfo('US/Eastern')
UTC = ZoneInfo('UTC')
CET = ZoneInfo('CET')
SYDNEY = ZoneInfo("Australia/Sydney")
TOKYO = ZoneInfo("Asia/Tokyo")
LONDON = ZoneInfo("Europe/London")

DEFAULT_FORMAT = "%Y-%m-%d %H:%M:%S"
def str_to_datetime(time_string: str, format: str = DEFAULT_FORMAT, tz=UTC) -> datetime:
    return datetime.strptime(time_string, format).replace(tzinfo=tz)
def str_to_unix(time_string: str, format: str = DEFAULT_FORMAT, tz=UTC) -> float:
    return str_to_datetime(time_string, format=format, tz=tz).timestamp()
def datetime_to_str(time: datetime, format: str = DEFAULT_FORMAT) -> str:
    return time.strftime(format)
def unix_to_str(time: float, format: str = DEFAULT_FORMAT, tz=UTC) -> str:
    return datetime_to_str(unix_to_datetime(time, tz=tz), format=format)
def unix_to_datetime(unix: float, tz=UTC) -> datetime:
    return datetime.fromtimestamp(unix, tz=tz)
def datetime_to_unix(time: datetime) -> float:
    return time.timestamp()
def localize(time: datetime, tz=UTC) -> datetime:
    return time.astimezone(tz)

@overload
def to_zero(time: datetime) -> datetime: ...
@overload
def to_zero(time: float, tz: ZoneInfo=...) -> float: ...
def to_zero(time: datetime|float, tz=UTC) -> datetime|float:
    if not isinstance(time, datetime):
        return to_zero(unix_to_datetime(time, tz=tz)).timestamp()
    return time.replace(hour=0, minute=0, second=0, microsecond=0)
def now(tz=UTC) -> datetime: return datetime.now(tz=tz)
_unix_time: float|None = None
def set(unix_time: float|None):
    global _unix_time
    _unix_time = unix_time
def add(seconds: float):
    global _unix_time
    _unix_time = _unix_time or 0
    _unix_time += seconds
def unix() -> float: return _unix_time or time.time()