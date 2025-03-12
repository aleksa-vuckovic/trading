#1
from datetime import datetime
from zoneinfo import ZoneInfo

"""
datetime stores the date components and an optional timezone (if unset, treated as the local timezone)
datetime.astimezone converts to a different timezone, without changing the intrinsic timestamp, while adapting date components
datetime.replace just sets the timezone, keeping the date components
"""
ET =  ZoneInfo('US/Eastern')
UTC = ZoneInfo('UTC')
CET = ZoneInfo('CET')

def str_to_datetime(time_string: str, format: str = "%Y-%m-%d %H:%M:%S", tz=UTC) -> datetime:
    return datetime.strptime(time_string, format).replace(tzinfo=tz)
def str_to_unix(time_string: str, format: str = "%Y-%m-%d %H:%M:%S", tz=UTC) -> float:
    return str_to_datetime(time_string, format=format, tz=tz).timestamp()
def unix_to_datetime(unix: float|int, tz=UTC) -> datetime:
    return datetime.fromtimestamp(unix, tz=tz)
def datetime_to_unix(time: datetime) -> float:
    return time.timestamp()
def localize(time: datetime, tz=UTC) -> datetime:
    return time.astimezone(tz)
def to_zero(time: datetime|float|int, tz=UTC) -> datetime|float|int:
    if not isinstance(time, datetime):
        return to_zero(unix_to_datetime(time, tz=tz)).timestamp()
    return time.replace(hour=0, minute=0, second=0, microsecond=0)
def now(tz=UTC) -> datetime:
    return datetime.now(tz=tz)