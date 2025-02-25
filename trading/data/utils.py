import logging
import calendar
from datetime import timedelta
from ..utils import dateutils
from ..utils.common import Interval

logger = logging.getLogger(__name__)

def combine_series(
    data: dict,
    reduce_keys: bool = True,
    must_be_there: list[str] = ['t', 'o', 'h', 'l', 'c', 'v'],
    must_be_truthy: list[str]|bool = True,
    as_list: bool = False,
    timestamp_key: str|None = 't',
    timestamp_from: float|int|None = None,
    timestamp_to: float|int|None = None
) -> list[dict] | list[list]:
    if reduce_keys:
        data = {key[0].lower():data[key] for key in data}
    keys = list(data.keys())
    if not keys: return []
    length = len(data[keys[0]])
    if not length: return []
    if not all(len(data[key]) == length for key in keys):
        raise Exception(f"Unequal series lengths. Data:\n{data}")
    for key in must_be_there:
        if key not in keys:
            raise Exception(f"Missing key {key}. Present keys: {keys}")
    must_be_truthy = must_be_truthy if isinstance(must_be_truthy, list) else keys if must_be_truthy else []
    if timestamp_from and timestamp_to and timestamp_key:
        if not timestamp_key in keys:
            raise Exception(f"Missing key {timestamp_key}. Present keys: {keys}")
        def is_ok(i: int):
            for key in must_be_truthy:
                if not data[key][i]: return False
            return data[timestamp_key][i] > timestamp_from and data[timestamp_key][i] <= timestamp_to
    else:
        def is_ok(i: int):
            for key in must_be_truthy:
                if not data[key][i]: return False
            return True
    if as_list: return [[data[key][i] for key in must_be_there] for i in range(length) if is_ok(i)]
    else: return [{key: data[key][i] for key in keys} for i in range(length) if is_ok(i)]

def separate_quotes(data: list[dict], quotes: list[str]) -> tuple[list[float], ...]:
    return tuple([it[quote[0]] for it in data] for quote in quotes)
def filter_by_timestamp(data: list[dict|list], unix_from: float, unix_to: float, timestamp_field: str | int = 't') -> list[dict]:
    return [it for it in data if it[timestamp_field] > unix_from and it[timestamp_field] <= unix_to]

def fix_long_timestamps(timestamps: list[float|int|None], interval: Interval) -> list[float]:
    result = []
    if interval < Interval.D1: raise Exception(f"Interval {interval} is not long.")
    for it in timestamps:
        if not it:
            result.append(None)
            continue
        # Usually returned as 00:00 in UTC
        date = dateutils.unix_to_datetime(it + 7.5*3600, dateutils.ET)
        # For timestamps returned as 00:00 UTC this will cross into the proper ET date.
        # For timestamps at the 00:00, market open or market close in ET it will remain in the same day.
        if date.hour not in [2,3,7,17,23]:
            logger.warning(f"Unexpected {interval} timestamp {date}.")
        date = dateutils.set_close(date)
        if interval == Interval.W1: date += timedelta(days = 4-date.weekday())
        if interval == Interval.L1: date = date.replace(day=dateutils.get_last_workday_of_month(date))
        result.append(date.timestamp())
    return result
