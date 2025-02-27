import json
import yfinance
import logging
import time
import math
from enum import Enum
from pathlib import Path
from ..utils import httputils, common, dateutils
from ..utils.common import Interval
from .utils import combine_series, fix_long_timestamps, separate_quotes
from .caching import cached_scalar, cached_series, CACHE_ROOT

"""
Hourly data from yahoo covers 1 hour periods starting from 9:30.
The last nonprepost period is at 15:30 and covers only the last 30 minutes.
The timestamps correspond to the START of the period (not the end!).
"""

logger = logging.getLogger(__name__)
_MODULE: str = __name__.split(".")[-1]
_CACHE: Path = CACHE_ROOT / _MODULE
_MIN_AFTER_FIRST_TRADE = 14*24*3600 # The minimum time after the first trade time to query for prices
_MIN_ADJUSTMENT_PERIOD = 10*24*3600

def _interval_to_str(interval: Interval) -> str:
    if interval == Interval.L1: return '1mo'
    if interval == Interval.W1: return '5d'
    if interval == Interval.H1: return '1h'
    if interval == Interval.D1: return '1d'
    if interval == Interval.M15: return '15m'
    if interval == Interval.M5: return '5m'
    raise Exception(f"Unknown interval {interval}.")

class Event(Enum):
    DIVIDEND = 'div'
    SPLIT = 'split'
    EARNINGS = 'earn'

def _get_pricing_raw(
    ticker: str,
    start_time: float, #unix
    end_time: float, #unix
    interval: Interval,
    events: list[Event] = [Event.DIVIDEND, Event.SPLIT, Event.EARNINGS],
    include_pre_post = False
) -> dict:
    result =  f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker.upper()}"
    result += f"?period1={int(start_time)}&period2={math.ceil(end_time)}&interval={_interval_to_str(interval)}"
    result += f"&incldePrePost={str(include_pre_post).lower()}&events={"|".join([it.value for it in events])}"
    result += f"&&lang=en-US&region=US"
    resp = httputils.get_as_browser(result)
    return json.loads(resp.text)

def _fix_timestamps(timestamps: list[float], interval: Interval):
    if interval >= Interval.D1: return fix_long_timestamps(timestamps, interval)
    size = interval.time() if interval != Interval.H1 else 1800
    result = []
    for it in timestamps:
        if not it:
            result.append(None)
            continue
        if interval == Interval.H1:
            date = dateutils.unix_to_datetime(it, tz=dateutils.ET)
            if date.hour == 15: it += 1800
            else: it += 3600
        else: it += size
        if not dateutils.is_interval_time_unix(it, interval, tz=dateutils.ET):
            logger.warning(f"Unexpected timestamp {dateutils.unix_to_datetime(it, tz=dateutils.ET)}. Skipping entry.")
            result.append(None)
        else: result.append(it)
    return result

@cached_series(
    unix_from_arg=1,
    unix_to_arg=2,
    include_args=[0,3],
    cache_root=_CACHE,
    time_step_fn= lambda args: 5000000 if args[1] < Interval.H1 else 10000000 if args[1] == Interval.H1 else 100000000,
    series_field="data",
    timestamp_field="t",
    live_delay_fn=5*60,
    live_refresh_fn=lambda args,last,now: dateutils.get_next_interval_time_unix(last, args[1]) < now,
    return_series_only=False
)
@httputils.backup_timeout()
def _get_pricing(
    ticker: str,
    unix_from: float, #unix
    unix_to: float, #unix
    interval: Interval
) -> dict:
    first_trade_time = get_first_trade_time(ticker)
    now = time.time()
    query_from = max(unix_from - interval.time(), first_trade_time + _MIN_AFTER_FIRST_TRADE)
    query_from = max(query_from, interval.start_unix())
    query_to = unix_to
    if query_to <= query_from:
        return {"meta": {}, "events": {}, "data": []}
    try:
        data = _get_pricing_raw(ticker, query_from, query_to, interval)
    except httputils.BadResponseException:
        logger.error(f"Bad response for {ticker} from {unix_from} to {unix_to} at {interval}. PERMANENT EMPTY RETURN!", exc_info=True)
        return { "meta": {}, "events": {}, "data": [] }
    def get_meta(data):
        return data['chart']['result'][0]['meta']
    def get_events(data):
        data = data['chart']['result'][0]
        if 'events' in data:
            return data['events']
        return {}
    def get_series(data) -> list[dict]:
        data = data['chart']['result'][0]
        if 'timestamp' not in data or not data['timestamp']: return []
        arrays = data['indicators']['quote'][0]
        arrays['timestamp'] = _fix_timestamps(data['timestamp'], interval)
        try:
            arrays['adjclose'] = data['indicators']['adjclose'][0]['adjclose']
        except:
            pass
        return combine_series(arrays, timestamp_from=unix_from, timestamp_to=unix_to)
    def try_adjust(series):
        try:
            d1data = _get_pricing_raw(ticker, unix_to - _MIN_ADJUSTMENT_PERIOD, unix_to, Interval.D1)
            d1arrays = get_series(d1data)
            close = d1arrays[-2]['c']
            time = d1arrays[-2]['t'] #Move the start of the day to the start of the last hour
            i = len(series) - 1
            while i >= 0 and series[i]['t'] > time: i -= 1
            if i >= 0 and series[i]['t'] == time:
                factor = close / series[i]['c']
                for key in series[i].keys():
                    if key != 't':
                        for i in range(len(series)):
                            series[i][key] *= factor if key != 'v' else 1/factor
                return
            logger.error(f"Failed to adjust {ticker}. No suitable timestamp found.")
        except:
            logger.error(f"Failed to adjust {ticker}.", exc_info=True)
    series = get_series(data)
    meta = get_meta(data)
    events = get_events(data)
    if interval <= Interval.H1 and unix_to < now - 15*24*3600:
        try_adjust(series)
    """
    Rearrange data to be comaptible with series caching by moving everything to one array.
    open - o, close - c, low - l, high - h, adjclose - a, volume - v, timestamp - t
    """
    return {
        "meta": meta,
        "events": events,
        "data": series
    }

def get_pricing(
    ticker: str,
    unix_from: float, #unix
    unix_to: float, #unix
    interval: Interval,
    return_quotes = ['close', 'volume'],
    **kwargs
) -> tuple[list[float], ...]:
    """
    Returns the pricing as two arrays - prices and volume.
    Zero volume entries are filtered out.
    """
    data = _get_pricing(ticker.upper(), unix_from, unix_to, interval, **kwargs)['data']
    return separate_quotes(data, return_quotes)

def get_splits(data: dict) -> dict:
    if 'splits' in data['events']:
        return data['events']['splits']
    return {}

@cached_scalar(
    include_args=[0],
    path_fn=lambda args: _CACHE / common.escape_filename(args[0]) / 'info'
)
def _get_info(ticker: str) -> dict:
    info = yfinance.Ticker(ticker).info
    mock_time = int(time.time() - 15*24*3600)
    try:
        meta = _get_pricing_raw(ticker, mock_time-3*24*3600, mock_time, Interval.D1)['chart']['result'][0]['meta']
    except httputils.BadResponseException:
        logger.error(f"Bad response for {ticker} in _get_info. PERMANENT EMPTY RETURN!", exc_info=True)
        meta = {}
    return {**info, **meta}

def get_info(ticker: str) -> dict:
    return _get_info(ticker.upper())
def get_shares(ticker: str) -> int:
    key = 'impliedSharesOutstanding'
    info = get_info(ticker)
    if key in info and info[key]:
        return float(info[key])
    key = 'sharesOutstanding'
    return float(info[key])
def get_summary(ticker: str) -> str:
    key = 'longBusinessSummary'
    return str(get_info(ticker)[key])
def get_first_trade_time(ticker: str) -> float:
    key = 'firstTradeDateEpochUtc'
    info = get_info(ticker)
    if key in info and info[key]:
        return float(info[key])
    key = 'firstTradeDate'
    return float(info[key])
def get_market_cap(symbol: str) -> float:
    key = 'marketCap'
    info = get_info(symbol)
    return float(info[key])