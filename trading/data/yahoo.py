import json
import yfinance
import logging
import time
import math
from enum import Enum
from pathlib import Path
from ..utils import httputils, common


logger = logging.getLogger(__name__)
_MODULE: str = __name__.split(".")[-1]
_CACHE: Path = common.CACHE / _MODULE
_MIN_AFTER_FIRST_TRADE = 14*24*3600 # The minimum time after the first trade time to query for prices
_MIN_ADJUSTMENT_PERIOD = 10*24*3600

class Interval(Enum):
    M1 = '1m'
    M2 = '2m'
    M5 = '5m'
    M15 = '15m'
    M30 = '30m'
    M60 = '60m'
    M90 = '90m'
    H1 = '1h'
    D1 = '1d'
    D5 = '5d'
    W1 = '1wk'
    MO1 = '1mo'
    MO3 = '3mo'

class Event(Enum):
    DIVIDEND = 'div'
    SPLIT = 'split'
    EARNINGS = 'earn'

def _get_yahoo_pricing_raw(
    ticker: str,
    start_time: float, #unix
    end_time: float, #unix
    interval: Interval,
    events: list[Event] = [Event.DIVIDEND, Event.SPLIT, Event.EARNINGS],
    include_pre_post = False
) -> dict:
    result =  f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker.upper()}"
    result += f"?period1={int(start_time)}&period2={math.ceil(end_time)}&interval={interval.value}"
    result += f"&incldePrePost={str(include_pre_post).lower()}&events={"|".join([it.value for it in events])}"
    result += f"&&lang=en-US&region=US"
    resp = httputils.get_as_browser(result)
    return json.loads(resp.text)

"""

"""

@common.cached_series(
    unix_from_arg=1,
    unix_to_arg=2,
    include_args=[0,3],
    cache_root=_CACHE,
    time_step_fn= lambda args: 10000000 if args[1] == Interval.H1 else 50000000,
    series_field="data",
    timestamp_field="t",
    live_delay=3600,
    return_series_only=False
)
@common.backup_timeout()
def _get_yahoo_pricing(
    ticker: str,
    unix_from: float, #unix
    unix_to: float, #unix
    interval: Interval
) -> dict:
    first_trade_time = get_first_trade_time(ticker)
    if interval == Interval.H1:
        unix_from = max(unix_from, time.time() - 729*24*3600)
    unix_from = max(unix_from, first_trade_time + _MIN_AFTER_FIRST_TRADE)
    if unix_to <= unix_from:
        return {"meta": {}, "events": [], "data": []}
    data = _get_yahoo_pricing_raw(ticker, unix_from, unix_to, interval)
    def get_meta(data):
        return data['chart']['result'][0]['meta']
    def get_events(data):
        data = data['chart']['result'][0]
        if 'events' in data:
            return data['events']
        return {}
    def get_arrays(data):
        data = data['chart']['result'][0]
        if 'timestamp' not in data or not data['timestamp']:
            return {'timestamp': [], 'open': [], 'close': [], 'low': [], 'high': [], 'volume': []}
        arrays = data['indicators']['quote'][0]
        arrays['timestamp'] = data['timestamp']
        try:
            arrays['adjclose'] = data['indicators']['adjclose'][0]['adjclose']
        except:
            pass
        vols = arrays['volume']
        times = arrays['timestamp']
        for key in arrays.keys():
            arrays[key] = [it for index,it in enumerate(arrays[key]) if vols[index] and times[index] >= unix_from and times[index] < unix_to]
        return arrays
    def try_adjust(arrays):
        try:
            d1data = _get_yahoo_pricing_raw(ticker, unix_to - _MIN_ADJUSTMENT_PERIOD, unix_to, Interval.D1)
            d1arrays = get_arrays(d1data)
            if not d1arrays['timestamp']:
                return
            close = d1arrays['close'][-1]
            time = d1arrays['timestamp'][-1] + 6*3600 #Move the start of the day to the start of the last hour
            times = arrays['timestamp']
            i = len(times) - 1
            while i >= 0 and times[i] > time and times[i]-time>=60:
                i -= 1
            if i >= 0 and abs(times[i]-time) < 60:
                factor = close / arrays['close'][i]
                for key in arrays.keys():
                    if key != 'timestamp':
                        for i in range(len(arrays[key])):
                            arrays[key][i] *= factor if key != 'volume' else 1/factor
                return
            logger.error(f"Failed to adjust {ticker}. No suitable timestamp found.")
        except:
            logger.error(f"Failed to adjust {ticker}.", exc_info=True)
    arrays = get_arrays(data)
    meta = get_meta(data)
    events = get_events(data)
    if interval == Interval.H1 and unix_to < time.time() - 15*24*3600:
        try_adjust(arrays)
    """
    Rearrange data to be comaptible with series caching by moving everything to one array.
    open - o, close - c, low - l, high - h, adjclose - a, volume - v, timestamp - t
    """
    processed = {"meta": meta, "events": events, "data": []}
    for i in range(len(arrays['timestamp'])):
        if arrays['volume'][i]:
            processed['data'].append({ it[0]: arrays[it][i] for it in arrays.keys() })
    return processed

def get_pricing(
    ticker: str,
    unix_from: float, #unix
    unix_to: float, #unix
    interval: Interval,
    return_quotes = ['close', 'volume']
) -> tuple[list[float], list[float]]:
    """
    Returns the pricing as two arrays - prices and volume.
    Zero volume entries are filtered out.
    """
    if interval != Interval.D1 and interval != Interval.H1:
        raise ValueError(f'Only H1 and D1 are supported. Got {interval.name}.')
    data = _get_yahoo_pricing(ticker.upper(), unix_from, unix_to, interval)['data']
    return tuple([it[quote[0]] for it in data] for quote in return_quotes)

def get_splits(data: dict) -> dict:
    if 'splits' in data['events']:
        return data['events']['splits']
    return {}

@common.cached_scalar(
    include_args=[0],
    path_fn=lambda args: _CACHE / common.escape_filename(args[0]) / 'info'
)
def _get_info(ticker: str) -> dict:
    info = yfinance.Ticker(ticker).info
    mock_time = int(time.time() - 15*24*3600)
    meta = _get_yahoo_pricing_raw(ticker, mock_time-100, mock_time, Interval.D1)['chart']['result'][0]['meta']
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