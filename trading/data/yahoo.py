from enum import Enum
import requests
import json
from ..utils import httputils, dateutils
import yfinance
from pathlib import Path
from datetime import datetime
import logging
import time
import math

_MODULE: str = __name__.split(".")[-1]
_CACHE: Path = Path(__file__).parent / 'cache'

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

def _create_yahoo_finance_pricing_query(
    ticker: str,
    start_time: float, #unix
    end_time: float, #unix
    interval: Interval,
    events: list[Event] = [Event.DIVIDEND, Event.SPLIT, Event.EARNINGS],
    include_pre_post = False
) -> str:
    result =  f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker.upper()}"
    result += f"?period1={int(start_time)}&period2={math.ceil(end_time)}&interval={interval.value}"
    result += f"&incldePrePost={str(include_pre_post).lower()}&events={"|".join([it.value for it in events])}"
    result += f"&&lang=en-US&region=US"
    return result

def _get_yahoo_pricing(
    ticker: str,
    start_time: float, #unix
    end_time: float, #unix
    interval: Interval,
    events: list[Event] = [Event.DIVIDEND, Event.SPLIT, Event.EARNINGS],
    include_pre_post = False,
    *, logger: logging.Logger = None
) -> dict:
    query = _create_yahoo_finance_pricing_query(ticker, start_time, end_time, interval, events, include_pre_post)
    resp = httputils.get_as_browser(query)
    data = json.loads(resp.text)
    data = data['chart']['result'][0]
    data['quotes'] = data['indicators']['quote'][0]
    #Move adjclose to quotes
    if 'adjclose' in data['indicators']:
        adjclose = data['indicators']['adjclose'][0]
        if 'adjclose' in adjclose:
            data['quotes']['adjclose'] = adjclose['adjclose']
    del data['indicators']
    return data

def get_timestamps(data: dict) -> list[int]:
    return data['timestamp']
def _get_available_quotes(data: dict) -> list[str]:
    return list(data['quotes'].keys())
def _get_quote_values(data: dict, type: str) -> list[float]:
    if type in data['quotes']:
        return data['quotes'][type]
    return None
_quotes = ['open', 'close', 'low', 'high', 'adjclose', 'volume']
def _filter_quotes(data:dict, quotes: list[str] = _quotes):
    volume = _get_quote_values(data, 'volume')
    for quote in set(quotes).intersection(_get_available_quotes(data)):
        data['quotes'][quote] = [it for index,it in enumerate(_get_quote_values(data, quote)) if volume[index]]
def _adjust_quotes(data:dict, factor: float):
    for quote in set(_quotes).difference(['adjclose']).intersection(_get_available_quotes(data)):
        arr = _get_quote_values(data, quote)
        for i in range(len(arr)):
            arr[i] *= 1/factor if quote == 'volume' else factor
def get_splits(data: dict) -> dict:
    if 'events' in data and 'splits' in data['events']:
        return data['events']['splits']
    return {}
def get_close(data: dict) -> list[float]:
    return _get_quote_values(data, 'close')
def get_volume(data: dict) -> list[float]:
    return _get_quote_values(data, 'volume')
def get_adjclose(data: dict) -> list[float]:
    return _get_quote_values(data, 'adjclose')

def _get_info(ticker: str) -> dict:
    return yfinance.Ticker(ticker).info

def get_shares(info: dict) -> int:
    key = 'impliedSharesOutstanding'
    if info and key in info:
        return info[key]
    return None
def get_summary(info: dict) -> dict:
    key = 'longBusinessSummary'
    if info and key in info:
        return info[key]
    return None
def get_first_trade_time(info: dict) -> float:
    key = 'firstTradeDate'
    if info and key in info:
        return info[key]
    return None

def get_yahoo_pricing(
    ticker: str,
    start_time: float, #unix
    end_time: float, #unix
    interval: Interval,
    return_quote = 'close',
    *, logger: logging.Logger = None
) -> tuple[list[float], list[float]]:
    """
    Returns the pricing as two arrays - prices and volume.
    Zero volume entries are filtered out.
    """
    path = _CACHE / _MODULE / ticker.lower()
    path.mkdir(parents = True, exist_ok = True)
    start_datetime = dateutils.unix_to_datetime(start_time, tz = dateutils.EST)
    end_datetime = dateutils.unix_to_datetime(end_time, tz = dateutils.EST)
    
    prices = []
    volumes = []
    timestamps = []
    def extend(data):
        prices.extend(_get_quote_values(data, return_quote))
        volumes.extend(get_volume(data))
        timestamps.extend(get_timestamps(data))
    if interval == Interval.D1:
        #Fetch per 3 years
        #D1 is always already adjusted for splits
        start_year = start_datetime.year - start_datetime.year % 3
        end_year = end_datetime.year - end_datetime.year % 3 + 3

        for year in range(start_year, end_year, 3):
            subpath = path / f'{Interval.D1.name}-{year}'
            if subpath.exists():
                data = json.loads(subpath.read_text())
            else:
                unix_start = int(datetime(year, 1, 1, tzinfo=dateutils.EST).timestamp())
                unix_end = int(datetime(year+3, 1, 1, tzinfo=dateutils.EST).timestamp())
                data = _get_yahoo_pricing(ticker, unix_start, unix_end, interval, logger = logger)
                _filter_quotes(data)
                subpath.write_text(json.dumps(data))
            extend(data)
    elif interval == Interval.H1:
        #Fetch per 4 months
        start_month = start_datetime.month - (start_datetime.month-1)%4
        end_month = end_datetime.month - (end_datetime.month-1)%4 + 4
        for year in range(start_datetime.year, end_datetime.year+1):
            for month in range(start_month if year == start_datetime.year else 1, end_month if year == end_datetime.year else 13, 4):
                subpath = path / f'{Interval.H1.name}-{year}-{month}'
                if subpath.exists():
                    data = json.loads(subpath.read_text())
                else:
                    unix_start = int(datetime(year, month, 1, tzinfo=dateutils.EST).timestamp())
                    unix_end = int(datetime(year+1 if month+4 > 12 else year, 1 if month+4>12 else month+4, 1, tzinfo=dateutils.EST).timestamp())
                    data = _get_yahoo_pricing(ticker, unix_start, unix_end, interval, logger = logger)
                    if data is None:
                        return (None, None)
                    _filter_quotes(data)
                    close = get_close(data)
                    if close: #adjust for splits
                        if get_adjclose(data):
                            factor = get_adjclose(data)[-1] / close[-1]
                        else:
                            #try with 1d
                            data1d = _get_yahoo_pricing(ticker, unix_start, unix_end, Interval.D1, logger = logger)
                            if data1d:
                                _filter_quotes(data1d, ['close', 'adjclose'])
                                close1d = get_adjclose(data1d) or get_close(data1d)
                                if close1d:
                                    factor = close1d[-1] / close[-1]
                                else:
                                    factor = 1
                            else:
                                factor = 1
                        _adjust_quotes(data, factor)
                    subpath.write_text(json.dumps(data))
                extend(data)
    else:
        raise ValueError(f'Only H1 and D1 are supported not {interval.name}')
    i = 0
    j = len(timestamps)
    while i < len(timestamps) and timestamps[i] < start_time:
        i += 1
    while j > 0 and timestamps[j-1] > end_time:
        j -= 1
    
    return (prices[i:j], volumes[i:j])

def get_info(ticker: str, *, logger: logging.Logger = None) -> dict:
    path = _MODULE / _CACHE / ticker.lower()
    path.mkdir(parents = True, exist_ok = True)
    path /= 'info'
    if path.exists():
        return json.loads(path.read_text())
    else:
        info = _get_info(ticker)
        mock_time = int(time.time())
        meta = _get_yahoo_pricing(ticker, mock_time-100, mock_time, interval = Interval.W1, logger=logger)['meta']
        info = {**info, **meta}
        path.write_text(json.dumps(info))
        return info