import json
import logging
import time
import math
from pathlib import Path
from ..utils.common import Interval
from ..utils import httputils, dateutils, common
from .utils import combine_series

logger = logging.getLogger(__name__)
_MODULE: str = __name__.split(".")[-1]
_CACHE: Path = common.CACHE / _MODULE

@common.cached_scalar(
    include_args=[0],
    path_fn=lambda args: _CACHE/args[0]/'info'
)
def _get_info(symbol: str, exchanges: str = ['NAQ', 'NSQ', 'NMQ']) -> dict:
    terms = [f"{symbol.upper()}:{exchange.upper()}" for exchange in exchanges]
    url = f"https://markets.ft.com/data/searchapi/searchsecurities"
    resp = httputils.get_as_browser(url, params={'query': symbol.upper()})
    data = json.loads(resp.text)
    data = [it for it in data['data']['security'] if 'symbol' in it and it['symbol'] in terms]
    if not data: return {}
    return data[0]

@common.backup_timeout(behavior=common.BackupBehavior.RETHROW)
def _get_pricing_raw(symbol: str, days: int, data_period: str, data_interval: int, realtime: bool):
    info = _get_info(symbol)
    if 'xid' not in info:
        logger.warning(f"No xid for '{symbol}'. Returning empty prices.")
        return {}
    url = "https://markets.ft.com/data/chartapi/series"
    request = {
        "days": days,
        "dataNormalized":False,
        "dataPeriod": data_period,
        "dataInterval":data_interval,
        "realtime":realtime,
        "yFormat":"0.###",
        "timeServiceFormat":"JSON",
        "returnDateType":"Unix",
        "elements": [
            {
                "Label":"266a0ba8",
                "Type":"price",
                "Symbol":"9023539",
                "OverlayIndicators":[],
                "Params":{}
            }
            ,
            {
                "Label":"b2b89a77",
                "Type":"volume",
                "Symbol":"9023539",
                "OverlayIndicators":[],
                "Params":{}
            }
        ]
    }
    resp = httputils.post_as_browser(url, request)
    return json.loads(resp.text)

    
def _get_period_for_interval(interval: Interval) -> tuple[str, int]:
    if interval == Interval.D1: return 'Day', 1
    if interval == Interval.H1: return 'Hour', 1
    raise Exception(f"Unknown interval {interval}")
def _fix_timestamps(timestamps: list[float], interval: Interval) -> list[float]:
    if interval == Interval.H1:
        for i in range(len(timestamps)):
            if not timestamps[i]:
                continue
            t = round(timestamps[i])
            date = dateutils.unix_to_datetime(t, tz=dateutils.ET)
            if date.hour == 16 and date.minute == 0:
                timestamps[i] = t - 30*60
            elif date.minute == 30:
                timestamps[i] = t - 3600
            else:
                raise Exception(f"Unexpected timestamp {t} for H1")
    elif interval == Interval.D1:
        for i in range(len(timestamps)):
            if not timestamps[i]:
                continue
            # For some reason these are returned as 00:00 in UTC
            date = dateutils.unix_to_datetime(timestamps[i] + 12*3600, dateutils.ET)
            date = date.replace(hour = 9, minute=30, second=0, microsecond=0)
            timestamps[i] = date.timestamp()
    else:
        raise Exception(f"Unknown interval {interval}")

@common.cached_series(
    unix_from_arg=1,
    unix_to_arg=2,
    include_args=[0,3],
    cache_root=_CACHE,
    live_delay_fn=lambda args: common.get_delay_for_interval(args[1]),
    return_series_only=False,
    series_field='data',
    time_step_fn=10000000,
    timestamp_field='t'
)
def _get_pricing(symbol: str, unix_from: float, unix_to: float, interval: Interval) -> dict:
    days = math.ceil((time.time() - unix_from)/(24*3600)) + 1
    if days <= 0: days = 1
    if days > 15: days = 15
    data = _get_pricing_raw(symbol, days, *_get_period_for_interval(interval), realtime=True)
    timestamps = data['Dates']
    _fix_timestamps(timestamps, interval)
    elements = data['Elements']
    if len(elements) < 2:
        raise Exception(f"Expected 2 objects in the Elements array (prices and volumes) but got less. Data:\n{data}")
    prices = [it for it in elements if it['Type'] == 'price'][0]
    volumes = [it for it in elements if it['Type'] == 'volume'][0]
    def extract_component_series_values(element):
        return {it['Type']:it['Values'] for it in element['ComponentSeries']}
    data = {**extract_component_series_values(prices), **extract_component_series_values(volumes)}
    data['Timestamp'] = timestamps
    result = prices
    del result['ComponentSeries']
    del result['Label']
    del result['Type']
    result['data'] = combine_series(data, timestamp_from=unix_from, timestamp_to=unix_to)
    return result 

def get_pricing(symbol: str, unix_from: float, unix_to: float, interval: Interval, return_quotes: list[str] = ['close', 'volume']) -> tuple:
    data = _get_pricing(symbol, unix_from, unix_to, interval)['data']
    return tuple([it[quote[0]] for it in data] for quote in return_quotes)