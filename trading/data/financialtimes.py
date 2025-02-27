import json
import logging
import time
import math
from pathlib import Path
from ..utils.common import Interval
from ..utils import httputils, dateutils, common
from .utils import combine_series, fix_long_timestamps, separate_quotes
from .caching import cached_scalar, cached_series, CACHE_ROOT

logger = logging.getLogger(__name__)
_MODULE: str = __name__.split(".")[-1]
_CACHE: Path = CACHE_ROOT / _MODULE

@cached_scalar(
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

"""
FT timestamps represent the start of the relevant interval BUT
they are often a few milliseconds off!
"""
def _get_pricing_raw(symbol: str, days: int, data_period: str, data_interval: int, realtime: bool):
    info = _get_info(symbol)
    if 'xid' not in info:
        raise Exception(f"No xid for '{symbol}'.")
    xid = info['xid']
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
                "Symbol":xid,
                "OverlayIndicators":[],
                "Params":{}
            }
            ,
            {
                "Label":"b2b89a77",
                "Type":"volume",
                "Symbol":xid,
                "OverlayIndicators":[],
                "Params":{}
            }
        ]
    }
    resp = httputils.post_as_browser(url, request)
    return json.loads(resp.text)

    
def _get_period_for_interval(interval: Interval) -> tuple[str, int]:
    if interval > Interval.D1: raise Exception(f"Unsupported interval {interval}.")
    if interval == Interval.D1: return 'Day', 1
    if interval == Interval.H1: return 'Hour', 1
    if interval == Interval.M15: return 'Minute', 15
    if interval == Interval.M5: return 'Minute', 5
    raise Exception(f"Unknown interval {interval}")
def _fix_timestamps(timestamps: list[float], interval: Interval) -> list[float]:
    if interval >= Interval.D1: return fix_long_timestamps(timestamps, interval)
    else:
        result = []
        for it in timestamps:
            if not it:
                result.append(None)
                continue
            it = round(it/10)*10
            if not dateutils.is_interval_time_unix(it, interval, tz=dateutils.ET):
                logger.error(f"Unexpected timestamp {dateutils.unix_to_datetime(it, tz=dateutils.ET)} for period H1. Skipping entry.")
                result.append(None)
            result.append(it)
        return result

@cached_series(
    unix_from_arg=1,
    unix_to_arg=2,
    include_args=[0,3],
    cache_root=_CACHE,
    live_delay_fn=15*60,
    live_refresh_fn=lambda args,last,now: dateutils.get_next_interval_time_unix(last, args[1]) < now,
    return_series_only=False,
    series_field='data',
    time_step_fn=10000000,
    timestamp_field='t'
)
@httputils.backup_timeout()
def _get_pricing(symbol: str, unix_from: float, unix_to: float, interval: Interval) -> dict:
    days = math.ceil((time.time() - unix_from)/(24*3600)) + 1
    if days <= 3: days = 4
    if days > 15: days = 15
    data = _get_pricing_raw(symbol, days, *_get_period_for_interval(interval), realtime=True)
    timestamps = _fix_timestamps(data['Dates'], interval)
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

def get_pricing(symbol: str, unix_from: float, unix_to: float, interval: Interval, return_quotes: list[str] = ['close', 'volume'], **kwargs) -> tuple[list[float], ...]:
    data = _get_pricing(symbol.upper(), unix_from, unix_to, interval, **kwargs)['data']
    return separate_quotes(data, return_quotes)