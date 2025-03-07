import json
import logging
import time
import math
from pathlib import Path
from ..utils import httputils 
from ..utils.common import Interval
from .utils import combine_series, fix_long_timestamps, separate_quotes
from .caching import cached_scalar, cached_series, CACHE_ROOT, FETCH, NOW, FilePersistor
from .abstract import PricingProvider, AbstractSecurity, WorkCalendar
from .nasdaq import NasdaqSecurity, NasdaqMarket

logger = logging.getLogger(__name__)
_MODULE: str = __name__.split(".")[-1]

def _get_exchange(security: AbstractSecurity) -> str:
    if isinstance(security, NasdaqSecurity):
        if security.market == NasdaqMarket.SELECT: return 'NSQ'
        if security.market == NasdaqMarket.GLOBAL: return 'NMQ'
        if security.market == NasdaqMarket.CAPITAL: return 'NAQ'
        raise Exception(f"Unknown nasdaq market {security.market}.")
    raise Exception(f"Unknown security type {type(security)}")
def _get_interval(interval: Interval) -> tuple[str, int]:
    if interval > Interval.D1: raise Exception(f"Unsupported interval {interval}.")
    if interval == Interval.D1: return 'Day', 1
    if interval == Interval.H1: return 'Hour', 1
    if interval == Interval.M15: return 'Minute', 15
    if interval == Interval.M5: return 'Minute', 5
    raise Exception(f"Unknown interval {interval}")

class FinancialTimes(PricingProvider):

    def _get_identifier(self, security: AbstractSecurity) -> str:
        return f"{security.symbol}:{_get_exchange(security)}"
    
    def _get_info_key_fn(self, security: AbstractSecurity) -> list[str]:
        return [self._get_identifier(security).replace(":", "_")]
    @cached_scalar(
        key_fn=_get_info_key_fn,
        persistor_fn=FilePersistor(CACHE_ROOT/_MODULE/'info')
    )
    def _get_info(self, security: AbstractSecurity) -> dict:
        url = f"https://markets.ft.com/data/searchapi/searchsecurities"
        id = self._get_identifier(security)
        resp = httputils.get_as_browser(url, params={'query': id})
        data = json.loads(resp.text)
        data = [it for it in data['data']['security'] if 'symbol' in it and it['symbol'] == id]
        if not data: return {}
        return data[0]

    """
    FT timestamps represent the start of the relevant interval BUT
    they are often a few milliseconds off!
    """
    def _get_pricing_raw(self, security: AbstractSecurity, days: int, data_period: str, data_interval: int, realtime: bool):
        info = self._get_info(security)
        if 'xid' not in info:
            raise Exception(f"No xid for '{(security.symbol)}'.")
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
    
    def _fix_timestamps(self, timestamps: list[float], interval: Interval, calendar: WorkCalendar) -> list[float]:
        if interval >= Interval.D1: return fix_long_timestamps(timestamps, interval)
        else:
            result = []
            for it in timestamps:
                if not it:
                    result.append(None)
                    continue
                it = round(it/10)*10
                if not calendar.is_timestamp(it, interval):
                    logger.error(f"Unexpected timestamp {calendar.unix_to_datetime(it)} for period H1. Skipping entry.")
                    result.append(None)
                result.append(it)
            return result

    def _get_pricing_key_fn(self, security: AbstractSecurity, interval: Interval) -> list[str]:
        return [self._get_identifier(security), interval.name]
    def _get_pricing_should_refresh_fn(self, security: AbstractSecurity, interval: Interval, *, fetch: float, now: float) -> bool:
        return security.exchange.calendar.get_next_timestamp(fetch, interval) < now
    @cached_series(
        unix_args=(2,3),
        series_field='data',
        timestamp_field='t',
        key_fn=_get_pricing_key_fn,
        persistor_fn=FilePersistor(CACHE_ROOT/_MODULE/'pricing'),
        live_delay_fn=15*60,
        should_refresh_fn=_get_pricing_should_refresh_fn,
        time_step_fn=10000000
    )
    @httputils.backup_timeout()
    def _get_pricing(self, security: AbstractSecurity, unix_from: float, unix_to: float, interval: Interval) -> dict:
        days = math.ceil((time.time() - unix_from)/(24*3600)) + 1
        if days <= 3: days = 4
        if days > 15: days = 15
        data = self._get_pricing_raw(security, days, *_get_interval(interval), realtime=True)
        timestamps = self._fix_timestamps(data['Dates'], interval)
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
    def get_pricing(self, security, unix_from, unix_to, interval, *, return_quotes = ..., **kwargs):
        return separate_quotes(self._get_pricing(security, unix_from, unix_to, interval, **kwargs)['data'], return_quotes)