import json
import logging
import time
import math
from typing import override
from trading.utils import httputils 
from trading.core.interval import Interval
from trading.providers.utils import combine_series, filter_by_timestamp
from base.caching import cached_scalar, CACHE_ROOT, DB_PATH, Persistor, FilePersistor, SqlitePersistor
from trading.core.securities import Security
from trading.core.pricing_provider import BasePricingProvider
from trading.providers.nasdaq import NasdaqSecurity, NasdaqMarket

logger = logging.getLogger(__name__)
_MODULE: str = __name__.split(".")[-1]

def _get_exchange(security: Security) -> str:
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

class FinancialTimes(BasePricingProvider):
    def __init__(self, use_file: bool = False):
        super().__init__({
            Interval.D1: None,
            Interval.H1: Interval.M5,
            Interval.M15: Interval.M5,
            Interval.M5: None
        })
        self.info_persistor = FilePersistor(CACHE_ROOT/_MODULE/'info') if use_file else SqlitePersistor(DB_PATH, f"{_MODULE}_info")
        self.pricing_persistor = FilePersistor(CACHE_ROOT/_MODULE/'pricing') if use_file else SqlitePersistor(DB_PATH, f"{_MODULE}_pricing")

    def _get_identifier(self, security: Security) -> str:
        return f"{security.symbol}:{_get_exchange(security)}"
    
    def _get_info_key_fn(self, security: Security) -> str:
        return f"{security.exchange.mic}_{security.symbol}"
    def _get_info_persistor_fn(self, security: Security) -> Persistor:
        return self.info_persistor
    @cached_scalar(
        key_fn=_get_info_key_fn,
        persistor_fn=_get_info_persistor_fn
    )
    def _get_info(self, security: Security) -> dict:
        url = f"https://markets.ft.com/data/searchapi/searchsecurities"
        id = self._get_identifier(security)
        resp = httputils.get_as_browser(url, params={'query': id})
        data = json.loads(resp.text)
        data = [it for it in data['data']['security'] if 'symbol' in it and it['symbol'] == id]
        if not data: return {}
        return data[0]

    @httputils.backup_timeout()
    def _fetch_pricing(self, xid: str, days: int, data_period: str, data_interval: int, realtime: bool):
        """
        FT timestamps represent the start of the relevant interval BUT
        they are often a few milliseconds off!
        """
        url = "https://markets.ft.com/data/chartapi/series"
        request = {
            "days": days,
            "dataNormalized": False,
            "dataPeriod": data_period,
            "dataInterval": data_interval,
            "realtime": realtime,
            "yFormat": "0.###",
            "timeServiceFormat": "JSON",
            "returnDateType": "Unix",
            "elements": [
                {
                    "Label": "266a0ba8",
                    "Type": "price",
                    "Symbol": xid,
                    "OverlayIndicators": [],
                    "Params": {}
                }
                ,
                {
                    "Label": "b2b89a77",
                    "Type": "volume",
                    "Symbol": xid,
                    "OverlayIndicators": [],
                    "Params": {}
                }
            ]
        }
        resp = httputils.post_as_browser(url, request)
        return json.loads(resp.text)
    
    def _fix_timestamps(self, dates: list[float], interval: Interval, security: Security) -> list[float]:
        timestamps = [it//10*10 if it else None for it in dates]
        result = []
        def skip(it):
            logger.warning(f"Unexpected {interval} timestamp {security.exchange.calendar.unix_to_datetime(it)}. Skipping.")
            result.append(None)
        for it in timestamps:
            if not it: result.append(None)
            elif interval <= Interval.D1 and not security.exchange.calendar.is_workday(it): skip(it)
            elif interval >= Interval.D1:
                if it != security.exchange.calendar.to_zero(it): skip(it)
                else: result.append(security.exchange.calendar.get_next_timestamp(it+1, interval))
            else:
                if not security.exchange.calendar.is_timestamp(it, interval): skip(it)
                else: result.append(it)
        return result

    @override
    def get_interval_start(self, interval):
        return time.time() - 15*24*3600
    @override
    def get_pricing_persistor(self, security: Security, interval: Interval) -> Persistor:
        return self.pricing_persistor
    @override
    def get_pricing_delay(self, security: Security, interval: Interval) -> float:
        return 17*60
    @override
    def get_pricing_raw(self, security, unix_from, unix_to, interval, **kwargs) -> list[dict]:
        days = math.ceil((time.time() - unix_from)/(24*3600)) + 1
        days = max(min(days, 15), 4)
        info = self._get_info(security)
        if 'xid' not in info or not info['xid']: raise Exception(f"No xid for '{security.symbol}'.")
        data = self._fetch_pricing(info['xid'], days, *_get_interval(interval), realtime=True)
        timestamps = self._fix_timestamps(data['Dates'], interval, security)
        elements = data['Elements']
        if len(elements) < 2:
            raise Exception(f"Expected 2 objects in the Elements array (prices and volumes) but got less. Data:\n{data}")
        prices = [it for it in elements if it['Type'] == 'price'][0]
        volumes = [it for it in elements if it['Type'] == 'volume'][0]
        def extract_component_series_values(element):
            return {it['Type']:it['Values'] for it in element['ComponentSeries']}
        data = {**extract_component_series_values(prices), **extract_component_series_values(volumes)}
        data['t'] = timestamps
        return filter_by_timestamp(combine_series(data), unix_from, unix_to)