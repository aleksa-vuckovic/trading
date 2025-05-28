#2
import json
import logging
import math
from typing import Sequence, TypedDict, override
from base import dates
from base.scraping import TooManyRequestsException, scraper, backup_timeout
from trading.core import Interval
from trading.providers.forex import ForexSecurity
from trading.providers.nyse import NYSE, NYSEAmerican, NYSEArca, NYSESecurity
from trading.providers.utils import arrays_to_ohlcv, filter_ohlcv
from base.caching import KeyValueStorage, cached_scalar
from base.key_value_storage import SqlKVStorage
from trading.core.securities import Security
from trading.core.pricing import OHLCV, BasePricingProvider, MongoKVStorage
from trading.providers.nasdaq import NasdaqSecurity, NasdaqGS, NasdaqMS, NasdaqCM
import injection

logger = logging.getLogger(__name__)
_MODULE: str = __name__.split(".")[-1]

def _get_identifiers(security: Security) -> list[str]:
    if isinstance(security, (NasdaqSecurity, NYSESecurity)):
        codes = ['NSQ'] if security.exchange == NasdaqGS.instance\
        else ['NMQ'] if security.exchange == NasdaqMS.instance\
        else ['NAQ'] if security.exchange == NasdaqCM.instance\
        else ['NYQ'] if security.exchange == NYSE.instance\
        else ['ASE', 'ASQ'] if security.exchange == NYSEAmerican.instance\
        else ['PCQ:USD'] if security.exchange == NYSEArca.instance\
        else None
        if not codes: raise Exception(f"Unknown exchange {security.exchange}")
        return [f"{security.symbol}:{it}" for it in codes]
    if isinstance(security, ForexSecurity):
        return [f"{security.base}{security.quote}"]
    raise Exception(f"Unsupported security {security}")
def _get_interval(interval: Interval) -> tuple[str, int]:
    if interval > Interval.D1: raise Exception(f"Unsupported interval {interval}.")
    if interval == Interval.D1: return 'Day', 1
    if interval == Interval.H1: return 'Hour', 1
    if interval == Interval.M30: return 'Minute', 30
    if interval == Interval.M15: return 'Minute', 15
    if interval == Interval.M5: return 'Minute', 5
    if interval == Interval.M1: return 'Minute', 1
    raise Exception(f"Unknown interval {interval}")

class FinancialTimes(BasePricingProvider):
    def __init__(self):
        super().__init__(
            native = [Interval.D1, Interval.M30, Interval.M15, Interval.M5, Interval.M1]
        )
        name = FinancialTimes.__name__.lower()
        self.local_info_storage = SqlKVStorage(injection.local_db, f"{name}_info")
        self.remote_info_storage = MongoKVStorage(injection.mongo_db[f"{name}_info"])
    
    #region info
    class _InfoDict(TypedDict):
        url: str
        urlChart: str
        name: str
        symbol: str
        xid: str
        assetClass: str
    @backup_timeout()
    def _fetch_info(self, security: Security) -> _InfoDict|None:
        url = f"https://markets.ft.com/data/searchapi/searchsecurities"
        ids = _get_identifiers(security)
        for id in ids:
            resp = scraper.get(url, params={'query': id})
            data = json.loads(resp.text)
            data = [it for it in data['data']['security'] if 'symbol' in it and it['symbol'] in ids]
            if data: return data[0]
        raise Exception(f"Can't find info for {security.symbol}.")

    def _get_info_key_fn(self, security: Security) -> str:
        return f"{security.exchange.mic}_{security.symbol}"
    def _get_info_storage_fn(self, security: Security) -> KeyValueStorage:
        return self.local_info_storage
    @cached_scalar(
        key=_get_info_key_fn,
        storage=_get_info_storage_fn
    )
    def _get_info(self, security: Security) -> _InfoDict|None:
        try:
            return self._fetch_info(security)
        except TooManyRequestsException:
            raise
        except:
            logger.warning(f"Setting info to None for {security.symbol}.", exc_info=True)
            return None
    #endregion

    #region pricing
    @backup_timeout()
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
        resp = scraper.post(url, request)
        return json.loads(resp.text)
    
    def _fix_timestamps(self, timestamps: Sequence[float|None], interval: Interval, security: Security) -> list[float|None]:
        calendar = security.exchange.calendar
        timestamps = [round(it/10)*10.0 if it else None for it in timestamps]
        result: list[float|None] = []
        def skip(it):
            logger.warning(f"Unexpected {interval} timestamp {calendar.unix_to_datetime(it)}. Skipping.")
            result.append(None)
        for it in timestamps:
            if not it: result.append(None)
            elif interval == Interval.D1: # Should be UTC 00:00
                utc = dates.unix_to_datetime(it, tz=dates.UTC)
                if utc != dates.to_zero(utc): skip(it)
                else:
                    time = utc.replace(tzinfo=calendar.tz).timestamp()+1
                    if calendar.is_off(time): skip(it)
                    else: result.append(calendar.get_next_timestamp(time, Interval.D1))
            else:
                if not security.exchange.calendar.is_timestamp(it, interval): skip(it)
                else: result.append(it)
        return result

    @override
    def get_interval_start(self, interval: Interval):
        return dates.unix() - 15*24*3600
    @override
    def get_pricing_delay(self, security: Security, interval: Interval) -> float:
        return 16*60
    @override
    def get_pricing_raw(self, unix_from: float, unix_to: float, security: Security, interval: Interval) -> list[OHLCV]:
        if isinstance(security, ForexSecurity): raise Exception(f"FinancialTimes returns sparse data for forex securities.")
        days = math.ceil((dates.unix() - unix_from)/(24*3600)) + 1
        days = max(min(days, 15), 4)
        info = self._get_info(security)
        if not info or 'xid' not in info or not info['xid']: raise Exception(f"No xid for '{security.symbol}'.")
        try:
            data = self._fetch_pricing(info['xid'], days, *_get_interval(interval), realtime=True)
        except TooManyRequestsException:
            raise
        except:
            logger.warning(f"Failed to fetch prices for {security.symbol} fro {dates.unix_to_str(unix_from)} to {dates.unix_to_str(unix_to)}. Returning [].", exc_info=True)
            return []
        timestamps = self._fix_timestamps(data['Dates'], interval, security)
        elements = data['Elements']
        if len(elements) < 2:
            raise Exception(f"Expected 2 objects in the Elements array (prices and volumes) but got less. Data:\n{data}")
        prices = [it for it in elements if it['Type'] == 'price'][0]
        volumes = [it for it in elements if it['Type'] == 'volume'][0]
        if 'Error' in volumes and volumes['Error']['Code'] == '0':
            volumes['ComponentSeries'] = [{'Type': 'Volume', 'Values': [None for _ in timestamps]}]
        def extract_component_series_values(element):
            return {it['Type']: it['Values'] for it in element['ComponentSeries']}
        data = {**extract_component_series_values(prices), **extract_component_series_values(volumes)}
        data['t'] = timestamps
        return filter_ohlcv(arrays_to_ohlcv(data), unix_from, unix_to)
    #endregion