import json
import logging
from datetime import timedelta
from ..utils import httputils
from ..utils.common import Interval
from ..utils.dateutils import XNAS
from .utils import combine_series, fix_long_timestamps, filter_by_timestamp, separate_quotes
from .caching import cached_scalar, cached_series, CACHE_ROOT, FilePersistor
from .abstract import PricingProvider, AbstractSecurity

logger = logging.getLogger(__name__)
_TOKEN_KEY='Dylan2010.Entitlementtoken'
_TOKEN_VALUE='57494d5ed7ad44af85bc59a51dd87c90'
_CKEY='57494d5ed7'
_MODULE: str = __name__.split(".")[-1]
_CACHE = CACHE_ROOT / _MODULE

class WallStreetJournal(PricingProvider):
    """
    WSJ timestamps represent the start of the relevant interval.
    1 hour data is provided at full hours (10:00, 11:00...).
    """
    def _get_pricing_raw(self, key: str, step: str, time_frame: str):
        url = "https://api.wsj.net/api/michelangelo/timeseries/history"
        request = {
            "Step": step,
            "TimeFrame": time_frame,
            "IncludeMockTick":True,
            "FilterNullSlots":False,
            "FilterClosedPoints":True,
            "IncludeClosedSlots":False,
            "IncludeOfficialClose":True,
            "InjectOpen":False,
            "ShowPreMarket":False,
            "ShowAfterHours":False,
            "UseExtendedTimeFrame":True,
            "WantPriorClose":False,
            "IncludeCurrentQuotes":False,
            "ResetTodaysAfterHoursPercentChange":False,
            "Series": [
                {
                    "Key":key,
                    "Dialect":"Charting",
                    "Kind":"Ticker",
                    "SeriesId":"s1",
                    "DataTypes": ["Open","High","Low","Last"],
                    "Indicators":[
                        {"Parameters":[],"Kind":"Volume","SeriesId":"i2"}
                    ]
                }
            ]
        }
        resp = httputils.get_as_browser(url, params={'json': json.dumps(request), 'ckey': _CKEY}, headers={_TOKEN_KEY: _TOKEN_VALUE})
        return json.loads(resp.text)
    
    def _get_interval(self, interval: Interval) -> str:
        if interval == Interval.D1: return 'P1D'
        elif interval == Interval.H1: return 'PT30M'
        elif interval == Interval.M15: return 'PT15M'
        elif interval == Interval.M5: return 'PT5M'
        else: raise ValueError(f"Unknown interval {interval}")

    def _fix_timestamps(self, timestamps: list[float|int|None], interval: Interval) -> list[float|None]:
        timestamps = [it//1000 if it else None for it in timestamps]
        if interval >= Interval.D1: return fix_long_timestamps(timestamps, interval)
        else:
            size = interval.time() if interval != Interval.H1 else 1800
            result = []
            for it in timestamps:
                if not it:
                    result.append(None)
                    continue
                it+=size
                if not XNAS.is_timestamp(it, interval)\
                    and not(\
                        interval == Interval.H1 and XNAS.is_timestamp(it+1800, interval)
                    ):
                    logger.warning(f"Unexpected timestamp {XNAS.unix_to_datetime(it)}. Skipping entry.")
                    result.append(None)
                else:
                    result.append(it)
            return result
        
    def _merge_data_1h(self, data):
        result = []
        dates = [XNAS.unix_to_datetime(it['t']) for it in data]
        half = timedelta(minutes=30)
        i = 0
        while i < len(data):
            if XNAS.is_timestamp(dates[i]+half, Interval.H1):
                if i+1<len(dates) and dates[i]+half == dates[i+1]:
                    #Merge with next
                    data[i]['h'] = max(data[i]['h'],data[i+1]['h'])
                    data[i]['l'] = min(data[i]['l'],data[i+1]['l'])
                    data[i]['c'] = data[i+1]['c']
                    data[i]['v'] += data[i+1]['v']
                    data[i]['t'] = data[i+1]['t']
                    result.append(data[i])
                    i += 1
                else:
                    logger.warning(f"Unpaired data point at {dates[i]}.")
                    data[i]['t'] += 1800
                    data[i]['v'] *= 2
                    result.append(data[i])
            elif dates[i] == XNAS.set_close(dates[i]):
                result.append(data[i])
            elif XNAS.is_timestamp(dates[i], Interval.H1):
                logger.warning(f"Unpaired data point at {dates[i]}.")
                data[i]['v'] *= 2
                result.append(data[i])
            else:
                logger.warning(f"Unexpected non full-hour entry at time {dates[i]}. Skipping.")
            i += 1
        return result 

    def _get_pricing_key_fn(self, security: AbstractSecurity, interval: Interval) -> list[str]:
        return [security.symbol, interval.name]
    def _get_pricing_should_refresh_fn(self, security: AbstractSecurity, interval: Interval, fetch: float, now: float) -> bool:
        return security.exchange.calendar.get_next_timestamp(fetch, interval) < now,
    @cached_series(
        unix_args=(2,3),
        series_field='data',
        timestamp_field='t',
        key_fn=_get_pricing_key_fn,
        persistor_fn=FilePersistor(CACHE_ROOT/_MODULE/"pricing"),
        live_delay_fn=5*60,
        should_refresh_fn=_get_pricing_should_refresh_fn,
        time_step_fn=10000000
    )
    @httputils.backup_timeout()
    def _get_pricing(self, security: AbstractSecurity, unix_from: float, unix_to: float, interval: Interval) -> dict:
        if interval > Interval.D1: raise Exception(f"Interval {interval} not supported for wsj.")
        data = self._get_pricing_raw(f"STOCK/US/XNAS/{security.symbol}", self._get_interval(interval), 'D5')
        def extract_data_points(series: dict) -> dict:
            return {key: [it[index] for it in series['DataPoints']] for index,key in enumerate(series['DesiredDataPoints'])}
        quotes = {'Timestamp': self._fix_timestamps(data['TimeInfo']['Ticks'], interval)}
        for series in data['Series']:
            quotes = {**quotes, **extract_data_points(series)}
        quotes['Close'] = quotes['Last']
        del quotes['Last']
        quotes = combine_series(quotes)
        if interval == Interval.H1: quotes = self._merge_data_1h(quotes)
        result = data['Series'][0]
        del result['DataPoints']
        del result['DesiredDataPoints']
        result['data'] = filter_by_timestamp(quotes, unix_from=unix_from, unix_to=unix_to)
        return result
    def get_pricing(self, security, unix_from, unix_to, interval, *, return_quotes = ..., **kwargs):
        return separate_quotes(self._get_pricing(security, unix_from, unix_to, interval, **kwargs)['data'], return_quotes)