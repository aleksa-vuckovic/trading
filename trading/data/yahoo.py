import json
import yfinance
import logging
import time
import math
from enum import Enum
from pathlib import Path
from ..utils import httputils
from ..utils.common import Interval, binary_search, BinarySearchEdge
from . import nasdaq
from .utils import combine_series, fix_long_timestamps, separate_quotes
from .caching import cached_scalar, cached_series, CACHE_ROOT, FilePersistor
from .abstract import PricingProvider, AbstractSecurity, WorkCalendar


logger = logging.getLogger(__name__)
_MODULE: str = __name__.split(".")[-1]
_MIN_AFTER_FIRST_TRADE = 14*24*3600 # The minimum time after the first trade time to query for prices
_ADJUSTMENT_PERIOD = 10*24*3600

class Yahoo(PricingProvider):
    """
    Hourly data from yahoo covers 1 hour periods starting from 9:30.
    The last nonprepost period is at 15:30 and covers only the last 30 minutes.
    The timestamps correspond to the START of the period.
    """
    def _get_pricing_raw(
        self,
        ticker: str,
        start_time: float, #unix
        end_time: float, #unix
        interval: str,
        events: list[str] = ['div', 'split', 'earn'],
        include_pre_post = False
    ) -> dict:
        result =  f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker.upper()}"
        result += f"?period1={int(start_time)}&period2={math.ceil(end_time)}&interval={interval}"
        result += f"&incldePrePost={str(include_pre_post).lower()}&events={"|".join(events)}"
        result += f"&&lang=en-US&region=US"
        resp = httputils.get_as_browser(result)
        return json.loads(resp.text)

    def _get_interval(self, interval: Interval) -> str:
        if interval == Interval.L1: return '1mo'
        if interval == Interval.W1: return '5d'
        if interval == Interval.H1: return '1h'
        if interval == Interval.D1: return '1d'
        if interval == Interval.M15: return '15m'
        if interval == Interval.M5: return '5m'
        raise Exception(f"Unknown interval {interval}.")

    def _fix_timestamps(self, timestamps: list[float], interval: Interval, calendar: WorkCalendar):
        if interval >= Interval.D1: return fix_long_timestamps(timestamps, interval)
        size = interval.time() if interval != Interval.H1 else 1800
        result = []
        for it in timestamps:
            if not it or not calendar.is_workday(it):
                result.append(None)
                continue
            if interval == Interval.H1:
                if it + 1800 == calendar.set_close(it): it += 1800
                else: it += 3600
            else: it += size
            if not calendar.is_timestamp(it, interval):
                logger.warning(f"Unexpected timestamp {calendar.unix_to_datetime(it)}. Skipping entry.")
                result.append(None)
            else: result.append(it)
        return result

    def _get_pricing_key_fn(self, security: AbstractSecurity, interval: Interval) -> list[str]:
        return [security.symbol, interval.name]
    def _get_pricing_timestamp_fn(self, security: AbstractSecurity, interval: Interval) -> float:
        return 5000000 if interval < Interval.H1 else 10000000 if interval == Interval.H1 else 50000000
    def _get_pricing_should_refresh_fn(self, security: AbstractSecurity, interval: Interval, fetch: float, now: float) -> bool:
        return security.exchange.calendar.get_next_timestamp(fetch, interval) < now,
    @cached_series(
        unix_args=(2,3),
        series_field="data",
        timestamp_field="t",
        key_fn=_get_pricing_key_fn,
        persistor_fn=FilePersistor(CACHE_ROOT/_MODULE/"pricing"),
        time_step_fn= _get_pricing_timestamp_fn,
        live_delay_fn=5*60,
        should_refresh_fn=_get_pricing_should_refresh_fn
    )
    @httputils.backup_timeout()
    def _get_pricing(self, security: AbstractSecurity, unix_from: float, unix_to: float, interval: Interval) -> dict:
        first_trade_time = self.get_first_trade_time(security)
        now = time.time()
        query_from = max(unix_from - interval.time(), first_trade_time + _MIN_AFTER_FIRST_TRADE)
        query_from = max(query_from, interval.start_unix())
        query_to = unix_to
        if query_to <= query_from:
            return {"meta": {}, "events": {}, "data": []}
        try:
            data = self._get_pricing_raw(security, query_from, query_to, self._get_interval(interval))
        except httputils.BadResponseException:
            logger.error(f"Bad response for {security.symbol} from {unix_from} to {unix_to} at {interval}. PERMANENT EMPTY RETURN!", exc_info=True)
            return { "meta": {}, "events": {}, "data": [] }
        def get_meta(data):
            return data['chart']['result'][0]['meta']
        def get_events(data):
            data = data['chart']['result'][0]
            if 'events' in data:
                return data['events']
            return {}
        def get_series(data: dict, unix_from: float, unix_to: float, interval: Interval) -> list[dict]:
            data = data['chart']['result'][0]
            if 'timestamp' not in data or not data['timestamp']: return []
            arrays = data['indicators']['quote'][0]
            arrays['timestamp'] = self._fix_timestamps(data['timestamp'], interval, security.exchange.calendar)
            try:
                arrays['adjclose'] = data['indicators']['adjclose'][0]['adjclose']
            except:
                pass
            return combine_series(arrays, timestamp_from=unix_from, timestamp_to=unix_to)
        def try_adjust(series: list[dict]) -> list[dict]:
            try:
                d1data = self._get_pricing(security, unix_to - _ADJUSTMENT_PERIOD, unix_to, Interval.D1)['data']
                close = d1data[-2]['c']
                time = d1data[-2]['t']
                i = binary_search(series, time, lambda x: x['t'], edge=BinarySearchEdge.NONE)
                if i is not None:
                    factor = close / series[i]['c']
                    return [{key:it[key]*(1 if key=='t' else 1/factor if key=='v' else factor) for key in it}  for it in series ]
                raise Exception(f"No suitable timestamp found.")
            except:
                logger.error(f"Failed to adjust {security.symbol}.", exc_info=True)
                return series
        series = get_series(data, unix_from, unix_to, interval)
        meta = get_meta(data)
        events = get_events(data)
        if interval <= Interval.H1 and unix_to < now - 15*24*3600:
            series = try_adjust(series)
        """
        Rearrange data to be comaptible with series caching by moving everything to one array.
        open - o, close - c, low - l, high - h, adjclose - a, volume - v, timestamp - t
        """
        return {
            "meta": meta,
            "events": events,
            "data": series
        }
    def get_pricing(self, security, unix_from, unix_to, interval, *, return_quotes = ..., **kwargs):
        separate_quotes(self._get_pricing(security, unix_from, unix_to, interval, **kwargs)['data'], return_quotes)

    def _get_info_key_fn(self, security: AbstractSecurity) -> list[str]:
        return [security.symbol]
    @cached_scalar(
        key_fn=_get_info_key_fn,
        persistor_fn=FilePersistor(CACHE_ROOT/_MODULE/'info')
    )
    def _get_info(self, security: AbstractSecurity) -> dict:
        try:
            info = yfinance.Ticker(security.symbol).info
        except json.JSONDecodeError:
            raise httputils.TooManyRequestsException()
        mock_time = int(time.time() - 15*24*3600)
        try:
            meta = self._get_pricing_raw(security, mock_time-3*24*3600, mock_time, Interval.D1)['chart']['result'][0]['meta']
        except httputils.BadResponseException:
            logger.error(f"Bad response for {security.symbol} in _get_info. PERMANENT EMPTY RETURN!", exc_info=True)
            meta = {}
        return {**info, **meta}
    def get_info(self, security: AbstractSecurity) -> dict:
        return self._get_info(security.symbol)
    
    def get_shares(self, ticker: nasdaq.NasdaqSecurity) -> int:
        key = 'impliedSharesOutstanding'
        info = self.get_info(ticker)
        if key in info and info[key]:
            return float(info[key])
        key = 'sharesOutstanding'
        return float(info[key])
    def get_summary(self, ticker: nasdaq.NasdaqSecurity) -> str:
        key = 'longBusinessSummary'
        return str(self.get_info(ticker)[key])
    def get_first_trade_time(self, ticker: nasdaq.NasdaqSecurity) -> float:
        key = 'firstTradeDateEpochUtc'
        info = self.get_info(ticker)
        if key in info and info[key]:
            return float(info[key])
        key = 'firstTradeDate'
        return float(info[key])
    def get_market_cap(self, ticker: nasdaq.NasdaqSecurity) -> float:
        key = 'marketCap'
        info = self.get_info(ticker)
        return float(info[key])