import json
import yfinance # type: ignore
import logging
import time
import math
from typing import Literal, Mapping, override
import config
from base.algos import BinarySearchEdge, binary_search
from base.caching import NullPersistor, cached_scalar, Persistor, FilePersistor, SqlitePersistor
from trading.core import Interval
from trading.core.securities import Security, DataProvider
from trading.core.pricing import OHLCV, BasePricingProvider
from base.scraping import scraper, backup_timeout, BadResponseException, TooManyRequestsException
from trading.providers.utils import arrays_to_ohlcv, filter_ohlcv



logger = logging.getLogger(__name__)
_MODULE: str = __name__.split(".")[-1]
_MIN_AFTER_FIRST_TRADE = 14*24*3600 # The minimum time after the first trade time to query for prices
_ADJUSTMENT_PERIOD = 10*24*3600

class Yahoo(BasePricingProvider, DataProvider):
    """
    Hourly data from yahoo covers 1 hour periods starting from 9:30.
    The last nonprepost period is at 15:30 and covers only the last 30 minutes.
    The timestamps correspond to the START of the period.
    """
    def __init__(self, storage: Literal['file','db','none']='db', merge: Mapping[Interval, Interval] = BasePricingProvider.DEFAULT_MERGE):
        if merge != BasePricingProvider.DEFAULT_MERGE and storage != 'none':
            raise Exception(f"Only change merge config when using 'none' storage.")
        BasePricingProvider.__init__(
            self,
            native = [Interval.L1, Interval.W1, Interval.D1, Interval.M30, Interval.M15, Interval.M5, Interval.M1],
            merge = merge
        )
        DataProvider.__init__(self)
        self.info_persistor = FilePersistor(config.caching.file_path/_MODULE/"info") if storage == 'file'\
            else SqlitePersistor(config.caching.db_path, f"{_MODULE}_info") if storage == 'db'\
            else NullPersistor()
        self.pricing_persistor = FilePersistor(config.caching.file_path/_MODULE/"pricing") if storage == 'file'\
            else SqlitePersistor(config.caching.db_path, f"{_MODULE}_pricing") if storage == 'db'\
            else NullPersistor()

    @backup_timeout()
    def _fetch_pricing(
        self,
        start_time: float, #unix
        end_time: float, #unix
        symbol: str,
        interval: str,
        events: list[str] = [],
        include_pre_post = False
    ) -> dict:
        result =  f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        result += f"?period1={int(start_time)}&period2={math.ceil(end_time)}&interval={interval}"
        result += f"&incldePrePost={str(include_pre_post).lower()}&events={"|".join(events)}"
        result += f"&&lang=en-US&region=US"
        resp = scraper.get(result)
        return json.loads(resp.text)

    def _get_interval(self, interval: Interval) -> str:
        if interval == Interval.L1: return '1mo'
        if interval == Interval.W1: return '1wk'
        if interval == Interval.H1: return '1h'
        if interval == Interval.D1: return '1d'
        if interval == Interval.M30: return '30m'
        if interval == Interval.M15: return '15m'
        if interval == Interval.M5: return '5m'
        if interval == Interval.M1: return '1m'
        raise Exception(f"Unknown interval {interval}.")

    def _fix_timestamps(self, timestamps: list[float], interval: Interval, security: Security) -> list[float | None]:
        if interval == Interval.H1: raise Exception(f"The h1 interval is unaligned in yahoo.")
        result: list[float|None] = []
        def skip(it: float):
            logger.warning(f"Unexpected {interval} timestamp {security.exchange.calendar.unix_to_datetime(it)}. Skipping.")
            result.append(None)
        for it in timestamps:
            if not it: result.append(None)
            elif interval <= Interval.D1 and not security.exchange.calendar.is_workday(it): skip(it)
            elif interval > Interval.D1:
                if it != security.exchange.calendar.to_zero(it): skip(it)
                else: result.append(security.exchange.calendar.get_next_timestamp(it + 1, interval))
            elif interval == Interval.D1:
                if it != security.exchange.calendar.set_open(it): skip(it)
                else: result.append(security.exchange.calendar.get_next_timestamp(it - 1, interval))
            else:
                timestamp = security.exchange.calendar.get_next_timestamp(it, interval)
                if interval.time() != timestamp-it: skip(it)
                else: result.append(timestamp)
        return result

    @override
    def get_interval_start(self, interval):
        now = time.time()
        if interval >= Interval.D1: return now - 10*365*24*3600
        if interval == Interval.H1: return now - 729*24*3600
        if interval in {Interval.M30, Interval.M15, Interval.M5}: return now - 60*24*3600
        if interval == Interval.M1: return now - 30*24*3600
        raise Exception(f"Unsupported interval {self}.")
    @override
    def get_pricing_persistor(self, security, interval):
        return self.pricing_persistor
    @override
    def get_pricing_delay(self, security, interval):
        return 60
    @override
    def get_pricing_raw(self, unix_from: float, unix_to: float, security: Security, interval: Interval) -> list[OHLCV]:
        first_trade_time = self.get_first_trade_time(security)
        now = time.time()
        query_from = max(unix_from - interval.time(), first_trade_time + _MIN_AFTER_FIRST_TRADE)
        query_from = max(query_from, self.get_interval_start(interval))
        query_to = unix_to
        if query_to <= query_from: return []
        try:
            data = self._fetch_pricing(query_from, query_to, security.symbol, self._get_interval(interval))
        except BadResponseException:
            logger.error(f"Bad response for {security.symbol} from {unix_from} to {unix_to} at {interval}. PERMANENT EMPTY RETURN!", exc_info=True)
            return []
        def get_series(data: dict, unix_from: float, unix_to: float, interval: Interval) -> list[OHLCV]:
            data = data['chart']['result'][0]
            if 'timestamp' not in data or not data['timestamp']: return []
            arrays: dict = data['indicators']['quote'][0]
            arrays['timestamp'] = self._fix_timestamps(data['timestamp'], interval, security)
            try:
                arrays['close'] = data['indicators']['adjclose'][0]['adjclose']
            except:
                pass
            return filter_ohlcv(arrays_to_ohlcv(arrays), unix_from, unix_to)
        def try_adjust(series: list[OHLCV]) -> list[OHLCV]:
            try:
                d1data = self.get_pricing(unix_to - _ADJUSTMENT_PERIOD, unix_to, security, Interval.D1)
                close = d1data[-1]['c']
                time = security.exchange.calendar.set_close(d1data[-1]['t'])
                i = binary_search(series, time, lambda x: x.t, edge=BinarySearchEdge.NONE)
                if i is not None:
                    factor = close / series[i]['c']
                    return [it.adjust(factor)  for it in series ]
                raise Exception(f"No suitable timestamp found.")
            except:
                logger.error(f"Failed to adjust {security.symbol}.", exc_info=True)
                return series
        series = get_series(data, unix_from, unix_to, interval)
        if interval <= Interval.H1 and unix_to < now - 15*24*3600:
            return try_adjust(series)
        return series

    def _get_info_key_fn(self, security: Security) -> str:
        return security.symbol
    def _get_info_persistor_fn(self, security: Security) -> Persistor:
        return self.info_persistor
    @cached_scalar(
        key_fn=_get_info_key_fn,
        persistor_fn=_get_info_persistor_fn
    )
    def _get_info(self, security: Security) -> dict:
        try:
            info = yfinance.Ticker(security.symbol).info
        except json.JSONDecodeError:
            raise TooManyRequestsException()
        mock_time = int(time.time() - 15*24*3600)
        try:
            meta = self._fetch_pricing(mock_time-3*24*3600, mock_time, security.symbol, self._get_interval(Interval.D1))['chart']['result'][0]['meta']
        except BadResponseException:
            logger.error(f"Bad response for {security.symbol} in _get_info. PERMANENT EMPTY RETURN!", exc_info=True)
            meta = {}
        return {**info, **meta}
    def get_info(self, security: Security) -> dict:
        return self._get_info(security)
    
    @override
    def get_outstanding_parts(self, security: Security) -> float:
        key = 'impliedSharesOutstanding'
        info = self.get_info(security)
        if key in info and info[key]:
            return float(info[key])
        key = 'sharesOutstanding'
        return float(info[key])
    @override
    def get_summary(self, security: Security) -> str:
        key = 'longBusinessSummary'
        return str(self.get_info(security)[key])
    @override
    def get_first_trade_time(self, security: Security) -> float:
        key = 'firstTradeDateEpochUtc'
        info = self.get_info(security)
        if key in info and info[key]:
            return float(info[key])
        key = 'firstTradeDate'
        return float(info[key])
    @override
    def get_market_cap(self, security: Security) -> float:
        key = 'marketCap'
        info = self.get_info(security)
        return float(info[key])
    