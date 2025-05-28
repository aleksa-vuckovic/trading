#2
import json
import logging
from typing import Literal, override
from datetime import timedelta

from requests import TooManyRedirects
from base.caching import KeySeriesStorage
from base import dates
from base.scraping import TooManyRequestsException, scraper, backup_timeout
from trading.core.interval import Interval
from trading.core.securities import Security, SecurityType
from trading.core.pricing import OHLCV, BasePricingProvider
from trading.providers.forex import ForexSecurity
from trading.providers.nyse import NYSESecurity
from trading.providers.nasdaq import NasdaqSecurity
from trading.providers.utils import arrays_to_ohlcv, filter_ohlcv

logger = logging.getLogger(__name__)
_TOKEN_KEY='Dylan2010.Entitlementtoken'
_TOKEN_VALUE='57494d5ed7ad44af85bc59a51dd87c90'
_CKEY='57494d5ed7'
_MODULE: str = __name__.split(".")[-1]

_security_types = {
    SecurityType.STOCK: 'STOCK',
    SecurityType.ETF: 'FUND',
    SecurityType.FX: 'CURRENCY'
}
def _get_symbol(security: Security) -> str:
    typestr = _security_types[security.type]
    if isinstance(security, NasdaqSecurity):
        return f"{typestr}/US/{security.exchange.operating_mic}/{security.symbol}"
    if isinstance(security, NYSESecurity):
        return f"{typestr}/US/{security.exchange.segment_mic}/{security.symbol}"
    if isinstance(security, ForexSecurity):
        return f"{typestr}/US/XTUP/{security.base}{security.quote}"
    raise Exception(f"Unsupported security {security}.")

class WallStreetJournal(BasePricingProvider):
    def __init__(self):
        super().__init__(
            native = [Interval.D1, Interval.M30, Interval.M15, Interval.M5, Interval.M1]
        )

    @backup_timeout()
    def _fetch_pricing(self, key: str, step: str, time_frame: Literal['D5', 'D10'], **kwargs):
        """
        WSJ timestamps represent the start of the relevant interval.
        1 hour data is provided at full hours (10:00, 11:00...).
        """
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
        resp = scraper.get(url, params={'json': json.dumps(request), 'ckey': _CKEY}, headers={_TOKEN_KEY: _TOKEN_VALUE})
        return json.loads(resp.text)
    
    def _get_interval(self, interval: Interval) -> str:
        if interval == Interval.D1: return 'P1D'
        elif interval == Interval.M30: return 'PT30M'
        elif interval == Interval.M15: return 'PT15M'
        elif interval == Interval.M5: return 'PT5M'
        elif interval == Interval.M1: return 'PT1M'
        else: raise ValueError(f"Unsupported interval {interval}")

    def _fix_timestamps(self, timestamps: list[float|int|None], interval: Interval, security: Security) -> list[float|None]:
        timestamps = [it//1000 if it else None for it in timestamps]
        result: list[float|None] = []
        for it in timestamps:
            if not it:
                result.append(None)
            elif interval == Interval.D1:
                date = dates.unix_to_datetime(it, tz=dates.UTC)
                if date != dates.to_zero(date):
                    logger.warning(f"Unexpected UTC timestamp {date}. Skipping.")
                    result.append(None)
                else:
                    date = date.replace(tzinfo=security.exchange.calendar.tz) + timedelta(hours=1)
                    date = security.exchange.calendar.get_next_timestamp(date, interval)
                    result.append(date.timestamp())
            elif interval in {Interval.M30, Interval.M15, Interval.M5, Interval.M1}:
                it += interval.time()
                if not security.exchange.calendar.is_timestamp(it, interval):
                    logger.warning(f"Unexpected {interval} timestamp {security.exchange.calendar.unix_to_datetime(it)}. Skipping.")
                    result.append(None)
                else:
                    result.append(it)
            else:
                raise Exception(f"Unsupported interval {interval}.")

        return result

    @override
    def get_interval_start(self, interval) -> float:
        if interval == Interval.D1: return dates.unix() - 365*24*3600
        if interval in {Interval.M30, Interval.M15, Interval.M5, Interval.M1}: return dates.unix() - 5*25*3600
        raise Exception(f"Unsupported interval {interval}.")
    @override
    def get_pricing_delay(self, security, interval) -> float:
        return 120
    @override
    def get_pricing_raw(self, unix_from, unix_to, security, interval) -> list[OHLCV]:
        time_frame = 'D5'
        try:
            data = self._fetch_pricing(_get_symbol(security), self._get_interval(interval), time_frame)
        except TooManyRequestsException:
            raise
        except:
            logger.warning(f"Failed to fetch pricing for {security.symbol} from {dates.unix_to_str(unix_from)} to {dates.unix_to_str(unix_to)}. Returning [].")
            return []
        def extract_data_points(series: dict) -> dict:
            return {key: [it[index] for it in series['DataPoints']] for index,key in enumerate(series['DesiredDataPoints'])}
        quotes = {'Timestamp': self._fix_timestamps(data['TimeInfo']['Ticks'], interval, security)}
        for series in data['Series']:
            quotes = {**quotes, **extract_data_points(series)}
        quotes['Close'] = quotes['Last']
        del quotes['Last']
        return filter_ohlcv(arrays_to_ohlcv(quotes), unix_from, unix_to)
