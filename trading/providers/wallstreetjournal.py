import json
import logging
import time
from typing import Literal, override
from datetime import timedelta
import config
from base import dates
from trading.utils import httputils
from trading.core.interval import Interval
from trading.providers.utils import arrays_to_ohlcv, filter_ohlcv
from base.caching import FilePersistor, NullPersistor, Persistor, SqlitePersistor
from trading.core.securities import Security
from trading.core.pricing import OHLCV, BasePricingProvider

logger = logging.getLogger(__name__)
_TOKEN_KEY='Dylan2010.Entitlementtoken'
_TOKEN_VALUE='57494d5ed7ad44af85bc59a51dd87c90'
_CKEY='57494d5ed7'
_MODULE: str = __name__.split(".")[-1]

class WallStreetJournal(BasePricingProvider):
    def __init__(self, storage: Literal['file','db','none']='db'):
        super().__init__(
            native = [Interval.D1, Interval.M15, Interval.M5],
            merge = {
                Interval.H1: Interval.M5,
                Interval.M15: Interval.M5
            }
        )
        self.pricing_persistor = FilePersistor(config.caching.file_path/_MODULE/"pricing") if storage == 'file'\
            else SqlitePersistor(config.caching.db_path, f"{_MODULE}_pricing") if storage == 'db'\
            else NullPersistor()

    @httputils.backup_timeout()
    def _fetch_pricing(self, key: str, step: str, time_frame: str, **kwargs):
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
        resp = httputils.get_as_browser(url, params={'json': json.dumps(request), 'ckey': _CKEY}, headers={_TOKEN_KEY: _TOKEN_VALUE})
        return json.loads(resp.text)
    
    def _get_interval(self, interval: Interval) -> str:
        if interval == Interval.D1: return 'P1D'
        elif interval == Interval.M15: return 'PT15M'
        elif interval == Interval.M5: return 'PT5M'
        else: raise ValueError(f"Unsupported interval {interval}")

    def _fix_timestamps(self, timestamps: list[float|int|None], interval: Interval, security: Security) -> list[float|None]:
        timestamps = [it//1000 if it else None for it in timestamps]
        result: list[float|None] = []
        for it in timestamps:
            if not it:
                result.append(None)
            elif interval >= Interval.D1:
                date = dates.unix_to_datetime(it, tz=dates.UTC)
                if date != dates.to_zero(date):
                    logger.warning(f"Unexpected UTC timestamp {date}. Skipping.")
                    result.append(None)
                else:
                    date = date.replace(tzinfo=security.exchange.calendar.tz) + timedelta(hours=1)
                    date = security.exchange.calendar.get_next_timestamp(date, interval)
                    result.append(date.timestamp())
            elif interval == Interval.H1:
                raise Exception(f"Can't fix H1 timestamps!")
            else:
                it += interval.time()
                if not security.exchange.calendar.is_timestamp(it, interval):
                    logger.warning(f"Unexpected {interval} timestamp {security.exchange.calendar.unix_to_datetime(it)}. Skipping.")
                    result.append(None)
                else:
                    result.append(it)
        return result

    @override
    def get_interval_start(self, interval) -> float:
        if interval == Interval.D1: return time.time() - 365*24*3600
        if interval < Interval.D1: return time.time() - 5*25*3600
        raise Exception(f"Unsupported interval {interval}.")
    @override
    def get_pricing_persistor(self, security, interval) -> Persistor:
        return self.pricing_persistor
    @override
    def get_pricing_delay(self, security, interval) -> float:
        return 120
    @override
    def get_pricing_raw(self, unix_from, unix_to, security, interval) -> list[OHLCV]:
        data = self._fetch_pricing(f"STOCK/US/XNAS/{security.symbol}", self._get_interval(interval), 'D5')
        def extract_data_points(series: dict) -> dict:
            return {key: [it[index] for it in series['DataPoints']] for index,key in enumerate(series['DesiredDataPoints'])}
        quotes = {'Timestamp': self._fix_timestamps(data['TimeInfo']['Ticks'], interval, security)}
        for series in data['Series']:
            quotes = {**quotes, **extract_data_points(series)}
        quotes['Close'] = quotes['Last']
        del quotes['Last']
        return filter_ohlcv(arrays_to_ohlcv(quotes), unix_from, unix_to)
