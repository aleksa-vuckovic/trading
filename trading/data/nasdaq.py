from __future__ import annotations
import logging
import re
import calendar
from enum import Enum
from datetime import datetime, timedelta
from ..utils import httputils, dateutils
from ..utils.common import Interval
from ..utils.dateutils import WorkCalendar, HolidaySchedule
from ..utils.jsonutils import serializable
from .caching import cached_scalar, CACHE_ROOT, FilePersistor
from .abstract import AbstractSecurity, SecurityType, Exchange

logger = logging.getLogger(__name__)
_MODULE: str = __name__.split(".")[-1]

class NasdaqCalendar(WorkCalendar):
    instance: NasdaqCalendar
    def __init__(self):
        super().__init__(tz=dateutils.ET)
        self.holidays = HolidaySchedule()
        self.holidays.add_off_days(
            '2021-01-01', '2021-01-18', '2021-02-15',
            '2021-04-02', '2021-05-31', '2021-07-05',
            '2021-09-06', '2021-11-25', '2021-12-24',
            '2022-01-17', '2022-02-21', '2022-04-15',
            '2022-05-30', '2022-06-20', '2022-07-04',
            '2022-09-05', '2022-11-24', '2022-12-26',
            '2023-01-02', '2023-01-16', '2023-02-20',
            '2023-04-07', '2023-05-29', '2023-06-19',
            '2023-07-04', '2023-09-04', '2023-11-23',
            '2023-12-25', '2024-01-01', '2024-01-15',
            '2024-02-19', '2024-03-29', '2024-05-27',
            '2024-06-19', '2024-07-04', '2024-09-02',
            '2024-11-28', '2024-12-25', '2025-01-01',
            '2025-01-20', '2025-02-17', '2025-04-18',
            '2025-05-26', '2025-06-19', '2025-07-04',
            '2025-09-01', '2025-11-27', '2025-12-25'
        )
        self.holidays.add_semi_days(
            '2021-11-26', '2022-11-25', '2023-07-03',
            '2023-11-24', '2024-07-03', '2024-11-29',
            '2024-12-24', '2025-07-03', '2025-11-28',
            '2025-12-24'
        )
    #region Overrides
    def is_workday(self, time: datetime|float|int) -> bool:
        if not isinstance(time, datetime): return self.is_workday(self.unix_to_datetime(time))
        time = self.to_zero(time)
        return time.weekday() < 5 and not self.holidays.is_off(time)
    def is_worktime(self, time: datetime|float|int) -> bool:
        if not isinstance(time, datetime): return self.is_worktime(self.unix_to_datetime(time))
        if not self.is_workday(time): return False
        return time > self.set_open(time) and time <= self.set_close(time)
    def set_open(self, time: datetime|float|int) -> datetime:
        if not isinstance(time, datetime): return self.set_open(self.unix_to_datetime(time)).timestamp()
        if not self.is_workday(time): raise Exception(f"Can't set open for non workday {time}.")
        return time.replace(hour=9, minute=30, second=0, microsecond=0)
    def set_close(self, time: datetime|float|int) -> datetime:
        if not isinstance(time, datetime): return self.set_close(self.unix_to_datetime(time)).timestamp()
        if not self.is_workday(time): raise Exception(f"Can't set close for non workday {time}.")
        return time.replace(hour=16 if not self.holidays.is_semi(time) else 13, minute=0, second=0, microsecond=0)
    def is_timestamp(self, time: datetime|float|int, interval: Interval):
        if not isinstance(time, datetime): return self.is_timestamp(self.unix_to_datetime(time), interval)
        if not self.is_workday(time): return False
        if time.second or time.microsecond: return False
        if time <= self.set_open(time) or time > self.set_close(time): return False
        if interval == Interval.L1:
            return time == self.get_next_timestamp(time.replace(day=1), interval)
        if interval == Interval.W1:
            return time == self.get_next_timestamp(time - timedelta(days=time.weekday()+1), interval)
        if interval == Interval.D1:
            return time == self.set_close(time)
        if interval == Interval.H1:
            return time == self.set_close(time) or time.minute==30
        if interval == Interval.M15:
            return not (time.minute % 15)
        if interval == Interval.M5:
            return not (time.minute % 5)
        raise Exception(f"Unknown interval {interval}")
    def _month_end(self, time: datetime) -> datetime:
        time = time.replace(day=calendar.monthrange(time.year, time.month)[1])
        while not self.is_workday(time): time -= timedelta(days=1)
        return self.set_close(time)
    def _week_end(self, time: datetime) -> datetime:
        time += timedelta(days=6-time.weekday())
        while not self.is_workday(time): time -= timedelta(days=1)
        return self.set_close(time)
    def get_next_timestamp(self, time: datetime|float|int, interval: Interval) -> datetime|float|int:
        if not isinstance(time, datetime):
            return self.get_next_timestamp(self.unix_to_datetime(time), interval).timestamp()
        if interval == Interval.L1:
            timestamp = self._month_end(time)
            if timestamp > time: return timestamp
            return self._month_end(timestamp+timedelta(days=15))
        if interval == Interval.W1:
            timestamp = self._week_end(time)
            if timestamp > time: return timestamp
            return self._week_end(timestamp+timedelta(days=7))
        if interval == Interval.D1:
            timestamp = time
            while not self.is_workday(timestamp): timestamp += timedelta(days=1)
            timestamp = self.set_close(timestamp)
            if timestamp > time: return timestamp
            timestamp += timedelta(days=1)
            while not self.is_workday(timestamp): timestamp += timedelta(days=1)
            return self.set_close(timestamp)
        if interval == Interval.H1:
            timestamp = time
            if self.is_workday(timestamp) and timestamp < self.set_close(timestamp):
                if timestamp < self.set_open(timestamp):
                    return timestamp.replace(hour=10,minute=30,second=0,microsecond=0)
                if timestamp + timedelta(minutes=30) >= self.set_close(timestamp):
                    return self.set_close(timestamp)
                if timestamp.minute < 30:
                    return timestamp.replace(minute=30,second=0,microsecond=0)
                return timestamp.replace(hour=timestamp.hour+1,minute=30,second=0,microsecond=0)
            timestamp += timedelta(days=1)
            while not self.is_workday(timestamp): timestamp += timedelta(days=1)
            return timestamp.replace(hour=10,minute=30,second=0,microsecond=0)
        if interval == Interval.M15:
            timestamp = time
            if self.is_workday(timestamp) and timestamp < self.set_close(timestamp):
                if timestamp < self.set_open(timestamp):
                    return timestamp.replace(hour=9, minute=45, second=0, microsecond=0)
                return timestamp.replace(minute = timestamp.minute//15*15, second=0, microsecond=0) + timedelta(minutes=15)
            timestamp += timedelta(days=1)
            while not self.is_workday(timestamp): timestamp += timedelta(days=1)
            return timestamp.replace(hour=9,minute=45,second=0,microsecond=0)
        if interval == Interval.M5:
            timestamp = time
            if self.is_workday(timestamp) and timestamp < self.set_close(timestamp):
                if timestamp < self.set_open(timestamp):
                    return timestamp.replace(hour=9, minute=35, second=0, microsecond=0)
                return timestamp.replace(minute = timestamp.minute//5*5, second=0, microsecond=0) + timedelta(minutes=5)
            timestamp += timedelta(days=1)
            while not self.is_workday(timestamp): timestamp += timedelta(days=1)
            return timestamp.replace(hour=9,minute=35,second=0,microsecond=0)
        raise Exception(f"Unknown interval {interval}")
    #endregion
NasdaqCalendar.instance = NasdaqCalendar()

class Nasdaq(Exchange):
    instance: Nasdaq
    def __init__(self):
        super().__init__('XNAS', 'Nasdaq', NasdaqCalendar.instance)
        self.securities = None
    
    @cached_scalar(
        key_fn=lambda _: ['entries'],
        persistor_fn=FilePersistor(CACHE_ROOT/_MODULE)
    )
    def _get_entries(self) -> list[str]:
        response = httputils.get_as_browser("https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt")
        return response.text.splitlines(False)

    def get_securities(self) -> list[NasdaqSecurity]:
        if self.securities is None:
            self.securities = []
            for row in self._get_entries():
                try:
                    self.securities.append(NasdaqSecurity.from_line(row))
                except:
                    logger.error(f'Failed to parse line:\n{row}', exc_info=True)
        return self.securities
Nasdaq.instance = Nasdaq()

class NasdaqMarket(Enum):
    SELECT = 'Q'
    GLOBAL = 'G'
    CAPITAL = 'S'

class FinancialStatus(Enum):
    NORMAL = 'N'
    DEFICIENT = 'D'
    DELINQUENT = 'E'
    BANKRUPT = 'Q'

def _yn_to_bool(yn: str) -> bool:
    if yn.lower() == 'y':
        return True
    if yn.lower() == 'n':
        return False
    raise ValueError(f'Invalid Y/N value: {yn}')

@serializable()
class NasdaqSecurity(AbstractSecurity):
    def __init__(self, symbol, name, type, market: NasdaqMarket, status: FinancialStatus):
        super().__init__(symbol, name, type, Nasdaq.instance)
        self.market = market
        self.status = status

    @staticmethod
    def from_line(line: str):
        data = line.split('|')
        symbol = data[0]
        name = data[1]
        market = NasdaqMarket(data[2])
        test = _yn_to_bool(data[3])
        status = FinancialStatus(data[4])
        lot_size = int(data[5])
        etf = _yn_to_bool(data[6])
        next_shares = NasdaqSecurity.yn_to_bool(data[7])

        if test: raise Exception(f"Line '{line}' represents a test security.")
        type = SecurityType.ETF if etf\
            else SecurityType.WARRANT if re.search(r'.*warrant.*', name, re.IGNORECASE)\
            else SecurityType.STOCK
        
        return NasdaqSecurity(symbol, name, type, market, status)
        