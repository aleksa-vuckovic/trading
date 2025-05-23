#1
from __future__ import annotations
import logging
import re
from typing import Sequence, override
from enum import Enum
from base.key_value_storage import FileKVStorage
import config
from base import dates
from base.types import Singleton
from base.utils import cached
from base.caching import cached_scalar
from base.scraping import scraper
from base.serialization import Serializable
from trading.core.work_calendar import WorkSchedule, BasicWorkCalendar, Hours
from trading.core.securities import Security, SecurityType, Exchange

logger = logging.getLogger(__name__)
_MODULE: str = __name__.split(".")[-1]

class NasdaqCalendar(BasicWorkCalendar, Singleton):
    def __init__(self):
        super().__init__(
            tz = dates.ET, 
            work_schedule= WorkSchedule.Builder(
                Hours(9, 16, open_minute=30)
            ).special(Hours(9, 13, open_minute=30),
                '2020-11-27', '2020-12-24', '2021-11-26',
                '2022-11-25', '2023-07-03', '2023-11-24',
                '2024-07-03', '2024-11-29', '2024-12-24',
                '2025-07-03', '2025-11-28', '2025-12-24'
            ).off(
                '2020-01-01', '2020-01-20', '2020-02-17',
                '2020-04-10', '2020-05-25', '2020-03-07',
                '2020-09-07', '2020-11-26', '2020-12-25',
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
            ).build()  
        )

class NasdaqGS(Exchange):
    def __init__(self):
        super().__init__('XNAS', 'XNGS', 'XNAS', 'Nasdaq Global Select', NasdaqCalendar.instance)

    @override
    @cached
    def securities(self) -> Sequence[Security]:
        return [it for it in Nasdaq.instance.securities() if it.exchange is NasdaqGS.instance]

class NasdaqMS(Exchange):
    def __init__(self):
        super().__init__('XNAS', 'XNMS', 'XNAS', 'Nasdaq Global Market', NasdaqCalendar.instance)

    @override
    @cached
    def securities(self) -> Sequence[Security]:
        return [it for it in Nasdaq.instance.securities() if it.exchange is NasdaqMS.instance]

class NasdaqCM(Exchange):
    def __init__(self):
        super().__init__('XNAS', 'XNCM', 'XNAS', 'Nasdaq Capital Market', NasdaqCalendar.instance)

    @override
    @cached
    def securities(self) -> Sequence[Security]:
        return [it for it in Nasdaq.instance.securities() if it.exchange is NasdaqCM.instance]

class Nasdaq(Exchange):
    def __init__(self):
        super().__init__('XNAS', 'XNAS', 'XNAS', 'Nasdaq All Markets', NasdaqCalendar.instance)
    
    @cached_scalar(
        storage_fn=FileKVStorage(config.storage.folder_path/_MODULE/"listed"),
        refresh_fn=7*24*3600
    )
    def _fetch_listed(self) -> list[str]:
        response = scraper.get("https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt")
        return response.text.splitlines(False)

    @override
    @cached
    def securities(self) -> Sequence[NasdaqSecurity]:
        result: list[NasdaqSecurity] = []
        tests = 0
        failed = 0
        error = None
        for row in self._fetch_listed():
            try:
                sec = NasdaqSecurity.from_line(row)
                if sec.type == SecurityType.TEST: tests += 1
                else: result.append(sec)
            except Exception as e:
                failed += 1
                error = error if failed > 2 else e
        logger.info(f"Successfully parsed {len(result)} securities.")
        logger.info(f"Skipped {tests} test securities.")
        logger.info(f"Failed to parse {failed} securities. {error}")
        return result

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

class NasdaqSecurity(Security, Serializable):
    def __init__(self, symbol: str, name: str, type: SecurityType, exchange: Exchange, status: FinancialStatus):
        super().__init__(symbol, name, type, exchange)
        self.status = status

    @staticmethod
    def from_line(line: str) -> NasdaqSecurity:
        data = line.split('|')
        symbol = data[0]
        name = data[1]
        exchange = NasdaqGS.instance if data[2] == 'Q' else NasdaqMS.instance if data[2] == 'G' else NasdaqCM.instance
        test = _yn_to_bool(data[3])
        status = FinancialStatus(data[4])
        lot_size = int(data[5])
        etf = _yn_to_bool(data[6])
        next_shares = _yn_to_bool(data[7])

        type = SecurityType.ETF if etf\
            else SecurityType.WARRANT if re.search(r'.*warrant.*', name, re.IGNORECASE)\
            else SecurityType.STOCK if not test\
            else SecurityType.TEST
        
        return NasdaqSecurity(symbol, name, type, exchange, status)
