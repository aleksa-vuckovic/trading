import requests
from enum import Enum
import logging
import re
from ..utils import httputils, common
from pathlib import Path

logger = logging.getLogger(__name__)
_MODULE: str = __name__.split(".")[-1]
_CACHE = common.CACHE / _MODULE

class NasdaqMarket(Enum):
    SELECT = 'Q'
    GLOBAL = 'G'
    CAPITAL = 'S'

class FinancialStatus(Enum):
    NORMAL = 'N'
    DELINQUENT = 'D'
    BANKRUPT = 'E'
    BANKRUPT2 = 'Q'

class NasdaqListedEntry:
    symbol: str
    name: str
    market: NasdaqMarket
    test: bool
    status: FinancialStatus
    lot_size: int
    etf: bool
    next_shares: bool
    _line: str

    def is_warrant(self) -> bool:
        warrant_pattern = r'.*warrant.*'
        return bool(re.search(warrant_pattern, self.name, re.IGNORECASE))
    
    def is_tradable(self) -> bool:
        return not self.test and self.status in [FinancialStatus.NORMAL, FinancialStatus.DELINQUENT]

    def short_name(self) -> str:
        try:
            return self.name[:self.name.index(',')].strip()
        except:
            pass
        try:
            return self.name[:self.name.index('Inc.')].strip()
        except:
            pass
        try:
            return self.name[:self.name.index('Corporation')].strip()
        except:
            pass
        try:
            return self.name[:self.name.index('Corp.')].strip()
        except:
            pass
        try:
            return self.name[:self.name.index('Limited')].strip()
        except:
            pass
        try:
            return self.name[:self.name.index('Ltd.')].strip()
        except:
            pass
        try:
            return self.name[:self.name.index('L.P.')].strip()
        except:
            pass
        try:
            return self.name[:self.name.index('AG')].strip()
        except:
            pass
        try:
            return self.name[:self.name.index(' - ')].strip()
        except:
            pass
        return self.name

    def long_name(self) -> str:
        try:
            return self.name[:self.name.index(' - ')].strip()
        except:
            return self.name

    @staticmethod
    def yn_to_bool(yn: str) -> bool:
        if yn.lower() == 'y':
            return True
        if yn.lower() == 'n':
            return False
        raise ValueError(f'Invalid Y/N value: {yn}')

    @staticmethod
    def from_line(line: str):
        data = line.split('|')
        if len(data) != 8:
            raise ValueError(f'Invalid line entry count. Expected 8 but got {len(data)}.')
        ret = NasdaqListedEntry()
        ret.symbol = data[0]
        ret.name = data[1]
        ret.market = NasdaqMarket(data[2])
        ret.test = NasdaqListedEntry.yn_to_bool(data[3])
        ret.status = FinancialStatus(data[4])
        ret.lot_size = int(data[5])
        ret.etf = NasdaqListedEntry.yn_to_bool(data[6])
        ret.next_shares = NasdaqListedEntry.yn_to_bool(data[7])
        ret._line = line
        return ret
    
    def to_line(self) -> str:
        return self._line
    
    def __str__(self):
        return repr(self)
    def __repr__(self):
        return self._line

@common.cached_scalar(
    cache_root=_CACHE/"entries" 
)
def _get_all_entries() -> list[str]:
    response = httputils.get_as_browser("https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt")
    return response.text.splitlines(False)

_entries = None
def get_all_entries() -> list[NasdaqListedEntry]:
    global _entries
    if _entries is None:
        _entries = []
        for row in _get_all_entries():
            try:
                _entries.append(NasdaqListedEntry.from_line(row))
            except:
                logger.error(f'Failed to parse line:\n{row}', exc_info=True)
    return _entries

def get_filtered_entries() -> list[NasdaqListedEntry]:
    return [it for it in get_all_entries() if it.is_tradable() and len(it.symbol) <= 4]

def get_filtered_tickers() -> list[str]:
    return [it.symbol for it in get_filtered_entries()]