#1
from __future__ import annotations
from typing import Sequence
from enum import Enum, auto
from trading.core.interval import Interval
from trading.core.work_calendar import WorkCalendar

class Exchange:
    def __init__(
        self,
        mic: str,
        name: str,
        calendar: WorkCalendar
    ):
        self.mic = mic
        self.name = name
        self.calendar = calendar
    
    def get_securities(self) -> list[Security]:
        raise NotImplementedError()
    
    def get_security(self, symbol: str) -> Security:
        sec = [it for it in self.get_securities() if it.symbol == symbol]
        return sec[0]

class SecurityType(Enum):
    STOCK = auto()
    ETF = auto()
    WARRANT = auto()
    TEST = auto()

class Security:
    def __init__(
        self,
        symbol: str,
        name: str, # Full name, e.g. Company XYZ Inc. - Class A Common Shares
        type: SecurityType
    ):
        self.symbol = symbol
        self.name = name
        self.type = type
    
    @property
    def exchange(self) -> Exchange:
        raise NotImplementedError()

class PricingProvider:
    """
    Pricing providers will:
        1. Raise an exception if info for a given security is not available.
        2. Raise an exception if the interval is not supported.
        3. Ignore interval parts that are unavailable.
    """
    def get_pricing(
        self,
        unix_from: float,
        unix_to: float,
        security: Security,
        interval: Interval,
        *,
        return_quotes: list[str],
        interpolate: bool,
        max_fill_ratio: float
    ) -> tuple[list[float], ...]:
        """
        Returns the requested pricing data fresh from the source.
        Args:
            interpolate: If true, will return values for all interval timestamps, interpolating with known values if necessary.
            max_fill_ratio: Max ratio of (number of missing or interpolated entries)/(total number of entries).
                Only used when interpolate=True.
        """
        raise NotImplementedError()
    
class NewsProvider:
    """
    News providers will:
        1. Raise an exception if info for a given security is not available.
        2. Ignore interval parts that are unavailable.
    """
    def get_news(
        self,
        unix_from: float,
        unix_to: float,
        security: Security
    ) -> Sequence[dict]:
        raise NotImplementedError()
    
    def get_titles(self, unix_from: float, unix_to: float, security: Security) -> Sequence[str]: raise NotImplementedError()

class DataProvider:
    """
    Data providers will:
        1. Raise an exception if info for a given security is not available.
        2. Raise an exception if the particular piece of information is not available.
    """
    def get_outstanding_parts(self, security: Security) -> float:
        raise NotImplementedError()
    def get_summary(self, security: Security) -> str:
        raise NotImplementedError()
    def get_first_trade_time(self, security: Security) -> float:
        raise NotImplementedError()
    def get_market_cap(self, security: Security) -> float:
        raise NotImplementedError()
    