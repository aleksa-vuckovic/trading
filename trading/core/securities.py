#1
from __future__ import annotations
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
    
    def get_security(self, symbol: str) -> Security|None:
        sec = [it for it in self.get_securities() if it.symbol == symbol]
        return sec[0] if sec else None

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
    def get_pricing(
        self,
        security: Security,
        unix_from: float,
        unix_to: float,
        interval: Interval,
        *,
        return_quotes: list[str] = ['close'],
        interpolate: bool = False,
        max_fill_ratio: float = 1,
        **kwargs
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
    def get_news(
        self,
        security: Security,
        unix_from: float,
        unix_to: float,
        **kwargs
    ) -> list[str]:
        raise NotImplementedError()

class DataProvider:
    def get_outstanding_parts(self, security: Security) -> int:
        raise NotImplementedError()
    def get_summary(self, security: Security) -> str:
        raise NotImplementedError()
    def get_first_trade_time(self, security: Security) -> float:
        raise NotImplementedError()
    def get_market_cap(self, security: Security) -> float:
        raise NotImplementedError()
    