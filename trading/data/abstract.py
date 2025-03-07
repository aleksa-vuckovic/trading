from __future__ import annotations
from enum import Enum, auto
from ..utils.common import Interval
from ..utils.dateutils import WorkCalendar

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
    
    def get_securities(self) -> list[AbstractSecurity]:
        raise NotImplementedError()

class SecurityType(Enum):
    STOCK = auto()
    ETF = auto()
    WARRANT = auto()
    TEST = auto()
    
class AbstractSecurity:
    def __init__(
        self,
        symbol: str,
        name: str, # Full name, e.g. Company XYZ Inc. - Class A Common Shares
        type: SecurityType,
        tradable: bool = True
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
        security: AbstractSecurity,
        unix_from: float,
        unix_to: float,
        interval: Interval,
        *,
        return_quotes: list[str] = ['close'],
        **kwargs
    ) -> tuple[list[float], ...]:
        raise NotImplementedError()