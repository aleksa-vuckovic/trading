#1
from __future__ import annotations
from typing import Sequence
from enum import Enum, auto

from base.reflection import get_classes
from base.types import Equatable, Singleton
from base.utils import cached
from base.serialization import Serializable
from trading.core.work_calendar import WorkCalendar

class Exchange(Singleton):
    def __init__(
        self,
        mic: str,
        segment_mic: str,
        operating_mic: str,
        name: str,
        calendar: WorkCalendar
    ):
        self.mic = mic
        self.segment_mic = segment_mic
        self.operating_mic = operating_mic
        self.name = name
        self.calendar = calendar
    
    def securities(self) -> Sequence[Security]: ...
    
    def get_security(self, symbol: str) -> Security:
        sec = [it for it in self.securities() if it.symbol == symbol]
        return sec[0]
    
    def __repr__(self) -> str: return f"{type(self).__name__}()"
    _exchanges: dict[str, Exchange]|None = None

    @staticmethod
    @cached
    def all() -> set[Exchange]:
        return set(it.instance for it in get_classes("trading.providers", recursive=False, base=Exchange))
    @staticmethod
    @cached
    def for_mic(mic: str) -> Exchange:
        return [it for it in Exchange.all() if it.mic == mic][0]
    @staticmethod
    @cached
    def for_segment_mic(mic: str) -> Exchange:
        return [it for it in Exchange.all() if it.segment_mic == mic][0]
    @staticmethod
    @cached
    def for_operating_mic(mic: str) -> Exchange:
        return [it for it in Exchange.all() if it.operating_mic == mic][0]


class SecurityType(Enum):
    STOCK = auto()
    ETF = auto()
    WARRANT = auto()
    FX = auto()
    TEST = auto()

class Security(Equatable, Serializable):
    def __init__(
        self,
        symbol: str,
        name: str, # Full name, e.g. Company XYZ Inc. - Class A Common Shares
        type: SecurityType,
        exchange: Exchange
    ):
        self.symbol = symbol
        self.name = name
        self.type = type
        self.exchange = exchange
    
    def __str__(self) -> str:
        return f"{self.exchange.segment_mic}/{self.type.name}/{self.symbol}"
    
    def __repr__(self) -> str:
        return str(self)

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
    