#1
from __future__ import annotations
from functools import cached_property
from typing import Sequence
from enum import Enum, auto
from base.reflection import get_classes
from base.serialization import Serializable, serializable_singleton
from trading.core.work_calendar import WorkCalendar

@serializable_singleton
class Exchange(Serializable):
    def __init__(
        self,
        mic: str,
        name: str,
        calendar: WorkCalendar
    ):
        self.mic = mic
        self.name = name
        self.calendar = calendar
    
    def securities(self) -> Sequence[Security]: ...
    
    def get_security(self, symbol: str) -> Security:
        sec = [it for it in self.securities() if it.symbol == symbol]
        return sec[0]
    
    _exchanges: dict[str, Exchange]|None = None
    @staticmethod
    def init():
        if Exchange._exchanges is None:
            exchanges: set[Exchange] = set(it.instance for it in get_classes("trading.providers", recursive=False, base=Exchange))
            Exchange._exchanges = {it.mic:it for it in exchanges}
    @staticmethod
    def all() -> list[Exchange]:
        Exchange.init()
        assert Exchange._exchanges is not None
        return list(Exchange._exchanges.values())
    @staticmethod
    def for_mic(mic: str) -> Exchange:
        Exchange.init()
        assert Exchange._exchanges is not None
        return Exchange._exchanges[mic]

class SecurityType(Enum):
    STOCK = auto()
    ETF = auto()
    WARRANT = auto()
    FX = auto()
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
    