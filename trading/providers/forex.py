#1
from __future__ import annotations
from enum import Enum, auto
from functools import cached_property
from typing import Sequence, override
from base.serialization import serializable_singleton
from base.types import Serializable
from base import dates
from trading.core.securities import Security, Exchange, SecurityType
from trading.core.work_calendar import BasicWorkCalendar, Hours
from trading.providers.nasdaq import WorkSchedule

class ForexSecurity(Security):
    class Subtype(Enum):
        MAJOR = auto()
        MINOR = auto()
    def __init__(self, base: str, quote: str, subtype: Subtype):
        super().__init__(f"{base}{quote}", f"{base}/{quote}", SecurityType.FX)
        self.base = base
        self.quote = quote
        self.subtype = subtype

    @property
    @override
    def exchange(self) -> Exchange: return Forex.instance

@serializable_singleton
class ForexWorkCalendar(BasicWorkCalendar, Serializable):
    instance: ForexWorkCalendar
    def __init__(self):
        super().__init__(
            tz=dates.UTC,
            work_schedule=WorkSchedule.Builder(Hours(0,0)).build()
        )
ForexWorkCalendar.instance = ForexWorkCalendar()

class Forex(Exchange):
    instance: Forex
    def __init__(self):
        super().__init__('XFX', 'Forex Exchange', ForexWorkCalendar.instance)

    @cached_property
    @override
    def securities(self) -> Sequence[ForexSecurity]:
        majors = [
            ForexSecurity(base, quote, ForexSecurity.Subtype.MAJOR) for base,quote in [
                ('EUR','USD'),
                ('USD','JPY'),
                ('GBP','USD'),
                ('USD','CHF'),
                ('USD','CAD'),
                ('AUD','USD'),
                ('NZD','USD'),
            ]
        ]
        minors = [
            ForexSecurity(base, quote, ForexSecurity.Subtype.MINOR) for base,quote in [
                ('EUR','JPY'),
                ('EUR','CHF'),
                ('EUR','GBP'),
                ('AUD','JPY'),
                ('AUD','NZD'),
                ('AUD','CHF'),
                ('GPB','JPY'),
                ('GBP','CHF'),
                ('CAD','CHF'),
                ('CAD','JPY'),
                ('CHF','JPY'),
                ('NZD','JPY'),
            ]
        ]
        return [*majors, *minors]
        
Forex.instance = Forex()
