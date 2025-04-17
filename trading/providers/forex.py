#1
from __future__ import annotations
from enum import Enum, auto
from functools import cached_property
from typing import Sequence, override
from datetime import datetime, timedelta
from base.serialization import serializable_singleton
from base.types import Serializable
from base import dates
from trading.core.interval import Interval
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
            work_schedule=WorkSchedule.Builder(Hours(0,0)).sunday(Hours(20,0)).build()
        )
    @override
    def _is_timestamp(self, time: datetime, interval: Interval) -> bool:
        if interval == Interval.D1: return dates.to_zero(time) == time and time.weekday() in range(1,6)
        return super()._is_timestamp(time, interval)
    @override
    def _get_next_timestamp(self, time: dates.datetime, interval: Interval) -> dates.datetime:
        if interval == Interval.D1:
            timestamp = self.to_zero(time + timedelta(days=1))
            while timestamp.weekday() not in range(1,6): timestamp += timedelta(days=1)
            return timestamp
        return super()._get_next_timestamp(time, interval)
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

@serializable_singleton
class SydneyForexWorkCalendar(BasicWorkCalendar, Serializable):
    instance: SydneyForexWorkCalendar
    def __init__(self):
        super().__init__(
            tz = dates.SYDNEY,
            work_schedule=WorkSchedule.Builder(Hours(7, 16)).build()
        )
SydneyForexWorkCalendar.instance = SydneyForexWorkCalendar()

@serializable_singleton
class TokyoForexWorkCalendar(BasicWorkCalendar, Serializable):
    instance: TokyoForexWorkCalendar
    def __init__(self):
        super().__init__(
            tz = dates.TOKYO,
            work_schedule=WorkSchedule.Builder(Hours(9, 18)).build()
        )
TokyoForexWorkCalendar.instance = TokyoForexWorkCalendar()

@serializable_singleton
class LondonForexWorkCalendar(BasicWorkCalendar, Serializable):
    instance: LondonForexWorkCalendar
    def __init__(self):
        super().__init__(
            tz = dates.LONDON,
            work_schedule=WorkSchedule.Builder(Hours(8, 17)).build()
        )
LondonForexWorkCalendar.instance = LondonForexWorkCalendar()

@serializable_singleton
class NewYorkForexWorkCalendar(BasicWorkCalendar, Serializable):
    instance: NewYorkForexWorkCalendar
    def __init__(self):
        super().__init__(
            tz = dates.ET,
            work_schedule=WorkSchedule.Builder(Hours(8, 17)).build()
        )
NewYorkForexWorkCalendar.instance = NewYorkForexWorkCalendar()
