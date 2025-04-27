#1
from __future__ import annotations
from enum import Enum, auto
from tokenize import Single
from typing import Sequence, override
from datetime import datetime, timedelta
from base.types import Serializable, Singleton
from base import dates
from base.utils import cached
from trading.core.interval import Interval
from trading.core.securities import Security, Exchange, SecurityType
from trading.core.work_calendar import BasicWorkCalendar, Hours
from trading.providers.nasdaq import WorkSchedule

class ForexSecurity(Security):
    class Subtype(Enum):
        MAJOR = auto()
        MINOR = auto()
    def __init__(self, base: str, quote: str, subtype: Subtype):
        super().__init__(f"{base}{quote}", f"{base}/{quote}", SecurityType.FX, Forex.instance)
        self.base = base
        self.quote = quote
        self.subtype = subtype

class ForexWorkCalendar(BasicWorkCalendar, Singleton):
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

class Forex(Exchange):
    def __init__(self):
        super().__init__('XFX', 'XFX', 'XFX', 'Forex Exchange', ForexWorkCalendar.instance)

    @override
    @cached
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

class SydneyForexWorkCalendar(BasicWorkCalendar, Singleton):
    def __init__(self):
        super().__init__(
            tz = dates.SYDNEY,
            work_schedule=WorkSchedule.Builder(Hours(7, 16)).build()
        )

class TokyoForexWorkCalendar(BasicWorkCalendar, Singleton):
    def __init__(self):
        super().__init__(
            tz = dates.TOKYO,
            work_schedule=WorkSchedule.Builder(Hours(9, 18)).build()
        )

class LondonForexWorkCalendar(BasicWorkCalendar, Singleton):
    def __init__(self):
        super().__init__(
            tz = dates.LONDON,
            work_schedule=WorkSchedule.Builder(Hours(8, 17)).build()
        )

class NewYorkForexWorkCalendar(BasicWorkCalendar, Singleton):
    def __init__(self):
        super().__init__(
            tz = dates.ET,
            work_schedule=WorkSchedule.Builder(Hours(8, 17)).build()
        )
