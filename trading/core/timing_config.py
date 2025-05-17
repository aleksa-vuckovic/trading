#3
from __future__ import annotations
from typing import Iterable, TypeVar, override
from datetime import datetime, tzinfo
from base import dates
from base.serialization import Serializable
from base.types import Equatable
from trading.core import Interval
from trading.core.securities import Exchange, Security

T = TypeVar('T', float, datetime)

class TimingConfig(Equatable, Serializable):
    """
    Delimits tradable and untradable periods. Can be exchange specific
    (e.g. trade the first 2 hours after open for any exchange) of general
    (e.g. trade 12 to 6 pm CET).
    """
    def matches(self, time: float|datetime, exchange: Exchange) -> bool: ...
    def next(self, time: T,interval: Interval, exchange: Exchange) -> T:
        if not isinstance(time, datetime):
            return self.next(exchange.calendar.unix_to_datetime(time), interval, exchange).timestamp()
        cur = exchange.calendar.get_next_timestamp(time, interval)
        while not self.matches(cur, exchange): cur = exchange.calendar.get_next_timestamp(cur, interval)
        return cur

class BasicTimingConfig(TimingConfig):
    def __init__(self, components: tuple[float|tuple[float,float],...], tz: tzinfo|None = None):
        self.components = components
        self.tz = tz
    class Builder:
        def __init__(self, tz: tzinfo|None = None):
            self.components = []
            self.tz = tz
        def at(self, hour: int = 9, minute: int = 30) -> BasicTimingConfig.Builder:
            self.components.append(float(hour*3600 + minute*60))
            return self
        class _Interval:
            def __init__(self, builder: BasicTimingConfig.Builder, start: float):
                self._builder = builder
                self._start = start
            def until(self, hour: int = 16, minute: int = 0) -> BasicTimingConfig.Builder:
                self._builder.components.append((self._start, float(hour*3600+minute*60)))
                return self._builder
        def starting(self, hour: int = 9, minute: int = 30) -> BasicTimingConfig.Builder._Interval:
            return BasicTimingConfig.Builder._Interval(self, float(hour*3600+minute*60))
        def around(self, hour: int = 10, minute: int = 0, delta_minute: int = 10):
            if not delta_minute: return self.at(hour = hour, minute = minute)
            time = float(hour*3600 + minute*60)
            self.components.append((time-delta_minute*60,time+delta_minute*60))
            return self
        def any(self) -> BasicTimingConfig.Builder:
            return self.starting(0,0).until(0,0)
        def build(self) -> TimingConfig:
            return BasicTimingConfig(tuple(self.components), self.tz)

    @override
    def matches(self, time: datetime|float, exchange: Exchange) -> bool:
        if isinstance(time, datetime): time = time.timestamp()
        if self.tz: date = dates.unix_to_datetime(time)
        else: date = exchange.calendar.unix_to_datetime(time)
        daysecs = date.hour*3600+date.minute*60+date.second+date.microsecond
        for it in self.components:
            if isinstance(it, tuple):
                if daysecs > it[0] and (daysecs <= it[1] or not it[1]): return True
            else:
                if daysecs == it: return True
        return False

class ForexTimingConfig(TimingConfig):
    def __init__(self, configs: list[tuple[Exchange, TimingConfig]]):
        self.configs = configs
    
    @override
    def matches(self, time: float | datetime, exchange: Exchange) -> bool:
        if isinstance(time, datetime): time = time.timestamp()
        for exchange, config in self.configs:
            if config.matches(time, exchange):
                return True
        return False

def execution_spots(securities: Iterable[Security], timing_config: TimingConfig, interval: Interval, start: float|None = None, end: float|None = None):
    unix_time = start or dates.unix()
    end = end or float('+inf')
    securities = set(securities)
    exchanges = set(it.exchange for it in securities)

    while True:
        times: dict[Exchange, float] = {it: timing_config.next(unix_time, interval, it) for it in exchanges}
        unix_time = min(times.values())
        if unix_time > end: break
        yield unix_time, {it for it in securities if times[it.exchange] == unix_time}
