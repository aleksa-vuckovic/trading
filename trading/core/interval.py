#1
from __future__ import annotations
from enum import Enum

class Interval(Enum):
    """
    Methods that fetch interval based time series should follow these rules:
        1. The timestamp is the end of the interval in the appropriate timezone.
        2. The 'from' and 'to' timestamps from the request refer to the same timestamps as the series entries itself.
        3. The time range is closed at the start and open at the end.
    """
    L1 = '1 month'
    W1 = '1 week'
    D1 = '1 day'
    H1 = '1 hour'
    M30 = '30 minutes'
    M15 = '15 minutes'
    M5 = '5 minutes'
    M1 = '1 minute'

    def time(self) -> float:
        if self == Interval.L1: return 31*24*3600
        if self == Interval.W1: return 7*24*3600
        if self == Interval.D1: return 24*3600
        if self == Interval.H1: return 3600
        if self == Interval.M30: return 1800
        if self == Interval.M15: return 900
        if self == Interval.M5: return 300
        if self == Interval.M1: return 60
        raise ValueError(f"Unknown interval {self}.")
    def __lt__(self, other: Interval):
        return self.time() < other.time()
    def __le__(self, other: Interval):
        return self.time() <= other.time()
    def __gt__(self, other: Interval):
        return self.time() > other.time()
    def __ge__(self, other: Interval):
        return self.time() >= other.time()
    
    ascending: list[Interval]
    descending: list[Interval]
Interval.ascending = sorted(Interval, reverse=False)
Interval.descending = sorted(Interval, reverse=True)
