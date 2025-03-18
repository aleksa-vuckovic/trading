#1
from __future__ import annotations
from enum import Enum

class Interval(Enum):
    """
    L1 covers an entire month of trading.
    The timestamp corresponds to the last working day of a month, at 16:00.
    """
    L1 = '1 month'
    """
    W1 covers an entire week of trading. The timestamp corresponds to friday at 16:00.
    """
    W1 = '1 week'
    """
    D1 covers the entire trading day, without pre/post data.
    The timestamp corresponds to the end of the day, 16:00 ET on workdays.
    """
    D1 = '1 day'
    """
    H1 covers an hour of trading, starting at 9:30, and up to 15:30.
    The last interval is an exception in that it only covers (the last) 30 minutes of trading.
    The timestamp corresponds to the end of the hour, i.e. 10:30 for the period from 9:30, but 16:00 for the last hour.
    """
    H1 = '1 hour'
    """
    M15 covers 30 minutes of trading. The timestamp corresponds to the end of a 15 minute period.
    """
    M15 = '15 minutes'
    """
    M5 covers 5 minutes of trading. The timestamp corresponds to the end of a 5 minute period.
    """
    M5 = '5 minutes'

    """
    Methods that fetch interval based time series should follow these rules:
        1. The timestamp is the end of the interval.
        2. The 'from' and 'to' timestamps from the request refer to the same timestamps as the series entries itself.
        3. The time range is closed at the start and open at the end.
    """

    def time(self) -> float:
        if self == Interval.L1: return 31*24*3600
        if self == Interval.W1: return 7*24*3600
        if self == Interval.D1: return 24*3600
        if self == Interval.H1: return 3600
        if self == Interval.M15: return 900
        if self == Interval.M5: return 300
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
