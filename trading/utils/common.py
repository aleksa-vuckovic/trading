from __future__ import annotations
import logging
import importlib
import time
from typing import Callable, Any
from enum import Enum

logger = logging.getLogger(__name__)

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

    def refresh_time(self) -> float:
        return 7*24*3600
        #return self.time()
        #raise ValueError(f"Unknown interval {self}.")
    def time(self) -> float:
        if self == Interval.L1: return 31*24*3600
        if self == Interval.W1: return 7*24*3600
        if self == Interval.D1: return 24*3600
        if self == Interval.H1: return 3600
        if self == Interval.M15: return 900
        if self == Interval.M5: return 300
        raise ValueError(f"Unknown interval {self}.")
    def __lt__(self, other):
        return self.time() < other.time()
    def __le__(self, other):
        return self.time() <= other.time()
    def __gt__(self, other):
        return self.time() > other.time()
    def __ge__(self, other):
        return self.time() >= other.time()
    
    def start_unix(self) -> float:
        now = time.time()
        if self >= Interval.D1: return now - 10*365*24*3600
        if self == Interval.H1: return now - 729*24*3600
        if self == Interval.M15: return now - 59*24*3600
        if self == Interval.M5: return now - 59*24*3600
        raise Exception(f"Unsupported interval {self}.")
    ascending: list[Interval]
    descending: list[Interval]
Interval.ascending = sorted(Interval, reverse=False)
Interval.descending = sorted(Interval, reverse=True)

def equatable(skip_keys: list[str] = []):
    def decorate(cls):
        def __eq__(self, other: object) -> bool:
            if not isinstance(other, cls): return False
            for key in self.__dict__:
                if key in skip_keys: continue
                if key not in other.__dict__: return False
                if self.__dict__[key] != other.__dict__[key]: return False
            for key in other.__dict__:
                if key not in skip_keys and key not in self.__dict__: return False
            return True
        cls.__eq__ = __eq__
        return cls
    return decorate


reserved_windows_filenames = {
    "CON", "PRN", "AUX", "NUL",
    *{f"COM{i}" for i in range(1, 10)},
    *{f"LPT{i}" for i in range(1, 10)}
}
def escape_filename(name: str) -> str:
    if name.upper() in reserved_windows_filenames: return f"[[{name}]]"
    return name
def unescape_filename(name: str) -> str:
    if name[:2] == "[[" and name[-2:] == "]]": return name[2:-2]
    return name

def get_full_classname(obj_or_cls: object) -> str:
    if not isinstance(obj_or_cls, type): cls = type(obj_or_cls)
    else: cls = obj_or_cls
    return f"{cls.__module__}.{cls.__name__}"
def get_class_by_full_classname(full_classname: str) -> type:
    module_name, class_name = full_classname.rsplit('.', 1) 
    module = importlib.import_module(module_name)
    return getattr(module, class_name)

def shorter(text: str):
    if len(text) > 500: return f"{text[:497]}..."
    return text

class BinarySearchEdge(Enum):
    LOW ='low'
    HIGH = 'high'
    NONE = 'none'
def binary_search(
    collection: list, value: object, key: Callable[[Any], object]=lambda x:x, edge: BinarySearchEdge = BinarySearchEdge.NONE) -> int | None:
    """
    Returns the index of value.
    If the value is not there, returns the index of:
        - The last smaller value or -1 if all values are larger (LOW).
        - The first bigger value or len if all values are smaller (HIGH).
        - None (NONE).
    The collection is assumed to be sorted in ascending order, based on the key.
    """
    if not collection: return -1 if edge == BinarySearchEdge.LOW else 0 if edge == BinarySearchEdge.HIGH else None
    i = 0 # Always strictly smaller
    j = len(collection)-1 # Always strictly larger
    #Ensure proper initial conditions
    ival = key(collection[i])
    jval = key(collection[j])
    if ival == value: return i
    if jval == value: return j
    if ival > value: return i if edge == BinarySearchEdge.HIGH else i-1 if edge == BinarySearchEdge.LOW else None
    if jval < value: return j if edge == BinarySearchEdge.LOW else j+1 if edge == BinarySearchEdge.HIGH else None
    while j - i > 1:
        mid = (i + j) // 2
        midval = key(collection[mid])
        if midval == value: return mid
        if midval > value: j = mid
        else: i = mid
    return j if edge == BinarySearchEdge.HIGH else i if edge == BinarySearchEdge.LOW else None
