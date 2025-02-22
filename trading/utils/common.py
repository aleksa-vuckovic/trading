from __future__ import annotations
import logging
import importlib
from typing import Callable, Any
from enum import Enum

logger = logging.getLogger(__name__)

class Interval(Enum):
    """
    D1 interval covers the entire trading day, without pre/post data.
    The timestamp corresponds to the end of the day, 16:00 ET on workdays.
    """
    D1 = '1 day'
    """
    H1 interval covers an hour of trading, starting at 9:30, and up to 15:30.
    The last interval is an exception in that it only covers (the last) 30 minutes of trading.
    The timestamp corresponds to the end of the hour, i.e. 10:30 for the period from 9:30, but 16:00 for the last hour.
    """
    H1 = '1 hour'
    """
    Methods that fetch interval based time series should follow these rules:
        1. The timestamp is the end of the interval.
        2. The 'from' and 'to' timestamps from the request refer to the same timestamps as the series entries itself.
        3. The time range is closed at the start and open at the end.
    """

    def refresh_time(self) -> float:
        if self == Interval.D1: return 24*3600#6*3600
        if self == Interval.H1: return 24*3600#1800
        raise ValueError(f"Unknown interval {self}.")
    def time(self) -> float:
        if self == Interval.D1: return 24*3600
        if self == Interval.H1: return 3600
        raise ValueError(f"Unknown interval {self}.")

reserved_windows_filenames = {
    "CON", "PRN", "AUX", "NUL",
    *{f"COM{i}" for i in range(1, 10)},
    *{f"LPT{i}" for i in range(1, 10)}
}
def escape_filename(name: str):
    if name.upper() in reserved_windows_filenames:
        return f"[[{name}]]"
    return name

def get_full_classname(obj: object) -> str:
    return f"{type(obj).__module__}.{type(obj).__name__}"
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
    collection: list, value: int|float, key: Callable[[Any], int|float]=lambda x:x, edge: BinarySearchEdge = BinarySearchEdge.NONE) -> int | None:
    """
    Returns the index of value.
    If the value is not there, returns the index of:
        - The last smaller value or -1 if all values are larger (LOW).
        - The first bigger value or len if all values are smaller (HIGH).
        - None (NONE).
    The collection is assumed to be sorted in ascending order, based on the key.
    """
    if not collection:
        return None
    i = 0 # Always strictly smaller
    j = len(collection)-1 # Always strictly larger
    #Ensure proper initial conditions
    ival = key(collection[i])
    jval = key(collection[j])
    if ival == value: return i
    if jval == value: return j
    if ival > value: return i if edge == BinarySearchEdge.HIGH else i-1
    if jval < value: return j if edge == BinarySearchEdge.LOW else j+1
    while j - i > 1:
        mid = (i + j) // 2
        midval = key(collection[mid])
        if midval == value: return mid
        if midval > value: j = mid
        else: i = mid
    return j if edge == BinarySearchEdge.HIGH else i if edge == BinarySearchEdge.LOW else None
