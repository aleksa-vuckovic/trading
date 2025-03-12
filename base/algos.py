#1
from typing import Callable
from enum import Enum

class BinarySearchEdge(Enum):
    LOW ='low'
    HIGH = 'high'
    NONE = 'none'
def binary_search(
    collection: list, value: object, key: Callable[[object], object]=lambda x:x, edge: BinarySearchEdge = BinarySearchEdge.NONE) -> int | None:
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
