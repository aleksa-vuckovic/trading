#1
from typing import Callable, Sequence
from enum import Enum
from typing import Iterable, Any, overload, Protocol, Literal

class BinarySearchEdge(Enum):
    LOW ='low'
    HIGH = 'high'
    NONE = 'none'

class Comparable(Protocol):
    def __lt__(self, other: Any, /) -> bool: ...

@overload
def binary_search[T](collection: Sequence[T], value: Comparable, key: Callable[[T], Comparable], *, edge: Literal[BinarySearchEdge.LOW,BinarySearchEdge.HIGH]) -> int: ...
@overload
def binary_search[T](collection: Sequence[T], value: Comparable, key: Callable[[T], Comparable], *, edge: Literal[BinarySearchEdge.NONE]) -> int|None: ...
@overload
def binary_search[T: Comparable](collection: Sequence[T], value: T, *, edge: Literal[BinarySearchEdge.LOW,BinarySearchEdge.HIGH]) -> int: ...
@overload
def binary_search[T: Comparable](collection: Sequence[T], value: T, *, edge: Literal[BinarySearchEdge.NONE]) -> int|None: ...
def binary_search(collection: Sequence, value: Comparable, key: Callable[..., Comparable]=lambda x:x, *, edge: BinarySearchEdge = BinarySearchEdge.NONE) -> int|None:
    """
    Returns the index of value.
    If the value is not there, returns the index of:
        - The last smaller value or -1 if all values are larger (LOW).
        - The first bigger value or len if all values are smaller (HIGH).
        - None (NONE).
    The collection is assumed to be sorted in ascending order, based on the key.
    Args:
        collection: A collection of comparable values (operators ==, > and < are used)
        value: The searched-for value.
        edge: Determines the result when the value is not found.
    """
    if not collection: return -1 if edge == BinarySearchEdge.LOW else 0 if edge == BinarySearchEdge.HIGH else None
    i = 0 # Always strictly smaller
    j = len(collection)-1 # Always strictly larger
    #Ensure proper initial conditions
    ival = key(collection[i])
    jval = key(collection[j])
    if ival == value: return i
    if jval == value: return j
    if value < ival: return i-1 if edge == BinarySearchEdge.LOW else i if edge == BinarySearchEdge.HIGH else None
    if jval < value: return j if edge == BinarySearchEdge.LOW else j+1 if edge == BinarySearchEdge.HIGH else None
    while j - i > 1:
        mid = (i + j) // 2
        midval = key(collection[mid])
        if midval == value: return mid
        if value < midval: j = mid
        else: i = mid
    return i if edge == BinarySearchEdge.LOW else j if edge == BinarySearchEdge.HIGH else None

result = binary_search([1], 1, key=lambda x:x, edge=BinarySearchEdge.LOW)

def is_sorted(collection: Iterable[Comparable]) -> bool:
    iter1 = iter(collection)
    iter2 = iter(collection)
    try:
        next(iter2)
        while True:
            if next(iter2) < next(iter1): return False
    except:
        return True