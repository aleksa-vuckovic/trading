#1
from typing import Callable, Sequence, Iterable, Any, overload, Protocol, Literal
from enum import Enum
import itertools
import math

class Comparable(Protocol):
    def __lt__(self, other: Any, /) -> bool: ...

class BinarySearchEdge(Enum):
    LOW ='low'
    HIGH = 'high'
    NONE = 'none'
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

def is_sorted(collection: Iterable[Comparable]) -> bool:
    iter1 = iter(collection)
    iter2 = iter(collection)
    try:
        next(iter2)
        while True:
            if next(iter2) < next(iter1): return False
    except:
        return True

class LineSegment:
    def __init__(self, pointA: tuple[float,float], pointB: tuple[float,float]):
        self.start = pointA[0]
        self.end = pointB[0]
        if math.isinf(pointA[0]):
            self.k = 0
            self.n = pointB[1]
        elif math.isinf(pointB[0]):
            self.k = 0 
            self.n = pointA[1]
        else:
            self.k = (pointB[1]-pointA[1])/(pointB[0]-pointA[0])
            self.n = -self.k*pointA[0]+pointA[1]
    def __contains__(self, x: float) -> bool:
        return x >= self.start and x <= self.end
    def __call__(self, x: float) -> float:
        return self.k*x+self.n
type InterpolationMetod = Literal['linear', 'linear_edge']
def interpolate(x: Sequence[float], y: Sequence[float], x_ret: Sequence[float], method: InterpolationMetod = 'linear_edge') -> list[float]:
    assert len(x) == len(y)
    i = 0 #x_ret cursor
    if method == 'linear_edge':
        y_ret: list[float] = []
        for segment in itertools.chain(
            [LineSegment((float('-inf'), y[0]), (x[0], y[0]))],
            [LineSegment((x[i-1], y[i-1]), (x[i], y[i])) for i in range(1,len(x))],
            [LineSegment((x[-1], y[-1]), (float('+inf'), y[-1]))]
        ):
            while i < len(x_ret) and x_ret[i] in segment:
                y_ret.append(segment(x_ret[i]))
                i += 1
            if i == len(x_ret): break
        return y_ret
    elif method == 'linear':
        N = len(x)
        if not N: k,n = 0,0
        elif N==1: k,n = 0,y[0]
        else:
            k = N*sum(a*b for a,b in zip(x,y))-sum(x)*sum(y)
            k /= N*sum(a**2 for a in x)-sum(x)**2
            n = (sum(y)-k*sum(x))/N
        return [k*a+n for a in x_ret]
    else: raise Exception(f"Unknown interpolation methods {method}.")
