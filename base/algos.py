#1
import base64
import os
from typing import Callable, Sequence, Iterable, Any, overload, Protocol, Literal
import itertools
import math

class Comparable(Protocol):
    def __lt__(self, other: Any, /) -> bool: ...

type SearchSide = Literal['EQ', 'GT','GE','LT','LE']

@overload
def binary_search[T](collection: Sequence[T], value: Comparable, key: Callable[[T], Comparable], *, side: Literal['GT','GE','LT','LE']) -> int: ...
@overload
def binary_search[T](collection: Sequence[T], value: Comparable, key: Callable[[T], Comparable], *, side: Literal['EQ']=...) -> int|None: ...
@overload
def binary_search[T: Comparable](collection: Sequence[T], value: T, *, side: Literal['GT','GE','LT','LE']) -> int: ...
@overload
def binary_search[T: Comparable](collection: Sequence[T], value: T, *, side: Literal['EQ']=...) -> int|None: ...
def binary_search(collection: Sequence, value: Comparable, key: Callable[..., Comparable]=lambda x:x, *, side: SearchSide = 'EQ') -> int|None:
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
    if not collection: return -1 if side[0]=='L' else 0 if side[0]=='G' else None
    i = 0 # Always strictly smaller
    j = len(collection)-1 # Always strictly larger
    #Ensure proper initial conditions
    ival = key(collection[i])
    jval = key(collection[j])
    if ival == value: return i-1 if side=='LT' else i+1 if side=='GT' else i
    if jval == value: return j-1 if side=='LT' else j+1 if side=='GT' else j
    if value < ival: return i-1 if side[0]=='L' else i if side[0]=='G' else None
    if jval < value: return j if side[0]=='L' else j+1 if side[0]=='G' else None
    while j - i > 1:
        mid = (i + j) // 2
        midval = key(collection[mid])
        if midval == value: return mid-1 if side == 'LT' else mid+1 if side == 'GT' else mid
        if value < midval: j = mid
        else: i = mid
    return i if side[0]=='L' else j if side[0]=='G' else None

@overload
def binsert[T, K](collection: list[T], item: T, key: Callable[[T], K]) -> None: ...
@overload
def binsert[T: Comparable](collection: list[T], item: T) -> None: ...
def binsert(collection: list, item: Any, key: Callable[[Any], Any]=lambda it:it) -> None:
    index = binary_search(collection, key(item), key=key, side='GT')
    collection.insert(index, item)


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
type InterpolationMethod = Literal['linear', 'linear_edge', 'stair']
@overload
def interpolate(x: Sequence[float], y: Sequence[float], x_ret: Iterable[float], method: InterpolationMethod = 'linear_edge') -> list[float]: ...
@overload
def interpolate[T](x: Sequence[float], y: Sequence[T], x_ret: Iterable[float], method: Literal['stair']) -> list[T]: ...
def interpolate(x: Sequence[float], y: Sequence, x_ret: Iterable[float], method: InterpolationMethod = 'linear_edge') -> list:
    i = 0 #x_ret cursor
    if method == 'linear_edge':
        ret = []
        try:
            items = iter(x_ret)
            cur = next(items)
            for segment in itertools.chain(
                [LineSegment((float('-inf'), y[0]), (x[0], y[0]))],
                (LineSegment((x[i-1], y[i-1]), (x[i], y[i])) for i in range(1,len(x))),
                [LineSegment((x[-1], y[-1]), (float('+inf'), y[-1]))]
            ):
                while cur in segment:
                    ret.append(segment(cur))
                    cur = next(items)
        except StopIteration:
            pass
        return ret
    elif method == 'linear':
        N = len(x)
        if not N: k,n = 0,0
        elif N==1: k,n = 0,y[0]
        else:
            k = N*sum(a*b for a,b in zip(x,y))-sum(x)*sum(y)
            k /= N*sum(a**2 for a in x)-sum(x)**2
            n = (sum(y)-k*sum(x))/N
        return [k*a+n for a in x_ret]
    elif method == 'stair':
        ret = []
        index = 0
        for val in x_ret:
            while index+1 < len(x) and x[index+1] <= val: index += 1
            ret.append(y[index])
        return ret
    else: raise Exception(f"Unknown interpolation methods {method}.")


def lower_whole(x: float, step: float) -> float: return math.floor(x/step)*step

def upper_whole(x: float, step: float) -> float: return math.ceil(x/step)*step

def next_whole(x: float, step: float) -> float: return upper_whole(x, step) if x%step else x+step

def random_b32(length: int = 16) -> str:
    return base64.b32encode(os.urandom(math.ceil(length*5/8)))[:length].decode()

def random_b64(length: int = 16) -> str:
    return base64.b64encode(os.urandom(math.ceil(length*6/8)))[:length].decode()