#1
from typing import Callable, TypeVar, TypeVarTuple

Args = TypeVarTuple('Args')
T = TypeVar('T')
def cached(func: Callable[[*Args], T]) -> Callable[[*Args], T]:
    values: dict[tuple[*Args], T] = {}
    def wrapper(*args: *Args) -> T:
        if args not in values: values[args] = func(*args)
        return values[args]
    return wrapper
