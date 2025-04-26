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

def get_or_set[K,V](data: dict[K,V], key: K, create: Callable[[K], V]) -> V:
    if key not in data: data[key] = create(key)
    return data[key]
