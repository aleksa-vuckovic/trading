#3
from typing import Callable, Iterable, Self
from base.reflection import get_no_args_cnst
from base.serialization import serializable, Serializable


_SKIP_KEYS = '_equatable_skip_keys'
_INCLUDE_KEYS = '_equatable_include_keys'

def equatable[T: type](skip_keys: list[str]|None = None, include_keys: list[str]|None = None, with_hash:bool=True) -> Callable[[T], T]:
    """
    Defines the __eq__ method for the decorated class as a recursive __eq__ for all contained keys.
    """
    def decorate(cls: T) -> T:
        skips: set[str]|None = set(skip_keys) if skip_keys else None
        includes: set[str]|None = set(include_keys) if include_keys else None
        for base in cls.__bases__:
            if hasattr(base, _SKIP_KEYS): skips = (skips or set()).union(getattr(base, _SKIP_KEYS))
            if hasattr(base, _INCLUDE_KEYS): includes = (includes or set()).union(getattr(base, _INCLUDE_KEYS))
        if skips and includes: raise Exception(f"A class cannot define both skip_keys and include_keys.")
        if skips: setattr(cls, _SKIP_KEYS, skips)
        if includes: setattr(cls, _INCLUDE_KEYS, includes)
        if skips: 
            def skip(key: str) -> bool: return key in skips
        elif includes:
            def skip(key: str) -> bool: return key not in includes
        else:
            def skip(key: str) -> bool: return False
        def __eq__(self, other: object) -> bool:
            if not isinstance(other, cls): return False
            for key in self.__dict__:
                if skip(key): continue
                if key not in other.__dict__: return False
                if self.__dict__[key] != other.__dict__[key]: return False
            for key in other.__dict__:
                if not skip(key) and key not in self.__dict__: return False
            return True
        cls.__eq__ = __eq__ # type: ignore
        if with_hash:
            def __hash__(self) -> int:
                ret = 17
                for key in self.__dict__:
                    if skip(key): continue
                    ret ^= hash(self.__dict__[key])
                return ret
            cls.__hash__ = __hash__ # type: ignore
        return cls
    return decorate

class ClassDict[T]:
    def __init__(self, key: Callable[[str], str] = lambda x:x):
        self.key = key
    
    def __getitem__(self, key: str) -> T:
        key = self.key(key)
        return self.__dict__[key]
    
    def __setitem__(self, key: str, value: T):
        key = self.key(key)
        if not key in self.__dict__: raise Exception(f"Key {key} does not exist in {self}.")
        self.__dict__[key] = value

    def keys(self) -> Iterable[str]:
        return self.__dict__.keys()
    
def _clone[T](obj: T) -> T:
    if isinstance(obj, Cloneable):
        instance = get_no_args_cnst(type(obj))()
        for key in obj.__dict__:
            instance.__dict__[key] = _clone(obj.__dict__[key])
        return instance
    elif isinstance(obj, list): return [_clone(it) for it in obj] # type: ignore
    elif isinstance(obj, tuple): return tuple(_clone(it) for it in obj) # type: ignore
    elif isinstance(obj, dict): return {key:_clone(value) for key,value in obj.items()} # type: ignore
    else: return obj
class Cloneable:
    def clone(self) -> Self:
        return _clone(self)

@serializable()
@equatable(with_hash=False)
class ReadonlyDict[TK, TV](Serializable):
    def __init__(self, data: dict[TK, TV]):
        self._data = data
    
    def __getitem__(self, key: TK) -> TV: return self._data[key]
    def keys(self) -> Iterable[TK]: return self._data.keys()
    def items(self) -> Iterable[tuple[TK, TV]]: return self._data.items()
    def __len__(self) -> int: return len(self._data)
    def __contains__(self, key: TV) -> bool: return key in self._data
    def __hash__(self) -> int: return hash(tuple((key,value) for key, value in self._data.items()))
