#4
from typing import Callable, Iterable, Self, override
from base.reflection import get_no_args_cnst, get_trainsent
from base.serialization import Serializable, json_type

class Equatable:
    def __eq__(self, other) -> bool:
        if not type(self) == type(other): return False
        skips = get_trainsent(type(self))
        for key in self.__dict__:
            if key in skips: continue
            if key not in other.__dict__: return False
            if self.__dict__[key] != other.__dict__[key]: return False
        for key in other.__dict__:
            if key not in skips and key not in self.__dict__: return False
        return True
    def __hash__(self) -> int:
        skips = get_trainsent(type(self))
        ret = 17
        for key in self.__dict__:
            if key not in skips: continue
            ret ^= hash(self.__dict__[key])
        return ret
    
class InstanceDescriptor:
    def __get__(self, obj, cls):
        if not hasattr(cls, '_instance'):
            cls._instance = cls()
        return cls._instance
class Singleton(Serializable):
    instance: Self
    instance = InstanceDescriptor() #type: ignore
    @override
    def to_json(self) -> json_type: return None
    @override
    @classmethod
    def from_json(cls: type[Self], data: json_type) -> Self:
        assert data is None
        return cls.instance

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

class ReadonlyDict[TK, TV](Equatable, Serializable):
    def __init__(self, data: dict[TK, TV]):
        self._data = data
    
    def __getitem__(self, key: TK) -> TV: return self._data[key]
    def keys(self) -> Iterable[TK]: return self._data.keys()
    def items(self) -> Iterable[tuple[TK, TV]]: return self._data.items()
    def __len__(self) -> int: return len(self._data)
    def __contains__(self, key: TV) -> bool: return key in self._data
    def __hash__(self) -> int: return hash(tuple((key,value) for key, value in self._data.items()))
