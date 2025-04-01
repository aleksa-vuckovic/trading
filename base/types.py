#1
import importlib
import sys
from types import ModuleType
from typing import Callable, Iterable, Self

def equatable[T: type](skip_keys: list[str] = []) -> Callable[[T], T]:
    """
    Defines the __eq__ method for the decorated class as a recursive __eq__ for all contained keys.
    """
    def decorate(cls: T) -> T:
        def __eq__(self, other: object) -> bool:
            if not isinstance(other, cls): return False
            for key in self.__dict__:
                if key in skip_keys: continue
                if key not in other.__dict__: return False
                if self.__dict__[key] != other.__dict__[key]: return False
            for key in other.__dict__:
                if key not in skip_keys and key not in self.__dict__: return False
            return True
        cls.__eq__ = __eq__ # type: ignore
        return cls
    return decorate

def get_full_classname(obj_or_cls: object) -> str:
    if not isinstance(obj_or_cls, type): cls = type(obj_or_cls)
    else: cls = obj_or_cls
    return f"{cls.__module__}.{cls.__name__}"
def get_class_by_full_classname(full_classname: str) -> type:
    module_name, class_name = full_classname.rsplit('.', 1) 
    module = importlib.import_module(module_name)
    return getattr(module, class_name)
def get_module(cls: type) -> ModuleType:
    return sys.modules[cls.__module__]


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
    

def get_no_args_cnst[T](cls: type[T]) -> Callable[[], T]:
    try:
        cls()
        return cls
    except:
        return lambda: object.__new__(cls)


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
