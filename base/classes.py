#1
import importlib
from typing import Callable, Iterable

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