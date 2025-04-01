#1
import importlib
import sys
from types import ModuleType
from typing import Callable

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
def get_no_args_cnst[T](cls: type[T]) -> Callable[[], T]:
    try:
        cls()
        return cls
    except:
        return lambda: object.__new__(cls)
