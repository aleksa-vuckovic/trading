#2
import functools
import importlib
import sys
import os
from pathlib import Path
from types import ModuleType
from typing import Callable, Iterable
from base.utils import cached

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
@cached
def get_no_args_cnst[T](cls: type[T]) -> Callable[[], T]:
    try:
        cls()
        return cls
    except:
        return lambda: object.__new__(cls)
def _get_bases(cls: type, visited: set[type]) -> Iterable[type]:
    visited.add(cls)
    for base in cls.__bases__:
        if base in visited: continue
        for it in _get_bases(base, visited):
            yield it
    yield cls
def get_bases(cls: type) -> Iterable[type]:
    """Iterate over the entire class hierarchy, in depth-first left-to-right order."""
    return _get_bases(cls, set())
_TRAINSENT = '_reflection_transient'
@cached
def get_trainsent(cls: type) -> set[str]:
    return functools.reduce(lambda skips, cls: skips.union(getattr(cls, _TRAINSENT, [])), get_bases(cls), set())
def transient[T: type](*keys: str) -> Callable[[T], T]:
    def decorate(cls: T) -> T:
        setattr(cls, _TRAINSENT, set(keys))
        return cls
    return decorate

_default_skip_folders = {"tests"}
def _get_modules(folder: Path, prefix: str, recursive: bool = True, skip_folders:set[str]=_default_skip_folders) -> Iterable[str]:
    for name in os.listdir(folder):
        path = folder/name
        if recursive and path.is_dir() and name not in skip_folders and (path/'__init__.py').exists():
            for it in _get_modules(path, f"{prefix}{name}.", recursive=True, skip_folders=skip_folders):
                yield it
        elif path.is_file() and name.endswith(".py") and name != "__init__.py":
            yield f"{prefix}{name[:-3]}"
def get_modules(top_module: str|None = None, recursive: bool = True, skip_folders:set[str]=_default_skip_folders) -> list[str]:
    root = Path(__file__)
    for it in __name__.split("."):
        root = root.parent
    if top_module:
        for segment in top_module.split("."):
            root /= segment
            if not root.exists(): raise Exception(f"Module {top_module} not found.")
            if not root.is_dir(): raise Exception(f"Path {root} is not a directory.")
            if not (root/"__init__.py").exists(): raise Exception(f"Folder {root} is not a python module (missing __init__.py).")
        prefix = f"{top_module}."
    else:
        prefix = ""
    return list(_get_modules(root, prefix, recursive=recursive, skip_folders=skip_folders))

def import_modules(top_module: str|None = None, recursive: bool=True, skip_folders:set[str]=_default_skip_folders) -> list[ModuleType]:
    return [importlib.import_module(it) for it in get_modules(top_module, recursive=recursive, skip_folders=skip_folders)]

def get_classes(top_module: str|None = None, recursive:bool=True, skip_folders:set[str]=_default_skip_folders, base:type=object) -> set[type]:
    result: list[type] = []
    for module in import_modules(top_module, recursive=recursive, skip_folders=skip_folders):
        for obj in module.__dict__.values():
            if isinstance(obj, type) and obj.__module__ == module.__name__ and issubclass(obj, base):
                result.append(obj)
    return set(result)
    