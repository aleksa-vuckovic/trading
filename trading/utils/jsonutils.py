import json
from typing import get_origin, Self
from types import GenericAlias
from enum import Enum
from .common import get_full_classname, get_class_by_full_classname

_TYPE = '$$type'

_SKIP_KEYS = '_serializable_skip_keys'
def serializable(skip_keys: list[str] = []):
    def decorate(cls):
        try:
            cls()
            create = cls
        except TypeError:
            create = lambda: object.__new__(cls)
        skips = skip_keys[:]
        for base in cls.__bases__:
            if hasattr(base, _SKIP_KEYS): skips.extend(getattr(base, _SKIP_KEYS))
        setattr(cls, _SKIP_KEYS, skips)
        if not skips:
            def to_dict(self) -> dict:
                return self.__dict__
        else:
            def to_dict(self) -> dict:
                return {key:self.__dict__[key] for key in self.__dict__ if key not in skips}
        def from_dict(obj:dict) -> object:
            result = create()
            result.__dict__.update(obj)
            return result
        cls.to_dict = to_dict
        cls.from_dict = from_dict
        return cls
    return decorate
def _is_serializable(obj_or_cls: object) -> bool:
    hasattr(obj_or_cls, '_jsonutils_serializable')

def _serialize_default(obj: object) -> dict:
    if hasattr(obj, 'to_dict'): result = obj.to_dict()
    elif isinstance(obj, Enum): result = {'name': obj.name}
    elif type(obj).__module__ == 'builtins': result = {'value': repr(obj)}
    elif _is_serializable(obj): result = obj.__dict__
    else: raise Exception(f"Can't serialize {obj} of type {type(obj)}.")
    return {_TYPE: get_full_classname(obj), **result}
def serialize(obj: object, indent:int|str|None=None) -> str:
    return json.dumps(obj, default=_serialize_default, indent=indent)

def deserialize(data: str) -> str:
    return _deserialize(json.loads(data))
def _deserialize(obj: dict|list|str|int|float|bool|None) -> object:
    if obj is None: return None
    if isinstance(obj, (bool, int, float, str)): return obj
    if isinstance(obj, list):
        return [_deserialize(it) for it in obj]
    if isinstance(obj, dict):
        for key in obj:
            if key == _TYPE: continue
            obj[key] = _deserialize(obj[key])
        if _TYPE not in obj: return obj
        cls = get_class_by_full_classname(obj[_TYPE])
        del obj[_TYPE]
        if hasattr(cls, 'from_dict'): return cls.from_dict(obj)
        if issubclass(cls, Enum): return cls[obj['name']]
        if cls.__module__ == 'builtins': return eval(obj['value'])
        if _is_serializable(cls):
            instance = cls()
            instance.__dict__.update(obj)
            return instance
        raise Exception(f"Can't deserialize {obj}.")