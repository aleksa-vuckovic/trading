#2
from __future__ import annotations
from typing import Any, Callable, overload, override
import json
from enum import Enum
import datetime
import zoneinfo
from base.classes import get_full_classname, get_class_by_full_classname

_TYPE = '$$type'
_SKIP_KEYS = '_serializable_skip_keys'

class Serializable:
    def to_dict(self) -> dict: ...
    @staticmethod
    def from_dict(data: dict) -> Any: ...

class Serializer:
    def serialize(self, obj: object) -> str:
        raise NotImplementedError()
    @overload
    def deserialize[T](self, data: str, assert_type: type[T]) -> T: ...
    @overload
    def deserialize(self, data: str) -> Any: ...
    def deserialize(self, data: str, assert_type: type|None = None) -> Any:
        raise NotImplementedError()

class BasicSerializer(Serializer):
    @override
    def serialize(self, obj: object) -> str:
        return json.dumps(obj)
    @overload
    def deserialize[T](self, data: str, assert_type: type[T]) -> T: ...
    @overload
    def deserialize(self, data: str) -> Any: ...
    @override
    def deserialize(self, data, assert_type: type|None = None):
        ret = json.loads(data)
        if assert_type: assert isinstance(ret, assert_type)
        return ret

def serializable[T: type](skip_keys: list[str] = []) -> Callable[[T], T]:
    def decorate(cls: T) -> T:
        create: Callable[[], object]
        try:
            cls() # type: ignore
            create = cls # type: ignore
        except TypeError:
            create = lambda: object.__new__(cls) # type: ignore
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
            result.__dict__.update(obj) # type: ignore
            return result
        cls.to_dict = to_dict # type: ignore
        cls.from_dict = from_dict # type: ignore
        return cls
    return decorate
    
class TypedSerializer(Serializer):
    def _serialize_default(self, obj: object, typed:bool) -> dict:
        if hasattr(obj, 'to_dict'): result = obj.to_dict() # type: ignore
        elif isinstance(obj, Enum): result = {'name': obj.name}
        elif isinstance(obj, datetime.datetime) or type(obj).__module__ == 'builtins': result = {'value': repr(obj)}
        else: raise Exception(f"Can't serialize {obj} of type {type(obj)}.")
        if typed: return {_TYPE: get_full_classname(obj), **result}
        else: return result
    @override
    def serialize(self, obj: object, typed:bool = True, indent:int|str|None=None) -> str:
        return json.dumps(obj, default=lambda it: self._serialize_default(it,typed), indent=indent)
    def _deserialize(self, obj: dict|list|str|int|float|bool|None) -> object:
        if obj is None: return None
        if isinstance(obj, (bool, int, float, str)): return obj
        if isinstance(obj, list):
            return [self._deserialize(it) for it in obj]
        if isinstance(obj, dict):
            for key in obj:
                if key == _TYPE: continue
                obj[key] = self._deserialize(obj[key])
            if _TYPE not in obj: return obj
            cls = get_class_by_full_classname(obj[_TYPE])
            del obj[_TYPE]
            if hasattr(cls, 'from_dict'): return cls.from_dict(obj)
            if issubclass(cls, Enum): return cls[obj['name']]
            if cls == datetime.datetime or cls.__module__ == 'builtins': return eval(obj['value'])
        raise Exception(f"Can't deserialize {obj}.")
    @overload
    def deserialize[T](self, data: str, assert_type: type[T]) -> T: ...
    @overload
    def deserialize(self, data: str) -> Any: ...
    @override
    def deserialize(self, data: str, assert_type: type|None = None) -> Any:
        ret = self._deserialize(json.loads(data))
        if assert_type: assert isinstance(ret, assert_type)
        return ret

serializer = TypedSerializer()
