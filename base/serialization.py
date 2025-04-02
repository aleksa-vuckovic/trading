#2
from __future__ import annotations
import json
import datetime
import zoneinfo
import builtins
from typing import Any, Callable, Iterable, cast, overload, override
from sqlalchemy import String, TypeDecorator
from enum import Enum
from pathlib import Path
from base.reflection import get_full_classname, get_class_by_full_classname, get_no_args_cnst

_SKIP_KEYS = '_serializable_skip_keys'
_INCLUDE_KEYS = '_serializable_include_keys'

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

def serializable[T: Serializable](skip_keys: Iterable[str]|None = None, include_keys: Iterable[str]|None = None) -> Callable[[type[T]], type[T]]:
    def decorate(cls: type[T]) -> type[T]:
        create = get_no_args_cnst(cls)
        skips: set[str]|None = set(skip_keys) if skip_keys else None
        includes: set[str]|None = set(include_keys) if include_keys else None
        for base in cls.__bases__:
            if hasattr(base, _SKIP_KEYS): skips = (skips or set()).union(getattr(base, _SKIP_KEYS))
            if hasattr(base, _INCLUDE_KEYS): includes = (includes or set()).union(getattr(base, _INCLUDE_KEYS))
        if skips and includes: raise Exception(f"A class cannot define both skip_keys and include_keys.")
        if skips: setattr(cls, _SKIP_KEYS, skips)
        if includes: setattr(cls, _INCLUDE_KEYS, includes)
        if skips:
            def to_dict(self) -> dict:
                return {key:self.__dict__[key] for key in self.__dict__ if key not in skips}
        elif includes:
            def to_dict(self) -> dict:
                return {key:self.__dict__[key] for key in self.__dict__ if key in includes}
        else:
            def to_dict(self) -> dict:
                return self.__dict__
        def from_dict(data:dict) -> Any:
            result = create()
            result.__dict__.update(data)
            return result
        cls.to_dict = to_dict
        cls.from_dict = from_dict
        return cls
    return decorate
    
_TYPE = '$T'
_VALUE = '$V'
class TypedSerializer(Serializer):
    def _serialize(self, obj: object, typed: bool) -> None|bool|int|float|str|list|dict:
        if obj is None: return None
        if isinstance(obj, (bool,int,float,str)): return obj
        if isinstance(obj, list): return [self._serialize(it, typed) for it in obj]
        if isinstance(obj, dict): return {key: self._serialize(value, typed) for key, value in obj.items()}

        # Types that are not directly mapped to json types
        cls = type(obj)
        if cls in (set, tuple): obj = list(obj) # type: ignore
        elif cls.__module__ == 'builtins': obj = repr(obj)
        elif isinstance(obj, Enum): obj = obj.name
        elif isinstance(obj, datetime.datetime): obj = repr(obj)
        elif isinstance(obj, Path): obj = str(obj)
        elif hasattr(obj, 'to_dict'): obj = obj.to_dict() # type: ignore
        else: raise Exception(f"Can't serialize {obj} of type {cls}.")

        if isinstance(obj, list): obj = [self._serialize(it, typed) for it in obj]
        elif isinstance(obj, dict): obj = {key: self._serialize(value, typed) for key,value in obj.items()}
        
        if typed: return {_TYPE: get_full_classname(cls), _VALUE: obj}
        else: return obj # type: ignore
    @override
    def serialize(self, obj: object, typed:bool = True, indent:int|str|None=None) -> str:
        return json.dumps(self._serialize(obj, typed), indent=indent)
    
    def _deserialize(self, obj: dict|list|str|int|float|bool|None) -> Any:
        if obj is None: return None
        if isinstance(obj, (bool,int,float,str)): return obj
        if isinstance(obj, list): return [self._deserialize(it) for it in obj]
        if not isinstance(obj, dict): raise Exception(f"Can't deserialize {obj}.")

        result = {key: self._deserialize(value) for key, value in obj.items()}
        if result.keys() != {_TYPE, _VALUE}: return result
        cls = get_class_by_full_classname(result[_TYPE])
        value = result[_VALUE]

        if cls in (set, tuple): return cls(value)
        elif cls.__module__ == 'builtins': return eval(value)
        elif issubclass(cls, Enum): return cls[value] 
        elif cls == datetime.datetime: return eval(value)
        elif issubclass(cls, Path): return Path(value)
        elif hasattr(cls, 'from_dict'): return cls.from_dict(value) # type: ignore
        else: raise Exception(f"Can't deserialize {obj} into type {cls}.")
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

class SerializedObject(TypeDecorator):
    impl = String
    cache_ok = True

    def __init__(self, serializer: Serializer = serializer):
        super().__init__()
        self.serializer = serializer
    @override
    def process_bind_param(self, value, dialect) -> str:
        return self.serializer.serialize(value)
    @override
    def process_result_value(self, value, dialect):
        return self.serializer.deserialize(cast(str, value))
    