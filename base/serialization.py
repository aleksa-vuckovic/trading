#3
from __future__ import annotations
import json
import datetime
import zoneinfo
import builtins
from typing import Any, Self, cast, overload, override
from sqlalchemy import String, TypeDecorator
from enum import Enum
from pathlib import Path
from base.reflection import get_full_classname, get_class_by_full_classname, get_no_args_cnst, get_trainsent

type json_type = None|bool|int|float|str|list|dict
class Serializable:
    def to_json(self) -> json_type:
        skips = get_trainsent(type(self))
        return {key:self.__dict__[key] for key in self.__dict__ if key not in skips}
    @classmethod
    def from_json(cls: type[Self], data: json_type) -> Self:
        assert isinstance(data, dict)
        result = get_no_args_cnst(cls)()
        result.__dict__.update(data)
        return result

class Serializer:
    def serialize(self, obj: object) -> str:
        raise NotImplementedError()
    def deserialize[T](self, data: str, assert_type: type[T]|None = None) -> T:
        raise NotImplementedError()

class BasicSerializer(Serializer):
    @override
    def serialize(self, obj: object) -> str:
        return json.dumps(obj)
    def deserialize[T](self, data, assert_type: type[T]|None = None) -> T:
        ret = json.loads(data)
        if assert_type: assert isinstance(ret, assert_type)
        return ret
    
_TYPE = '$T'
_VALUE = '$V'
class TypedSerializer(Serializer):
    def _serialize(self, obj: object, typed: bool) -> json_type:
        if obj is None: return None
        if isinstance(obj, (bool,int,float,str)): return obj
        elif isinstance(obj, list): return [self._serialize(it, typed) for it in obj]
        
        # classes that are not directly mapped to json
        if isinstance(obj, dict): val = [(self._serialize(key, typed), self._serialize(value, typed)) for key, value in obj.items()]
        elif isinstance(obj, (set, tuple)): val = [self._serialize(it, typed) for it in obj]
        elif type(obj).__module__ == 'builtins': val = repr(obj)
        elif isinstance(obj, Enum): val = obj.name
        elif isinstance(obj, datetime.datetime): val = repr(obj)
        elif isinstance(obj, Path): val = str(obj)
        elif isinstance(obj, Serializable):
            val = obj.to_json()
            if isinstance(val, list): val = [self._serialize(it, typed) for it in val]
            elif isinstance(val, dict): val = {key: self._serialize(value, typed) for key,value in val.items()}
        else: raise Exception(f"Can't serialize {obj} of type {type(obj)}.")
        
        if typed: return {_TYPE: get_full_classname(obj), _VALUE: val}
        else: return val
    @override
    def serialize(self, obj: object, *, indent:int|str|None=None, typed:bool = True) -> str:
        """
        Args:
            typed - When set to false, type info will not be preserved, and deserialization will
                yield undefined results! Use only for display, and never for preservation.
        """
        return json.dumps(self._serialize(obj, typed), indent=indent)
    
    def _deserialize(self, obj: json_type) -> Any:
        if obj is None: return None
        if isinstance(obj, (bool,int,float,str)): return obj
        if isinstance(obj, list): return [self._deserialize(it) for it in obj]
        if not isinstance(obj, dict) or obj.keys() != {_TYPE, _VALUE}: raise Exception(f"Can't deserialize {obj}.")

        cls = get_class_by_full_classname(obj[_TYPE])
        val = obj[_VALUE]

        if cls == dict: return {self._deserialize(key):self._deserialize(value) for key,value in cast(list[tuple], val)}
        elif cls in (set, tuple): return cls(self._deserialize(it) for it in cast(list, val))
        elif cls.__module__ == 'builtins': return eval(cast(str, val))
        elif issubclass(cls, Enum): return cls[cast(str, val)] 
        elif cls == datetime.datetime: return eval(cast(str, val))
        elif issubclass(cls, Path): return Path(cast(str, val))
        elif issubclass(cls, Serializable):
            if isinstance(val, list): val = [self._deserialize(it) for it in val]
            elif isinstance(val, dict): val = {key: self._deserialize(value) for key,value in val.items()}
            return cls.from_json(val)
        else: raise Exception(f"Can't deserialize {val} into type {cls}.")
    @override
    def deserialize[T](self, data: str, assert_type: type[T]|None = None) -> T:
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
    