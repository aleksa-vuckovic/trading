#3
from __future__ import annotations
import json
import datetime
import zoneinfo
import builtins
from typing import Any, Self, cast, get_origin, override
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
    def to_json(self, obj: object) -> json_type: ...
    def from_json[T](self, data: json_type, assert_type: type[T]|None = None) -> T: ...
    def serialize(self, obj: object) -> str:
        return json.dumps(self.to_json(obj))
    def deserialize[T](self, data: str, assert_type: type[T]|None = None) -> T: 
        return self.from_json(json.loads(data), assert_type)

class BasicSerializer(Serializer):
    @override
    def serialize(self, obj: object) -> str:
        return json.dumps(obj)
    def deserialize[T](self, data, assert_type: type[T]|None = None) -> T:
        ret = json.loads(data)
        if assert_type: assert isinstance(ret, get_origin(assert_type) or assert_type)
        return ret
    
_TYPE = '#T'
_VALUE = '#V'
class GenericSerializer(Serializer):
    def __init__(self, typed: bool = True):
        self.typed = typed

    def _serialize(self, obj: object) -> json_type:
        if obj is None: return None
        if isinstance(obj, (bool,int,float,str)): return obj
        elif isinstance(obj, list):
            return [self._serialize(it) for it in obj]
        elif isinstance(obj, dict) and all(isinstance(it, str) for it in obj):
            return {key: self._serialize(value) for key, value in obj.items()}
        
        # objects that are not directly mapped to json
        if isinstance(obj, dict): val = [(self._serialize(key), self._serialize(value)) for key, value in obj.items()]
        elif isinstance(obj, (set, tuple)): val = [self._serialize(it) for it in obj]
        elif type(obj).__module__ == 'builtins': val = repr(obj)
        elif isinstance(obj, Enum): val = obj.name
        elif isinstance(obj, datetime.datetime): val = repr(obj)
        elif isinstance(obj, Path): val = str(obj)
        elif isinstance(obj, Serializable):
            val = obj.to_json()
            if isinstance(val, list): val = [self._serialize(it) for it in val]
            elif isinstance(val, dict): val = {key: self._serialize(value) for key,value in val.items()}
        else: raise Exception(f"Can't serialize {obj} of type {type(obj)}.")
        
        if self.typed:
            if isinstance(val, dict): val[_TYPE] = get_full_classname(obj)
            else: val = {_TYPE: get_full_classname(obj), _VALUE: val}
        return val
    @override
    def serialize(self, obj: object, *, indent:int|str|None=None) -> str:
        """
        Args:
            typed - When set to false, type info will not be preserved, and deserialization will
                yield undefined results! Use only for display, and never for preservation.
        """
        return json.dumps(self._serialize(obj), indent=indent)
    
    def _deserialize(self, obj: json_type) -> Any:
        if obj is None: return None
        if isinstance(obj, (bool,int,float,str)): return obj
        if isinstance(obj, list):
            return [self._deserialize(it) for it in obj]
        if isinstance(obj, dict) and _TYPE not in obj:
            return {key: self._deserialize(value) for key,value in obj.items()}
        if not isinstance(obj, dict): raise Exception(f"Can't deserialize {obj}.")

        cls = get_class_by_full_classname(obj[_TYPE])
        del obj[_TYPE]
        if _VALUE in obj: val = obj[_VALUE]
        else: val = obj
        
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
        if assert_type: assert isinstance(ret, get_origin(assert_type) or assert_type)
        return ret

serializer = GenericSerializer()

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
    