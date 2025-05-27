#3
from __future__ import annotations
import json
import datetime
import zoneinfo
import builtins
from typing import Any, Self, cast, final, get_origin, override
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
    @final
    def serialize(self, obj: object, indent: int|str|None=None) -> str:
        return json.dumps(self.to_json(obj), indent=indent)
    @final
    def deserialize[T](self, data: str, assert_type: type[T]|None = None) -> T: 
        return self.from_json(json.loads(data), assert_type)

class BasicSerializer(Serializer):
    @override
    def to_json(self, obj: object) -> json_type: return cast(json_type, obj)
    @override
    def from_json[T](self, data: json_type, assert_type: type[T]|None = None) -> T:
        if assert_type: assert isinstance(data, assert_type)
        return cast(T, data)
    
_TYPE = '#T'
_VALUE = '#V'
class GenericSerializer(Serializer):
    def __init__(self, typed: bool = True):
        self.typed = typed

    @override
    def to_json(self, obj: object) -> json_type:
        if obj is None: return None
        if isinstance(obj, (bool,int,float,str)): return obj
        elif isinstance(obj, list):
            return [self.to_json(it) for it in obj]
        elif isinstance(obj, dict) and all(isinstance(it, str) for it in obj):
            return {key: self.to_json(value) for key, value in obj.items()}
        
        # objects that are not directly mapped to json
        if isinstance(obj, dict): val = [(self.to_json(key), self.to_json(value)) for key, value in obj.items()]
        elif isinstance(obj, (set, tuple)): val = [self.to_json(it) for it in obj]
        elif type(obj).__module__ == 'builtins': val = repr(obj)
        elif isinstance(obj, Enum): val = obj.name
        elif isinstance(obj, datetime.datetime): val = repr(obj)
        elif isinstance(obj, Path): val = str(obj)
        elif isinstance(obj, Serializable):
            val = obj.to_json()
            if isinstance(val, list): val = [self.to_json(it) for it in val]
            elif isinstance(val, dict): val = {key: self.to_json(value) for key,value in val.items()}
        else: raise Exception(f"Can't serialize {obj} of type {type(obj)}.")
        
        if self.typed:
            if isinstance(val, dict): val[_TYPE] = get_full_classname(obj)
            else: val = {_TYPE: get_full_classname(obj), _VALUE: val}
        return val
    
    @override
    def from_json[T](self, data: json_type, assert_type: type[T]|None=None) -> Any:
        if data is None: return None
        if isinstance(data, (bool,int,float,str)): return data
        if isinstance(data, list):
            return [self.from_json(it) for it in data]
        if isinstance(data, dict) and _TYPE not in data:
            return {key: self.from_json(value) for key,value in data.items()}
        if not isinstance(data, dict): raise Exception(f"Can't deserialize {data}.")

        cls = get_class_by_full_classname(data[_TYPE])
        del data[_TYPE]
        if _VALUE in data: val = data[_VALUE]
        else: val = data
        
        ret: Any = None
        if cls == dict: ret = {self.from_json(key):self.from_json(value) for key,value in cast(list[tuple], val)}
        elif cls in (set, tuple): ret = cls(self.from_json(it) for it in cast(list, val))
        elif cls.__module__ == 'builtins': ret = eval(cast(str, val))
        elif issubclass(cls, Enum): ret = cls[cast(str, val)] 
        elif cls == datetime.datetime: ret = eval(cast(str, val))
        elif issubclass(cls, Path): ret = Path(cast(str, val))
        elif issubclass(cls, Serializable):
            if isinstance(val, list): ret = cls.from_json([self.from_json(it) for it in val])
            elif isinstance(val, dict): ret = cls.from_json({key: self.from_json(value) for key,value in val.items()})
            else: ret = cls.from_json(val)
        else: raise Exception(f"Can't deserialize {val} into type {cls}.")

        if assert_type: assert isinstance(ret, assert_type)
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
    