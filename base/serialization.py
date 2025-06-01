#3
from __future__ import annotations
import json
import datetime
import zoneinfo #ignore unused import
import builtins #ignore unused import
from types import NoneType, UnionType
from typing import Any, Literal, Self, Union, cast, final, get_args, get_origin, override
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
        if isinstance(obj, (bool, int, float, str)): return obj
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
    
    def _from_json(self, data: json_type) -> Any:
        if data is None: return None
        if isinstance(data, (bool, int, float, str)):
            return data
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
        if cls == dict:
            return {self.from_json(key):self.from_json(value) for key,value in cast(list[tuple], val)}
        elif cls in (set, tuple):
            return cls(self.from_json(it) for it in cast(list, val))
        elif cls.__module__ == 'builtins':
            return eval(cast(str, val))
        elif issubclass(cls, Enum):
            return cls[cast(str, val)] 
        elif cls == datetime.datetime:
            return eval(cast(str, val))
        elif issubclass(cls, Path):
            return Path(cast(str, val))
        elif issubclass(cls, Serializable):
            if isinstance(val, list):
                return cls.from_json([self.from_json(it) for it in val])
            elif isinstance(val, dict):
                return cls.from_json({key: self.from_json(value) for key,value in val.items()})
            else:
                return cls.from_json(val)
        else: raise Exception(f"Can't deserialize {val} into type {cls}.")

    @override
    def from_json[T](self, data: json_type, assert_type: type[T]|None=None) -> T:
        ret = self._from_json(data)
        if assert_type: assert isinstance(ret, get_origin(assert_type) or assert_type)
        return ret

def _assert_is[T](obj: object, cls: type[T]) -> T:
    assert isinstance(obj, cls)
    return obj
class ContractSerializer(Serializer):
    @override
    def to_json(self, obj: object) -> json_type:
        if obj is None:
            return None
        if isinstance(obj, (bool, int, float, str)):
            return obj
        if isinstance(obj, list):
            return [self.to_json(it) for it in obj]
        if isinstance(obj, dict):
            return {_assert_is(key, str): self.to_json(value) for key, value in obj.items()}
        return {key: self.to_json(value) for key, value in obj.__dict__.items()}
    
    @override
    def from_json[T](self, data: json_type, assert_type: type[T] | None = None) -> T:
        assert assert_type, "ContractSerializer requires a type to be specified."
        type = get_origin(assert_type) or assert_type
        args = get_args(assert_type)
        if type in (Union, UnionType):
            for arg in args:
                try:
                    return self.from_json(data, arg)
                except AssertionError:
                    pass
            raise AssertionError(f"Object {data} can't be deserialized as {assert_type}.")
        if type == Literal:
            assert any(data == it for it in args)
            return data #type: ignore
        if type == NoneType:
            assert data is None
            return data #type: ignore
        if type in (bool, int, float, str):
            assert isinstance(data, type)
            return data #type: ignore
        if type == list:
            assert isinstance(data, list)
            if args:
                return [self.from_json(it, args[0]) for it in data] #type: ignore
            else:
                return data #type: ignore
        assert isinstance(data, dict)
        if type == dict:
            if args:
                assert args[0] == str
                return {key: self.from_json(value, args[1]) for key, value in data.items()} #type: ignore
            else:
                return data #type: ignore
        assert not args
        try:
            ret = object.__new__(type)
        except:
            return None #type: ignore
        for field_name, field_type in type.__annotations__.items():
            ret.__dict__[field_name] = self.from_json(data[field_name], field_type)
        return  ret

class SerializedObject(TypeDecorator):
    impl = String
    cache_ok = True

    def __init__(self, serializer: Serializer = GenericSerializer()):
        super().__init__()
        self.serializer = serializer
    @override
    def process_bind_param(self, value, dialect) -> str:
        return self.serializer.serialize(value)
    @override
    def process_result_value(self, value, dialect):
        return self.serializer.deserialize(cast(str, value))
    