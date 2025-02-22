import json
from typing import get_origin
from types import GenericAlias
from enum import Enum
from .common import get_full_classname, get_class_by_full_classname

_TYPE = '$$type'

def _serialize_default(obj: object) -> dict:
    result = None
    if hasattr(obj, 'to_dict'): result = obj.to_dict()
    elif isinstance(obj, Enum): result = {'name': obj.name}
    if result is not None: return {_TYPE: get_full_classname(obj), **result}
    raise Exception(f"Can't serialize {obj}.")
def serialize(obj: object, indent:int|str|None=None) -> str:
    return json.dumps(obj, default=_serialize_default)

def deserialize(data: str) -> str:
    return _deserialize(json.loads(data))
def _deserialize(obj: dict|list|str|int|float|bool):
    if isinstance(obj, (bool, int, float, str)): return obj
    if isinstance(obj, list):
        return [_deserialize(it) for it in obj]
    if isinstance(obj, dict):
        if _TYPE not in obj: return obj
        cls = get_class_by_full_classname(obj[_TYPE])
        if hasattr(cls, 'from_dict'): return cls.from_dict(obj)
        if issubclass(cls, Enum): return cls[obj['name']]
        raise Exception(f"Can't deserialize {obj}.")