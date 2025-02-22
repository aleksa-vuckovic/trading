import json
from typing import get_origin
from types import GenericAlias

def _fail(obj: object):
    raise Exception(f"Can't serialize {obj}.")

def serialize(obj: object, indent:int|str|None=None) -> str:
    return json.dumps(obj, default=lambda it: \
        it.to_dict() if hasattr(it, 'to_dict')\
        else it.to_list() if hasattr(it, 'to_list')\
        else it.to_str() if hasattr(it, 'to_str')\
        else _fail(obj), indent=indent)

def deserialize(data: str, type: type) -> str:
    return _deserialize(json.loads(data), type)
def _deserialize(obj: dict|list|str|int|float|bool, type: type):
    if type is bool or type is int or type is float or type is str or type is list or type is dict or type is tuple:
        return type(obj)
    if isinstance(obj, dict) and hasattr(type, 'from_dict'):
        return type.from_dict(obj)
    if isinstance(obj, list) and hasattr(type, 'from_list'):
        return type.from_list(obj)
    if isinstance(obj, str) and hasattr(type, 'from_str'):
        return type.from_str(obj)
    if isinstance(type, GenericAlias):
        underlying = get_origin(type)
        args = type.__args__
        if isinstance(obj, list) and underlying is list:
            return [_deserialize(it, args[0]) for it in obj]
        if isinstance(obj, dict) and underlying is dict and args[0] is str:
            return { key: _deserialize(obj[key], args[1]) for key in obj }
    raise Exception(f"Can't deserialize {obj} to {type}.")