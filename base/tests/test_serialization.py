from __future__ import annotations
import unittest
import json
from enum import Enum
from base.serialization import serializable, TypedSerializer, Serializable, _TYPE, _VALUE
from base.types import equatable
from base import dates

class MyEnum(Enum):
    A = 'a'
    B = 'b'

@serializable(skip_keys=['b'])
@equatable(skip_keys=['b'])
class A(Serializable):
    def __init__(self, a:int|None, b:str|None, c: list|None, d: A|None, e: object = None):
        self.a = a
        self.b = b
        self.c = c
        self.d = d
        self.e = e

    def f(self):
        pass

@serializable(skip_keys=['x'])
class B(A):
    def __init__(self, a:int|None, b:str|None, c: list|None, d: A|None, e: object = None, x: int = 3, y: int = 5):
        super().__init__(a,b,c,d,e)
        self.x=x
        self.y=y

@serializable()
@equatable()
class C(Serializable):
    def __init__(self, a: tuple):
        self.a = a

class TestJsonutils(unittest.TestCase):

    def test_typed_serializer(self):
        serializer = TypedSerializer()
        a = A(1, 'hello', [1,2,3], None)
        b = A(2, 'world', [], a)
        b_s = serializer.serialize(b)
        b_d = serializer.deserialize(b_s)
        self.assertEqual(b, b_d)

        c = A(3, None, [a,b], a, {'x': a, 'y': b, 'z': MyEnum.A})
        c_s = serializer.serialize(c)
        c_d = serializer.deserialize(c_s)
        self.assertEqual(c, c_d)

        a = B(None, None, None, None, None)
        a_s = serializer.serialize(a)
        b = json.loads(a_s)
        self.assertFalse('x' in b[_VALUE])
        self.assertTrue('y' in b[_VALUE])

    def test_typed_serializer_datetime(self):
        serializer = TypedSerializer()
        data = {
            'somedate': dates.now(tz=dates.ET),
            'otherdate': dates.str_to_datetime('2020-01-01 00:00:00', tz=dates.CET)
        }
        serialized = serializer.serialize(data)
        result = serializer.deserialize(serialized)

        self.assertEqual(data, result)
    
    def test_typed_serializer_tuple(self):
        serializer = TypedSerializer()
        data = {
            't1': (1,2,3),
            't2': ((10,20,30), 11, 12, 13, [1, 2, 3, (4,5,6)]),
            't3': {
                'c': C(('a', 'b', 'c', 1))
            }
        }
        data_s = serializer.serialize(data, True)
        data_d = serializer.deserialize(data_s, dict)
        self.assertEqual(data, data_d)
