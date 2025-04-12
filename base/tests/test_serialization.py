from __future__ import annotations
import unittest
import json
from enum import Enum
from pathlib import Path
from base.serialization import serializable, TypedSerializer, Serializable, _TYPE, _VALUE
from base.types import equatable
from base import dates

class MyEnum(Enum):
    A = 'a'
    B = 'b'

# skip_keys hierarchy
@serializable(skip_keys=['b'])
@equatable(skip_keys=['b'])
class A(Serializable):
    def __init__(self, a:int|None, b:str|None, c: list|None, d: A|None, e: object|None = None):
        self.a = a
        self.b = b
        self.c = c
        self.d = d
        self.e = e

    def f(self):
        pass

@serializable(skip_keys=['x'])
@equatable(skip_keys=['x'])
class B(A):
    def __init__(self, a:int|None, b:str|None, c: list|None, d: A|None, e: object|None = None, x: int = 3, y: int = 5):
        super().__init__(a,b,c,d,e)
        self.x=x
        self.y=y

# include_keys hierarchy
@serializable(include_keys='a')
@equatable(include_keys=['a'])
class C(Serializable):
    def __init__(self, a: int, b: int):
        self.a = a
        self.b = b
@serializable(include_keys='x')
@equatable(include_keys=['x'])
class D(C):
    def __init__(self, a: int, b: int, x: int, y: int):
        super().__init__(a, b)
        self.x = x
        self.y = y

@serializable()
@equatable()
class E(Serializable):
    def __init__(self, a: tuple):
        self.a = a

class TestJsonutils(unittest.TestCase):

    def test_equatable_with_skips(self):
        self.assertEqual(A(1, 'a', [1,'c'], A(2, 'b', [], None)), A(1, 'b', [1,'c'], A(2, 'c', [], None)))
        self.assertNotEqual(A(1, 'b', [], A(3, 'c', [], None)), A(1, 'b', [], A(2, 'c', [], None)))
        self.assertNotEqual(A(1, 'a', [1,'c'], A(2, 'b', [], None)), A(1, 'b', [2,'c'], A(2, 'c', [], None)))
        self.assertNotEqual(A(1, 'a', [1,'c'], A(2, 'b', [], None)), A(1, 'b', [1,'c'], None))

        self.assertEqual(B(1, 'a', [], None, x=1, y=2), B(1, 'b', [], None, x=2, y=2))
        self.assertNotEqual(B(1, 'a', [], None, x=1, y=3), B(1, 'b', [], None, x=2, y=2))
    
    def test_equatable_with_includes(self):
        self.assertEqual(C(1, 2), C(1, 3))
        self.assertNotEqual(C(2, 2), C(1, 3))

        self.assertEqual(D(1,2,3,4), D(1,20,3,40))
        self.assertNotEqual(D(2,2,3,4), D(1,20,3,40))
        self.assertNotEqual(D(1,2,4,4), D(1,20,3,40))

    def test_typed_serializer_with_skips(self):
        serializer = TypedSerializer()
        def do(obj): return serializer.deserialize(serializer.serialize(obj))
        a = A(1, 'hello', [1,2,3], None)
        b = A(2, 'world', [], a)
        self.assertEqual(b, do(b))

        c = A(3, None, [a,b], a, {'x': a, 'y': b, 'z': MyEnum.A})
        self.assertEqual(c, do(c))

        a = B(None, None, None, None, None)
        a_s = serializer.serialize(a)
        b = json.loads(a_s)
        self.assertFalse('x' in b[_VALUE])
        self.assertFalse('b' in b[_VALUE])
        self.assertTrue('y' in b[_VALUE])
    
    def test_typed_serializer_with_includes(self):
        serializer = TypedSerializer()
        def do(obj): return serializer.deserialize(serializer.serialize(obj))
        c = C(1,2)
        self.assertEqual(c, do(c))
        d = D(1,2,3,4)
        self.assertEqual(d, do(d))

        d = json.loads(serializer.serialize(d))
        self.assertTrue('a' in d[_VALUE] and 'x' in d[_VALUE])
        self.assertFalse('b' in d[_VALUE] or 'y' in d[_VALUE])

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
                'c': E(('a', 'b', 'c', 1))
            }
        }
        data_s = serializer.serialize(data, True)
        data_d = serializer.deserialize(data_s, dict)
        self.assertEqual(data, data_d)

    def test_typed_serializer_path(self):
        serializer = TypedSerializer()
        data = {
            "p1": Path("./hello.txt"),
            "p2": Path("D:/test/abc")
        }
        data_s = serializer.serialize(data)
        data_d = serializer.deserialize(data_s, dict)
        self.assertEqual(data, data_d)

    def test_typed_serializer_dict_with_object_keys(self):
        serializer = TypedSerializer()
        data = {
            MyEnum.A: "Value 1",
            C(1, 0): "Value 2",
            C(2, 0): "Value 3",
            "abc": "Value 4"
        }
        data_s = serializer.serialize(data)
        print("--------------------")
        print(data_s)
        print("---------------------")
        data_d = serializer.deserialize(data_s)
        self.assertEqual(data, data_d)
        self.assertEqual({MyEnum.A, C(1,0), C(2,0), "abc"}, set(data_d.keys()))
