from __future__ import annotations
import unittest
import json
from enum import Enum
from pathlib import Path
from base.reflection import transient
from base.serialization import TypedSerializer, Serializable, _TYPE, _VALUE
from base import dates
from base.types import Equatable

class MyEnum(Enum):
    A = 'a'
    B = 'b'

# skip_keys hierarchy
@transient('b')
class A(Equatable, Serializable):
    def __init__(self, a:int|None, b:str|None, c: list|None, d: A|None, e: object|None = None):
        self.a = a
        self.b = b
        self.c = c
        self.d = d
        self.e = e

    def f(self):
        pass

@transient('x')
class B(A):
    def __init__(self, a:int|None, b:str|None, c: list|None, d: A|None, e: object|None = None, x: int = 3, y: int = 5):
        super().__init__(a,b,c,d,e)
        self.x=x
        self.y=y

class C(Equatable, Serializable):
    def __init__(self, a: int):
        self.a = a

class E(Equatable, Serializable):
    def __init__(self, a: tuple):
        self.a = a

class Base(Serializable):
    def __init__(self):
        pass
class Derived(Base):
    def __init__(self):
        pass

class TestJsonutils(unittest.TestCase):

    def test_equatable_with_skips(self):
        self.assertEqual(A(1, 'a', [1,'c'], A(2, 'b', [], None)), A(1, 'b', [1,'c'], A(2, 'c', [], None)))
        self.assertNotEqual(A(1, 'b', [], A(3, 'c', [], None)), A(1, 'b', [], A(2, 'c', [], None)))
        self.assertNotEqual(A(1, 'a', [1,'c'], A(2, 'b', [], None)), A(1, 'b', [2,'c'], A(2, 'c', [], None)))
        self.assertNotEqual(A(1, 'a', [1,'c'], A(2, 'b', [], None)), A(1, 'b', [1,'c'], None))

        self.assertEqual(B(1, 'a', [], None, x=1, y=2), B(1, 'b', [], None, x=2, y=2))
        self.assertNotEqual(B(1, 'a', [], None, x=1, y=3), B(1, 'b', [], None, x=2, y=2))

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
        data_s = serializer.serialize(data, typed=True)
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
            C(1): "Value 2",
            C(2): "Value 3",
            "abc": "Value 4"
        }
        data_s = serializer.serialize(data)
        data_d = serializer.deserialize(data_s)
        self.assertEqual(data, data_d)
        self.assertEqual({MyEnum.A, C(1), C(2), "abc"}, set(data_d.keys()))

    def test_serializable_inheritance(self):
        serializer = TypedSerializer()
        a = Derived()
        a_s = serializer.serialize(a)
        a_d = serializer.deserialize(a_s, Derived)
        self.assertIsInstance(a_d, Derived)
