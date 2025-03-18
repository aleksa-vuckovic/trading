from __future__ import annotations
import unittest
import json
from enum import Enum
from base.serialization import serializable, TypedSerializer
from base.classes import equatable

class MyEnum(Enum):
    A = 'a'
    B = 'b'

serializer = TypedSerializer()
@serializable(skip_keys=['b'])
@equatable(skip_keys=['b'])
class A:
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

class TestJsonutils(unittest.TestCase):

    def test_serializable(self):
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
        self.assertFalse('x' in b)
        self.assertTrue('y' in b)