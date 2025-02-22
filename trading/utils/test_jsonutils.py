from __future__ import annotations
import unittest
import json
from enum import Enum
from . import jsonutils, common

class MyEnum(Enum):
    A = 'a'
    B = 'b'

@jsonutils.serializable(skip_keys=['b'])
@common.equatable(skip_keys=['b'])
class A:
    def __init__(self, a:int, b:str, c: list, d: A, e: object = None):
        self.a = a
        self.b = b
        self.c = c
        self.d = d
        self.e = e

    def f(self):
        pass

@jsonutils.serializable(skip_keys=['x'])
class B(A):
    def __init__(self, a:int, b:str, c: list, d: A, e: object = None, x: int = 3, y: int = 5):
        super().__init__(a,b,c,d,e)
        self.x=x
        self.y=y

class TestJsonutils(unittest.TestCase):

    def test_serializable(self):
        a = A(1, 'hello', [1,2,3], None)
        b = A(2, 'world', [], a)
        b_s = jsonutils.serialize(b)
        b_d = jsonutils.deserialize(b_s)
        self.assertEqual(b, b_d)

        c = A(3, None, [a,b], a, {'x': a, 'y': b, 'z': MyEnum.A})
        c_s = jsonutils.serialize(c)
        c_d = jsonutils.deserialize(c_s)
        self.assertEqual(c, c_d)

        a = B(None, None, None, None, None, None, None)
        a_s = jsonutils.serialize(a)
        b = json.loads(a_s)
        self.assertFalse('x' in b)
        self.assertTrue('y' in b)