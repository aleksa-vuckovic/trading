import unittest
from base.classes import equatable

class TestCommon(unittest.TestCase):
    def test_equatable(self):
        @equatable(skip_keys=['a'])
        class A:
            def __init__(self, a, b, c):
                self.a = a
                self.b = b
                self.c = c
        
        obj1 = A(1,2,3)
        obj2 = A(2,2,3)
        obj3 = A(1,3,3)
        self.assertEqual(obj1, obj2)
        self.assertNotEqual(obj1, obj3)

        obj4 = A(0,0,obj1)
        obj5 = A(1,0,obj2)
        obj6 = A(0,0,obj3)
        self.assertEqual(obj4, obj5)
        self.assertNotEqual(obj4, obj6)
