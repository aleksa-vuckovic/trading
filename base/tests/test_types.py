import unittest
from base.reflection import transient
from base.serialization import GenericSerializer
from base.types import Cloneable, Equatable, ReadonlyDict, Singleton

class TestSingleton(Singleton):
    def __init__(self):
        pass

class TestTypes(unittest.TestCase):
    def test_equatable(self):
        @transient('a')
        class A(Equatable):
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
    
    def test_equatable_hashing(self):
        class A(Equatable):
            def __init__(self, a, b, c):
                self.a = a
                self.b = b
                self.c = c
        
        obj1 = A(1,2,3)
        obj2 = A(1,2,obj1)
        obj3 = A(1, obj1, obj2)
        obj1_c = A(1,2,3)
        obj2_c = A(1,2,obj1_c)
        obj3_c = A(1, obj1_c, obj2_c)
        
        data = {obj1: 1, obj2: 2, obj3: 3}
        self.assertEqual(3, len(data))
        data[obj1_c] = 100
        data[obj2_c] = 200
        data[obj3_c] = 300
        self.assertEqual(3, len(data))
        self.assertEqual(300, data[obj3])
        self.assertEqual(300, data[obj3_c])

    def test_cloneable(self):
        class A(Equatable, Cloneable):
            def __init__(self, data: dict):
                self.data = data
        class B(Equatable):
            pass
        class C(Equatable, Cloneable):
            def __init__(self):
                self.a = {"a": [A({}), A({})]}
                self.b = 10
                self.c = (B(), B())
        a = C()
        b = a.clone()
        self.assertEqual(a, b)
        self.assertIsNot(a, b)
        self.assertIsNot(a.a, b.a)
        self.assertIsNot(a.a["a"][0], b.a["a"][0])
        self.assertIs(a.c[1], b.c[1])
        
    def test_readonly_dict(self):
        serializer = GenericSerializer()
        a = ReadonlyDict({"a": 1, "b": 2})
        self.assertEqual(1, a["a"])
        self.assertEqual({"a", "b"}, set(a.keys()))
        self.assertEqual(2, len(a))
        b = serializer.serialize(a)
        c = serializer.deserialize(b, ReadonlyDict)
        self.assertEqual(a,c)

    def test_singleton(self):
        serializer = GenericSerializer()
        self.assertIs(TestSingleton.instance, TestSingleton.instance)
        obj = serializer.deserialize(serializer.serialize(TestSingleton.instance), TestSingleton)
        self.assertIs(TestSingleton.instance, obj)
