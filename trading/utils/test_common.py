import unittest
from . import common

class TestCommon(unittest.TestCase):
    def test_equatable(self):
        @common.equatable(skip_keys=['a'])
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

    def test_binary_search(self):
        collection = [
            {"time": 1},
            {"time": 2},
            {"time": 5},
            {"time": 8},
            {"time": 10}
        ]
        def get_time(item):
            return item["time"]
        self.assertEqual(0, common.binary_search(collection, 0, key=get_time, edge=common.BinarySearchEdge.HIGH))
        self.assertEqual(-1, common.binary_search(collection, 0, key=get_time, edge=common.BinarySearchEdge.LOW))
        self.assertEqual(0, common.binary_search(collection, 1, key=get_time, edge=common.BinarySearchEdge.HIGH))
        self.assertEqual(1, common.binary_search(collection, 1.5, key=get_time, edge=common.BinarySearchEdge.HIGH))
        self.assertEqual(0, common.binary_search(collection, 1.5, key=get_time, edge=common.BinarySearchEdge.LOW))
        self.assertEqual(None, common.binary_search(collection, 1.5, key=get_time, edge=common.BinarySearchEdge.NONE))
        self.assertEqual(4, common.binary_search(collection, 10.5, key=get_time, edge=common.BinarySearchEdge.LOW))
        collection.pop()
        self.assertEqual(2, common.binary_search(collection, 3, key=get_time, edge=common.BinarySearchEdge.HIGH))
