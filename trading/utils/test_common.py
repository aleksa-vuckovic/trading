import unittest
from . import common

class TestCommon(unittest.TestCase):
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
