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
        self.assertEqual(0, common.binary_search(collection, get_time, 0, common.BinarySearchEdge.HIGH))
        self.assertEqual(None, common.binary_search(collection, get_time, 0, common.BinarySearchEdge.LOW))
        self.assertEqual(0, common.binary_search(collection, get_time, 1, common.BinarySearchEdge.HIGH))
        self.assertEqual(1, common.binary_search(collection, get_time, 1.5, common.BinarySearchEdge.HIGH))
        self.assertEqual(0, common.binary_search(collection, get_time, 1.5, common.BinarySearchEdge.LOW))
        self.assertEqual(None, common.binary_search(collection, get_time, 1.5, common.BinarySearchEdge.NONE))
        self.assertEqual(4, common.binary_search(collection, get_time, 10.5, common.BinarySearchEdge.LOW))
        collection.pop()
        self.assertEqual(2, common.binary_search(collection, get_time, 3, common.BinarySearchEdge.HIGH))
