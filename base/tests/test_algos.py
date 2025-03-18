import unittest
from base.algos import binary_search, BinarySearchEdge, is_sorted

class TestAlgos(unittest.TestCase):
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
        self.assertEqual(0, binary_search(collection, 0, key=get_time, edge=BinarySearchEdge.HIGH))
        self.assertEqual(-1, binary_search(collection, 0, key=get_time, edge=BinarySearchEdge.LOW))
        self.assertEqual(0, binary_search(collection, 1, key=get_time, edge=BinarySearchEdge.HIGH))
        self.assertEqual(1, binary_search(collection, 1.5, key=get_time, edge=BinarySearchEdge.HIGH))
        self.assertEqual(0, binary_search(collection, 1.5, key=get_time, edge=BinarySearchEdge.LOW))
        self.assertIsNone(binary_search(collection, 1.5, key=get_time, edge=BinarySearchEdge.NONE))
        self.assertEqual(4, binary_search(collection, 10.5, key=get_time, edge=BinarySearchEdge.LOW))
        collection.pop()
        self.assertEqual(2, binary_search(collection, 3, key=get_time, edge=BinarySearchEdge.HIGH))

    def test_is_sorted(self):
        true_examples = [[1,1,5,6],[],[1],[1.2,1.2,1.20001,1231.2]]
        false_examples = [[2,1,23456],[2,1.999],[1,2,3,4,5,4.99]]

        for example in true_examples: self.assertTrue(is_sorted(example), f"Expected sorted for {example}.")
        for example in false_examples: self.assertFalse(is_sorted(example), f"Expected not sorted for {example}.")

