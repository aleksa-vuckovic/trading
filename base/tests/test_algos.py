from unittest import TestCase
from base.algos import SearchSide, binary_search, binsert, interpolate, is_sorted, lower_whole, upper_whole
from base.types import Equatable

class TestAlgos(TestCase):
    def test_binary_search(self):
        collection = [
            {"time": 1},
            {"time": 3},
            {"time": 5},
            {"time": 8},
            {"time": 10}
        ]
        def get_time(item):
            return item["time"]
        examples: list[tuple[float, SearchSide, int|None]] = [
            (0, 'EQ', None),    (0, 'GE', 0),   (0, 'GT', 0),   (0, 'LE', -1),  (0, 'LT', -1),
            (1, 'EQ', 0),       (1, 'GE', 0),   (1, 'GT', 1),   (1, 'LE', 0),   (1, 'LT', -1),
            (2, 'EQ', None),    (2, 'GE', 1),   (2, 'GT', 1),   (2, 'LE', 0),   (2, 'LT', 0),
            (9, 'EQ', None),    (9, 'GE', 4),   (9, 'GT', 4),   (9, 'LE', 3),   (9, 'LT', 3),
            (10, 'EQ', 4),      (10, 'GE', 4),  (10, 'GT', 5),  (10, 'LE', 4),  (10, 'LT', 3),
            (11, 'EQ', None),   (11, 'GE', 5),  (11, 'GT', 5),  (11, 'LE', 4),  (11, 'LT', 4),
        ]
        for num, side, expect in examples:
            self.assertEqual(expect, binary_search(collection, num, key=get_time, side=side))

        collection.pop()
        examples = [(4, 'EQ', None), (4, 'GE', 2), (4, 'GT', 2), (4, 'LE', 1), (4, 'LT', 1)]
        for num, side, expect in examples:
            self.assertEqual(expect, binary_search(collection, num, key=get_time, side=side))
    
    def test_binsert(self):
        class A(Equatable):
            def __init__(self, x: int):
                self.x = x
        collection = [A(1), A(3)]
        binsert(collection, A(2), key=lambda it: it.x)
        binsert(collection, A(0), key=lambda it: it.x)
        binsert(collection, A(4), key=lambda it: it.x)
        self.assertEqual([A(0),A(1),A(2),A(3),A(4)], collection)

        collection = [1,3]
        binsert(collection, 4)
        binsert(collection, 2)
        binsert(collection, 0)
        self.assertEqual([0,1,2,3,4], collection)

    def test_is_sorted(self):
        true_examples = [[1,1,5,6],[],[1],[1.2,1.2,1.20001,1231.2]]
        false_examples = [[2,1,23456],[2,1.999],[1,2,3,4,5,4.99]]

        for example in true_examples: self.assertTrue(is_sorted(example), f"Expected sorted for {example}.")
        for example in false_examples: self.assertFalse(is_sorted(example), f"Expected not sorted for {example}.")

    def test_interpolate_linear_edge(self):
        x = [2,5,7]
        y = [1,2,3]
        x_ret = list(range(1,11))
        expect = [1.0,1.0,4/3,5/3,2.0,2.5,3.0,3.0,3.0,3.0]
        result = interpolate(x, y, x_ret, method='linear_edge')
        for i in range(len(expect)):
            self.assertAlmostEqual(expect[i], result[i], None, f"\n{expect} !=\n{result}")

        x = [1,5,7]
        y = [1,2,4]
        x_ret = [1,2,5,6,7]
        expect = [1,5/4,2,3,4]
        result = interpolate(x, y, x_ret, method='linear_edge')
        self.assertEqual(expect, result)
        
        x_ret = [0,*x_ret,8]
        expect = [1, *expect, 4]
        result = interpolate(x, y, x_ret, method='linear_edge')
        self.assertEqual(expect, result)

    def test_interpolate_linear(self):
        cases: list[tuple[list[float],list[float],float,float]] = [
            ([], [], 0, 0),
            ([4], [10], 0, 10),
            (
                [1,1.4,2,2.5,5],
                [10,12,14,15,25],
                3.683252427, 6.433859223
            )
        ]

        y_ret = [1,20,50,123,500]
        for x,y,k,n in cases:
            expect = [k*it+n for it in y_ret]
            result = interpolate(x,y,y_ret,method='linear')
            for a,b in zip(expect,result):
                self.assertAlmostEqual(a,b,3)


    def test_interpolate_stair(self):
        x = [1, 10, 100, 1000]
        y = [2, 20, 200, 2000]
        x_ret = [0, 1, 5, 10, 50, 100, 500, 1000, 1500]
        expect = [2, 2, 2, 20, 20, 200, 200, 2000, 2000]
        result = interpolate(x, y, x_ret, 'stair')
        self.assertEqual(expect, result)

        class A:
            pass
        y = [A(), A(), A(), A()]
        expect = [y[0], y[0], y[0], y[1], y[1], y[2], y[2], y[3], y[3]]
        result = interpolate(x, y, x_ret, 'stair')
        self.assertEqual(expect, result)

    def test_upper_whole(self):
        self.assertEqual(10.5, upper_whole(9, 3.5))
        self.assertEqual(10.5, upper_whole(10.5, 3.5))
    
    def test_lower_whole(self):
        self.assertEqual(7, lower_whole(9, 3.5))
        self.assertEqual(7, lower_whole(9, 3.5))
    