import unittest
from . import aggregate

class TestAggregate(unittest.TestCase):
    def test_interpolate_pricing(self):
        raw_data = ([1,2,3],[4,5,6])
        raw_timestamps = [2,5,7]
        timestamps = list(range(1,11))
        data = aggregate.interpolate_pricing(raw_timestamps, raw_data, timestamps)
        t1 = sum(x-y for x,y in zip(data[0], [1.0,1.0,4/3,5/3,2.0,2.5,3.0,3.0,3.0,3.0]))
        t2 = sum(x-y for x,y in zip(data[1], [4.0,4.0,13/3,14/3,5.0,5.5,6.0,6.0,6.0,6.0]))
        self.assertAlmostEqual(t1, 0)
        self.assertAlmostEqual(t2, 0)