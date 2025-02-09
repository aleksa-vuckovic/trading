import unittest
from ..utils import dateutils
from . import wsj

class TestWsj(unittest.TestCase):

    def test_merge_30m_to_1h(self):
        start = dateutils.str_to_unix('2025-01-10 08:00:00')
        input = [{'t': start+i*1800, 'o': i, 'h': i, 'l': i, 'c': i, 'v': i}for i in range(25)]
        expect = [{'t': start+(i+(1 if i < 16 else 0))*1800 , 'o': i, 'h': i+1 if i < 16 else i, 'l': i, 'c': i+1 if i < 16 else i, 'v': 2*i+1 if i < 16 else i} for i in range(4, 17, 2)]
        result = wsj._merge_data_1h(input)
        self.assertEqual(expect, result)

        start += 4*1800
        good = [{'t': start+i*1800, 'o': i, 'h': i, 'l': i, 'c': i, 'v': i} for i in [0,2]]
        bad = {'t': start + 5*1800, 'o': 3, 'h': 3, 'l': 3, 'c': 3, 'v': 3}
        input = [*good, bad]
        expect = good
        result = wsj._merge_data_1h(input)
        self.assertEqual(expect, result)

    def test_get_pricing(self):
        pass