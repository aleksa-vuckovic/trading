import unittest
import config
import time
from ..utils.dateutils import XNAS
from ..utils.common import Interval
from . import wsj

class TestWsj(unittest.TestCase):

    def test_merge_30m_to_1h(self):
        start = XNAS.str_to_unix('2025-01-10 10:00:00')
        input = [{'t': start+i*1800, 'o': i, 'h': i, 'l': i, 'c': i, 'v': i}for i in range(7)]
        expect = [{'t': start+(i+1)*1800 , 'o': i, 'h': i+1 if i < 6 else i, 'l': i, 'c': i+1 if i < 6 else i, 'v': 2*i+1 if i < 6 else 2*i} for i in range(0, 7, 2)]
        result = wsj._merge_data_1h(input)
        self.assertEqual(expect, result)

        good = [{'t': start+i*1800, 'o': i, 'h': i, 'l': i, 'c': i, 'v': i} for i in [0,2]]
        bad = {'t': start+5*1800, 'o': 3, 'h': 3, 'l': 3, 'c': 3, 'v': 3}
        input = [*good, bad]
        expect = [*good, bad]
        result = wsj._merge_data_1h(input)
        self.assertEqual(expect, result)

        input = [{'t': start + 5*3600 + i*1800, 'o': 1, 'h': 1, 'l': 1, 'c': 1, 'v': 1} for i in range(3)]
        expect = [{'t': start + 5*3600+1800, 'o':1, 'h':1, 'l': 1, 'c': 1, 'v': 2}, {'t': start + 6*3600, 'o':1, 'h':1, 'l':1, 'c':1, 'v':1}]
        result =wsj._merge_data_1h(input)
        self.assertEqual(expect, result)

    def test_pricing_d1(self):
        now = time.time()
        lows, highs, vols, times = wsj.get_pricing('nvda', now - 5*24*3600, now, interval=Interval.D1, return_quotes=['low', 'high', 'volume', 'timestamp'], skip_cache=config.test.skip_cache)
        self.assertTrue(lows and highs and vols and times)
        self.assertTrue(all(highs[i] >= lows[i] and vols[i] for i in range(len(lows))))
        self.assertTrue(all(XNAS.is_timestamp(it, Interval.D1) for it in times))

    def test_pricing_h1(self):
        now = time.time()
        lows, highs, vols, times = wsj.get_pricing('nvda', now - 5*24*3600, now, interval=Interval.H1, return_quotes=['low', 'high', 'volume', 'timestamp'], skip_cache=config.test.skip_cache)
        self.assertTrue(lows and highs and vols and times)
        self.assertGreater(len(lows), 8)
        self.assertTrue(all(highs[i] >= lows[i] and vols[i] for i in range(len(lows))))
        self.assertTrue(all(XNAS.is_timestamp(it, Interval.H1) for it in times))

    def test_pricing_m15(self):
        now = time.time()
        lows, highs, vols, times = wsj.get_pricing('nvda', now - 3*24*3600, now, Interval.M15, return_quotes=['low', 'high', 'volume', 'timestamp'], skip_cache=config.test.skip_cache)
        self.assertTrue(lows and highs and vols and times)
        self.assertGreater(len(lows), 24)
        self.assertTrue(all(highs[i] >= lows[i] and vols[i] for i in range(len(lows))))
        self.assertTrue(all(XNAS.is_timestamp(it, Interval.M15) for it in times))

    def test_pricing_m5(self):
        now = time.time()
        lows, highs, vols, times = wsj.get_pricing('nvda', now - 3*24*3600, now, Interval.M5, return_quotes=['low', 'high', 'volume', 'timestamp'], skip_cache=config.test.skip_cache)
        self.assertTrue(lows and highs and vols and times)
        self.assertGreater(len(lows), 70)
        self.assertTrue(all(highs[i] >= lows[i] and vols[i] for i in range(len(lows))))
        self.assertTrue(all(XNAS.is_timestamp(it, Interval.M5) for it in times))
