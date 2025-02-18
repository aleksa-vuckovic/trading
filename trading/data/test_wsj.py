import unittest
import config
import time
from ..utils import dateutils
from ..utils.common import Interval
from . import wsj

class TestWsj(unittest.TestCase):

    def test_merge_30m_to_1h(self):
        start = dateutils.str_to_unix('2025-01-10 10:00:00')
        input = [{'t': start+i*1800, 'o': i, 'h': i, 'l': i, 'c': i, 'v': i}for i in range(7)]
        expect = [{'t': start+(i+1)*1800 , 'o': i, 'h': i+1 if i < 6 else i, 'l': i, 'c': i+1 if i < 6 else i, 'v': 2*i+1 if i < 6 else i} for i in range(0, 7, 2)]
        result = wsj._merge_data_1h(input)
        self.assertEqual(expect, result)

        good = [{'t': start+i*1800, 'o': i, 'h': i, 'l': i, 'c': i, 'v': i} for i in [0,2]]
        bad = {'t': start + 5*1800, 'o': 3, 'h': 3, 'l': 3, 'c': 3, 'v': 3}
        input = [*good, bad]
        expect = good
        result = wsj._merge_data_1h(input)
        self.assertEqual(expect, result)

        input = [{'t': start + 5*3600 + i*1800, 'o': 1, 'h': 1, 'l': 1, 'c': 1, 'v': 1} for i in range(3)]
        expect = [{'t': start + 5*3600+1800, 'o':1, 'h':1, 'l': 1, 'c': 1, 'v': 2}, {'t': start + 6*3600, 'o':1, 'h':1, 'l':1, 'c':1, 'v':1}]
        result =wsj._merge_data_1h(input)
        self.assertEqual(expect, result)

    def test_pricing_hourly(self):
        now = time.time()
        lows, highs, vols, times = wsj.get_pricing('bhat', now - 5*24*3600, now, interval=Interval.H1, return_quotes=['low', 'high', 'volume', 'timestamp'], skip_cache=config.test.skip_cache)
        self.assertTrue(lows and highs and vols and times)
        self.assertGreater(len(lows), 8)
        self.assertTrue(all(highs[i] >= lows[i] and vols[i] for i in range(len(lows))))
        dates = [dateutils.unix_to_datetime(time, tz=dateutils.ET) for time in times]
        self.assertTrue(all(date.minute == 30 and date.hour < 16 and date.hour >= 9 or date.minute == 0 and date.hour == 16 for date in dates))

    def test_pricing_daily(self):
        now = time.time()
        lows, highs, vols, times = wsj.get_pricing('bhat', now - 5*24*3600, now, interval=Interval.D1, return_quotes=['low', 'high', 'volume', 'timestamp'], skip_cache=config.test.skip_cache)
        self.assertTrue(lows and highs and vols and times)
        self.assertTrue(all(highs[i] >= lows[i] and vols[i] for i in range(len(lows))))
        dates = [dateutils.unix_to_datetime(time, tz=dateutils.ET) for time in times]
        self.assertTrue(all(date.hour == 16 and date.minute == 0 and not date.second and not date.microsecond for date in dates))
        