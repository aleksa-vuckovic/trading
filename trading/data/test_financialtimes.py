import unittest
import time
import config
from ..utils.common import Interval
from ..utils import dateutils
from . import financialtimes

ticker = 'nvda'
class TestFinancialtimes(unittest.TestCase):
    def test_pricing_d1(self):
        now = time.time()
        lows, highs, vols, times = financialtimes.get_pricing(ticker, now - 5*24*3600, now, interval=Interval.D1, return_quotes=['low', 'high', 'volume', 'timestamp'], skip_cache=config.test.skip_cache)
        self.assertTrue(lows and highs and vols and times)
        self.assertTrue(all(highs[i] >= lows[i] and vols[i] for i in range(len(lows))))
        dates = [dateutils.unix_to_datetime(time, tz=dateutils.ET) for time in times]
        self.assertTrue(all(dateutils.is_interval_time_unix(it, Interval.D1) for it in times))

    def test_pricing_h1(self):
        now = time.time()
        lows, highs, vols, times = financialtimes.get_pricing(ticker, now - 5*24*3600, now, interval=Interval.H1, return_quotes=['low', 'high', 'volume', 'timestamp'], skip_cache=config.test.skip_cache)
        self.assertTrue(lows and highs and vols and times)
        self.assertGreater(len(lows), 8)
        self.assertTrue(all(highs[i] >= lows[i] and vols[i] for i in range(len(lows))))
        self.assertTrue(all(dateutils.is_interval_time_unix(it, Interval.H1) for it in times))

    def test_pricing_m15(self):
        now = time.time()
        lows, highs, vols, times = financialtimes.get_pricing(ticker, now - 3*24*3600, now, interval=Interval.M15, return_quotes=['low', 'high', 'volume', 'timestamp'], skip_cache=config.test.skip_cache)
        self.assertTrue(lows and highs and vols and times)
        self.assertGreater(len(lows), 24)
        self.assertTrue(all(highs[i] >= lows[i] and vols[i] for i in range(len(lows))))
        self.assertTrue(all(dateutils.is_interval_time_unix(it, Interval.M15) for it in times))

    def test_pricing_m5(self):
        now = time.time()
        lows, highs, vols, times = financialtimes.get_pricing(ticker, now - 3*24*3600, now, interval=Interval.M5, return_quotes=['low', 'high', 'volume', 'timestamp'], skip_cache=config.test.skip_cache)
        self.assertTrue(lows and highs and vols and times)
        self.assertGreater(len(lows), 70)
        self.assertTrue(all(highs[i] >= lows[i] and vols[i] for i in range(len(lows))))
        self.assertTrue(all(dateutils.is_interval_time_unix(it, Interval.M5) for it in times))
