import unittest
import time
from ..utils.common import Interval
from ..utils import dateutils
from . import financialtimes

class TestFinancialtimes(unittest.TestCase):
    def test_pricing_hourly(self):
        now = time.time()
        lows, highs, vols, times = financialtimes.get_pricing('bhat', now - 5*24*3600, now, interval=Interval.H1, return_quotes=['low', 'high', 'volume', 'timestamp'])
        self.assertTrue(lows and highs and vols and times)
        self.assertGreater(len(lows), 8)
        self.assertTrue(all(highs[i] >= lows[i] and vols[i] for i in range(len(lows))))
        dates = [dateutils.unix_to_datetime(time, tz=dateutils.ET) for time in times]
        self.assertTrue(all(date.minute == 30 and date.hour < 16 and date.hour >= 9 for date in dates))

    def test_pricing_daily(self):
        now = time.time()
        lows, highs, vols, times = financialtimes.get_pricing('bhat', now - 5*24*3600, now, interval=Interval.D1, return_quotes=['low', 'high', 'volume', 'timestamp'])
        self.assertTrue(lows and highs and vols and times)
        self.assertTrue(all(highs[i] >= lows[i] and vols[i] for i in range(len(lows))))
        dates = [dateutils.unix_to_datetime(time, tz=dateutils.ET) for time in times]
        self.assertTrue(all(date.hour == 9 and date.minute == 30 and not date.second and not date.microsecond for date in dates))