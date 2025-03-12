import unittest
import config
import time
from trading.core.interval import Interval
from trading.securities import Nasdaq, WallStreetJournal

security = Nasdaq.instance.get_security('NVDA')
calendar = Nasdaq.instance.calendar
provider = WallStreetJournal()

class TestWallStreetJournal(unittest.TestCase):
    def test_pricing_d1(self):
        now = time.time()
        lows, highs, vols, times = provider.get_pricing(security, now - 5*24*3600, now, interval=Interval.D1, return_quotes=['low', 'high', 'volume', 'timestamp'], skip_cache=config.test.skip_cache)
        self.assertTrue(lows and highs and vols and times)
        self.assertTrue(all(highs[i] >= lows[i] and vols[i] for i in range(len(lows))))
        self.assertTrue(all(calendar.is_timestamp(it, Interval.D1) for it in times))

    def test_pricing_h1(self):
        now = time.time()
        lows, highs, vols, times = provider.get_pricing(security, now - 5*24*3600, now, interval=Interval.H1, return_quotes=['low', 'high', 'volume', 'timestamp'], skip_cache=config.test.skip_cache)
        self.assertTrue(lows and highs and vols and times)
        self.assertGreater(len(lows), 8)
        self.assertTrue(all(highs[i] >= lows[i] and vols[i] for i in range(len(lows))))
        self.assertTrue(all(calendar.is_timestamp(it, Interval.H1) for it in times))

    def test_pricing_m15(self):
        now = time.time()
        lows, highs, vols, times = provider.get_pricing(security, now - 3*24*3600, now, Interval.M15, return_quotes=['low', 'high', 'volume', 'timestamp'], skip_cache=config.test.skip_cache)
        self.assertTrue(lows and highs and vols and times)
        self.assertGreater(len(lows), 24)
        self.assertTrue(all(highs[i] >= lows[i] and vols[i] for i in range(len(lows))))
        self.assertTrue(all(calendar.is_timestamp(it, Interval.M15) for it in times))

    def test_pricing_m5(self):
        now = time.time()
        lows, highs, vols, times = provider.get_pricing(security, now - 3*24*3600, now, Interval.M5, return_quotes=['low', 'high', 'volume', 'timestamp'], skip_cache=config.test.skip_cache)
        self.assertTrue(lows and highs and vols and times)
        self.assertGreater(len(lows), 70)
        self.assertTrue(all(highs[i] >= lows[i] and vols[i] for i in range(len(lows))))
        self.assertTrue(all(calendar.is_timestamp(it, Interval.M5) for it in times))
