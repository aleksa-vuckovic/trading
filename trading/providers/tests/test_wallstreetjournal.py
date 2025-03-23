import unittest
import config
import time
from trading.core.interval import Interval
from trading.providers import Nasdaq, WallStreetJournal

security = Nasdaq.instance.get_security('NVDA')
calendar = Nasdaq.instance.calendar
provider = WallStreetJournal(config.caching.storage)

class TestWallStreetJournal(unittest.TestCase):
    def test_pricing_d1(self):
        now = time.time()
        data = provider.get_pricing(now - 5*24*3600, now, security, interval=Interval.D1)
        self.assertGreater(len(data), 1)
        self.assertTrue(all(it.is_valid() for it in data))
        self.assertTrue(all(calendar.is_timestamp(it.t, Interval.D1) for it in data))

    def test_pricing_h1(self):
        now = time.time()
        data = provider.get_pricing(now - 5*24*3600, now, security, interval=Interval.H1)
        self.assertGreater(len(data), 8)
        self.assertTrue(all(it.is_valid() for it in data))
        self.assertTrue(all(calendar.is_timestamp(it.t, Interval.H1) for it in data))

    def test_pricing_m15(self):
        now = time.time()
        data = provider.get_pricing(now - 3*24*3600, now, security, Interval.M15)
        self.assertGreater(len(data), 24)
        self.assertTrue(all(it.is_valid() for it in data))
        self.assertTrue(all(calendar.is_timestamp(it.t, Interval.M15) for it in data))

    def test_pricing_m5(self):
        now = time.time()
        data = provider.get_pricing(now - 3*24*3600, now, security, Interval.M5)
        self.assertGreater(len(data), 70)
        self.assertTrue(all(it.is_valid() for it in data))
        self.assertTrue(all(calendar.is_timestamp(it.t, Interval.M5) for it in data))
