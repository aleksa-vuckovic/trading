import unittest
import time
import config
from trading.core.interval import Interval
from trading.providers import FinancialTimes, Nasdaq

security = Nasdaq.instance.get_security('NVDA')
provider = FinancialTimes(config.caching.storage)
class TestFinancialtimes(unittest.TestCase):
    def test_pricing_d1(self):
        now = time.time()
        lows, highs, vols, times = provider.get_pricing(now - 5*24*3600, now, security, Interval.D1, return_quotes=['low', 'high', 'volume', 'timestamp'])
        self.assertTrue(lows and highs and vols and times)
        self.assertTrue(all(highs[i] >= lows[i] and vols[i] for i in range(len(lows))))
        self.assertTrue(all(security.exchange.calendar.is_timestamp(it, Interval.D1) for it in times))

    def test_pricing_h1(self):
        now = time.time()
        lows, highs, vols, times = provider.get_pricing(now - 5*24*3600, now, security, Interval.H1, return_quotes=['low', 'high', 'volume', 'timestamp'])
        self.assertTrue(lows and highs and vols and times)
        self.assertGreater(len(lows), 8)
        self.assertTrue(all(highs[i] >= lows[i] and vols[i] for i in range(len(lows))))
        self.assertTrue(all(security.exchange.calendar.is_timestamp(it, Interval.H1) for it in times))

    def test_pricing_m15(self):
        now = time.time()
        lows, highs, vols, times = provider.get_pricing(now - 3*24*3600, now, security, Interval.M15, return_quotes=['low', 'high', 'volume', 'timestamp'])
        self.assertTrue(lows and highs and vols and times)
        self.assertGreater(len(lows), 24)
        self.assertTrue(all(highs[i] >= lows[i] and vols[i] for i in range(len(lows))))
        self.assertTrue(all(security.exchange.calendar.is_timestamp(it, Interval.M15) for it in times))

    def test_pricing_m5(self):
        now = time.time()
        lows, highs, vols, times = provider.get_pricing(now - 3*24*3600, now, security, Interval.M5, return_quotes=['low', 'high', 'volume', 'timestamp'])
        self.assertTrue(lows and highs and vols and times)
        self.assertGreater(len(lows), 70)
        self.assertTrue(all(highs[i] >= lows[i] and vols[i] for i in range(len(lows))))
        self.assertTrue(all(security.exchange.calendar.is_timestamp(it, Interval.M5) for it in times))
