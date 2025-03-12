import unittest
import config
from trading.core.interval import Interval
from trading.securities import Yahoo, Nasdaq

security = Nasdaq.get_security('NVDA')
calendar = Nasdaq.instance.calendar
provider = Yahoo()

class TestQuery(unittest.TestCase):
    def test_pricing_l1(self):
        prices, volume, low, high, open, times = provider.get_pricing(
            security,
            calendar.str_to_unix("2021-11-26 16:00:00"),
            calendar.str_to_unix("2023-05-13 16:00:00"),
            Interval.L1,
            return_quotes=['close', 'volume', 'low', 'open', 'high', 'timestamp'],
            skip_cache=config.test.skip_cache
        )
        self.assertTrue(prices and volume and low and high and open and times)
        self.assertEqual(18, len(prices))
        self.assertTrue(all(calendar.is_timestamp(it, Interval.L1) for it in times))
        self.assertAlmostEqual(prices[0], 32.67599868774414)

    def test_pricing_w1(self):
        prices, volume, low, high, open, times = provider.get_pricing(
            security,
            calendar.str_to_unix("2021-11-26 16:00:00"),
            calendar.str_to_unix("2022-05-13 16:00:00"),
            Interval.W1,
            return_quotes=['close', 'volume', 'low', 'open', 'high', 'timestamp'],
            skip_cache=config.test.skip_cache
        )
        self.assertTrue(prices and volume and low and high and open and times)
        self.assertEqual(23 if config.test.skip_cache else 24,len(prices))
        self.assertTrue(all(calendar.is_timestamp(it, Interval.W1) for it in times))
        if not config.test.skip_cache: self.assertAlmostEqual(prices[0], 32.67599868774414)

    def test_pricing_d1(self):
        prices, volume, low, high, open, times = provider.get_pricing(
            security,
            calendar.str_to_unix("2021-11-30 16:00:00"),
            calendar.str_to_unix("2022-01-14 16:00:00"),
            Interval.D1,
            return_quotes=['close', 'volume', 'low', 'open', 'high', 'timestamp'],
            skip_cache=config.test.skip_cache
        )
        self.assertEqual(calendar.str_to_unix('2021-12-01 16:00:00'), times[0])
        self.assertEqual(calendar.str_to_unix('2022-01-14 16:00:00'), times[-1])
        self.assertTrue(prices and volume and low and high and open and times)
        self.assertAlmostEqual(31.434999465942383, prices[0])
        self.assertEqual(484368000, volume[0])
        self.assertEqual(len(prices), len(volume))
        self.assertEqual(32, len(prices))

    def test_pricing_h1(self):
        prices, volume, low, high, open, times = provider.get_pricing(
            security,
            calendar.str_to_unix("2023-12-01 00:00:00"),
            calendar.str_to_unix("2024-01-15 00:00:00"),
            Interval.H1,
            return_quotes=['close', 'volume', 'low', 'high', 'open', 'timestamp'],
            skip_cache=config.test.skip_cache
        )
        self.assertEqual(calendar.str_to_unix('2023-12-01 10:30:00'), times[0])
        self.assertEqual(calendar.str_to_unix('2024-01-12 16:00:00'), times[-1])
        self.assertTrue(prices and volume and low and high and open)
        self.assertGreater(prices[0], 46)
        self.assertLess(prices[0], 47)
        self.assertGreater(sum(volume[:7]), 340000000)
        self.assertLess(sum(volume[:7]), 380000000)
        self.assertEqual(len(prices), len(volume))
        self.assertTrue(len(prices)> 150 and len(prices) < 300)

    def test_pricing_m15(self):
        prices, volume, low, high, open, times = provider.get_pricing(
            security,
            calendar.str_to_unix("2025-02-10 16:00:00"),
            calendar.str_to_unix("2025-02-12 16:00:00"),
            Interval.M15,
            return_quotes=['close', 'volume', 'low', 'open', 'high', 'timestamp'],
            skip_cache=config.test.skip_cache
        )
        self.assertTrue(prices and volume and low and high and open and times)
        self.assertEqual(52, len(prices))
        self.assertTrue(all(calendar.is_timestamp(it, Interval.M15) for it in times))
        self.assertAlmostEqual(133.8594422343359 if config.test.skip_cache else 133.92999267578125, prices[0])

    def test_pricing_m5(self):
        prices, volume, low, high, open, times = provider.get_pricing(
            security,
            calendar.str_to_unix("2025-02-11 15:00:00"),
            calendar.str_to_unix("2025-02-12 10:00:00"),
            Interval.M5,
            return_quotes=['close', 'volume', 'low', 'open', 'high', 'timestamp'],
            skip_cache=config.test.skip_cache
        )
        self.assertTrue(prices and volume and low and high and open and times)
        self.assertEqual(18, len(prices))
        self.assertTrue(all(calendar.is_timestamp(it, Interval.M5) for it in times))
        self.assertAlmostEqual(prices[0], 133.5915985107422)
    
    def test_get_info(self):
        tnya = Nasdaq.instance.get_security('TNYA')
        info = provider.get_first_trade_time(tnya)
        self.assertEqual(1627651800, info)

