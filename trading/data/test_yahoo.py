import unittest
from . import yahoo
from ..utils import dateutils, logutils

class TestQuery(unittest.TestCase):
    def test_yahoo_pricing_h1(self):
        prices, volume, low, high, open = yahoo.get_yahoo_pricing(
            'nvda',
            dateutils.str_to_unix("2023-12-01 00:00:00", tz = dateutils.ET),
            dateutils.str_to_unix("2024-01-15 00:00:00", tz = dateutils.ET),
            yahoo.Interval.H1,
            return_quotes=['close', 'volume', 'low', 'high', 'open']
        )
        self.assertTrue(prices and volume and low and high and open)
        self.assertGreater(prices[0], 46)
        self.assertLess(prices[0], 47)
        self.assertGreater(sum(volume[:7]), 340000000)
        self.assertLess(sum(volume[:7]), 380000000)
        self.assertEqual(len(prices), len(volume))
        self.assertEqual(len(prices), len(low))
        self.assertEqual(len(prices), len(high))
        self.assertEqual(len(prices), len(open))
        self.assertTrue(len(prices)> 150 and len(prices) < 300)
    
    def test_yahoo_pricing_d1(self):
        prices, volume, low, high, open = yahoo.get_yahoo_pricing(
            'nvda',
            dateutils.str_to_unix("2021-12-01 00:00:00", tz = dateutils.ET),
            dateutils.str_to_unix("2022-01-15 00:00:00", tz = dateutils.ET),
            yahoo.Interval.D1,
            return_quotes=['close', 'volume', 'low', 'open', 'high']
        )
        self.assertTrue(prices)
        self.assertTrue(volume)
        self.assertAlmostEqual(31.434999465942383, prices[0])
        self.assertEqual(484368000, volume[0])
        self.assertEqual(len(prices), len(volume))
        self.assertEqual(len(prices), len(low))
        self.assertEqual(len(prices), len(high))
        self.assertEqual(len(prices), len(open))
        self.assertEqual(32, len(prices))

    def test_get_info(self):
        info = yahoo.get_first_trade_time('tnya')
        self.assertEqual(1627651800, info)

