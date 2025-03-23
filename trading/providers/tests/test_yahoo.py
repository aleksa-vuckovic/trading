import unittest
import config
from trading.core.interval import Interval
from trading.providers import Yahoo, Nasdaq

security = Nasdaq.instance.get_security('NVDA')
calendar = Nasdaq.instance.calendar
provider = Yahoo(config.caching.storage)

class TestYahoo(unittest.TestCase):
    def test_pricing_l1(self):
        data = provider.get_pricing(
            calendar.str_to_unix("2021-11-27 00:00:00"),
            calendar.str_to_unix("2023-05-14 00:00:00"),
            security,
            Interval.L1
        )
        self.assertEqual(18, len(data))
        self.assertTrue(all(calendar.is_timestamp(it.t, Interval.L1) for it in data))
        self.assertEqual(calendar.str_to_unix("2021-12-01 00:00:00"), data[0].t)
        self.assertAlmostEqual(32.6147575378418, data[0].c, 5)

    def test_pricing_w1(self):
        data = provider.get_pricing(
            calendar.str_to_unix("2021-11-29 16:00:00"),
            calendar.str_to_unix("2022-05-16 00:00:00"),
            security,
            Interval.W1   
        )
        self.assertEqual(24,len(data))
        self.assertTrue(all(calendar.is_timestamp(it.t, Interval.W1) for it in data))
        self.assertEqual(calendar.str_to_unix('2021-12-06 00:00:00'), data[0].t)
        self.assertAlmostEqual(30.63547134399414, data[0].c, 5)

    def test_pricing_d1(self):
        data = provider.get_pricing(
            calendar.str_to_unix("2021-12-01 00:00:00"),
            calendar.str_to_unix("2022-01-15 00:00:00"),
            security,
            Interval.D1
        )
        self.assertEqual(32, len(data))
        self.assertEqual(calendar.str_to_unix('2021-12-02 00:00:00'), data[0].t)
        self.assertEqual(calendar.str_to_unix('2022-01-15 00:00:00'), data[-1].t)
        self.assertAlmostEqual(31.37991714477539, data[0].c, 5)
        self.assertEqual(484368000, data[0].v)

    def test_pricing_h1(self):
        data = provider.get_pricing(
            calendar.str_to_unix("2023-12-01 00:00:00"),
            calendar.str_to_unix("2024-01-15 00:00:00"),
            security,
            Interval.H1
        )
        self.assertTrue(len(data)> 150 and len(data) < 300)
        self.assertEqual(calendar.str_to_unix('2023-12-01 10:30:00'), data[0].t)
        self.assertEqual(calendar.str_to_unix('2024-01-12 16:00:00'), data[-1].t)
        self.assertGreater(data[0].c, 46)
        self.assertLess(data[0].c, 47)
        self.assertGreater(sum(it.v for it in data[:7]), 340000000)
        self.assertLess(sum(it.v for it in data[:7]), 380000000)


    def test_pricing_m15(self):
        data = provider.get_pricing(
            calendar.str_to_unix("2025-02-10 16:00:00"),
            calendar.str_to_unix("2025-02-12 16:00:00"),
            security,
            Interval.M15
        )
        self.assertEqual(52, len(data))
        self.assertTrue(all(calendar.is_timestamp(it.t, Interval.M15) for it in data))
        self.assertEqual(calendar.str_to_unix("2025-02-11 09:45:00"), data[0].t)
        self.assertAlmostEqual(133.9080628304134, data[0].c)

    def test_pricing_m5(self):
        data = provider.get_pricing(
            calendar.str_to_unix("2025-02-11 15:00:00"),
            calendar.str_to_unix("2025-02-12 10:00:00"),
            security,
            Interval.M5
        )
        self.assertEqual(18, len(data))
        self.assertTrue(all(calendar.is_timestamp(it.t, Interval.M5) for it in data))
        self.assertEqual(calendar.str_to_unix("2025-02-11 15:05:00"), data[0].t)
        self.assertAlmostEqual(133.56972407440978, data[0].c)
    
    def test_get_info(self):
        tnya = Nasdaq.instance.get_security('TNYA')
        self.assertEqual(1627651800, provider.get_first_trade_time(tnya))

