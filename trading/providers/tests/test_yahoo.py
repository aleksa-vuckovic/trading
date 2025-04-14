from typing import override
import unittest
import config
from trading.core.interval import Interval
from trading.core.pricing import PricingProvider
from trading.core.securities import Security
from trading.core.tests.test_pricing import TestPricingProviderRecent
from trading.providers import Yahoo, Nasdaq

security = Nasdaq.instance.get_security('NVDA')
calendar = Nasdaq.instance.calendar
provider = Yahoo(config.caching.storage)

class TestYahoo(TestPricingProviderRecent):
    @override
    def get_provider(self) -> PricingProvider:
        return provider
    @override
    def get_securities(self) -> list[Security]:
        return [security]
    
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
        self.assertAlmostEqual(32.6147575378418, data[0].c, 4)

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
            calendar.str_to_unix("2025-03-01 00:00:00"),
            calendar.str_to_unix("2025-04-15 00:00:00"),
            security,
            Interval.H1
        )
        self.assertGreater(len(data), 150)
        self.assertLess(len(data), 300)
        self.assertEqual(calendar.str_to_unix('2025-03-03 10:00:00'), data[0].t)
        self.assertEqual(calendar.str_to_unix('2025-04-11 16:00:00'), data[-1].t)
        self.assertGreater(data[0].c, 118)
        self.assertLess(data[0].c, 119)
        self.assertGreater(sum(it.v for it in data[:7]), 340000000)
        self.assertLess(sum(it.v for it in data[:7]), 380000000)

    def test_pricing_m15(self):
        data = provider.get_pricing(
            calendar.str_to_unix("2025-04-09 16:00:00"),
            calendar.str_to_unix("2025-04-11 16:00:00"),
            security,
            Interval.M15
        )
        self.assertEqual(52, len(data))
        self.assertTrue(all(calendar.is_timestamp(it.t, Interval.M15) for it in data))
        self.assertEqual(calendar.str_to_unix("2025-04-10 09:45:00"), data[0].t)
        self.assertAlmostEqual(108.90000152587894, data[0].c)

    def test_pricing_m5(self):
        data = provider.get_pricing(
            calendar.str_to_unix("2025-04-09 15:00:00"),
            calendar.str_to_unix("2025-04-10 10:00:00"),
            security,
            Interval.M5
        )
        self.assertEqual(18, len(data))
        self.assertTrue(all(calendar.is_timestamp(it.t, Interval.M5) for it in data))
        self.assertEqual(calendar.str_to_unix("2025-04-09 15:05:00"), data[0].t)
        self.assertAlmostEqual(111.11000061035156, data[0].c)
    
    def test_info(self):
        tnya = Nasdaq.instance.get_security('TNYA')
        self.assertEqual(1627651800, provider.get_first_trade_time(tnya))

        result = provider.get_market_cap(tnya)
        expect = 92490220
        self.assertGreater(result, expect*0.8)
        self.assertLess(result, expect*1.2)

        result = provider.get_summary(tnya)
        self.assertTrue(result.startswith("Tenaya Therapeutics, Inc., a clinical-stage biotechnology company, discovers, develops, and delivers therapies"))

        result = provider.get_outstanding_parts(tnya)
        expect = 162583008
        self.assertGreater(result, expect*0.8)
        self.assertLess(result,expect*1.2)

