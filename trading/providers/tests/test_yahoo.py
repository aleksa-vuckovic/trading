from typing import override
import unittest
from base import dates
from trading.core.interval import Interval
from trading.core.pricing import PricingProvider
from trading.core.securities import Security
from trading.core.tests.test_pricing import TestPricingProvider
from trading.providers.yahoo import Yahoo
from trading.providers.nasdaq import Nasdaq, NasdaqGS, NasdaqMS, NasdaqCM
from trading.providers.forex import Forex
from trading.providers.nyse import NYSE, NYSEAmerican, NYSEArca

stock = Nasdaq.instance.get_security('NVDA')
calendar = Nasdaq.instance.calendar
provider = Yahoo()

class TestYahoo(TestPricingProvider):
    @override
    def get_provider(self) -> PricingProvider:
        return provider
    @override
    def get_securities(self) -> list[tuple[Security, float]]:
        return [
            (NasdaqGS.instance.get_security('NVDA'), 0.8),
            (NasdaqMS.instance.get_security('LUNR'), 0.8),
            (NasdaqCM.instance.get_security('RGTI'), 0.7),
            (NYSE.instance.get_security('KO'), 0.8),
            (NYSEAmerican.instance.get_security('IMO'), 0.5),
            (NYSEArca.instance.get_security('SPY'), 0.8),
            (Forex.instance.get_security('EURUSD'), 0.8)
        ]
    
    def test_pricing_l1(self):
        data = provider.get_pricing(
            calendar.str_to_unix("2021-11-27 00:00:00"),
            calendar.str_to_unix("2023-05-14 00:00:00"),
            stock,
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
            stock,
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
            stock,
            Interval.D1
        )
        self.assertEqual(32, len(data))
        self.assertEqual(calendar.str_to_unix('2021-12-02 00:00:00'), data[0].t)
        self.assertEqual(calendar.str_to_unix('2022-01-15 00:00:00'), data[-1].t)
        self.assertAlmostEqual(31.37991, data[0].c, 4)
        self.assertEqual(484368000, data[0].v)

    def test_pricing_m15(self):
        data = provider.get_pricing(
            calendar.str_to_unix("2025-04-09 16:00:00"),
            calendar.str_to_unix("2025-04-11 16:00:00"),
            stock,
            Interval.M15
        )
        self.assertEqual(52, len(data))
        self.assertTrue(all(calendar.is_timestamp(it.t, Interval.M15) for it in data))
        self.assertEqual(calendar.str_to_unix("2025-04-10 09:45:00"), data[0].t)
        self.assertAlmostEqual(108.99998, data[0].c, 4)

    def test_pricing_m5(self):
        data = provider.get_pricing(
            calendar.str_to_unix("2025-04-09 15:00:00"),
            calendar.str_to_unix("2025-04-10 10:00:00"),
            stock,
            Interval.M5
        )
        self.assertEqual(18, len(data))
        self.assertTrue(all(calendar.is_timestamp(it.t, Interval.M5) for it in data))
        self.assertEqual(calendar.str_to_unix("2025-04-09 15:05:00"), data[0].t)
        self.assertAlmostEqual(111.19764, data[0].c, 4)
    
    def test_info(self):
        tnya = Nasdaq.instance.get_security('TNYA')
        self.assertEqual(1627651800, provider.get_first_trade_time(tnya))

        result = provider.get_market_cap(tnya)
        self.assertGreater(result, 10000000)

        result = provider.get_summary(tnya)
        self.assertTrue(result.startswith("Tenaya Therapeutics, Inc., a clinical-stage biotechnology company, discovers, develops, and delivers therapies"))

        result = provider.get_outstanding_parts(tnya)
        self.assertGreater(result, 100000000)

    @unittest.skip("Avoid http calls")
    def test_merge(self):
        nonmerged = Yahoo(merge={}, local=True)
        start = stock.exchange.calendar.get_next_timestamp(dates.unix() - 5*24*3600, Interval.D1)
        end = stock.exchange.calendar.get_next_timestamp(dates.unix()-3*24*3600, Interval.D1)

        result1 = provider.get_pricing(start, end, stock, Interval.M30)
        result2 = nonmerged.get_pricing(start, end, stock, Interval.M30)

        for a,b in zip(result1, result2):
            self.assertAlmostEqual(a.o, b.o)
            self.assertAlmostEqual(a.h, b.h)
            self.assertAlmostEqual(a.l, b.l)
            self.assertAlmostEqual(a.c, b.c)
            self.assertGreater(a.v/b.v, 0.8)
            self.assertLessEqual(a.v/b.v, 1.2)
