import unittest
from base import dates
from trading.core.pricing import OHLCV, PricingProvider, merge_pricing
from trading.core import Interval
from trading.core.securities import Exchange, Security, SecurityType
from trading.core.work_calendar import BasicWorkCalendar
import time

calendar = BasicWorkCalendar(tz=dates.ET, open_hour=9, open_minute=30, close_hour=16, semi_close_hour=16)
exchange = Exchange('XNAS', 'Nasdaq', calendar)
class MockSecurity(Security):
    def __init__(self):
        super().__init__('NVDA', 'Nvidia', SecurityType.STOCK)
    @property
    def exchange(self):
        return exchange
security = MockSecurity()

class TestOHLCV(unittest.TestCase):
    def test_ohlcv_interpolate(self):
        data = [OHLCV(t, x, x, x, x, x) for t,x in zip([1,5,7],[1,2,4])]
        timestamps = [1,2,5,6,7]
        expect = [OHLCV(t, x, x, x, x, x) for t,x in zip(timestamps, [1, 5/4, 2, 3, 4])]
        result = OHLCV.interpolate(data, timestamps)
        self.assertEqual(expect, result)

        timestamps = [0,*timestamps,8]
        expect = [OHLCV(0, 1, 1, 1, 1, 1), *expect, OHLCV(8, 4, 4, 4, 4, 4)]
        result = OHLCV.interpolate(data, timestamps)
        self.assertEqual(expect, result)

    def test_ohlcv_is_valid(self):
        self.assertFalse(OHLCV(0,5,8,2,5,1).is_valid())
        self.assertTrue(OHLCV(1,5,8,2,5,1).is_valid())
        self.assertTrue(OHLCV(1,9,8,2,5,1).is_valid())
        self.assertTrue(OHLCV(1,5,8,2,1,1).is_valid())
        self.assertTrue(OHLCV(1,5,8,2,0,0).is_valid())
        self.assertFalse(OHLCV(1,5,8,2,-1,1).is_valid())
        self.assertFalse(OHLCV(1,float('inf'),1,1,1,1).is_valid())
        self.assertFalse(OHLCV(1,float('nan'),1,1,1,1).is_valid())

class TestMerge(unittest.TestCase):
    def test_merge(self):
        start = calendar.str_to_unix('2025-01-10 10:30:00')
        #test regular merge at 11:00, 12:00, 13:00
        #test first half only merge at 13:30
        input = [OHLCV(start+i*1800, i, i, i, i, i)for i in range(7)]
        expect = [OHLCV(start+(i+1)*1800, i, i+1, i, i+1, 2*i+1) for i in range(0, 5, 2)]
        expect = [*expect, OHLCV(start+7*1800, 6, 6, 6, 6, 6)]
        result = merge_pricing(input, start, start+7*1800, Interval.H1, security)
        self.assertEqual(expect, result)

    def test_merge_cutoffs(self):
        start = calendar.str_to_unix('2025-01-10 10:30:00')
        #test no left cutoff even though the subinterval is at lower bound
        #test right cutoff
        input = [OHLCV(start+i*1800, i, i, i, i, i) for i in [0,3]]
        input = [*input, OHLCV(start+9*1800, 3, 3, 3, 3, 3)]
        expect: list[OHLCV] = [OHLCV.from_dict({**input[0].to_dict(), 't': start+1800}), input[1]]
        result = merge_pricing(input, start, start+5*1800, Interval.H1, security)
        self.assertEqual(expect, result)
        
        #test left cutoff and no right cutoff
        input = [OHLCV(start+i*15*60, i, i, i, i, i) for i in range(8)]
        expect = [
            #{'t':start+1800,'o':0,'h':2,'l':0,'c':2,'v':3},
            OHLCV(start+1800+3600, 3, 6, 3, 6, 18),
            OHLCV(start+1800+2*3600, 7, 7, 7, 7, 7)
        ]
        result = merge_pricing(input, start+1800, start+1800+2*3600, Interval.H1, security)
        self.assertEqual(expect, result)

    def test_merge_intermittent(self):
        #test intermittent data
        t1 = calendar.str_to_unix('2025-01-10 15:30:00')
        t2 = calendar.str_to_unix('2025-01-13 09:30:00')
        input = [OHLCV(t1+i*15*60, i, i, i, i, i) for i in range(1,3)]
        input = [*input, *[OHLCV(t2+i*15*60, i, i, i, i, i) for i in range(1,5)]]
        expect = [OHLCV(t1+1800, 1, 2, 1, 2, 3), OHLCV(t2+1800, 1, 2, 1, 2, 3), OHLCV(t2+5400, 3, 4, 3, 4, 7)]
        result = merge_pricing(input, t1, t2+5400, Interval.H1, security)
        self.assertEqual(expect, result)

class TestPricingProviderRecent(unittest.TestCase):
    def get_provider(self) -> PricingProvider: ...
    def get_securities(self) -> list[Security]: ...

    def test_get_pricing(self):
        if type(self) == TestPricingProviderRecent: return
        now = time.time()
        provider = self.get_provider()
        securities = self.get_securities()
        unix_from = now - 5*24*3600
        unix_to = now
        for interval in provider.get_intervals():
            if interval > Interval.D1: continue
            for security in securities:
                context = f"Provider {type(self).__name__}, Interval {interval}, Security {security.symbol}"
                data = provider.get_pricing(unix_from, unix_to, security, interval)
                expect = len(security.exchange.calendar.get_timestamps(unix_from, unix_to, interval))
                self.assertGreater(len(data), 0.7*expect, context)
                self.assertTrue(all(it.is_valid() for it in data), context)
                self.assertTrue(all(security.exchange.calendar.is_timestamp(it.t, interval) for it in data), context)

