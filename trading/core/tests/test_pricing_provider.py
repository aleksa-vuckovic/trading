import unittest
from base import dates
from trading.core.pricing_provider import interpolate_linear, merge_pricing
from trading.core import Interval
from trading.core.securities import Exchange, Security, SecurityType
from trading.core.work_calendar import BasicWorkCalendar


calendar = BasicWorkCalendar(tz=dates.ET, open_hour=9, open_minute=30, close_hour=16, semi_close_hour=16)
exchange = Exchange('XNAS', 'Nasdaq', calendar)
class MockSecurity(Security):
    def __init__(self):
        super().__init__('NVDA', 'Nvidia', SecurityType.STOCK)
    @property
    def exchange(self):
        return exchange
security = MockSecurity()

class TestPricingProvider(unittest.TestCase):
    def test_interpolate_linear_indices(self):
        data = [{'a':1,'b':4,'t':2},{'a':2,'b':5,'t':5},{'a':3,'b':6,'t':7}]
        timestamps = list(range(1,11))
        result = interpolate_linear(data, timestamps)
        expect_a = [1.0,1.0,4/3,5/3,2.0,2.5,3.0,3.0,3.0,3.0]
        expect_b = [4.0,4.0,13/3,14/3,5.0,5.5,6.0,6.0,6.0,6.0]
        expect_t = timestamps
        t1 = sum(x-y for x,y in zip([it['a'] for it in result], expect_a))
        t2 = sum(x-y for x,y in zip([it['b'] for it in result], expect_b))
        self.assertAlmostEqual(0, t1)
        self.assertAlmostEqual(0, t2)
        self.assertEqual(expect_t, [it['t'] for it in result])
    
    def test_interpolate_linear_timestamps(self):
        data = [{'a':1,'t':1},{'a':2,'t':5},{'a':4,'t':7}]
        timestamps = [1,2,5,6,7]
        expect = [{'a':1,'t':1},{'a':1/4*2+3/4,'t':2},{'a':2,'t':5},{'a':3,'t':6},{'a':4,'t':7}]
        result = interpolate_linear(data, timestamps)
        self.assertEqual(expect, result)

        timestamps = [0,*timestamps,8]
        expect = [{'a':1,'t':0}, *expect, {'a':4,'t':8}]
        result = interpolate_linear(data, timestamps)
        self.assertEqual(expect, result)

    def test_merge(self):
        start = calendar.str_to_unix('2025-01-10 10:00:00')
        #test regular merge at 11:30, 12:30
        #test first half only merge ar 13:00
        input = [{'t': start+i*1800, 'o': i, 'h': i, 'l': i, 'c': i, 'v': i}for i in range(7)]
        expect = [{'t': start+(i+1)*1800 , 'o': i, 'h': i+1 if i < 6 else i, 'l': i, 'c': i+1 if i < 6 else i, 'v': 2*i+1 if i < 6 else i} for i in range(0, 7, 2)]
        result = merge_pricing(input, start, start+7*1800, Interval.H1, security)
        self.assertEqual(expect, result)

    def test_merge_cutoffs(self):
        start = calendar.str_to_unix('2025-01-10 10:00:00')
        #test no left cutoff even though the subinterval is at lower bound
        #test right cutoff
        input = [{'t': start+i*1800, 'o': i, 'h': i, 'l': i, 'c': i, 'v': i} for i in [0,3]]
        input = [*input, {'t': start+9*1800, 'o': 3, 'h': 3, 'l': 3, 'c': 3, 'v': 3}]
        expect = [{**input[0], 't': start+1800}, input[1]]
        result = merge_pricing(input, start, start+5*1800, Interval.H1, security)
        self.assertEqual(expect, result)
        
        #test left cutoff and no right cutoff
        input = [{'t': start+i*15*60,'o':i,'h':i,'l':i,'c':i,'v':i} for i in range(8)]
        expect = [
            #{'t':start+1800,'o':0,'h':2,'l':0,'c':2,'v':3},
            {'t':start+1800+3600,'o':3,'h':6,'l':3,'c':6,'v':18},
            {'t':start+1800+2*3600,'o':7,'h':7,'l':7,'c':7,'v':7}
        ]
        result = merge_pricing(input, start+1800, start+1800+2*3600, Interval.H1, security)
        self.assertEqual(expect, result)

    def test_merge_intermittent(self):
        #test intermittent data
        t1 = calendar.str_to_unix('2025-01-10 15:30:00')
        t2 = calendar.str_to_unix('2025-01-13 09:30:00')
        input = [{'t':t1+i*15*60,'o':i,'h':i,'l':i,'c':i,'v':i} for i in range(1,3)]
        input = [*input, *[{'t':t2+i*15*60,'o':i,'h':i,'l':i,'c':i,'v':i} for i in range(1,5)]]
        expect = [{'t':t1+1800,'o':1,'h':2,'l':1,'c':2,'v':3},{'t':t2+3600,'o':1,'h':4,'l':1,'c':4,'v':10}]
        result = merge_pricing(input, t1, t2+3600, Interval.H1, security)
        self.assertEqual(expect, result)

