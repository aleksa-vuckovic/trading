import unittest
from trading.securities import Nasdaq
from trading.core.pricing_provider import interpolate_linear, merge_pricing
from trading.core import Interval

security = Nasdaq.instance.get_security('NVDA')
calendar = security.exchange.calendar

class TestAggregate(unittest.TestCase):
    def test_interpolate_linear(self):
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

    def test_merge(self):
        start = calendar.str_to_unix('2025-01-10 10:00:00')
        input = [{'t': start+i*1800, 'o': i, 'h': i, 'l': i, 'c': i, 'v': i}for i in range(7)]
        expect = [{'t': start+(i+1)*1800 , 'o': i, 'h': i+1 if i < 6 else i, 'l': i, 'c': i+1 if i < 6 else i, 'v': 2*i+1 if i < 6 else i} for i in range(2, 7, 2)]
        result = merge_pricing(input, start+1800, start+6*1800, Interval.H1, security)
        self.assertEqual(expect, result)

        input = [{'t': start+i*1800, 'o': i, 'h': i, 'l': i, 'c': i, 'v': i} for i in [0,3]]
        input = [*input, {'t': start+9*1800, 'o': 3, 'h': 3, 'l': 3, 'c': 3, 'v': 3}]
        expect = [{**input[0], 't': start+1800}, input[1], input[2]]
        result = merge_pricing(input, start, start+5*1800, Interval.H1, security)
        self.assertEqual(expect, result)
        
        input = [{'t': start+i*15*60,'o':i,'h':i,'l':i,'c':i,'v':i} for i in range(8)]
        expect = [
            #{'t':start+1800,'o':0,'h':2,'l':0,'c':2,'v':3},
            {'t':start+1800+3600,'o':3,'h':6,'l':3,'c':6,'v':18},
            {'t':start+1800+2*3600,'o':7,'h':7,'l':7,'c':7,'v':7}
        ]
        result = merge_pricing(input, start+1800, start+10*3600, Interval.H1, security)
        self.assertEqual(expect, result)

        t1 = calendar.str_to_unix('2025-01-10 15:30:00')
        t2 = calendar.str_to_unix('2025-01-13 09:30:00')
        input = [{'t':t1+i*15*60,'o':i,'h':i,'l':i,'c':i,'v':i} for i in range(1,3)]
        input = [*input, *[{'t':t2+i*15*60,'o':i,'h':i,'l':i,'c':i,'v':i} for i in range(1,5)]]
        expect = [{'t':t1+1800,'o':1,'h':2,'l':1,'c':2,'v':3},{'t':t2+3600,'o':1,'h':4,'l':1,'c':4,'v':10}]
        result = merge_pricing(input, t1, t2+3600, Interval.H1, security)
        self.assertEqual(expect, result)

