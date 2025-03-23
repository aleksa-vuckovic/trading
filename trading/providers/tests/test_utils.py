import unittest
from trading.core.pricing import OHLCV
from trading.providers.utils import arrays_to_ohlcv, filter_ohlcv

class TestUtils(unittest.TestCase):

    def test_arrays_to_ohlcv(self):
        input = {'t': [None,2,3,4,5],'o':[5,5,1,5,None],'h':[9,9,9,9,9],'l':[1,-1,2,1,1],'c':[5,5,5,5,5],'v':[2,None,2,0,2]}
        expect = [OHLCV(3,1,9,2,5,2),OHLCV(4,5,9,1,5,0)]
        result = arrays_to_ohlcv(input)
        self.assertEqual(expect, result)

    def test_filter_ohlcv(self):
        # check timestamp filtering
        input = [OHLCV(it,1,1,1,1,1) for it in [5,1,3,3,10,11]]
        expect = [OHLCV(it,1,1,1,1,1) for it in [3,5,10]]
        result = filter_ohlcv(input, 1, 10)
        self.assertEqual(expect, result)
        
        # check validation filtering
        input = [OHLCV(0,5,9,1,5,2),OHLCV(2,5,9,-1,5,2),OHLCV(3,1,9,2,5,2),OHLCV(4,5,9,1,5,0),OHLCV(5,5,9,1,5,2),OHLCV(6,5,9,1,5,2)]
        expect = [OHLCV(it,5,9,1,5,2) if it != 3 else OHLCV(3,1,9,2,5,2) for it in [0,3,5,6]]
        result = filter_ohlcv(input, -1, 10)
        self.assertEqual(expect, result)