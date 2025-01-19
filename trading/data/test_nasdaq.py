from . import nasdaq
import unittest
from ..utils import logutils

class NasdaqTest(unittest.TestCase):

    def test_line_parsing(self):
        nvda = nasdaq.NasdaqListedEntry.from_line('NVDA|NVIDIA Corporation - Common Stock|Q|N|N|100|N|N')
        etf = nasdaq.NasdaqListedEntry.from_line('NUSI|NEOS Nasdaq-100 Hedged Equity Income ETF|G|N|N|100|Y|N')
        self.assertEqual(nvda.symbol, 'NVDA')
        self.assertEqual(nvda.market, nasdaq.NasdaqMarket.SELECT)
        self.assertFalse(nvda.etf)
        self.assertEqual(nvda.lot_size, 100)
        self.assertFalse(nvda.next_shares)
        self.assertTrue(etf.etf)
    
    def test_is_warrant(self):
        warrant = nasdaq.NasdaqListedEntry.from_line('ABLLW|Abacus Life, Inc. - Warrant|S|N|N|100|N|N')
        nonwarrant = nasdaq.NasdaqListedEntry.from_line('NVDA|NVIDIA Corporation - Common Stock|Q|N|N|100|N|N')
        self.assertTrue(warrant.is_warrant())
        self.assertFalse(nonwarrant.is_warrant())
    
    def test_fetch(self):
        data = nasdaq.get_all_entries()
        nvda = list(filter(lambda x: x.symbol == 'NVDA', data))
        self.assertEqual(len(nvda), 1)

