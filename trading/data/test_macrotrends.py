import unittest
from . import macrotrends
from . import nasdaq

nvda = nasdaq.NasdaqListedEntry.from_line('NVDA|NVIDIA Corporation - Common Stock|Q|N|N|100|N|N')

class MacrotrendsTest(unittest.TestCase):

    def test_shares_outstanding(self):
        data = macrotrends.get_shares_outstanding(nvda)
        self.assertEqual(1230786000, data[0]['unix_time'])
        self.assertEqual(21925000000, data[0]['shares'])

    def test_shares_outstanding_at(self):
        self.assertEqual(23547000000, macrotrends.get_shares_outstanding_at(nvda, 1296440000))