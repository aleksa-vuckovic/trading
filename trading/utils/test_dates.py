import unittest
from . import dateutils

class TestDates(unittest.TestCase):

    def test_str_to_unix(self):
        unix = dateutils.str_to_unix('2023-05-06 06:20:30', tz = dateutils.EST)
        self.assertEqual(1683368430, unix)
        self.assertIsInstance(unix, int)

    def test_market_open(self):
        open_time = dateutils.market_open_unix('2023-05-06')
        self.assertEqual(dateutils.str_to_unix('2023-05-06 09:30:00', tz = dateutils.EST), open_time)
    
    def test_market_close(self):
        close_time = dateutils.market_close_unix('2023-05-06')
        self.assertEqual(dateutils.str_to_unix('2023-05-06 16:00:00', tz = dateutils.EST), close_time)