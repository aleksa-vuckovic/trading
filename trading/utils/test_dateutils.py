import unittest
from . import dateutils

class TestDates(unittest.TestCase):

    def test_str_to_unix(self):
        unix = dateutils.str_to_unix('2023-05-06 06:20:30', tz = dateutils.ET)
        self.assertEqual(1683368430, unix)
        self.assertIsInstance(unix, float)

    def test_market_open(self):
        open_time = dateutils.market_open_unix('2023-05-06')
        self.assertEqual(dateutils.str_to_unix('2023-05-06 09:30:00', tz = dateutils.ET), open_time)
    
    def test_market_close(self):
        close_time = dateutils.market_close_unix('2023-05-06')
        self.assertEqual(dateutils.str_to_unix('2023-05-06 16:00:00', tz = dateutils.ET), close_time)

    def test_add_business_days(self):
        unix = dateutils.str_to_unix('2025-01-25 18:00:00', tz=dateutils.ET)
        result = dateutils.add_business_days_unix(unix, 1, tz=dateutils.ET)
        self.assertEqual(dateutils.str_to_unix('2025-01-28 00:00:00', tz=dateutils.ET), result)

        unix = dateutils.str_to_unix('2025-01-24 12:01:02', tz=dateutils.ET)
        result = dateutils.add_business_days_unix(unix, 2, tz=dateutils.ET)
        self.assertEqual(dateutils.str_to_unix('2025-01-28 12:01:02', tz=dateutils.ET), result)