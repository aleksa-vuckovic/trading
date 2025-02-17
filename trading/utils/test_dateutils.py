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

    def test_get_next_time(self):
        input = dateutils.str_to_unix('2025-01-23 02:12:22', tz=dateutils.ET)
        expect = dateutils.str_to_unix('2025-01-23 09:00:00', tz=dateutils.ET)
        self.assertEqual(expect, dateutils.get_next_working_time_unix(input))

        input = dateutils.str_to_unix('2025-01-17 20:01:12', tz=dateutils.ET)
        expect = dateutils.str_to_unix('2025-01-20 09:00:00', tz=dateutils.ET)
        self.assertEqual(expect, dateutils.get_next_working_time_unix(input))

        input = dateutils.str_to_unix('2025-01-20 11:00:00', tz=dateutils.ET)
        expect = dateutils.str_to_unix('2025-01-20 12:00:00', tz=dateutils.ET)
        self.assertEqual(expect, dateutils.get_next_working_time_unix(input))

    def test_get_next_time_by_hour(self):
        input = dateutils.str_to_unix('2025-01-23 02:12:22', tz=dateutils.ET)
        expect = dateutils.str_to_unix('2025-01-23 11:00:00', tz=dateutils.ET)
        self.assertEqual(expect, dateutils.get_next_working_time_unix(input, hour=11))

        input = dateutils.str_to_unix('2025-01-17 15:01:12', tz=dateutils.ET)
        expect = dateutils.str_to_unix('2025-01-20 15:00:00', tz=dateutils.ET)
        self.assertEqual(expect, dateutils.get_next_working_time_unix(input, hour=15))

        input = dateutils.str_to_unix('2025-01-20 11:00:00', tz=dateutils.ET)
        expect = dateutils.str_to_unix('2025-01-20 13:00:00', tz=dateutils.ET)
        self.assertEqual(expect, dateutils.get_next_working_time_unix(input, hour=13))

        input = dateutils.str_to_unix('2025-01-24 11:00:00', tz=dateutils.ET)
        expect = dateutils.str_to_unix('2025-01-27 11:00:00', tz=dateutils.ET)
        self.assertEqual(expect, dateutils.get_next_working_time_unix(input, hour=11))

    def test_datetime_to_daysecs(self):
        date = dateutils.str_to_datetime('2020-05-05 10:12:13')
        result = dateutils.datetime_to_daysecs(date)
        expect = 10*3600 + 12*60 + 13
        self.assertEqual(expect, result)

        result = dateutils.str_to_daysecs('2020-05-05 00:00:00')
        self.assertEqual(0, result)

        result = dateutils.unix_to_daysecs(dateutils.str_to_unix('2020-05-01 23:59:59', tz=dateutils.ET), dateutils.ET)
        expect = 23*3600 + 59*60 + 59
        self.assertEqual(expect, result)