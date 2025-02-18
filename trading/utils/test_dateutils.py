import unittest
import random
from .common import Interval
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
        expect = dateutils.str_to_unix('2025-01-23 10:00:00', tz=dateutils.ET)
        self.assertEqual(expect, dateutils.get_next_working_time_unix(input))

        input = dateutils.str_to_unix('2025-01-17 20:01:12', tz=dateutils.ET)
        expect = dateutils.str_to_unix('2025-01-20 10:00:00', tz=dateutils.ET)
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

    def test_get_next_interval_time(self):
        pairs = [
            ('2025-02-18 09:15:12', '2025-02-18 10:30:00'),
            ('2025-02-18 14:30:00', '2025-02-18 15:30:00'),
            ('2025-02-18 15:30:00', '2025-02-18 16:00:00'),
            ('2025-02-18 12:31:12', '2025-02-18 13:30:00'),
            ('2025-02-18 12:29:59', '2025-02-18 12:30:00')
        ]
        for input, expect in pairs:
            input = dateutils.str_to_datetime(input)
            expect = dateutils.str_to_datetime(expect)
            if random.random() < 0.5:
                self.assertEqual(expect, dateutils.get_next_interval_time_datetime(input, interval=Interval.H1))
            else:
                self.assertEqual(expect.timestamp(), dateutils.get_next_interval_time_unix(input.timestamp(), Interval.H1))

        pairs = [
            ('2025-02-15 05:00:12', '2025-02-17 16:00:00'),
            ('2025-02-17 10:44:44', '2025-02-17 16:00:00'),
            ('2025-02-17 16:00:00', '2025-02-18 16:00:00')
        ]
        for input, expect in pairs:
            input = dateutils.str_to_datetime(input)
            expect = dateutils.str_to_datetime(expect)
            if random.random() < 0.5:
                self.assertEqual(expect, dateutils.get_next_interval_time_datetime(input, interval=Interval.D1))
            else:
                self.assertEqual(expect.timestamp(), dateutils.get_next_interval_time_unix(input.timestamp(), Interval.D1))

    def test_get_interval_timestamps(self):
        start = dateutils.str_to_unix('2025-02-16 00:00:00')
        end = dateutils.str_to_unix('2025-02-19 15:30:00')
        times = dateutils.get_interval_timestamps(start, end, Interval.D1)
        self.assertEqual([1739826000.0, 1739912400.0], times)
        times = dateutils.get_interval_timestamps(start, end, Interval.H1)
        self.assertEqual([
            1739806200.0, 1739809800.0, 1739813400.0, 1739817000.0, 
            1739820600.0, 1739824200.0, 1739826000.0, 1739892600.0, 
            1739896200.0, 1739899800.0, 1739903400.0, 1739907000.0, 
            1739910600.0, 1739912400.0, 1739979000.0, 1739982600.0, 
            1739986200.0, 1739989800.0, 1739993400.0
        ], times)