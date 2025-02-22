import unittest
import random
from . import dateutils, jsonutils
from .dateutils import TimingConfig
from .common import Interval

class TestDates(unittest.TestCase):

    def test_str_to_unix(self):
        unix = dateutils.str_to_unix('2023-05-06 06:20:30', tz = dateutils.ET)
        self.assertEqual(1683368430, unix)
        self.assertIsInstance(unix, float)

    def test_add_intervals_d1(self):
        examples = [
            ('2025-01-25 18:00:00', 1, '2025-01-27 16:00:00'),
            ('2025-01-24 12:01:02', 2, '2025-01-28 12:01:02'),
            ('2025-01-28 09:30:00', 2, '2025-01-29 16:00:00'),
            ('2025-01-28 16:00:00', 3, '2025-01-31 16:00:00'),
            ('2025-01-28 15:35:00', 14, '2025-02-17 15:35:00')
        ]
        for date, count, expect in examples:
            date = dateutils.str_to_unix(date, tz=dateutils.ET)
            expect = dateutils.str_to_unix(expect, tz=dateutils.ET)
            result = dateutils.add_intervals_unix(date, Interval.D1, count, tz=dateutils.ET)
            self.assertEqual(expect, result)
    
    def test_add_intervals_h1(self):
        examples = [
            ('2025-01-28 10:01:02', 1, '2025-01-28 11:01:02'),
            ('2025-01-28 08:13:11', 2, '2025-01-28 11:30:00'),
            ('2025-01-28 08:00:00', 7, '2025-01-28 16:00:00'),
            ('2025-01-28 08:00:00', 8, '2025-01-29 10:30:00'),
            ('2025-01-28 11:23:45', 72, '2025-02-11 13:23:45'),
            ('2025-01-28 09:30:00', 70, '2025-02-10 16:00:00'),
            ('2025-01-28 15:30:00', 1, '2025-01-28 16:00:00'),
            ('2025-01-28 15:31:00', 1, '2025-01-29 09:32:00')
        ]
        for date, count, expect in examples:
            date = dateutils.str_to_unix(date, tz=dateutils.ET)
            expect = dateutils.str_to_unix(expect, tz=dateutils.ET)
            result = dateutils.add_intervals_unix(date, Interval.H1, count, tz=dateutils.ET)
            self.assertEqual(expect, result)

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
            ('2025-02-18 12:29:59', '2025-02-18 12:30:00'),
            ('2025-02-18 15:39:00', '2025-02-18 16:00:00'),
            ('2025-02-14 16:00:00', '2025-02-17 10:30:00')
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
            ('2025-02-17 16:00:00', '2025-02-18 16:00:00'),
            ('2025-02-14 20:00:00', '2025-02-17 16:00:00')
        ]
        for input, expect in pairs:
            input = dateutils.str_to_datetime(input)
            expect = dateutils.str_to_datetime(expect)
            if random.random() < 0.5:
                self.assertEqual(expect, dateutils.get_next_interval_time_datetime(input, interval=Interval.D1))
            else:
                self.assertEqual(expect.timestamp(), dateutils.get_next_interval_time_unix(input.timestamp(), Interval.D1))

    def test_get_next_interval_time_dst(self):
        pairs = [
            ('2024-03-09 16:00:00', '2024-03-11 10:30:00'),
            ('2024-11-01 16:00:00', '2024-11-04 10:30:00')
        ]
        for input, expect in pairs:
            input = dateutils.str_to_datetime(input)
            expect = dateutils.str_to_datetime(expect)
            if random.random() < 0.5:
                self.assertEqual(expect, dateutils.get_next_interval_time_datetime(input, interval=Interval.H1))
            else:
                self.assertEqual(expect.timestamp(), dateutils.get_next_interval_time_unix(input.timestamp(), Interval.H1))
    
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

        start = dateutils.str_to_unix('2024-03-08 14:30:00')
        end = dateutils.str_to_unix('2024-03-12 10:30:00')
        times = dateutils.get_interval_timestamps(start, end, Interval.D1)
        expect = [dateutils.str_to_unix(it, tz=dateutils.ET) for it in ['2024-03-08 16:00:00', '2024-03-11 16:00:00']]
        self.assertEqual(expect, times)
        times = dateutils.get_interval_timestamps(start, end, Interval.H1)
        expect = [dateutils.str_to_unix(it, tz=dateutils.ET) for it in [
            '2024-03-08 14:30:00', '2024-03-08 15:30:00', '2024-03-08 16:00:00',
            '2024-03-11 10:30:00', '2024-03-11 11:30:00', '2024-03-11 12:30:00', '2024-03-11 13:30:00',
            '2024-03-11 14:30:00', '2024-03-11 15:30:00', '2024-03-11 16:00:00'
        ]]
        self.assertEqual(expect, times)

    def test_skip_weekend(self):
        examples = [
            ('2025-02-22 01:22:33', '2025-02-24 00:00:00'),
            ('2025-02-21 23:59:59', '2025-02-21 23:59:59'),
            ('2025-02-23 20:00:00', '2025-02-24 00:00:00'),
            ('2025-02-18 23:45:12', '2025-02-18 23:45:12')
        ]

        for input, expect in examples:
            input = dateutils.str_to_datetime(input)
            expect = dateutils.str_to_datetime(expect)
            if random.random() < 0.5:
                self.assertEqual(expect.timestamp(), dateutils.skip_weekend_unix(input.timestamp(), tz=dateutils.ET))
            else:
                self.assertEqual(expect, dateutils.skip_weekend_datetime(input))
        
    def test_timing_config(self):
        config = TimingConfig.Builder()\
            .at(hour = 11, minute = 0)\
            .around(hour = 14, minute = 30, delta_minute=20)\
            .starting(hour = 15, minute = 0).until(hour = 16, minute = 0)\
            .build()
        config_ = jsonutils.deserialize(jsonutils.serialize(config))
        self.assertEqual(config, config_)
        
        examples = [(it, True) for it in ['2025-02-21 11:00:00', '2025-02-21 14:30:00', '2025-02-21 14:50:00',
            '2025-02-21 15:00:01', '2025-02-21 16:00:00', '2025-02-21 14:11:00']]
        examples.extend((it, False) for it in ['2025-02-21 11:00:01', '2025-02-21 14:10:00', '2025-02-21 14:50:01',
            '2025-02-21 15:00:00', '2025-02-22 11:00:00', '2025-02-22 14:30:00'])
        
        for input, expect in examples:
            if random.random() < 0.5: self.assertEqual(expect, dateutils.str_to_datetime(input) in config)
            else: self.assertEqual(expect, dateutils.str_to_unix(input) in config)

    def test_timing_config_next(self):
        config = TimingConfig.Builder()\
            .at(hour = 11, minute = 0)\
            .around(hour = 14, minute = 30, delta_minute=20)\
            .starting(hour = 15, minute = 0).until(hour = 16, minute = 0)\
            .build()
        config_ = jsonutils.deserialize(jsonutils.serialize(config))
        self.assertEqual(config, config_)
        
        expect = [dateutils.str_to_datetime(it, tz=dateutils.ET) for it in 
            ['2025-02-21 11:00:00', '2025-02-21 14:50:00', '2025-02-21 16:00:00', '2025-02-24 11:00:00']]
        result = []
        cur = dateutils.str_to_datetime('2025-02-21 10:00:00', tz=dateutils.ET)
        for i in range(len(expect)):
            if random.random() < 0.5:
                cur = config.get_next_datetime(cur, step=3600)
                result.append(cur)
            else:
                cur = dateutils.unix_to_datetime(config.get_next_unix(cur.timestamp(), step=3600), tz=dateutils.ET)
                result.append(cur)
        self.assertEqual(expect, result)

        config = TimingConfig.Builder()\
            .around(hour = 11, minute = 0, delta_minute = 30)\
            .build()
        
        expect = [dateutils.str_to_datetime(it, tz=dateutils.ET) for it in 
            ['2025-02-21 10:31:00', '2025-02-21 10:32:00', '2025-02-21 10:33:00', '2025-02-21 10:34:00']]
        result = []
        cur = dateutils.str_to_datetime('2025-02-21 10:00:00', tz=dateutils.ET)
        for i in range(len(expect)):
            if random.random() < 0.5:
                cur = config.get_next_datetime(cur, step=60)
                result.append(cur)
            else:
                cur = dateutils.unix_to_datetime(config.get_next_unix(cur.timestamp(), step=60), tz=dateutils.ET)
                result.append(cur)
        self.assertEqual(expect, result)