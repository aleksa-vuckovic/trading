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

    def assert_intervals(self, examples: list[tuple[str,int,str]], interval: Interval):
        for input, count, expect in examples:
            date = dateutils.str_to_unix(input)
            expect = dateutils.str_to_unix(expect)
            result = dateutils.add_intervals_unix(date, interval, count, tz=dateutils.ET)
            self.assertEqual(expect, result)
    def test_add_intervals_l1(self):
        examples = [
            ('2025-01-31 16:00:00', 1, '2025-02-28 16:00:00'),
            ('2025-01-31 16:00:00', 4, '2025-05-30 16:00:00'),
            ('2024-01-31 16:00:00', 15, '2025-04-30 16:00:00')
        ]
        examples = [*examples, *((expect, -count, input) for input, count, expect in examples)]
        examples = [
            *examples,
            ('2024-10-12 12:56:12', 3, '2025-01-10 16:00:00'),
            ('2024-10-10 12:56:12', 3, '2025-01-10 12:56:12'),
            ('2025-01-15 12:34:56', -1, '2024-12-16 09:30:00'),
            ('2025-02-01 18:12:34', -3, '2024-11-04 09:30:00')
        ]
        self.assert_intervals(examples, Interval.L1)

    def test_add_intervals_w1(self):
        examples = [
            ('2025-01-31 16:00:00', 1, '2025-02-07 16:00:00'),
            ('2025-01-31 16:00:00', 9, '2025-04-04 16:00:00'),
            ('2025-01-31 16:00:00', 59, '2026-03-20 16:00:00')
        ]
        examples = [*examples, *((expect, -count, input) for input, count, expect in examples)]
        examples = [
            *examples,
            ('2025-02-01 22:00:01', 1, '2025-02-07 16:00:00'),
            ('2025-02-01 22:00:01', -2, '2025-01-20 09:30:00')
        ]
        self.assert_intervals(examples, Interval.W1)

    def test_add_intervals_d1(self):
        examples = [
            ('2025-01-24 12:01:02', 2, '2025-01-28 12:01:02'),
            ('2025-01-28 09:30:00', 2, '2025-01-29 16:00:00'),
            ('2025-01-28 15:35:00', 14, '2025-02-17 15:35:00')
        ]
        examples = [*examples, *((expect, -count, input) for input, count, expect in examples)]
        examples = [
            *examples,
            ('2025-01-25 18:00:00', 1, '2025-01-27 16:00:00'),
            ('2025-01-28 16:00:00', 3, '2025-01-31 16:00:00'),
            ('2025-01-26 20:00:00', -7, '2025-01-16 09:30:00')
        ]
        self.assert_intervals(examples, Interval.D1)
    
    def test_add_intervals_h1(self):
        examples = [
            ('2025-01-28 10:01:02', 1, '2025-01-28 11:01:02'),
            ('2025-01-28 11:23:45', 72, '2025-02-11 13:23:45'),
            ('2025-01-28 09:30:00', 70, '2025-02-10 16:00:00'),
            ('2025-01-28 09:30:00', 1, '2025-01-28 10:30:00'),
            ('2025-01-28 15:30:00', 1, '2025-01-28 16:00:00'),
            ('2025-01-28 15:31:00', 1, '2025-01-29 09:32:00')
        ]
        examples = [*examples, *((expect, -count, input) for input, count, expect in examples)]
        examples = [
            *examples,
            ('2025-01-28 08:13:11', 2, '2025-01-28 11:30:00'),
            ('2025-01-28 08:00:00', 7, '2025-01-28 16:00:00'),
            ('2025-01-28 08:00:00', 8, '2025-01-29 10:30:00'),
            ('2025-01-28 08:13:11', -2, '2025-01-27 14:30:00'),
            ('2025-01-27 18:00:12', -7, '2025-01-27 09:30:00'),
            ('2025-01-27 18:00:12', -10, '2025-01-24 13:30:00')
        ]
        self.assert_intervals(examples, Interval.H1)

    def test_add_intervals_m15(self):
        examples = [
            ('2025-01-27 09:30:00', 1, '2025-01-27 09:45:00'),
            ('2025-01-27 10:12:23', 1, '2025-01-27 10:27:23'),
            ('2025-01-27 15:58:00', 2, '2025-01-28 09:58:00'),
            ('2025-02-21 15:00:00', 4+5*6.5*4+2, '2025-03-03 10:00:00')
        ]
        examples = [*examples, *((expect, -count, input) for input, count, expect in examples)]
        examples = [
            *examples,
            ('2025-02-20 08:23:00', 3, '2025-02-20 10:15:00'),
            ('2025-02-20 08:23:00', -3, '2025-02-19 15:15:00')
        ]
        self.assert_intervals(examples, Interval.M15)

    def test_add_intervals_m5(self):
        examples = [
            ('2025-01-27 09:30:00', 1, '2025-01-27 09:35:00'),
            ('2025-01-27 10:12:23', 1, '2025-01-27 10:17:23'),
            ('2025-01-27 15:58:00', 2, '2025-01-28 09:38:00'),
            ('2025-02-21 15:00:00', 12+5*6.5*12+2, '2025-03-03 09:40:00')
        ]
        examples = [*examples, *((expect, -count, input) for input, count, expect in examples)]
        examples = [
            *examples,
            ('2025-02-20 08:23:00', 3, '2025-02-20 09:45:00'),
            ('2025-02-20 08:23:00', -3, '2025-02-19 15:45:00')
        ]
        self.assert_intervals(examples, Interval.M5)


    def assert_next_interval(self, pairs: list[tuple[str, str]], interval: Interval):
        for input, expect in pairs:
            input = dateutils.str_to_datetime(input, tz=dateutils.ET)
            expect = dateutils.str_to_datetime(expect, tz=dateutils.ET)
            if random.random() < 0.5:
                self.assertEqual(expect, dateutils.get_next_interval_time_datetime(input, interval))
            else:
                self.assertEqual(expect.timestamp(), dateutils.get_next_interval_time_unix(input.timestamp(), interval))

    def test_get_next_interval_time_l1_w1(self):
        pairs = [
            ('2025-01-05 12:12:12', '2025-01-31 16:00:00'),
            ('2025-01-31 16:00:00', '2025-02-28 16:00:00'),
            ('2024-03-15 12:12:12', '2024-03-29 16:00:00'),
            ('2024-03-29 16:01:00', '2024-04-30 16:00:00')
        ]
        self.assert_next_interval(pairs, Interval.L1)

        pairs = [
            ('2025-02-24 12:00:00', '2025-02-28 16:00:00'),
            ('2025-02-28 16:00:01', '2025-03-07 16:00:00'),
            ('2025-02-28 16:00:00', '2025-03-07 16:00:00')
        ]
        self.assert_next_interval(pairs, Interval.W1)

    def test_get_next_interval_time_d1_h1(self):
        pairs = [
            ('2025-02-15 05:00:12', '2025-02-17 16:00:00'),
            ('2025-02-17 10:44:44', '2025-02-17 16:00:00'),
            ('2025-02-17 16:00:00', '2025-02-18 16:00:00'),
            ('2025-02-14 20:00:00', '2025-02-17 16:00:00')
        ]
        self.assert_next_interval(pairs, Interval.D1)

        pairs = [
            ('2025-02-18 09:15:12', '2025-02-18 10:30:00'),
            ('2025-02-18 14:30:00', '2025-02-18 15:30:00'),
            ('2025-02-18 15:30:00', '2025-02-18 16:00:00'),
            ('2025-02-18 12:31:12', '2025-02-18 13:30:00'),
            ('2025-02-18 12:29:59', '2025-02-18 12:30:00'),
            ('2025-02-18 15:39:00', '2025-02-18 16:00:00'),
            ('2025-02-14 16:00:00', '2025-02-17 10:30:00')
        ]
        self.assert_next_interval(pairs, Interval.H1)

    def test_get_next_interval_time_m15_m5(self):
        pairs = [
            ('2025-02-24 09:30:00', '2025-02-24 09:45:00'),
            ('2025-02-24 09:31:00', '2025-02-24 09:45:00'),
            ('2025-02-24 15:55:00', '2025-02-24 16:00:00'),
            ('2025-02-28 16:00:00', '2025-03-03 09:45:00'),
            ('2025-03-01 20:00:00', '2025-03-03 09:45:00'),
        ]
        self.assert_next_interval(pairs, Interval.M15)

        pairs = [
            ('2025-02-24 09:30:00', '2025-02-24 09:35:00'),
            ('2025-02-24 09:31:00', '2025-02-24 09:35:00'),
            ('2025-02-24 15:55:00', '2025-02-24 16:00:00'),
            ('2025-02-28 16:00:00', '2025-03-03 09:35:00'),
            ('2025-03-01 20:00:00', '2025-03-03 09:35:00'),
        ]
        self.assert_next_interval(pairs, Interval.M5)

    def test_get_next_interval_time_dst(self):
        pairs = [
            ('2024-03-09 16:00:00', '2024-03-11 10:30:00'),
            ('2024-11-01 16:00:00', '2024-11-04 10:30:00')
        ]
        self.assert_next_interval(pairs, Interval.H1)
    
    def test_get_interval_timestamps_l1_w1(self):
        start = dateutils.str_to_unix('2025-02-28 16:00:00')
        end = dateutils.str_to_unix('2025-05-02 16:00:00')
        expect = [dateutils.str_to_unix(it) for it in [
            '2025-03-07 16:00:00', '2025-03-14 16:00:00', '2025-03-21 16:00:00', '2025-03-28 16:00:00', 
            '2025-04-04 16:00:00', '2025-04-11 16:00:00', '2025-04-18 16:00:00', '2025-04-25 16:00:00', '2025-05-02 16:00:00'
        ]]
        result = dateutils.get_interval_timestamps(start, end, Interval.W1)
        self.assertEqual(expect, result)


        start = dateutils.str_to_unix('2025-02-28 16:00:00')
        end = dateutils.str_to_unix('2025-05-02 16:00:00')
        expect = [dateutils.str_to_unix(it) for it in [
            '2025-03-07 16:00:00', '2025-03-14 16:00:00', '2025-03-21 16:00:00', '2025-03-28 16:00:00', 
            '2025-04-04 16:00:00', '2025-04-11 16:00:00', '2025-04-18 16:00:00', '2025-04-25 16:00:00', '2025-05-02 16:00:00'
        ]]
        result = dateutils.get_interval_timestamps(start, end, Interval.W1)
        self.assertEqual(expect, result)

    def test_get_interval_timestamps_d1_h1(self):
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
            1739986200.0, 1739989800.0, 1739993400.0, 1739997000.0
        ], times)

        start = dateutils.str_to_unix('2024-03-08 14:30:00')
        end = dateutils.str_to_unix('2024-03-12 10:30:00')
        times = dateutils.get_interval_timestamps(start, end, Interval.D1)
        expect = [dateutils.str_to_unix(it, tz=dateutils.ET) for it in ['2024-03-08 16:00:00', '2024-03-11 16:00:00']]
        self.assertEqual(expect, times)
        times = dateutils.get_interval_timestamps(start, end, Interval.H1)
        expect = [dateutils.str_to_unix(it, tz=dateutils.ET) for it in [
            '2024-03-08 15:30:00', '2024-03-08 16:00:00',
            '2024-03-11 10:30:00', '2024-03-11 11:30:00', '2024-03-11 12:30:00', '2024-03-11 13:30:00',
            '2024-03-11 14:30:00', '2024-03-11 15:30:00', '2024-03-11 16:00:00', '2024-03-12 10:30:00'
        ]]
        self.assertEqual(expect, times)

    def test_get_interval_timestamps_m15_m5(self):
        start = dateutils.str_to_unix('2025-02-28 14:30:00')
        end = dateutils.str_to_unix('2025-03-03 10:00:00')
        expect = [dateutils.str_to_unix(it) for it in [
            '2025-02-28 14:45:00', '2025-02-28 15:00:00', '2025-02-28 15:15:00', '2025-02-28 15:30:00',
            '2025-02-28 15:45:00', '2025-02-28 16:00:00', '2025-03-03 09:45:00', '2025-03-03 10:00:00'
        ]]
        result = dateutils.get_interval_timestamps(start, end, Interval.M15)
        self.assertEqual(expect, result)


        start = dateutils.str_to_unix('2025-02-28 15:45:00')
        end = dateutils.str_to_unix('2025-03-03 09:40:00')
        expect = [dateutils.str_to_unix(it) for it in [
            '2025-02-28 15:50:00', '2025-02-28 15:55:00', '2025-02-28 16:00:00',
            '2025-03-03 09:35:00', '2025-03-03 09:40:00'   
        ]]
        result = dateutils.get_interval_timestamps(start, end, Interval.M5)
        self.assertEqual(expect, result)

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