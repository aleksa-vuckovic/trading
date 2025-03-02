import unittest
import random
from . import jsonutils
from .dateutils import TimingConfig, XNAS
from .common import Interval


class TestDates(unittest.TestCase):

    def test_str_to_unix(self):
        unix = XNAS.str_to_unix('2023-05-06 06:20:30')
        self.assertEqual(1683368430, unix)
        self.assertIsInstance(unix, float)

    def assert_next_interval(self, pairs: list[tuple[str, str]], interval: Interval):
        for input, expect in pairs:
            input = XNAS.str_to_datetime(input)
            expect = XNAS.str_to_datetime(expect)
            if random.random() < 0.5:
                result = XNAS.get_next_timestamp(input, interval)
                self.assertEqual(expect, result, f"Expect {expect} for {input} on {interval}, but got {result}.")
            else:
                result = XNAS.get_next_timestamp(input.timestamp(), interval)
                self.assertEqual(expect.timestamp(), result, f"Expect {expect} for {input}, but got {XNAS.unix_to_datetime(result)}.")

    def test_get_next_interval_time_l1(self):
        pairs = [
            ('2025-01-05 12:12:12', '2025-01-31 16:00:00'),
            ('2025-01-31 16:00:00', '2025-02-28 16:00:00'),
            ('2024-03-15 12:12:12', '2024-03-28 16:00:00'),
            ('2024-03-29 16:01:00', '2024-04-30 16:00:00')
        ]
        self.assert_next_interval(pairs, Interval.L1)

    def test_get_next_interval_time_w1(self):
        pairs = [
            ('2025-02-24 12:00:00', '2025-02-28 16:00:00'),
            ('2025-02-28 16:00:01', '2025-03-07 16:00:00'),
            ('2025-02-28 16:00:00', '2025-03-07 16:00:00')
        ]
        self.assert_next_interval(pairs, Interval.W1)

    def test_get_next_interval_time_d1(self):
        pairs = [
            ('2025-02-15 05:00:12', '2025-02-18 16:00:00'),
            ('2025-02-18 10:44:44', '2025-02-18 16:00:00'),
            ('2025-02-18 16:00:00', '2025-02-19 16:00:00'),
            ('2025-02-14 20:00:00', '2025-02-18 16:00:00'),
            ('2025-12-24 11:00:00', '2025-12-24 13:00:00'),
            ('2025-12-24 15:00:00', '2025-12-26 16:00:00'),
        ]
        self.assert_next_interval(pairs, Interval.D1)
    
    def test_get_next_interval_time_h1(self):
        pairs = [
            ('2025-02-18 09:15:12', '2025-02-18 10:30:00'),
            ('2025-02-18 14:30:00', '2025-02-18 15:30:00'),
            ('2025-02-18 15:30:00', '2025-02-18 16:00:00'),
            ('2025-02-18 12:31:12', '2025-02-18 13:30:00'),
            ('2025-02-18 12:29:59', '2025-02-18 12:30:00'),
            ('2025-02-18 15:39:00', '2025-02-18 16:00:00'),
            ('2025-02-14 16:00:00', '2025-02-18 10:30:00'),
            ('2025-12-24 12:30:00', '2025-12-24 13:00:00'),
            ('2025-12-24 15:00:00', '2025-12-26 10:30:00'),
        ]
        self.assert_next_interval(pairs, Interval.H1)

    def test_get_next_interval_time_m15(self):
        pairs = [
            ('2025-02-24 09:30:00', '2025-02-24 09:45:00'),
            ('2025-02-24 09:31:00', '2025-02-24 09:45:00'),
            ('2025-02-24 15:55:00', '2025-02-24 16:00:00'),
            ('2025-02-28 16:00:00', '2025-03-03 09:45:00'),
            ('2025-03-01 20:00:00', '2025-03-03 09:45:00'),
            ('2025-01-01 13:00:00', '2025-01-02 09:45:00'),
        ]
        self.assert_next_interval(pairs, Interval.M15)

    def test_get_next_interval_time_m5(self):
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
    
    def test_get_timestamps_l1(self):
        start = XNAS.str_to_unix('2024-11-25 16:00:00')
        end = XNAS.str_to_unix('2025-05-02 16:00:00')
        expect = [XNAS.str_to_unix(it) for it in [
            '2024-11-29 13:00:00', '2024-12-31 16:00:00', '2025-01-31 16:00:00',
            '2025-02-28 16:00:00', '2025-03-31 16:00:00', '2025-04-30 16:00:00'
        ]]
        result = XNAS.get_timestamps(start, end, Interval.L1)
        self.assertEqual(expect, result)

    def test_get_timestamps_w1(self):
        start = XNAS.str_to_unix('2025-02-28 16:00:00')
        end = XNAS.str_to_unix('2025-05-02 16:00:00')
        expect = [XNAS.str_to_unix(it) for it in [
            '2025-03-07 16:00:00', '2025-03-14 16:00:00', '2025-03-21 16:00:00', '2025-03-28 16:00:00', 
            '2025-04-04 16:00:00', '2025-04-11 16:00:00', '2025-04-17 16:00:00', '2025-04-25 16:00:00', '2025-05-02 16:00:00'
        ]]
        result = XNAS.get_timestamps(start, end, Interval.W1)
        self.assertEqual(expect, result)

    def test_get_timestamps_d1(self):
        start = XNAS.str_to_unix('2025-02-16 00:00:00')
        end = XNAS.str_to_unix('2025-02-19 15:30:00')
        times = XNAS.get_timestamps(start, end, Interval.D1)
        self.assertEqual([1739912400.0], times)
        start = XNAS.str_to_unix('2024-03-08 14:30:00')
        end = XNAS.str_to_unix('2024-03-12 10:30:00')
        times = XNAS.get_timestamps(start, end, Interval.D1)
        expect = [XNAS.str_to_unix(it) for it in ['2024-03-08 16:00:00', '2024-03-11 16:00:00']]
        self.assertEqual(expect, times)


    def test_get_timestamps_h1(self):
        start = XNAS.str_to_unix('2025-02-16 00:00:00')
        end = XNAS.str_to_unix('2025-02-19 15:30:00')
        times = XNAS.get_timestamps(start, end, Interval.H1)
        self.assertEqual([
            1739892600.0, 1739896200.0, 1739899800.0, 1739903400.0, 1739907000.0, 
            1739910600.0, 1739912400.0, 1739979000.0, 1739982600.0, 
            1739986200.0, 1739989800.0, 1739993400.0, 1739997000.0
        ], times)
        start = XNAS.str_to_unix('2024-03-08 14:30:00')
        end = XNAS.str_to_unix('2024-03-12 10:30:00')
        times = XNAS.get_timestamps(start, end, Interval.H1)
        expect = [XNAS.str_to_unix(it) for it in [
            '2024-03-08 15:30:00', '2024-03-08 16:00:00',
            '2024-03-11 10:30:00', '2024-03-11 11:30:00', '2024-03-11 12:30:00', '2024-03-11 13:30:00',
            '2024-03-11 14:30:00', '2024-03-11 15:30:00', '2024-03-11 16:00:00', '2024-03-12 10:30:00'
        ]]
        self.assertEqual(expect, times)

    def test_get_timestamps_m15(self):
        start = XNAS.str_to_unix('2025-02-28 14:30:00')
        end = XNAS.str_to_unix('2025-03-03 10:00:00')
        expect = [XNAS.str_to_unix(it) for it in [
            '2025-02-28 14:45:00', '2025-02-28 15:00:00', '2025-02-28 15:15:00', '2025-02-28 15:30:00',
            '2025-02-28 15:45:00', '2025-02-28 16:00:00', '2025-03-03 09:45:00', '2025-03-03 10:00:00'
        ]]
        result = XNAS.get_timestamps(start, end, Interval.M15)
        self.assertEqual(expect, result)
    
    def test_get_timestamps_m5(self):
        start = XNAS.str_to_unix('2025-02-28 15:45:00')
        end = XNAS.str_to_unix('2025-03-03 09:40:00')
        expect = [XNAS.str_to_unix(it) for it in [
            '2025-02-28 15:50:00', '2025-02-28 15:55:00', '2025-02-28 16:00:00',
            '2025-03-03 09:35:00', '2025-03-03 09:40:00'   
        ]]
        result = XNAS.get_timestamps(start, end, Interval.M5)
        self.assertEqual(expect, result)

    def test_timing_config(self):
        config = TimingConfig.Builder()\
            .at(hour = 11, minute = 0)\
            .around(hour = 14, minute = 30, delta_minute=20)\
            .starting(hour = 15, minute = 0).until(hour = 16, minute = 0)\
            .build()
        config_ = jsonutils.deserialize(jsonutils.serialize(config))
        self.assertEqual(config, config_)
        config = config.with_calendar(XNAS)
        
        examples = [(it, True) for it in ['2025-02-21 11:00:00', '2025-02-21 14:30:00', '2025-02-21 14:50:00',
            '2025-02-21 15:00:01', '2025-02-21 16:00:00', '2025-02-21 14:11:00']]
        examples.extend((it, False) for it in ['2025-02-21 11:00:01', '2025-02-21 14:10:00', '2025-02-21 14:50:01',
            '2025-02-21 15:00:00', '2025-02-22 11:00:00', '2025-02-22 14:30:00'])
        
        for input, expect in examples:
            if random.random() < 0.5: self.assertEqual(expect, XNAS.str_to_datetime(input) in config, f"Expected {expect} for {input}.")
            else: self.assertEqual(expect, XNAS.str_to_unix(input) in config, f"Expected {expect} for {input}.")

    def test_timing_config_next(self):
        config = TimingConfig.Builder()\
            .at(hour = 11, minute = 30)\
            .around(hour = 14, minute = 30, delta_minute=20)\
            .starting(hour = 15, minute = 0).until(hour = 16, minute = 0)\
            .build()
        config_ = jsonutils.deserialize(jsonutils.serialize(config))
        self.assertEqual(config, config_)
        config = config.with_calendar(XNAS).with_interval(Interval.H1)
        
        expect = [XNAS.str_to_unix(it) for it in 
            ['2025-02-21 11:30:00', '2025-02-21 14:30:00', '2025-02-21 15:30:00', '2025-02-21 16:00:00', '2025-02-24 11:30:00']]
        result = []
        cur = XNAS.str_to_unix('2025-02-21 10:00:00')
        for i in range(len(expect)):
            cur = config.get_next_time(cur)
            result.append(cur)
        self.assertEqual(expect, result)

        config = TimingConfig.Builder()\
            .around(hour = 11, minute = 0, delta_minute = 30)\
            .build()
        config = config.with_calendar(XNAS).with_interval(Interval.M5)
        
        expect = [XNAS.str_to_unix(it) for it in 
            ['2025-02-21 10:35:00', '2025-02-21 10:40:00', '2025-02-21 10:45:00', '2025-02-21 10:50:00']]
        result = []
        cur = XNAS.str_to_unix('2025-02-21 10:00:00')
        for i in range(len(expect)):
            cur = config.get_next_time(cur)
            result.append(cur)
        self.assertEqual(expect, result)

    def test_add_intervals(self):
        dates = ['2025-02-21 13:34:12', '2025-02-21 16:00:00', '2025-02-21 18:23:23', '2025-02-22 12:12:12', '2025-02-24 07:12:13']
        counts = range(-29,30,2)
        
        for unix_time in [XNAS.str_to_unix(it) for it in dates]:
            for count in counts:
                for interval in Interval:
                    time = XNAS.add_intervals(unix_time, interval, count)
                    timestamps = XNAS.get_timestamps(time, unix_time, interval) if count < 0 else XNAS.get_timestamps(unix_time, time, interval)
                    self.assertEqual(len(timestamps), abs(count), f"Time {XNAS.unix_to_datetime(unix_time)} count {count} interval {interval}")