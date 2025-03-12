import unittest
import random
from base import dates
from base.serialization import serializer
from trading.core import Interval
from trading.core.work_calendar import WorkCalendar, BasicWorkCalendar, TimingConfig

calendar = BasicWorkCalendar(tz=dates.ET, open_hour=9, open_minute=30, close_hour=16)
class TestTimingConfig(unittest.TestCase):
    def test_timing_config_next(self):
        config = TimingConfig.Builder()\
            .at(hour = 11, minute = 30)\
            .around(hour = 14, minute = 30, delta_minute=20)\
            .starting(hour = 15, minute = 0).until(hour = 16, minute = 0)\
            .build()
        config_ = serializer.deserialize(serializer.serialize(config))
        self.assertEqual(config, config_)
        config = config.with_calendar(calendar).with_interval(Interval.H1)
        
        expect = [calendar.str_to_unix(it) for it in 
            ['2025-02-21 11:30:00', '2025-02-21 14:30:00', '2025-02-21 15:30:00', '2025-02-21 16:00:00', '2025-02-24 11:30:00']]
        result = []
        cur = calendar.str_to_unix('2025-02-21 10:00:00')
        for i in range(len(expect)):
            cur = config.get_next_time(cur)
            result.append(cur)
        self.assertEqual(expect, result)

        config = TimingConfig.Builder()\
            .around(hour = 11, minute = 0, delta_minute = 30)\
            .build()
        config = config.with_calendar(calendar).with_interval(Interval.M5)
        
        expect = [calendar.str_to_unix(it) for it in 
            ['2025-02-21 10:35:00', '2025-02-21 10:40:00', '2025-02-21 10:45:00', '2025-02-21 10:50:00']]
        result = []
        cur = calendar.str_to_unix('2025-02-21 10:00:00')
        for i in range(len(expect)):
            cur = config.get_next_time(cur)
            result.append(cur)
        self.assertEqual(expect, result)

    @unittest.skip('Takes too long')
    def test_add_intervals(self):
        dates = [
            '2025-02-21 13:34:12', '2025-02-21 16:00:00', '2025-02-21 18:23:23',
            '2025-02-22 12:12:12', '2025-02-24 07:12:13', '1974-07-15 10:00:00',
            '1977-07-15 10:00:00'
        ]
        counts = range(-29,30,2)
        
        for unix_time in [calendar.str_to_unix(it) for it in dates]:
            for count in counts:
                for interval in Interval:
                    time = calendar.add_intervals(unix_time, interval, count)
                    timestamps = calendar.get_timestamps(time, unix_time, interval) if count < 0 else calendar.get_timestamps(unix_time, time, interval)
                    self.assertEqual(len(timestamps), abs(count), f"Time {calendar.unix_to_datetime(unix_time)} count {count} interval {interval}")

@unittest.skip('Abstract test class')
class TestCalendar(unittest.TestCase):
    def get_calendar(self) -> WorkCalendar:
        raise NotImplementedError()
    def get_next_timestamp_examples(self, interval: Interval) -> list[tuple[str,str]]:
        return []
    def get_timestamps_examples(self, interval: Interval) -> list[tuple[str, str, list[str]]]:
        return []

    def test_get_next_timestamp(self):
        calendar = self.get_calendar()
        name = calendar.__class__.__name__
        for interval in Interval.descending:
            for input, expect in self.get_next_timestamp_examples(interval):
                input = calendar.str_to_datetime(input)
                expect = calendar.str_to_datetime(expect)
                if random.random() < 0.5:
                    result = calendar.get_next_timestamp(input, interval)
                    self.assertEqual(expect, result, f"({name})Expect {expect} for {input} on {interval}, but got {result}.")
                else:
                    result = calendar.get_next_timestamp(input.timestamp(), interval)
                    self.assertEqual(expect.timestamp(), result, f"({name})Expect {expect} for {input} on {interval}, but got {calendar.unix_to_datetime(result)}.")

    def test_get_timestamps(self):
        calendar = self.get_calendar()
        name = calendar.__class__.__name__
        for interval in Interval.descending:
            for start,end,expect in self.get_timestamps_examples(interval):
                start = calendar.str_to_unix(start)
                end = calendar.str_to_unix(end)
                expect = [calendar.str_to_unix(it) for it in expect]
                result = calendar.get_timestamps(start, end, interval)
                self.assertEqual(expect, result, f"({name}) Unexpected timestamps for interval {interval}, start {calendar.unix_to_datetime(start)}, end {calendar.unix_to_datetime(end)}.")
                