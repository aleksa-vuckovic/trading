import random
from unittest import TestCase
from base import dates
from trading.core import Interval
from trading.core.securities import Exchange
from trading.core.work_calendar import WorkCalendar, BasicWorkCalendar, WorkSchedule, Hours

calendar = BasicWorkCalendar(tz=dates.ET, work_schedule=WorkSchedule.Builder(Hours(9, 16, open_minute=30)).build())
exchange = Exchange('XTST', 'XTST', 'XTST', 'Test', calendar)

class TestCalendar(TestCase):
    def get_calendar(self) -> WorkCalendar:
        return calendar
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
                self.assertEqual(expect, result, f"""
({name}) Unexpected timestamps for interval {interval}, start {calendar.unix_to_datetime(start)}, end {calendar.unix_to_datetime(end)}.
Expect {[str(calendar.unix_to_datetime(it)) for it in expect]}.
Got {[str(calendar.unix_to_datetime(it)) for it in result]}.
""")

class TestBasicWorkCalendar(TestCalendar):
    def get_calendar(self) -> WorkCalendar:
        return BasicWorkCalendar(tz = dates.CET, work_schedule=WorkSchedule.Builder(Hours(12, 13, open_minute=30, close_minute=30)).build())
    def get_next_timestamp_examples(self, interval: Interval) -> list[tuple[str,str]]:
        if interval == Interval.H1:
            return [
                ('2025-04-16 00:00:00', '2025-04-16 13:00:00'),
                ('2025-04-16 13:00:00', '2025-04-16 14:00:00'),
                ('2025-04-16 14:00:00', '2025-04-17 13:00:00')
            ]
        return []
    
    def get_timestamps_examples(self, interval: Interval) -> list[tuple[str, str, list[str]]]:
        return []

    def test_set_open_close(self):
        examples = [
            ('2025-04-17 00:00:00', '2025-04-16 09:30:00', '2025-04-16 16:00:00'),
            ('2025-04-21 00:00:01', '2025-04-21 09:30:00', '2025-04-21 16:00:00')
        ]
        for time, open, close in examples:
            time = calendar.str_to_datetime(time)
            open = calendar.str_to_datetime(open)
            close = calendar.str_to_datetime(close)
            self.assertEqual(open, calendar.set_open(time))
            self.assertEqual(close, calendar.set_close(time))
            
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
