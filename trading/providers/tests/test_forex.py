from typing import override

from trading.core import Interval
from trading.core.work_calendar import WorkCalendar
from trading.core.tests.test_work_calendar import TestCalendar
from trading.providers.forex import ForexWorkCalendar

class TestForexWorkCalendar(TestCalendar):
    @override
    def get_calendar(self) -> WorkCalendar:
        return ForexWorkCalendar.instance
    
    @override
    def get_next_timestamp_examples(self, interval: Interval) -> list[tuple[str,str]]:
        if interval == Interval.D1: return [
            ('2024-12-31 00:00:00', '2025-01-01 00:00:00'),
            ('2024-12-31 01:00:00', '2025-01-01 00:00:00'),
            ('2025-03-01 01:00:00', '2025-03-02 00:00:00') #no weekends
        ]
        if interval == Interval.H1: return [
            ('2024-12-31 23:10:00', '2025-01-01 00:00:00'),
            ('2024-12-31 00:00:00', '2024-12-31 01:00:00')
        ]
        if interval == Interval.M1: return [
            ('2025-02-28 23:59:00', '2025-03-01 00:00:00'),
            ('2025-02-28 23:10:02', '2025-02-28 23:11:00')
        ]
        return []
    
    @override
    def get_timestamps_examples(self, interval: Interval) -> list[tuple[str, str, list[str]]]:
        if interval == Interval.H1: return [
            ('2025-01-01 00:00:00', '2025-01-01 12:00:00', [
                '2025-01-01 01:00:00', '2025-01-01 02:00:00', '2025-01-01 03:00:00', '2025-01-01 04:00:00',
                '2025-01-01 05:00:00', '2025-01-01 06:00:00', '2025-01-01 07:00:00', '2025-01-01 08:00:00',
                '2025-01-01 09:00:00', '2025-01-01 10:00:00', '2025-01-01 11:00:00', '2025-01-01 12:00:00',
            ])
        ]
        return []
    