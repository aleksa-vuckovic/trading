import unittest
from base import dates
from base.serialization import serializer
from trading.core import Interval
from trading.core.securities import Exchange
from trading.core.work_calendar import BasicWorkCalendar
from trading.core.timing_config import BasicTimingConfig

calendar = BasicWorkCalendar(tz=dates.ET, open_hour=9, open_minute=30, close_hour=16, semi_close_hour=16)
exchange = Exchange('XTST', 'Test Exchange', calendar)
class TestBasicTimingConfig(unittest.TestCase):
    def test_timing_config_next(self):
        config = BasicTimingConfig.Builder()\
            .at(hour = 11, minute = 00)\
            .around(hour = 14, minute = 00, delta_minute=20)\
            .starting(hour = 15, minute = 0).until(hour = 16, minute = 0)\
            .build()
        config_ = serializer.deserialize(serializer.serialize(config))
        self.assertEqual(config, config_)
        
        expect = [calendar.str_to_datetime(it) for it in 
            ['2025-02-21 11:00:00', '2025-02-21 14:00:00', '2025-02-21 16:00:00', '2025-02-24 11:00:00']]
        result = []
        cur = calendar.str_to_datetime('2025-02-21 10:00:00')
        for i in range(len(expect)):
            cur = config.next(cur, Interval.H1, exchange)
            result.append(cur)
        self.assertEqual(expect, result)

        config = BasicTimingConfig.Builder()\
            .around(hour = 11, minute = 0, delta_minute = 30)\
            .build()
        
        expect = [calendar.str_to_unix(it) for it in 
            ['2025-02-21 10:35:00', '2025-02-21 10:40:00', '2025-02-21 10:45:00', '2025-02-21 10:50:00']]
        result = []
        cur = calendar.str_to_unix('2025-02-21 10:00:00')
        for i in range(len(expect)):
            cur = config.next(cur, Interval.M5, exchange)
            result.append(cur)
        self.assertEqual(expect, result)