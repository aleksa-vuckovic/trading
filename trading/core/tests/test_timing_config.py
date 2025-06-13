import unittest
from base import dates
from base.serialization import GenericSerializer
from trading.core import Interval
from trading.core.news import Security
from trading.core.securities import Exchange, SecurityType
from trading.core.work_calendar import BasicWorkCalendar, WorkSchedule, Hours
from trading.core.timing_config import BasicTimingConfig, execution_spots

serializer = GenericSerializer()
calendar = BasicWorkCalendar(tz=dates.ET, work_schedule=WorkSchedule.Builder(Hours(9, 16, open_minute=30)).build())
calendar2 = BasicWorkCalendar(tz=dates.ET, work_schedule=WorkSchedule.Builder(Hours(15, 17)).build())
exchange = Exchange('XTST', 'XTST', 'XTST', 'Test', calendar)
exchange2 = Exchange('XTST2', 'XTST2', 'XTST2', 'Test2', calendar2)
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

    def test_execution_spots(self):
        sec1 = Security('TST', 'Test', SecurityType.STOCK, exchange)
        sec2 = Security('TST2', 'Test2', SecurityType.STOCK, exchange2)
        timing_config = BasicTimingConfig.Builder().starting(14, 0).until(17).build()
        start = calendar.str_to_unix('2025-04-21 00:00:00')
        end = calendar.str_to_unix('2025-04-22 00:00:00')
        expect = [
            (calendar.str_to_unix('2025-04-21 14:30:00'), set([sec1])),
            (calendar.str_to_unix('2025-04-21 15:00:00'), set([sec1])),
            (calendar.str_to_unix('2025-04-21 15:30:00'), set([sec1, sec2])),
            (calendar.str_to_unix('2025-04-21 16:00:00'), set([sec1, sec2])),
            (calendar.str_to_unix('2025-04-21 16:30:00'), set([sec2])),
            (calendar.str_to_unix('2025-04-21 17:00:00'), set([sec2])),
        ]
        result = list(execution_spots([sec1, sec2], timing_config, Interval.M30, start, end))
        self.assertEqual(expect, result)
    