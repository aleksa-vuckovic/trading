from typing import override
import unittest
from base.serialization import serializer
from trading.core.securities import Exchange, SecurityType
from trading.providers.nasdaq import NasdaqSecurity, Nasdaq, NasdaqGS
from trading.core.interval import Interval
from trading.core.work_calendar import WorkCalendar
from trading.core.tests.test_work_calendar import TestCalendar

class TestNasdaq(unittest.TestCase):
    def test_line_parsing(self):
        nvda = NasdaqSecurity.from_line('NVDA|NVIDIA Corporation - Common Stock|Q|N|N|100|N|N')
        nusi = NasdaqSecurity.from_line('NUSI|NEOS Nasdaq-100 Hedged Equity Income ETF|G|N|N|100|Y|N')
        abblw = NasdaqSecurity.from_line('ABLLW|Abacus Life, Inc. - Warrant|S|N|N|100|N|N')
        self.assertEqual('NVDA', nvda.symbol)
        self.assertEqual('NVIDIA Corporation - Common Stock', nvda.name)
        self.assertIs(NasdaqGS.instance, nvda.exchange)
        self.assertEqual(SecurityType.STOCK, nvda.type)
        self.assertEqual(SecurityType.ETF, nusi.type)
        self.assertEqual(SecurityType.WARRANT, abblw.type)
        self.assertIs(NasdaqGS.instance, nvda.exchange)

    def test_serialization(self):
        serialized = serializer.serialize(Nasdaq.instance)
        deserialized = serializer.deserialize(serialized, Exchange)
        self.assertIs(Nasdaq.instance, deserialized)

class TestNasdaqCalendar(TestCalendar):
    @override
    def get_calendar(self) -> WorkCalendar:
        return Nasdaq.instance.calendar
    
    @override
    def get_next_timestamp_examples(self, interval: Interval) -> list[tuple[str,str]]:
        if interval == Interval.L1: return [
            ('2025-01-05 12:12:12', '2025-02-01 00:00:00'),
            ('2025-01-31 16:00:00', '2025-02-01 00:00:00'),
            ('2024-03-15 12:12:12', '2024-04-01 00:00:00'),
            ('2024-03-29 16:01:00', '2024-04-01 00:00:00')
        ]
        if interval == Interval.W1: return [
            ('2025-02-24 00:00:00', '2025-03-03 00:00:00'),
            ('2025-02-24 12:00:00', '2025-03-03 00:00:00'),
            ('2025-02-28 16:00:00', '2025-03-03 00:00:00'),
            ('2025-02-28 23:59:59', '2025-03-03 00:00:00')
        ]
        if interval == Interval.D1: return [
            ('2025-02-15 05:00:12', '2025-02-19 00:00:00'),
            ('2025-02-18 10:44:44', '2025-02-19 00:00:00'),
            ('2025-02-18 16:00:00', '2025-02-19 00:00:00'),
            ('2025-02-14 20:00:00', '2025-02-15 00:00:00'),
            ('2025-12-24 11:00:00', '2025-12-25 00:00:00'),
            ('2025-12-24 15:00:00', '2025-12-25 00:00:00'),
        ]
        if interval == Interval.H1: return [
            ('2025-02-18 09:15:12', '2025-02-18 10:00:00'),
            ('2025-02-18 14:30:00', '2025-02-18 15:00:00'),
            ('2025-02-18 15:00:00', '2025-02-18 16:00:00'),
            ('2025-02-18 15:30:00', '2025-02-18 16:00:00'),
            ('2025-02-18 12:31:12', '2025-02-18 13:00:00'),
            ('2025-02-18 12:29:59', '2025-02-18 13:00:00'),
            ('2025-02-18 15:39:00', '2025-02-18 16:00:00'),
            ('2025-02-14 16:00:00', '2025-02-18 10:00:00'),
            ('2025-12-24 12:30:00', '2025-12-24 13:00:00'),
            ('2025-12-24 13:00:00', '2025-12-26 10:00:00'),
            ('2024-03-09 16:00:00', '2024-03-11 10:00:00'),
            ('2024-11-01 16:00:00', '2024-11-04 10:00:00')
        ]
        if interval == Interval.M15: return [
            ('2025-02-24 09:30:00', '2025-02-24 09:45:00'),
            ('2025-02-24 09:31:00', '2025-02-24 09:45:00'),
            ('2025-02-24 15:55:00', '2025-02-24 16:00:00'),
            ('2025-02-28 16:00:00', '2025-03-03 09:45:00'),
            ('2025-03-01 20:00:00', '2025-03-03 09:45:00'),
            ('2025-01-01 13:00:00', '2025-01-02 09:45:00'),
        ]
        if interval == Interval.M5: return [
            ('2025-02-24 09:30:00', '2025-02-24 09:35:00'),
            ('2025-02-24 09:31:00', '2025-02-24 09:35:00'),
            ('2025-02-24 15:55:00', '2025-02-24 16:00:00'),
            ('2025-02-28 16:00:00', '2025-03-03 09:35:00'),
            ('2025-03-01 20:00:00', '2025-03-03 09:35:00'),
        ]
        return []

    @override
    def get_timestamps_examples(self, interval: Interval) -> list[tuple[str, str, list[str]]]:
        if interval == Interval.L1: return [
            ('2024-11-25 16:00:00', '2025-05-02 16:00:00', [
                '2024-12-01 00:00:00', '2025-01-01 00:00:00', '2025-02-01 00:00:00',
                '2025-03-01 00:00:00', '2025-04-01 00:00:00', '2025-05-01 00:00:00'
            ])
        ]
        if interval == Interval.W1: return [
            ('2025-02-28 16:00:00', '2025-05-02 16:00:00', [
                '2025-03-03 00:00:00',
                '2025-03-10 00:00:00', '2025-03-17 00:00:00', '2025-03-24 00:00:00', '2025-03-31 00:00:00', 
                '2025-04-07 00:00:00', '2025-04-14 00:00:00', '2025-04-21 00:00:00', '2025-04-28 00:00:00'
            ])
        ]
        if interval == Interval.D1: return [
            ('2025-02-16 00:00:00', '2025-02-19 15:30:00', ['2025-02-19 00:00:00']),
            ('2024-03-08 14:30:00', '2024-03-12 10:30:00', ['2024-03-09 00:00:00', '2024-03-12 00:00:00'])
        ]
        if interval == Interval.H1: return [
            ('2025-02-16 00:00:00', '2025-02-19 15:00:00', [
                '2025-02-18 10:00:00', '2025-02-18 11:00:00', '2025-02-18 12:00:00', '2025-02-18 13:00:00',
                '2025-02-18 14:00:00', '2025-02-18 15:00:00', '2025-02-18 16:00:00', '2025-02-19 10:00:00',
                '2025-02-19 11:00:00', '2025-02-19 12:00:00', '2025-02-19 13:00:00', '2025-02-19 14:00:00',
                '2025-02-19 15:00:00'
            ]),
            ('2024-03-08 14:30:00', '2024-03-12 10:30:00', [
                '2024-03-08 15:00:00', '2024-03-08 16:00:00', '2024-03-11 10:00:00', '2024-03-11 11:00:00',
                '2024-03-11 12:00:00', '2024-03-11 13:00:00', '2024-03-11 14:00:00', '2024-03-11 15:00:00',
                '2024-03-11 16:00:00', '2024-03-12 10:00:00'
            ])
        ]
        if interval == Interval.M15: return [
            ('2025-02-28 14:30:00', '2025-03-03 10:00:00', [
                '2025-02-28 14:45:00', '2025-02-28 15:00:00', '2025-02-28 15:15:00', '2025-02-28 15:30:00',
                '2025-02-28 15:45:00', '2025-02-28 16:00:00', '2025-03-03 09:45:00', '2025-03-03 10:00:00'
            ])
        ]
        if interval == Interval.M5: return [
            ('2025-02-28 15:45:00', '2025-03-03 09:40:00', [
                '2025-02-28 15:50:00', '2025-02-28 15:55:00', '2025-02-28 16:00:00',
                '2025-03-03 09:35:00', '2025-03-03 09:40:00'  
            ])
        ]
        return []
    