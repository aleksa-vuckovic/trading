from . import generator
from ...utils import dateutils
import unittest


class TestGenerator(unittest.TestCase):

    def test_get_next_time(self):
        input = dateutils.str_to_unix('2025-01-23 02:12:22', tz=dateutils.ET)
        expect = dateutils.str_to_unix('2025-01-23 09:00:00', tz=dateutils.ET)
        self.assertEqual(expect, generator.get_next_time(input))

        input = dateutils.str_to_unix('2025-01-17 20:01:12', tz=dateutils.ET)
        expect = dateutils.str_to_unix('2025-01-20 09:00:00', tz=dateutils.ET)
        self.assertEqual(expect, generator.get_next_time(input))

        input = dateutils.str_to_unix('2025-01-20 11:00:00', tz=dateutils.ET)
        expect = dateutils.str_to_unix('2025-01-20 12:00:00', tz=dateutils.ET)
        self.assertEqual(expect, generator.get_next_time(input))

    def test_get_next_time_by_hour(self):
        input = dateutils.str_to_unix('2025-01-23 02:12:22', tz=dateutils.ET)
        expect = dateutils.str_to_unix('2025-01-23 11:00:00', tz=dateutils.ET)
        self.assertEqual(expect, generator.get_next_time(input, hour=11))

        input = dateutils.str_to_unix('2025-01-17 15:01:12', tz=dateutils.ET)
        expect = dateutils.str_to_unix('2025-01-20 15:00:00', tz=dateutils.ET)
        self.assertEqual(expect, generator.get_next_time(input, hour=15))

        input = dateutils.str_to_unix('2025-01-20 11:00:00', tz=dateutils.ET)
        expect = dateutils.str_to_unix('2025-01-20 13:00:00', tz=dateutils.ET)
        self.assertEqual(expect, generator.get_next_time(input, hour=13))

        input = dateutils.str_to_unix('2025-01-24 11:00:00', tz=dateutils.ET)
        expect = dateutils.str_to_unix('2025-01-27 11:00:00', tz=dateutils.ET)
        self.assertEqual(expect, generator.get_next_time(input, hour=11))
