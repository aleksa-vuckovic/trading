import unittest
from base import dates


class TestDates(unittest.TestCase):
    def test_str_to_unix(self):
        unix = dates.str_to_unix('2023-05-06 06:20:30', dates.ET)
        self.assertEqual(1683368430, unix)
        self.assertIsInstance(unix, float)