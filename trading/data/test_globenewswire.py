import unittest
import config
from . import globenewswire, nasdaq
from ..utils import dateutils


class TestGlobenewswire(unittest.TestCase):
    def test_get_news(self):
        ticker = nasdaq.NasdaqListedEntry.from_line('NVDA|NVIDIA Corporation - Common Stock|Q|N|N|100|N|N')
        start_time = dateutils.str_to_unix('2023-01-01 00:00:00', tz=dateutils.ET)
        end_time = dateutils.str_to_unix('2024-06-01 00:00:00', tz=dateutils.ET)
        
        first_title = "NVIDIA Brings RTX 4080 to GeForce NOW"
        last_title = "NVIDIA Announces Upcoming Event for Financial Community"
        total_count = 98
        data = globenewswire.get_news(ticker, unix_from=start_time, unix_to=end_time, skip_cache=config.test.skip_cache)
        self.assertEqual(first_title, data[0])
        self.assertEqual(last_title, data[-1])
        self.assertEqual(total_count, len(data))