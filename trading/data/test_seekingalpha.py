import config
from . import seekingalpha
from . import nasdaq
from ..utils.dateutils import XNAS
import unittest

class MacrotrendsTest(unittest.TestCase):
    def test_news(self):
        nvda = nasdaq.NasdaqListedEntry.from_line('NVDA|NVIDIA Corporation - Common Stock|Q|N|N|100|N|N')
        unix_from = XNAS.str_to_unix('2020-01-01 00:00:00')
        unix_to = XNAS.str_to_unix('2020-01-20 00:00:00')
        news = seekingalpha.get_news(nvda.symbol, unix_from, unix_to, skip_cache=config.test.skip_cache)
        self.assertGreater(len(news), 1)
        self.assertIsInstance(news[0], str)