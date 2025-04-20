import config
import unittest
from trading.providers.nasdaq import Nasdaq
from trading.providers.seekingalpha import SeekingAlpha

security = Nasdaq.instance.get_security('NVDA')
calendar = Nasdaq.instance.calendar
provider = SeekingAlpha(config.caching.storage)

class TestSeekingAlpha(unittest.TestCase):
    def test_news(self):
        unix_from = calendar.str_to_unix('2020-01-01 00:00:00')
        unix_to = calendar.str_to_unix('2020-01-20 00:00:00')
        news = provider.get_news(unix_from, unix_to, security)
        self.assertEqual(2, len(news))
        self.assertEqual(news[0].title, "Post-CES, Citi puts 'positive catalyst watch' on hot-handed tech name")
        self.assertEqual(news[0].time, 1578665011)