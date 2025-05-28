import config
import unittest
from trading.providers.nasdaq import Nasdaq
from trading.providers.seekingalpha import SeekingAlpha

security = Nasdaq.instance.get_security('NVDA')
calendar = Nasdaq.instance.calendar
provider = SeekingAlpha()

class TestSeekingAlpha(unittest.TestCase):
    def test_news(self):
        unix_from = calendar.str_to_unix('2020-01-01 00:00:00')
        unix_to = calendar.str_to_unix('2020-01-20 00:00:00')
        news = provider.get_news(unix_from, unix_to, security)
        self.assertEqual(2, len(news))
        self.assertEqual(news[0].title, "Post-CES, Citi puts 'positive catalyst watch' on hot-handed tech name")
        content = news[0].content
        assert content
        self.assertIn("management team \"sounded positive\" on growth", content)
        self.assertEqual(news[0].unix_time, 1578665011)
