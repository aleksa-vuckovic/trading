import config
import unittest
from trading.securities import NasdaqSecurity, Nasdaq, SeekingAlpha

security = Nasdaq.instance.get_security('NVDA')
calendar = Nasdaq.instance.calendar
provider = SeekingAlpha()

class MacrotrendsTest(unittest.TestCase):
    def test_news(self):
        unix_from = calendar.str_to_unix('2020-01-01 00:00:00')
        unix_to = calendar.str_to_unix('2020-01-20 00:00:00')
        news = provider.get_news(security, unix_from, unix_to, skip_cache=config.test.skip_cache)
        self.assertGreater(len(news), 1)
        self.assertIsInstance(news[0], str)