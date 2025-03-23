import unittest
import config
from trading.providers import GlobeNewswire, Nasdaq

security = Nasdaq.instance.get_security("NVDA")
provider = GlobeNewswire(config.caching.storage)
class TestGlobenewswire(unittest.TestCase):
    def test_get_news(self):
        start_time = security.exchange.calendar.str_to_unix('2023-01-01 00:00:00')
        end_time = security.exchange.calendar.str_to_unix('2024-06-01 00:00:00')
        
        data = provider.get_news(start_time, end_time, security)
        self.assertEqual(98, len(data))
        self.assertEqual("NVIDIA Brings RTX 4080 to GeForce NOW", data[0].title)
        self.assertEqual("NVIDIA Announces Upcoming Event for Financial Community", data[-1].title)
        