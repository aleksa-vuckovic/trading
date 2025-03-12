import unittest
import config
from trading.securities import GlobeNewswire, Nasdaq

security = Nasdaq.instance.get_security("NVDA")
provider = GlobeNewswire()
class TestGlobenewswire(unittest.TestCase):
    def test_get_news(self):
        start_time = security.exchange.calendar.str_to_unix('2023-01-01 00:00:00')
        end_time = security.exchange.calendar.str_to_unix('2024-06-01 00:00:00')
        
        first_title = "NVIDIA Brings RTX 4080 to GeForce NOW"
        last_title = "NVIDIA Announces Upcoming Event for Financial Community"
        total_count = 98
        data = provider.get_news(security, unix_from=start_time, unix_to=end_time, skip_cache=config.test.skip_cache)
        self.assertEqual(first_title, data[0])
        self.assertEqual(last_title, data[-1])
        self.assertEqual(total_count, len(data))