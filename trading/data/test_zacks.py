from . import zacks
from ..utils import dateutils
import unittest
from datetime import datetime

class ZacksTest(unittest.TestCase):

    def test_summary(self):
        time = datetime(2025, 1, 15, 12, tzinfo=dateutils.EST).timestamp()
        summary = zacks.get_summary(time)
        self.assertTrue(summary)
        self.assertTrue(summary.startswith("Wall Street closed mixed on Tuesday"))
        self.assertTrue("Consumer Price Index" in summary)
