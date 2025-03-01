from . import zacks
import unittest
from datetime import datetime
from ..utils.dateutils import XNAS


class ZacksTest(unittest.TestCase):

    def test_summary(self):
        time = XNAS.str_to_unix('2025-01-15 12:00:00')
        summary = zacks.get_summary(time)
        self.assertTrue(summary)
        self.assertTrue(summary.startswith("Wall Street closed mixed on Tuesday"))
        self.assertTrue("Consumer Price Index" in summary)
