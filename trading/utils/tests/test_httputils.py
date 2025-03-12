import unittest
import time
from trading.utils import httputils

class TestHttputils(unittest.TestCase):
    def test_backup_timeout_decorator(self):
        class TestException(Exception):
            pass
        base_timeout = 0.05
        #create a method that always throws an exception
        invocations = 0
        @httputils.backup_timeout(exc_type=TestException, default_behavior=httputils.BackupBehavior.DEFAULT, base_timeout=base_timeout, backoff_factor=2)
        def test_method():
            nonlocal invocations
            invocations += 1
            if invocations > 3:
                return 'Success'
            raise TestException()
        
        self.assertIsNone(test_method())
        time.sleep(base_timeout)
        self.assertIsNone(test_method())
        time.sleep(base_timeout*2)
        self.assertIsNone(test_method())
        time.sleep(base_timeout*2)
        self.assertIsNone(test_method())
        time.sleep(base_timeout*2)
        self.assertEqual('Success', test_method())