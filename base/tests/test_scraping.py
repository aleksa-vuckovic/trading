import unittest
import time
from base.scraping import backup_timeout, BackupBehavior, BackupException

class TestHttputils(unittest.TestCase):
    def test_backup_timeout_decorator(self):
        class TestException(Exception):
            pass
        base_timeout = 0.05
        invocations = 0
        @backup_timeout(exc_type=TestException, default_behavior=BackupBehavior.DEFAULT, base_timeout=base_timeout, backoff_factor=2)
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

    def test_backup_timeout_with_raise(self):
        class TestException(Exception):
            pass
        base_timeout = 0.05
        invocations = 0
        @backup_timeout(exc_type=TestException, default_behavior=BackupBehavior.RERAISE, base_timeout=base_timeout, backoff_factor=2)
        def test_method():
            nonlocal invocations
            invocations += 1
            if invocations > 1:
                return 'Success'
            raise TestException()
        
        self.assertRaises(BackupException, test_method)
        self.assertRaises(BackupException, test_method)
        try:
            test_method()
            self.assertTrue(False)
        except BackupException as ex:
            time.sleep(ex.backup_time)
        self.assertEqual('Success', test_method())
    
    def test_backup_timeout_with_sleep(self):
        class TestException(Exception):
            pass
        base_timeout = 0.05
        invocations = 0
        @backup_timeout(exc_type=TestException, default_behavior=BackupBehavior.SLEEP|BackupBehavior.RERAISE, base_timeout=base_timeout, backoff_factor=2)
        def test_method():
            nonlocal invocations
            invocations += 1
            if invocations > 3:
                return 'Success'
            raise TestException()
        
        self.assertRaises(BackupException, test_method) #invocation 1
        self.assertRaises(BackupException, test_method) #sleep + invocation 2
        self.assertRaises(BackupException, test_method) #sleep + invocation 3
        self.assertEqual('Success', test_method())