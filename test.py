import unittest
import sys
import config
config.logging.root = "logs/test"
config.storage.local_root_path = "storage/test"
config.storage.local_db_path = "storage/test.db"
config.storage.mongo_db_name = "trading_test"
config.http.request_log = "short"
config.http.response_log = "short"
import log
log.configure_logging()

start_dir = "." if len(sys.argv) < 2 else sys.argv[1]
loader = unittest.TestLoader()
suite = loader.discover(start_dir=start_dir, pattern="test_*.py", top_level_dir=".")

runner = unittest.TextTestRunner()
runner.run(suite)
