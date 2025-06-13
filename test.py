import unittest
import sys
from pathlib import Path
import config
config.logging.root = "logs/test"
config.storage.local_root_path = "storage/test"
config.storage.local_db_path = "storage/test.db"
config.storage.mongo_db_name = "trading_test"
config.http.request_log = "short"
config.http.response_log = "short"
import log
log.configure_logging()

__import__('sys').modules['unittest.util']._MAX_LENGTH = 999999999

default_dir = "base/tests/serialization"
start_dir = Path(default_dir if len(sys.argv) < 2 else sys.argv[1])
if start_dir.is_dir():
    pattern = "test_*.py"
elif start_dir.parent.is_dir():
    pattern = f"*{start_dir.name}*"
    start_dir = start_dir.parent
else:
    raise Exception(f"Invalid start directory {start_dir}.")

loader = unittest.TestLoader()
suite = loader.discover(start_dir=str(start_dir), pattern=pattern, top_level_dir=".")
runner = unittest.TextTestRunner()
runner.run(suite)
