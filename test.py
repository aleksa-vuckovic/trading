import unittest
import sys
from trading import log

log.configure_logging(True)
start_dir = "trading" if len(sys.argv) < 2 else sys.argv[1]
loader = unittest.TestLoader()
suite = loader.discover(start_dir=start_dir, pattern="test_*.py", top_level_dir=".")

runner = unittest.TextTestRunner()
runner.run(suite)
