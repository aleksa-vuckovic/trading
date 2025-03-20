import unittest
import sys
import config
import config_test
config.__dict__.update(config_test.__dict__)
from trading import log

log.configure_logging(True)
start_dir = "trading" if len(sys.argv) < 2 else sys.argv[1]
loader = unittest.TestLoader()
suite = loader.discover(start_dir=start_dir, pattern="test_*.py", top_level_dir=".")

runner = unittest.TextTestRunner()
runner.run(suite)
