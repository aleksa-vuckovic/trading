import unittest

loader = unittest.TestLoader()
suite = loader.discover(start_dir="trading", pattern="test_*.py", top_level_dir=".")

runner = unittest.TextTestRunner()
runner.run(suite)
