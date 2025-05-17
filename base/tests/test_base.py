import gc
from unittest import TestCase
from pathlib import Path
import shutil
from base import dates


class TestBase(TestCase):
    TEST_DATA = Path("./test_data")
    def drop_files(self):
        if self.TEST_DATA.exists():
            if self.TEST_DATA.is_file(): self.TEST_DATA.unlink()
            else: shutil.rmtree(self.TEST_DATA)
    def setUp(self):
        super().setUp()
        self.drop_files()
        dates.set(None)
    def tearDown(self):
        super().tearDown()
        gc.collect()
        self.drop_files()
        dates.set(None)