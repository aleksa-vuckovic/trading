from typing import Any, TypedDict
from unittest import TestCase
import time
import shutil
import math
from enum import Enum
from pathlib import Path

from numpy.testing import assert_equal

from base import dates
from base.db import sqlite_engine
from base.key_value_storage import FolderKVStorage, KeyValueStorage, MemoryKVStorage, FileKVStorage, SqlKVStorage
from base.tests.test_base import TestBase
from base.types import Equatable, Serializable


class SimpleDict(TypedDict):
    t: float
    d: Any

class A(Equatable, Serializable):
    def __init__(self, t: float, d: Any):
        self.t = t
        self.d = d
    def __repr__(self) -> str: return f"A(t='{self.t}',d={self.d})"

class TestKVStorage(TestBase):
    def _test_kv_storage_simple(self, storage: KeyValueStorage):
        self.assertEqual(0, len(list(storage.keys())))
        data = "This is some data."
        key1 = "a-b-123"
        key2 = "a-c-123"

        storage.set(key1, data)
        self.assertEqual(data, storage.get(key1))
        storage.set(key2, data)
        self.assertEqual(data, storage.get(key2))
        self.assertEqual({key1, key2}, set(storage.keys()))
        storage.delete(key1)
        self.assertEqual({key2}, set(storage.keys()))
        self.assertFalse(storage.has(key1))
        self.assertTrue(storage.has(key2))
        self.assertFalse(storage.has(data))
    def _test_kv_storage_special(self, storage: KeyValueStorage, keys: list[str] = ["", "1", "COM", "a.b.c"]):
        self.assertEqual(0, len(set(storage.keys())))
        data = "Some data."
        for key in keys:
            storage.set(key, data)
            self.assertEqual({key}, set(storage.keys()))
            self.assertEqual(data, storage.get(key))
            self.assertTrue(storage.has(key))
            storage.delete(key)

    def test_file_storage_simple(self):
        self._test_kv_storage_simple(FileKVStorage(self.TEST_DATA))
    def test_file_storage_special(self):
        self._test_kv_storage_special(FileKVStorage(self.TEST_DATA))
    def test_folder_storage_simple(self):
        self._test_kv_storage_simple(FolderKVStorage(self.TEST_DATA))
    def test_folder_storage_special(self):
        self._test_kv_storage_special(FolderKVStorage(self.TEST_DATA))
    def test_sqlite_storage_simple(self):
        self._test_kv_storage_simple(SqlKVStorage(sqlite_engine(self.TEST_DATA), "testtable"))
    def test_sqlite_storage_special(self):
        self._test_kv_storage_special(SqlKVStorage(sqlite_engine(self.TEST_DATA), "testtable"))

