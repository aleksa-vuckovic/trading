from typing import Any, TypedDict
from base.db import sqlite_engine
from base.key_series_storage import KeySeriesStorage, MemoryKSStorage, SqlKSStorage
from base.tests.test_base import TestBase
from base.types import Equatable, Serializable

class SimpleDict(TypedDict):
    t: float
    d: Any

class A(Equatable, Serializable):
    def __init__(self, t: float, d: int):
        self.t = float(t)
        self.d = d
    def __repr__(self) -> str: return f"A(t={self.t},d={self.d})"
class TestKSStorage(TestBase):
    def _test_key_series_storage_simple(self, storage: KeySeriesStorage[A]):
        self.assertEqual(0, len(list(storage.keys())))
        KEY1 = "key1"
        KEY2 = "key2"
        data_100_200 = [A(100+i,i) for i in range(1,101)]
        storage.set(KEY1, 100, 200, data_100_200)
        self.assertEqual(data_100_200, storage.get(KEY1, 0, 300)) #Persistence
        self.assertEqual(data_100_200, storage.get(KEY1, 100, 200)) #Boundry inclusivity
        self.assertEqual([(50,100),(200,250)], list(storage.missing_spans(KEY1, 50, 250))) #missing spans
        storage.delete(KEY1, 150, 160) # deletion
        self.assertEqual([it for it in data_100_200 if it.t <= 150 or it.t > 160], storage.get(KEY1, 0, 300))
        self.assertEqual([(50,100),(150,160),(200,250)], list(storage.missing_spans(KEY1, 50, 250)))
        storage.set(KEY1, 130, 170, [A(150, 50)]) #span merging and overrides
        data_100_200 = [*data_100_200[:30], A(150,50), *data_100_200[70:]]
        self.assertEqual(data_100_200, storage.get(KEY1, 100, 200))
        self.assertEqual([(50,100),(200,250)], list(storage.missing_spans(KEY1, 50, 250)))
        
        storage.set(KEY2, 400, 500, [A(450, 50)]) #multiple keys
        self.assertEqual([A(450,50)], storage.get(KEY2, 420, 470))
        self.assertEqual(data_100_200, storage.get(KEY1, 100, 200))
        self.assertEqual({KEY1, KEY2}, set(storage.keys()))

    def _test_key_series_storage_edge(self, storage: KeySeriesStorage[A]):
        KEY = "key"
        storage.set(KEY, 100, 200, [A(150, 150)]) #adjacent merge
        storage.set(KEY, 300, 400, [A(350, 350)])
        self.assertEqual([(200,300)], list(storage.missing_spans(KEY, 100, 400)))
        storage.set(KEY, 200, 300, [A(250, 250)])
        self.assertEqual([], list(storage.missing_spans(KEY, 100, 400)))
        storage.delete(KEY, 100, 400)
        self.assertEqual([(0, 500)], list(storage.missing_spans(KEY, 0, 500)))

    def test_memory_ks_storage_simple(self):
        self._test_key_series_storage_simple(MemoryKSStorage(lambda it: it.t))
    def test_memory_ks_storage_edge(self):
        self._test_key_series_storage_edge(MemoryKSStorage(lambda it: it.t))
    def test_sql_ks_storage_simple(self):
        self._test_key_series_storage_simple(SqlKSStorage(sqlite_engine(self.TEST_DATA), "test", timestamp=lambda it: it.t))
    def test_sql_ks_storage_edge(self):
        self._test_key_series_storage_edge(SqlKSStorage(sqlite_engine(self.TEST_DATA), "test", timestamp=lambda it: it.t))
