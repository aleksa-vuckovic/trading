from typing import Any, TypedDict
from unittest import TestCase
import time
import shutil
import math
from enum import Enum
from pathlib import Path

from numpy.testing import assert_equal

from base import dates
from base.caching import MemoryPersistor, cached_scalar, cached_series, FilePersistor, SqlitePersistor, Persistor, Metadata
from base.types import Equatable, Serializable


TEST_DATA = Path("./test_data")
class SimpleDict(TypedDict):
    t: float
    d: Any
class SimpleProvider:
    def __init__(self, persistor: Persistor|None = None, timestep: float = 10):
        self.persistor = persistor or FilePersistor(TEST_DATA)
        self.timestep = timestep
        self.invocations = 0
    @staticmethod
    def _timestamp_fn(it: SimpleDict) -> float: return it['t']
    def _key_fn(self) -> str: return ""
    def _persistor_fn(self) -> Persistor: return self.persistor
    def _timestep_fn(self) -> float: return self.timestep
    @cached_series(
        timestamp_fn = _timestamp_fn,
        key_fn=_key_fn,
        persistor_fn=_persistor_fn,
        timestep_fn=_timestep_fn
    )
    def get_series(self, unix_from: float, unix_to: float) -> list[SimpleDict]:
        self.invocations += 1
        return [{'t': it, 'd': it} for it in range(int(unix_from), int(unix_to)+1) if it >unix_from and it <=unix_to ] 
class KeyedProvider:
    def __init__(self, persistor: Persistor|None = None, timestep: float = 10):
        self.persistor = persistor or FilePersistor(TEST_DATA)
        self.timestep = timestep
        self.invocations = 0
    @staticmethod
    def _timestamp_fn(it: SimpleDict) -> float: return it['t']
    def _key_fn(self, key: str) -> str: return key
    def _persistor_fn(self, key: str) -> Persistor: return self.persistor
    def _timestep_fn(self, key: str) -> float: return self.timestep
    @cached_series(
        timestamp_fn = _timestamp_fn,
        key_fn=_key_fn,
        persistor_fn=_persistor_fn,
        timestep_fn=_timestep_fn
    )
    def get_series(self, unix_from: float, unix_to: float, key: str) -> list[SimpleDict]:
        self.invocations += 1
        return [{'t': it, 'd': key} for it in range(int(unix_from), int(unix_to)+1) if it>unix_from and it<=unix_to ]
class EdgeProvider:
    def __init__(self, persistor: Persistor|None = None, timestep: float = 10, live_delay: float = 0, refresh_after: float = 0):
        self.persistor = persistor or FilePersistor(TEST_DATA)
        self.timestep = timestep
        self.live_delay = live_delay
        self.refresh_after = refresh_after
        self.invocations = 0
    @staticmethod
    def _timestamp_fn(it: SimpleDict) -> float: return it['t']
    def _key_fn(self) -> str: return ""
    def _persistor_fn(self) -> Persistor: return self.persistor
    def _timestep_fn(self) -> float: return self.timestep
    def _live_delay_fn(self) -> float: return self.live_delay
    def _should_refresh_fn(self, last: float, now: float) -> bool: return now-last >= self.refresh_after
    @cached_series(
        timestamp_fn = _timestamp_fn,
        key_fn=_key_fn,
        persistor_fn=_persistor_fn,
        timestep_fn=_timestep_fn,
        live_delay_fn=_live_delay_fn,
        should_refresh_fn=_should_refresh_fn
    )
    def get_series(self, unix_from: float, unix_to: float) -> list[SimpleDict]:
        self.invocations += 1
        return [{'t': int(unix_from)+1, 'd': None}, {'t': int(unix_to), 'd': None}]

class A(Equatable, Serializable):
    def __init__(self, t: float, d: Any):
        self.t = t
        self.d = d
    def __repr__(self) -> str: return f"A(t='{self.t}',d={self.d})"
class SimpleScalarProvider:
    def __init__(self, persistor: Persistor|None = None, refresh_after: float = float('+inf')):
        self.persistor = persistor or FilePersistor(TEST_DATA)
        self.refresh_after = refresh_after
        self.invocations = 0
    def _key_fn(self, key: str) -> str: return key
    def _persistor_fn(self, key: str) -> Persistor: return self.persistor
    def _refresh_fn(self, key: str) -> float: return self.refresh_after
    @cached_scalar(
        key_fn=_key_fn,
        persistor_fn=FilePersistor(TEST_DATA),
        refresh_fn=_refresh_fn
    )
    def get_data(self, key: str) -> A:
        self.invocations += 1
        return A(dates.unix(), key)

class TestCaching(TestCase):
    def drop_files(self):
        if TEST_DATA.exists():
            if TEST_DATA.is_file(): TEST_DATA.unlink()
            else: shutil.rmtree(TEST_DATA)
    def make_files(self):
        TEST_DATA.mkdir(parents=True, exist_ok=False)
    def setUp(self):
        super().setUp()
        self.drop_files()
        dates.set(None)
        self.make_files()
    def tearDown(self):
        super().tearDown()
        self.drop_files()
        dates.set(None)

    def _test_persistor_simple(self, persistor: Persistor):
        self.assertEqual(0, len(list(persistor.keys())))
        data = "This is some data."
        key1 = "a/b/123"
        key2 = "a/c/123"

        persistor.persist(key1, data)
        self.assertEqual(data, persistor.read(key1))
        persistor.persist(key2, data)
        self.assertEqual(data, persistor.read(key2))
        self.assertEqual({key1, key2}, set(persistor.keys()))
        persistor.delete(key1)
        self.assertEqual({key2}, set(persistor.keys()))
        self.assertFalse(persistor.has(key1))
        self.assertTrue(persistor.has(key2))
        self.assertFalse(persistor.has(data))
    def _test_persistor_special(self, persistor: Persistor, keys: list[str] = ["", "1", "COM", "a.b.c"]):
        self.assertEqual(0, len(set(persistor.keys())))
        data = "Some data."
        for key in keys:
            persistor.persist(key, data)
            self.assertEqual({key}, set(persistor.keys()))
            self.assertEqual(data, persistor.read(key))
            self.assertTrue(persistor.has(key))
            persistor.delete(key)

    def test_file_persistor_simple(self):
        self._test_persistor_simple(FilePersistor(TEST_DATA))
    def test_file_persistor_special(self):
        self._test_persistor_special(FilePersistor(TEST_DATA/'test'))
    def test_sqlite_persistor_simple(self):
        self._test_persistor_simple(SqlitePersistor(TEST_DATA/"test.db", "testtable"))
    def test_sqlite_persistor_special(self):
        self._test_persistor_special(SqlitePersistor(TEST_DATA/"test.db", "testtable"))
    
    def _test_cached_series_simple(self, start: int, end: int, step: int):
        provider = SimpleProvider(timestep=step)
        start_id = start//step
        end_id = end//step if end%step else end//step-1
        data = provider.get_series(start, end)
        self.assertEqual(data, [{'t': it, 'd': it} for it in range(start+1, end+1)])
        self.assertEqual(provider.invocations, end_id-start_id+1)
        provider.get_series(start_id*step, (end_id+1)*step-0.01)
        self.assertEqual(provider.invocations, end_id-start_id+1)

    def test_cached_series_decorator_simple(self):
        for args in [(7,8,10),(7,15,10),(7,25,10),(7,30,10),(0,100,10),(12,12345,12)]:
            self.setUp()
            self._test_cached_series_simple(*args)
            self.tearDown()
    
    def test_cached_series(self):
        KEY1 = "k1"
        KEY2 = "abc/123"
        provider = KeyedProvider(timestep=10)
        test1 = provider.get_series(16, 30, KEY1)
        test2 = provider.get_series(16, 30, KEY2)
        self.assertEqual(14, len(test1))
        self.assertEqual(14, len(test2))
        self.assertEqual(17, test1[0]['t'])
        self.assertEqual(30, test2[-1]['t'])
        self.assertEqual(4, provider.invocations)
        self.assertEqual(test1, provider.get_series(16, 30, KEY1))
        self.assertEqual(test2, provider.get_series(16, 30, KEY2))
        self.assertEqual(4, provider.invocations)

    def test_cached_series_refresh(self):
        provider = EdgeProvider(timestep = 100, refresh_after=10)
        unix_from = 50
        unix_to = 80
        dates.set(unix_to)
        series = provider.get_series(unix_from, unix_to)
        self.assertEqual(1, len(series))
        dates.set(85)
        self.assertEqual(series, provider.get_series(unix_from, unix_to))
        self.assertEqual(series, provider.get_series(unix_from, unix_to+100))
        self.assertEqual(1, provider.invocations)

        new_unix_to = 91
        dates.set(new_unix_to)
        series = provider.get_series(unix_from, new_unix_to)
        self.assertEqual(3, len(series))
        self.assertEqual(series[2]['t'], dates.unix())
        self.assertEqual(series[0]['t'] + 1, series[1]['t'])
        self.assertEqual(2, provider.invocations)
        self.assertEqual(series, provider.get_series(unix_from, new_unix_to))
        self.assertEqual(2, provider.invocations)

    def test_cached_series_decorator_live_delay(self):
        persistor = MemoryPersistor()
        provider = EdgeProvider(persistor=persistor, live_delay=15, timestep=100)
        unix_from = 50
        unix_to = 60
        dates.set(unix_to)
        test1 = provider.get_series(unix_from, unix_to)
        meta: Metadata = persistor.data[f"/{Metadata.__name__}"]
        self.assertEqual(next(iter(meta.partials.values())), unix_to-15) 
        self.assertEqual(0, len(test1))
        dates.add(10)
        test2 = provider.get_series(unix_from, unix_to)
        self.assertEqual(1, len(test2))
        self.assertEqual(55, test2[0]['t'])

    def test_cached_series_invalidate(self):
        invocations = 0
        class Provider:
            @staticmethod
            def get_series_timestamp_fn(it: dict) -> float: return it['time']
            def get_series_key_fn(self, type: str) -> str: return type
            def get_series_time_step_fn(self, type: str) -> float: return 10 if type == 'type10' else 30
            @cached_series(
                timestamp_fn=get_series_timestamp_fn,
                key_fn=get_series_key_fn,
                persistor_fn=FilePersistor(TEST_DATA),
                timestep_fn=get_series_time_step_fn
            )
            def get_series(self, unix_from: float, unix_to: float, type: str) -> list[dict]:
                nonlocal invocations
                invocations += 1
                return [{"time": float(it)} for it in range(math.floor(unix_from)+1, math.floor(unix_to)+1 )]
        provider = Provider()
        provider.get_series
        dates.set(5)
        provider.get_series(0, 10, 'a')

    def test_cached_scalar(self):
        invocations = 0
        class Test(Enum):
            A = 'aa'
        class Provider:
            def get_scalar_key_fn(self, name: str, typ: Test) -> str: return f"{name}/{typ.name}"
            @cached_scalar(
                key_fn=get_scalar_key_fn,
                persistor_fn=FilePersistor(TEST_DATA)
            )
            def get_scalar(self, name: str, typ: Test) -> dict:
                nonlocal invocations
                invocations += 1
                return {'name': name, 'typ': typ.name}
            
            def get_scalar_key_fn2(self, name: str) -> str: return f"test-{name}"
            @cached_scalar(
                key_fn=get_scalar_key_fn2,
                persistor_fn=FilePersistor(TEST_DATA)
            )
            def get_scalar2(self, name: str) -> str: return name
        
        provider = Provider()
        data = provider.get_scalar('test', Test.A)
        self.assertEqual({'name':'test','typ':Test.A.name}, data)
        self.assertEqual(data, provider.get_scalar('test', Test.A))
        self.assertEqual(1, invocations)
        self.assertEqual('test', provider.get_scalar2('test'))
    
    def test_cached_scalar_decorator_refresh(self):        
        provider = SimpleScalarProvider(refresh_after=10)
        KEY1 = "a"
        KEY2 = "b"
        dates.set(10)
        data1 = provider.get_data(KEY1)
        dates.add(5)
        data2 = provider.get_data(KEY1)
        self.assertEqual(data1, data2)
        dates.add(5)
        data2 = provider.get_data(KEY1)
        self.assertNotEqual(data1, data2)
        self.assertGreater(data2.t, data1.t)
        data2 = provider.get_data(KEY2) and provider.get_data(KEY2)
        self.assertNotEqual(data1, data2)
        self.assertEqual(3, provider.invocations)

    def test_cached_scalar_invalidate(self):
        provider = SimpleScalarProvider()
        KEY1 = "1"
        KEY2 = "2"
        dates.set(10)
        a1 = provider.get_data(KEY1)
        a2 = provider.get_data(KEY2)
        dates.add(10)
        self.assertEqual(a1, provider.get_data(KEY1))
        SimpleScalarProvider.get_data.invalidate(provider, KEY1)
        a12 = provider.get_data(KEY1)
        self.assertNotEqual(a1, a12)
        self.assertEqual(a2, provider.get_data(KEY2))
        self.assertEqual(3, provider.invocations)
