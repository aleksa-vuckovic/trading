import unittest
import time
import shutil
import json
import math
from enum import Enum
from pathlib import Path
from base.caching import cached_scalar, cached_series, FilePersistor, SqlitePersistor, Persistor

TEST_DATA = Path("./test_data")
class TestCaching(unittest.TestCase):
    def setUp(self):
        if TEST_DATA.exists():
            if TEST_DATA.is_file(): TEST_DATA.unlink()
            else: shutil.rmtree(TEST_DATA)
        TEST_DATA.mkdir(parents=True, exist_ok=True)

    def _test_persistor_multi(self, persistor: Persistor):
        self.assertEqual(0, len(list(persistor.keys())))
        data = "This is some data."
        key = ['a', 'b', '123']
        persistor.persist(key, data)
        self.assertEqual(data, persistor.read(key))
        key2 = ['a', 'c', '123']
        persistor.persist(key2, data)
        self.assertEqual(data, persistor.read(key2))
        self.assertEqual(sorted([key, key2]), sorted(persistor.keys()))
        persistor.delete(key)
        self.assertEqual([key2], list(persistor.keys()))
    def _test_persistor_none(self, persistor: Persistor):
        self.assertEqual(0, len(list(persistor.keys())))
        data = "Some data."
        persistor.persist([], data)
        self.assertEqual([[]], list(persistor.keys()))
        self.assertEqual(data, persistor.read([]))

    def test_file_persistor_multi(self):
        self._test_persistor_multi(FilePersistor(TEST_DATA))
    def test_file_persistor_none(self):
        self._test_persistor_none(FilePersistor(TEST_DATA))
    def test_sqlite_persistor_multi(self):
        self._test_persistor_multi(SqlitePersistor(TEST_DATA/"test.db", "testtable"))
    def test_sqlite_persistor_none(self):
        self._test_persistor_none(SqlitePersistor(TEST_DATA/"test.db", "testtable"))
    
    def test_cached_series_decorator(self):
        invocations = 0
        def get_series_key_fn(type: str) -> list[str]:
            return [type]
        def get_series_time_step_fn(type: str) -> float:
            return 10 if type == 'type10' else 30
        @cached_series(
            unix_args=(0,"unix_to"),
            series_field="series",
            timestamp_field="time",
            key_fn=get_series_key_fn,
            persistor_fn=FilePersistor(TEST_DATA),
            time_step_fn=get_series_time_step_fn,
            return_series_only=True
        )
        def get_series(unix_from: float, *, unix_to: float, type: str):
            nonlocal invocations
            invocations += 1
            return {
                "name": type,
                "series": [{"time": float(it), "data": it} for it in range(math.floor(unix_from)+1, math.floor(unix_to)+1 )]
            }
        
        test1 = get_series(16, unix_to=30, type="type10")
        test2 = get_series(16, unix_to=30, type="other")
        self.assertEqual(14, len(test1))
        self.assertEqual(14, len(test2))
        self.assertEqual(17, test1[0]['data'])
        self.assertEqual(30, test2[-1]['data'])
        self.assertEqual(4, invocations)

        self.assertEqual(test1, get_series(16, unix_to=30, type="type10"))
        self.assertEqual(test2, get_series(16, unix_to=30, type="other"))
        self.assertEqual(4, invocations)

    def test_cached_series_decorator_live(self):
        invocations = 0
        time_step = 24*3600
        def get_series_key_fn() -> list[str]:
            return []
        @cached_series(
            unix_args=(0,1),
            series_field=None,
            timestamp_field="time",
            key_fn=get_series_key_fn,
            persistor_fn=FilePersistor(TEST_DATA),
            time_step_fn= time_step,
            live_delay_fn=0,
            should_refresh_fn=1
        )
        def get_series(unix_from: float,  unix_to: float):
            nonlocal invocations
            invocations += 1
            return [{"time": unix_from+0.1}, {"time": unix_to-0.1}]
        
        unix_to = time.time()
        unix_from = unix_to - 1000
        test1 = get_series(unix_from, unix_to)
        self.assertEqual(1, len(test1)) #get_series will be invoked with the lower chunk border, therefore the first entry will be filtered out
        self.assertEqual(test1, get_series(unix_from, unix_to))

        get_series(unix_from, unix_to + 1)
        self.assertEqual(1, invocations)
        time.sleep(1)
        new_unix_to = time.time()
        series = get_series(unix_from, new_unix_to)
        self.assertEqual(3, len(series)) #now get_series will be invoked with the previous upper border
        self.assertGreaterEqual(series[2]["time"], unix_to)
        self.assertAlmostEqual(series[0]["time"] + 0.2, series[1]["time"], places=6)
        self.assertEqual(2, invocations)

        series = get_series(unix_from, new_unix_to)
        self.assertEqual(3, len(series))
        self.assertEqual(2, invocations)

    def test_cached_series_decorator_live_delay(self):
        time_step = 24*3600
        now = time.time()
        def get_series_key_fn() -> list[str]:
            return []
        @cached_series(
            unix_args=(0,1),
            series_field=None,
            timestamp_field="time",
            key_fn=get_series_key_fn,
            persistor_fn=FilePersistor(TEST_DATA),
            time_step_fn= time_step,
            live_delay_fn=2,
            should_refresh_fn=0
        )
        def get_series(unix_from: float,  unix_to: float):
            if now-1 > unix_from and now-1 < unix_to:
                return [{"time": now-1}]
            return []
        
        unix_from = now-3
        unix_to = now
        test1 = get_series(unix_from, unix_to)
        metapath = TEST_DATA / "meta"
        meta = json.loads(metapath.read_text())["live"]
        self.assertLess(meta['fetch'], now-1)
        self.assertEqual(0, len(test1))
        time.sleep(1.1)
        test2= get_series(unix_from, unix_to)
        self.assertEqual(1, len(test2))

    def test_cached_series_decorator_live_refresh(self):
        now = time.time()
        invocations = 0
        def get_series_key_fn() -> list[str]:
            return []
        def get_series_refresh_fn(fetch: float, now: float) -> bool:
            return now-fetch > 1
        @cached_series(
            unix_args=(0,1),
            series_field=None,
            timestamp_field="time",
            key_fn=get_series_key_fn,
            persistor_fn=FilePersistor(TEST_DATA),
            time_step_fn= 1000,
            live_delay_fn=0,
            should_refresh_fn=get_series_refresh_fn
        )
        def get_series(unix_from: float,  unix_to: float):
            nonlocal invocations
            invocations += 1
            return [{"time": unix_from+0.1}, {"time": unix_to-0.1}]
        
        unix_from = now-3
        test = get_series(unix_from, time.time())
        self.assertEqual(1, invocations)
        self.assertTrue(test)
        time.sleep(0.2)
        test = get_series(unix_from, time.time())
        self.assertEqual(1, invocations)
        time.sleep(1)
        test = get_series(unix_from, time.time())
        self.assertEqual(2, invocations)
        self.assertEqual(3, len(test))    

    def test_cache_scalar_decorator(self):
        invocations = 0
        class Test(Enum):
            A = 'aa'
        def get_scalar_key_fn(name: str, typ: Test) -> list[str]:
            return [name, typ.name]
        @cached_scalar(
            key_fn=get_scalar_key_fn,
            persistor_fn=FilePersistor(TEST_DATA)
        )
        def get_scalar(name: str, typ: Test) -> dict:
            nonlocal invocations
            invocations += 1
            return {'name': name, 'typ': typ.name}
        
        data = get_scalar('test', Test.A)
        self.assertEqual({'name':'test','typ':Test.A.name}, data)
        self.assertEqual(data, get_scalar('test', Test.A))
        self.assertEqual(1, invocations)

        def get_scalar_key_fn(name: str) -> list[str]:
            return [f"test-{name}"]
        @cached_scalar(
            key_fn=get_scalar_key_fn,
            persistor_fn=FilePersistor(TEST_DATA)
        )
        def get_scalar(name: str):
            return name
        self.assertEqual('test', get_scalar('test'))
