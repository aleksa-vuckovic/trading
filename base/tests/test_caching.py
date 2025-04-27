from unittest import TestCase
import time
import shutil
import math
from enum import Enum
from pathlib import Path
from base.caching import MemoryPersistor, cached_scalar, cached_series, FilePersistor, SqlitePersistor, Persistor, Metadata
from base.types import Equatable, Serializable

class A(Serializable, Equatable):
    def __init__(self, a: str, b: float):
        self.a = a
        self.b = b
    def __repr__(self) -> str: return f"A(a='{self.a}',b={self.b:.3f})"

TEST_DATA = Path("./test_data")
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
        self.make_files()
    def tearDown(self):
        super().tearDown()
        self.drop_files()

    def _test_persistor_multi(self, persistor: Persistor):
        self.assertEqual(0, len(list(persistor.keys())))
        data = "This is some data."
        key = "a/b/123"
        persistor.persist(key, data)
        self.assertEqual(data, persistor.read(key))
        key2 = "a/c/123"
        persistor.persist(key2, data)
        self.assertEqual(data, persistor.read(key2))
        self.assertEqual(sorted([key, key2]), sorted(persistor.keys()))
        persistor.delete(key)
        self.assertEqual([key2], list(persistor.keys()))
        self.assertFalse(persistor.has(key))
        self.assertTrue(persistor.has(key2))
    def _test_persistor_none(self, persistor: Persistor):
        self.assertEqual(0, len(list(persistor.keys())))
        data = "Some data."
        persistor.persist("", data)
        self.assertEqual([""], list(persistor.keys()))
        self.assertEqual(data, persistor.read(""))
        self.assertTrue(persistor.has(""))
        self.assertFalse(persistor.has(data))

    def test_file_persistor_multi(self):
        self._test_persistor_multi(FilePersistor(TEST_DATA))
    def test_file_persistor_none(self):
        self._test_persistor_none(FilePersistor(TEST_DATA/'test'))
    def test_sqlite_persistor_multi(self):
        self._test_persistor_multi(SqlitePersistor(TEST_DATA/"test.db", "testtable"))
    def test_sqlite_persistor_none(self):
        self._test_persistor_none(SqlitePersistor(TEST_DATA/"test.db", "testtable"))
    
    def _test_cached_series_decorator_simple(self, start: int, end: int, step: int):
        invocations = 0
        class Provider:
            @staticmethod
            def _timestamp_fn(it: dict) -> float: return it['t']
            def _key_fn(self) -> str: return ""
            @cached_series(
                timestamp_fn = _timestamp_fn,
                key_fn=_key_fn,
                persistor_fn=FilePersistor(TEST_DATA),
                time_step_fn=step
            )
            def get_series(self, unix_from: float, unix_to: float) -> list[dict]:
                nonlocal invocations
                invocations += 1
                return [{"t": it} for it in range(int(unix_from), int(unix_to)+1) if it >unix_from and it <=unix_to ]
        provider = Provider()
        start_id = start//step
        end_id = end//step if end%step else end//step-1
        data = provider.get_series(start, end)
        self.assertEqual(data, [{"t": it} for it in range(start+1, end+1)])
        self.assertEqual(invocations, end_id-start_id+1)
        provider.get_series(start_id*step, (end_id+1)*step-0.01)
        self.assertEqual(invocations, end_id-start_id+1)

    def test_cached_series_decorator_simple(self):
        for args in [(7,8,10),(7,15,10),(7,25,10),(7,30,10),(0,100,10),(12,12345,12)]:
            self.setUp()
            self._test_cached_series_decorator_simple(*args)
    
    def test_cached_series_decorator(self):
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
                time_step_fn=get_series_time_step_fn
            )
            def get_series(self, unix_from: float, unix_to: float, type: str) -> list[dict]:
                nonlocal invocations
                invocations += 1
                return [{"time": float(it), "data": it} for it in range(math.floor(unix_from)+1, math.floor(unix_to)+1 )]
            
        provider = Provider()
        test1 = provider.get_series(16, 30, "type10")
        test2 = provider.get_series(16, 30, "other")
        self.assertEqual(14, len(test1))
        self.assertEqual(14, len(test2))
        self.assertEqual(17, test1[0]['data'])
        self.assertEqual(30, test2[-1]['data'])
        self.assertEqual(3, invocations)

        self.assertEqual(test1, provider.get_series(16, 30, "type10"))
        self.assertEqual(test2, provider.get_series(16, 30, "other"))
        self.assertEqual(3, invocations)

    def test_cached_series_decorator_live(self):
        invocations = 0
        time_step = 24*3600
        class Provider:
            @staticmethod
            def timestamp_fn(it: dict) -> float: return it['time']
            def get_series_key_fn(self) -> str: return ""
            @cached_series(
                timestamp_fn=timestamp_fn,
                key_fn=get_series_key_fn,
                persistor_fn=FilePersistor(TEST_DATA),
                time_step_fn= time_step,
                live_delay_fn=0,
                should_refresh_fn=1
            )
            def get_series(self, unix_from: float,  unix_to: float) -> list[dict]:
                nonlocal invocations
                invocations += 1
                return [{"time": unix_from+0.1}, {"time": unix_to-0.1}]
        
        provider = Provider()
        unix_to = time.time()
        unix_from = unix_to - 1000
        test1 = provider.get_series(unix_from, unix_to)
        self.assertEqual(1, len(test1)) #get_series will be invoked with the lower chunk border, therefore the first entry will be filtered out
        self.assertEqual(test1, provider.get_series(unix_from, unix_to))

        provider.get_series(unix_from, unix_to + 1)
        self.assertEqual(1, invocations)
        time.sleep(1)
        new_unix_to = time.time()
        series = provider.get_series(unix_from, new_unix_to)
        self.assertEqual(3, len(series)) #now get_series will be invoked with the previous upper border
        self.assertGreaterEqual(series[2]["time"], unix_to)
        self.assertAlmostEqual(series[0]["time"] + 0.2, series[1]["time"], places=6)
        self.assertEqual(2, invocations)
        series = provider.get_series(unix_from, new_unix_to)

        self.assertEqual(3, len(series))
        self.assertEqual(2, invocations)

    def test_cached_series_decorator_live_delay(self):
        time_step = 24*3600
        now = time.time()
        persistor = MemoryPersistor()
        class Provider:
            @staticmethod
            def get_series_timestamp_fn(it: dict): return it['time']
            def get_series_key_fn(self) -> str: return ""
            @cached_series(
                timestamp_fn=get_series_timestamp_fn,
                key_fn=get_series_key_fn,
                persistor_fn=persistor,
                time_step_fn= time_step,
                live_delay_fn=2,
                should_refresh_fn=0
            )
            def get_series(self, unix_from: float,  unix_to: float):
                if now-1 > unix_from and now-1 < unix_to:
                    return [{"time": now-1}]
                return []
        
        provider = Provider()
        unix_from = now-3
        unix_to = now
        test1 = provider.get_series(unix_from, unix_to)
        meta: Metadata = persistor.data[f"/{Metadata.__name__}"]
        self.assertLess(next(iter(meta.partials.values())), now-1) 
        self.assertEqual(0, len(test1))
        time.sleep(1.1)
        test2= provider.get_series(unix_from, unix_to)
        self.assertEqual(1, len(test2))

    def test_cached_series_decorator_live_refresh(self):
        now = time.time()
        invocations = 0
        class Provider:
            @staticmethod
            def get_series_timestamp_fn(it: dict) -> float: return it['time']
            def get_series_key_fn(self) -> str: return ""
            def get_series_refresh_fn(self, fetch: float, now: float) -> bool: return now-fetch > 1
            @cached_series(
                timestamp_fn=get_series_timestamp_fn,
                key_fn=get_series_key_fn,
                persistor_fn=MemoryPersistor(),
                time_step_fn= 1000,
                live_delay_fn=0,
                should_refresh_fn=get_series_refresh_fn
            )
            def get_series(self, unix_from: float,  unix_to: float):
                nonlocal invocations
                invocations += 1
                return [{"time": unix_from+0.1}, {"time": unix_to-0.1}]
        
        provider = Provider()
        unix_from = now-3
        test = provider.get_series(unix_from, time.time())
        self.assertEqual(1, invocations)
        self.assertTrue(test)
        time.sleep(0.2)
        test = provider.get_series(unix_from, time.time())
        self.assertEqual(1, invocations)
        time.sleep(1)
        test = provider.get_series(unix_from, time.time())
        self.assertEqual(2, invocations)
        self.assertEqual(3, len(test))    

    def test_cached_scalar_decorator(self):
        invocations = 0
        class Test(Enum):
            A = 'aa'
        def get_scalar_key_fn(name: str, typ: Test) -> str: return f"{name}/{typ.name}"
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

        def get_scalar_key_fn2(name: str) -> str: return f"test-{name}"
        @cached_scalar(
            key_fn=get_scalar_key_fn2,
            persistor_fn=FilePersistor(TEST_DATA)
        )
        def get_scalar2(name: str) -> str: return name
        self.assertEqual('test', get_scalar2('test'))
    
    def test_cached_scalar_decorator_refresh(self):        
        invocations = 0
        def key_fn(a: str) -> str: return a
        @cached_scalar(
            key_fn=key_fn,
            persistor_fn=FilePersistor(TEST_DATA),
            refresh_after=0.1
        )
        def get_data(a: str) -> A:
            nonlocal invocations
            invocations += 1
            return A(a, time.time())
        
        data1 = get_data("a")
        time.sleep(0.05)
        data2 = get_data("a")
        self.assertEqual(data1, data2)
        time.sleep(0.05)
        data2 = get_data("a")
        self.assertNotEqual(data1, data2)
        self.assertGreater(data2.b, data1.b)
        data2 = get_data("b") and get_data("b")
        self.assertNotEqual(data1, data2)
        self.assertEqual(invocations, 3)
