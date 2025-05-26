from __future__ import annotations
from parameterized import parameterized
from base import dates
from base.algos import lower_whole, upper_whole
from base.caching import CachedSeriesDescriptor, KeySeriesStorage, KeyValueStorage, cached_scalar, cached_series
from base.tests.common import A, TestPersistence, storage_type, kv_types, ks_types

class SimpleScalarProvider:
    def __init__(self, kv_storage: KeyValueStorage, refresh_after: float = float('+inf')):
        self.storage = kv_storage
        self.refresh_after = refresh_after
        self.invocations = 0
    def _key(self, key: str) -> str: return key
    def _storage(self, key: str): return self.storage
    def _refresh_interval(self, key: str) -> float: return self.refresh_after
    @cached_scalar(
        key=_key,
        storage=_storage,
        refresh_interval=_refresh_interval
    )
    def get_data(self, key: str) -> A:
        self.invocations += 1
        return A(dates.unix(), key)

class SimpleProvider:
    def __init__(self, kv_storage: KeyValueStorage, ks_storage: KeySeriesStorage[A], min_chunk: float|None = 10, max_chunk: float|None = None):
        self.kv_storage = kv_storage
        self.ks_storage = ks_storage
        self.min_chunk = min_chunk
        self.max_chunk = max_chunk
        self.invocations = 0
    def _key_fn(self) -> str: return ""
    def _kv_storage(self) -> KeyValueStorage: return self.kv_storage
    def _ks_storage(self) -> KeySeriesStorage[A]: return self.ks_storage
    def _min_chunk(self) -> float|None: return self.min_chunk
    def _max_chunk(self) -> float|None: return self.max_chunk
    @cached_series(
        key=_key_fn,
        ks_storage=_ks_storage,
        kv_storage=_kv_storage,
        min_chunk=_min_chunk,
        max_chunk=_max_chunk
    )
    def get_series(self, unix_from: float, unix_to: float) -> list[A]:
        self.invocations += 1
        return [A(it) for it in range(int(unix_from), int(unix_to)+1) if it >unix_from and it <=unix_to ] 
class KeyedProvider:
    def __init__(self, kv_storage: KeyValueStorage, ks_storage: KeySeriesStorage[A], min_chunk: float = 10):
        self.kv_storage = kv_storage
        self.ks_storage = ks_storage
        self.min_chunk = min_chunk
        self.invocations = 0
    def _key(self, key: str) -> str: return key
    def _kv_storage(self, key: str) -> KeyValueStorage: return self.kv_storage
    def _ks_storage(self, key: str): return self.ks_storage
    
    def _min_chunk(self, key: str) -> float: return self.min_chunk
    @cached_series(
        key=_key,
        kv_storage=_kv_storage,
        ks_storage=_ks_storage,
        min_chunk=_min_chunk
    )
    def get_series(self, unix_from: float, unix_to: float, key: str) -> list[A]:
        self.invocations += 1
        return [A(it, key) for it in range(int(unix_from), int(unix_to)+1) if it>unix_from and it<=unix_to ]
class EdgeProvider():
    def __init__(self, kv_storage: KeyValueStorage, ks_storage: KeySeriesStorage[A], min_chunk: float = 10, live_delay: float = 0, refresh_after: float = 0):
        self.kv_storage = kv_storage
        self.ks_storage = ks_storage
        self.batch_size = min_chunk
        self.live_delay = live_delay
        self.refresh_after = refresh_after
        self.invocations = 0
    def _key(self) -> str: return ""
    def _kv_storage(self) -> KeyValueStorage: return self.kv_storage
    def _ks_storage(self) -> KeySeriesStorage[A]: return self.ks_storage
    def _min_chunk(self) -> float: return self.batch_size
    def _live_delay(self) -> float: return self.live_delay
    def _should_refresh(self, last: float, now: float) -> bool: return now-last >= self.refresh_after
    @cached_series(
        key=_key,
        kv_storage=_kv_storage,
        ks_storage=_ks_storage,
        min_chunk=_min_chunk,
        live_delay=_live_delay,
        should_refresh=_should_refresh
    )
    def get_series(self, unix_from: float, unix_to: float) -> list[A]:
        self.invocations += 1
        return [A(int(unix_from)+1), A(int(unix_to))]

class TestCaching(TestPersistence):
    #region cached_scalar
    @parameterized.expand(kv_types)
    def test_cached_scalar(self, storage_type: storage_type):   
        KEY1 = "1"
        KEY2 = "2"
        provider = SimpleScalarProvider(self.get_kv_storage(storage_type), refresh_after=50)
        dates.set(10)
        data1 = provider.get_data(KEY1)
        self.assertEqual(KEY1, data1.d)
        dates.add(10)
        self.assertEqual(data1, provider.get_data(KEY1))
        data2 = provider.get_data(KEY2)
        self.assertNotEqual(data1, data2)
        self.assertEqual(2, provider.invocations)

        dates.add(50)
        self.assertLess(data1.t, provider.get_data(KEY1).t)
        self.assertEqual(3, provider.invocations)
    #endregion

    #region cached_series
    def test_missing_spans(self):
        examples: list[tuple[list[tuple[float,float]], tuple[float,float], list[tuple[float,float]]]] = [
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (17, 38), [(20, 25), (30, 35)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (20, 25), [(20, 25)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (20, 30), [(20, 25)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (15, 30), [(20, 25)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (0, 10), [(0, 5)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (0, 5), [(0, 5)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (0, 15), [(0, 5), (10,15)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (45, 100), [(50,100)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (48, 100), [(50,100)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (0, 100), [(0,5),(10,15),(20,25),(30,35),(40,45),(50,100)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (7, 48), [(10,15),(20,25),(30,35),(40,45)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (11, 12), [(11,12)]),
        ]
        for existing, target, expect in examples:
            result = list(CachedSeriesDescriptor.missing_spans(existing, target))
            self.assertEqual(expect, result)

    def test_break_span(self):
        examples: list[tuple[tuple[float,float], float, list[tuple[float,float]]]] = [
            ((10,20), 5, [(10,15),(15,20)]),
            ((10,16), 5, [(10,15),(15,16)]),
            ((10,14), 5, [(10,14)]),
            ((11,14), 5, [(11,14)]),
            ((11,16), 5, [(11,16)])
        ]
        for target, max_chunk, expect in examples:
            result = list(CachedSeriesDescriptor.break_span(target, max_chunk))
            self.assertEqual(expect, result)

    def test_cover_spans(self):
        examples: list[tuple[list[tuple[float,float]], tuple[float,float], list[tuple[float,float]]]] = [
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (17, 38), [(5,10),(15,40),(45,50)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (20, 25), [(5,10),(15,30),(35,40),(45,50)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (20, 30), [(5,10),(15,30),(35,40),(45,50)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (15, 30), [(5,10),(15,30),(35,40),(45,50)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (0, 10), [(0,10), (15,20), (25, 30), (35, 40), (45, 50)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (0, 5), [(0,10), (15,20), (25, 30), (35, 40), (45, 50)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (0, 15), [(0,20), (25, 30), (35, 40), (45, 50)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (45, 100), [(5,10), (15,20), (25, 30), (35, 40), (45, 100)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (48, 100), [(5,10), (15,20), (25, 30), (35, 40), (45, 100)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (0, 100), [(0,100)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (7, 48), [(5,50)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (11, 12), [(5,10), (11,12), (15,20), (25, 30), (35, 40), (45, 50)]),
        ]
        for existing, target, expect in examples:
            result = CachedSeriesDescriptor.cover_spans(existing, target)
            self.assertEqual(expect, result)

    def test_remove_span(self):
        examples: list[tuple[list[tuple[float,float]], tuple[float,float], list[tuple[float,float]]]] = [
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (17, 38), [(5,10),(15,17),(38,40),(45,50)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (20, 25), [(5,10), (15,20), (25, 30), (35, 40), (45, 50)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (20, 30), [(5,10), (15,20), (35, 40), (45, 50)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (15, 30), [(5,10), (35, 40), (45, 50)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (0, 10), [(15,20), (25, 30), (35, 40), (45, 50)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (0, 5), [(5,10), (15,20), (25, 30), (35, 40), (45, 50)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (0, 15), [(15,20), (25, 30), (35, 40), (45, 50)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (45, 100), [(5,10), (15,20), (25, 30), (35, 40)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (48, 100), [(5,10), (15,20), (25, 30), (35, 40), (45, 48)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (0, 100), []),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (7, 48), [(5,7),(48,50)]),
            ([(5,10), (15,20), (25, 30), (35, 40), (45, 50)], (11, 12), [(5,10), (15,20), (25, 30), (35, 40), (45, 50)]),
        ]
        for existing, target, expect in examples:
            result = CachedSeriesDescriptor.remove_span(existing, target)
            self.assertEqual(expect, result)

    @parameterized.expand(ks_types)
    def test_cached_series_simple(self, storage_type: storage_type):
        def test(start: int, end: int, min_chunk: int):
            provider = SimpleProvider(self.get_kv_storage(storage_type), self.get_ks_storage(storage_type), min_chunk=min_chunk)
            expect = [A(it) for it in range(start+1, end+1)]
            data = provider.get_series(start, end)
            self.assertEqual(expect, data)
            self.assertEqual(provider.invocations, 1)
            provider.get_series(lower_whole(start, min_chunk), upper_whole(end, min_chunk))
            self.assertEqual(provider.invocations, 1)
            self.assertEqual(expect, provider.get_series(start, end))

        for start, end, min_chunk in [(7,8,10),(7,15,10),(7,25,10),(7,30,10),(0,100,10),(12,12345,12)]:
            self.setUp()
            test(start, end, min_chunk)
            self.tearDown()
    
    @parameterized.expand(ks_types)
    def test_cached_series(self, storage_type: storage_type):
        KEY1 = "k1"
        KEY2 = "abc123"
        provider = KeyedProvider(self.get_kv_storage(storage_type), self.get_ks_storage(storage_type), min_chunk=10)
        test1 = provider.get_series(16, 30, KEY1)
        test2 = provider.get_series(16, 30, KEY2)
        self.assertEqual(14, len(test1))
        self.assertEqual(14, len(test2))
        self.assertEqual(17, test1[0].t)
        self.assertEqual(30, test2[-1].t)
        self.assertEqual(2, provider.invocations)
        self.assertEqual(test1, provider.get_series(16, 30, KEY1))
        self.assertEqual(test2, provider.get_series(16, 30, KEY2))
        self.assertEqual(2, provider.invocations)

    @parameterized.expand(ks_types)
    def test_cached_series_refresh(self, storage_type: storage_type):
        provider = EdgeProvider(self.get_kv_storage(storage_type), self.get_ks_storage(storage_type), min_chunk = 100, refresh_after=10)
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
        self.assertEqual(series[2].t, dates.unix())
        self.assertEqual(series[0].t + 1, series[1].t)
        self.assertEqual(2, provider.invocations)
        self.assertEqual(series, provider.get_series(unix_from, new_unix_to))
        self.assertEqual(2, provider.invocations)

    @parameterized.expand(ks_types)
    def test_cached_series_live_delay(self, storage_type: storage_type):
        provider = EdgeProvider(self.get_kv_storage(storage_type), self.get_ks_storage(storage_type), live_delay=15, min_chunk=100)
        unix_from = 50
        unix_to = 60
        dates.set(unix_to)
        test1 = provider.get_series(unix_from, unix_to)
        self.assertEqual(0, len(test1))
        dates.add(10)
        test2 = provider.get_series(unix_from, unix_to)
        self.assertEqual(1, len(test2))
        self.assertEqual(55, test2[0].t)

    @parameterized.expand(ks_types)
    def test_cached_series_invalidate(self, storage_type: storage_type):
        provider = EdgeProvider(self.get_kv_storage(storage_type), self.get_ks_storage(storage_type), min_chunk=10)
        dates.set(15)
        series = provider.get_series(0, 20)
        self.assertEqual([1, 15], [it.t for it in series])

        EdgeProvider.get_series.invalidate(provider, 13, 15)
        series = provider.get_series(0, 20)
        self.assertEqual([1,14,15], [it.t for it in series])

        EdgeProvider.get_series.invalidate(provider, 17, 150)
        series = provider.get_series(0, 20)
        self.assertEqual([1,14,15], [it.t for it in series])

        EdgeProvider.get_series.invalidate(provider, 1, 5)
        EdgeProvider.get_series.invalidate(provider, 8, 10)
        dates.add(5)
        series = provider.get_series(0, 20)
        self.assertEqual([1,2,5,9,10,14,15, 16, 20], [it.t for it in provider.get_series(0, 20)])

        self.assertEqual(5, provider.invocations)
        provider.get_series(0,20)
        self.assertEqual(5, provider.invocations)

    @parameterized.expand(ks_types)
    def test_cached_series_delete_chunking(self, storage_type: storage_type):
        provider = SimpleProvider(self.get_kv_storage(storage_type), self.get_ks_storage(storage_type), min_chunk=5, max_chunk=10)
        dates.set(50)
        series = provider.get_series(2,48)
        self.assertEqual(list(range(3,49)), [it.t for it in series])
        self.assertEqual(5, provider.invocations)

        SimpleProvider.get_series.invalidate(provider, 20, 35)
        SimpleProvider.get_series.invalidate(provider, 45, 50)
        dates.set(60)
        series = provider.get_series(0, 60)
        self.assertEqual(list(range(1,61)), [it.t for it in series])
        self.assertEqual(9, provider.invocations)

        dates.set(65)
        self.assertEqual(list(range(1,66)), [it.t for it in provider.get_series(0, 65)])
        self.assertEqual(10, provider.invocations)
        dates.set(75)
        self.assertEqual(list(range(1,76)), [it.t for it in provider.get_series(0, 75)])
        self.assertEqual(11, provider.invocations)
    #endregion