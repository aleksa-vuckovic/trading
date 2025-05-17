from typing import Any, TypedDict
from pathlib import Path
from base import dates
from base.algos import lower_whole, upper_whole
from base.caching import KeySeriesStorage, KeyValueStorage, cached_scalar, cached_series
from base.key_series_storage import MemoryKSStorage
from base.key_value_storage import MemoryKVStorage
from base.tests.test_base import TestBase


TEST_DATA = Path("./test_data")
class SimpleDict(TypedDict):
    t: float
    d: Any
class SimpleProvider:
    def __init__(self, storage: KeySeriesStorage|None = None, batch_size: float = 10):
        self.storage: KeySeriesStorage[SimpleDict] = storage or MemoryKSStorage(lambda it: it['t'])
        self.batch_size = batch_size
        self.invocations = 0
    @staticmethod
    def _timestamp_fn(it: SimpleDict) -> float: return it['t']
    def _key_fn(self) -> str: return ""
    def _storage_fn(self) -> KeySeriesStorage[SimpleDict]: return self.storage
    def _batch_size_fn(self) -> float: return self.batch_size
    @cached_series(
        timestamp_fn = _timestamp_fn,
        key_fn=_key_fn,
        storage_fn=_storage_fn,
        batch_size_fn=_batch_size_fn
    )
    def get_series(self, unix_from: float, unix_to: float) -> list[SimpleDict]:
        self.invocations += 1
        return [{'t': it, 'd': it} for it in range(int(unix_from), int(unix_to)+1) if it >unix_from and it <=unix_to ] 
class KeyedProvider:
    def __init__(self, storage: KeySeriesStorage|None = None, batch_size: float = 10):
        self.storage: KeySeriesStorage[SimpleDict] = storage or MemoryKSStorage(lambda it: it['t'])
        self.batch_size = batch_size
        self.invocations = 0
    @staticmethod
    def _timestamp_fn(it: SimpleDict) -> float: return it['t']
    def _key_fn(self, key: str) -> str: return key
    def _storage_fn(self, key: str): return self.storage
    def _batch_size_fn(self, key: str) -> float: return self.batch_size
    @cached_series(
        timestamp_fn = _timestamp_fn,
        key_fn=_key_fn,
        storage_fn=_storage_fn,
        batch_size_fn=_batch_size_fn
    )
    def get_series(self, unix_from: float, unix_to: float, key: str) -> list[SimpleDict]:
        self.invocations += 1
        return [{'t': it, 'd': key} for it in range(int(unix_from), int(unix_to)+1) if it>unix_from and it<=unix_to ]
class EdgeProvider:
    def __init__(self, storage: KeySeriesStorage|None = None, batch_size: float = 10, live_delay: float = 0, refresh_after: float = 0):
        self.storage: KeySeriesStorage[SimpleDict] = storage or MemoryKSStorage(lambda it: it['t'])
        self.batch_size = batch_size
        self.live_delay = live_delay
        self.refresh_after = refresh_after
        self.invocations = 0
    @staticmethod
    def _timestamp_fn(it: SimpleDict) -> float: return it['t']
    def _key_fn(self) -> str: return ""
    def _storage_fn(self) -> KeySeriesStorage: return self.storage
    def _batch_size_fn(self) -> float: return self.batch_size
    def _live_delay_fn(self) -> float: return self.live_delay
    def _should_refresh_fn(self, last: float, now: float) -> bool: return now-last >= self.refresh_after
    @cached_series(
        timestamp_fn = _timestamp_fn,
        key_fn=_key_fn,
        storage_fn=_storage_fn,
        batch_size_fn=_batch_size_fn,
        live_delay_fn=_live_delay_fn,
        should_refresh_fn=_should_refresh_fn
    )
    def get_series(self, unix_from: float, unix_to: float) -> list[SimpleDict]:
        self.invocations += 1
        return [{'t': int(unix_from)+1, 'd': None}, {'t': int(unix_to), 'd': None}]

class SimpleScalarProvider:
    def __init__(self, storage: KeyValueStorage|None = None, refresh_after: float = float('+inf')):
        self.storage = storage or MemoryKVStorage()
        self.refresh_after = refresh_after
        self.invocations = 0
    def _key_fn(self, key: str) -> str: return key
    def _storage_fn(self, key: str): return self.storage
    def _refresh_fn(self, key: str) -> float: return self.refresh_after
    @cached_scalar(
        key_fn=_key_fn,
        storage_fn=_storage_fn,
        refresh_fn=_refresh_fn
    )
    def get_data(self, key: str) -> SimpleDict:
        self.invocations += 1
        return {'t': dates.unix(), 'd': key}

class TestCaching(TestBase):
    def _test_cached_series_simple(self, start: int, end: int, batch_size: int):
        provider = SimpleProvider(batch_size=batch_size)
        expect = [{'t': it, 'd': it} for it in range(start+1, end+1)]
        data = provider.get_series(start, end)
        self.assertEqual(expect, data)
        self.assertEqual(provider.invocations, 1)
        provider.get_series(lower_whole(start, batch_size), upper_whole(end, batch_size))
        self.assertEqual(provider.invocations, 1)
        self.assertEqual(expect, provider.get_series(start, end))

    def test_cached_series_simple(self):
        for args in [(7,8,10),(7,15,10),(7,25,10),(7,30,10),(0,100,10),(12,12345,12)]:
            self.setUp()
            self._test_cached_series_simple(*args)
            self.tearDown()
    
    def test_cached_series(self):
        KEY1 = "k1"
        KEY2 = "abc/123"
        provider = KeyedProvider(batch_size=10)
        test1 = provider.get_series(16, 30, KEY1)
        test2 = provider.get_series(16, 30, KEY2)
        self.assertEqual(14, len(test1))
        self.assertEqual(14, len(test2))
        self.assertEqual(17, test1[0]['t'])
        self.assertEqual(30, test2[-1]['t'])
        self.assertEqual(2, provider.invocations)
        self.assertEqual(test1, provider.get_series(16, 30, KEY1))
        self.assertEqual(test2, provider.get_series(16, 30, KEY2))
        self.assertEqual(2, provider.invocations)

    def test_cached_series_refresh(self):
        provider = EdgeProvider(batch_size = 100, refresh_after=10)
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

    def test_cached_series_live_delay(self):
        provider = EdgeProvider(live_delay=15, batch_size=100)
        unix_from = 50
        unix_to = 60
        dates.set(unix_to)
        test1 = provider.get_series(unix_from, unix_to)
        self.assertEqual(0, len(test1))
        dates.add(10)
        test2 = provider.get_series(unix_from, unix_to)
        self.assertEqual(1, len(test2))
        self.assertEqual(55, test2[0]['t'])

    def test_cached_series_delete(self):
        storage = MemoryKSStorage[SimpleDict](lambda it: it['t'])
        provider = EdgeProvider(storage=storage, batch_size=10)
        dates.set(15)
        series = provider.get_series(0, 20)
        self.assertEqual([1, 15], [it['t'] for it in series])

        storage.delete("", 13, 15)
        series = provider.get_series(0, 20)
        self.assertEqual([1,14,15], [it['t'] for it in series])

        storage.delete("", 17, 150)
        series = provider.get_series(0, 20)
        self.assertEqual([1,14,15], [it['t'] for it in series])

        self.assertEqual(2, provider.invocations)

    def test_cached_series_delete_multi(self):
        storage = MemoryKSStorage[SimpleDict](lambda it: it['t'])
        provider = SimpleProvider(storage=storage, batch_size=5)

        series = provider.get_series(5,55)
        self.assertEqual(list(range(6,56)), [it['t'] for it in series])
        self.assertEqual(1, provider.invocations)
        
        storage.delete("", 20, 35)
        storage.delete("", 45, 50)
        series = provider.get_series(0, 60)
        self.assertEqual(list(range(1,61)), [it['t'] for it in series])
        self.assertEqual(5, provider.invocations)

    def test_cached_scalar(self):        
        provider = SimpleScalarProvider()
        data = provider.get_data('test')
        self.assertEqual(data['d'], 'test')
        self.assertEqual(data, provider.get_data('test'))
        self.assertEqual(1, provider.invocations)
    
    def test_cached_scalar_refresh(self):        
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
        self.assertGreater(data2['t'], data1['t'])
        data2 = provider.get_data(KEY2) and provider.get_data(KEY2)
        self.assertNotEqual(data1, data2)
        self.assertEqual(3, provider.invocations)

    def test_cached_scalar_delete(self):
        storage = MemoryKVStorage()
        provider = SimpleScalarProvider(storage=storage)
        KEY1 = "1"
        KEY2 = "2"
        dates.set(10)
        a1 = provider.get_data(KEY1)
        a2 = provider.get_data(KEY2)
        dates.add(10)
        self.assertEqual(a1, provider.get_data(KEY1))
        storage.delete(KEY1)
        a12 = provider.get_data(KEY1)
        self.assertNotEqual(a1, a12)
        self.assertEqual(a2, provider.get_data(KEY2))
        self.assertEqual(3, provider.invocations)
