from . import common
import unittest
import time
from pathlib import Path
import shutil
import json
import torch

class TestCommon(unittest.TestCase):

    def test_stats(self):
        class TestCollectorTensor(common.StatCollector):
            def __init__(self):
                super().__init__('tensor')
            def _calculate(self, expect, output):
                return torch.logical_and(expect, output).sum()
            
        class TestCollectorFloat(common.StatCollector):
            def __init__(self):
                super().__init__('float')
            def _calculate(self, expect, output):
                return torch.logical_or(expect, output).sum().item()
            
        output1 = torch.Tensor([True, True, False])
        expect1 = torch.Tensor([True, False, False])

        output2 = torch.Tensor([True, True, True])
        expect2 = torch.Tensor([False, False, False])

        c1 = TestCollectorTensor()
        c2 = TestCollectorFloat()
        self.assertIsInstance(c1.update(expect1, output1), torch.Tensor)
        self.assertIsInstance(c2.update(expect1, output1), (float, int))
        c1.update(expect2, output2)
        c2.update(expect2, output2)
        self.assertEqual(2, c1.count)
        self.assertEqual(0, c1.last)
        self.assertEqual(0.5, c1.running)
        self.assertEqual(2.5, c2.running)
        
        c = common.StatContainer(c1, c2, name='test')
        c.clear()
        c.update(expect1, output1)
        c.update(expect2, output2)
        d = c.to_dict()
        self.assertEqual(d['test']['tensor']['running'], 0.5)


    def test_normalize_in_place(self):
        tensor = torch.tensor([[1,2,3,4],[5,6,7,8]], dtype=torch.float32)
        expect = torch.tensor([[1,2/3,1,4],[5,6/7,1,8]], dtype=torch.float32)
        maxes = common.normalize_in_place(tensor, 1, 2, dim=1)
        self.assertEqual(8, (tensor==expect).sum().item())
        self.assertEqual((2,1), tuple(maxes.shape))

        tensor = torch.tensor([[1,2,torch.nan,4],[5,torch.inf,7,8]], dtype=torch.float32)
        expect = torch.tensor([[1,torch.nan,torch.nan,4],[5,torch.nan,0,8]], dtype=torch.float32)
        common.normalize_in_place(tensor, 1,2, dim=1)
        self.assertEqual(5, (tensor==expect).sum().item())
        self.assertEqual(expect.isnan().sum().item(), tensor.isnan().sum().item())

        tensor = torch.tensor([[[1,2,3],[4,5,6]],[[7,8,9],[10,11,12]]], dtype=torch.float32)
        expect = torch.tensor([[[1/7,2/8,3/9],[4/10,5/11,6/12]],[[1,1,1],[1,1,1]]], dtype=torch.float32)
        maxes = common.normalize_in_place(tensor)
        self.assertEqual(12, (tensor==expect).sum().item())
        self.assertEqual((1,2,3), tuple(maxes.shape))

    def test_binary_search(self):
        collection = [
            {"time": 1},
            {"time": 2},
            {"time": 5},
            {"time": 8},
            {"time": 10}
        ]
        def get_time(item):
            return item["time"]
        self.assertEqual(0, common.binary_search(collection, get_time, 0, common.BinarySearchEdge.HIGH))
        self.assertEqual(None, common.binary_search(collection, get_time, 0, common.BinarySearchEdge.LOW))
        self.assertEqual(0, common.binary_search(collection, get_time, 1, common.BinarySearchEdge.HIGH))
        self.assertEqual(1, common.binary_search(collection, get_time, 1.5, common.BinarySearchEdge.HIGH))
        self.assertEqual(0, common.binary_search(collection, get_time, 1.5, common.BinarySearchEdge.LOW))
        self.assertEqual(None, common.binary_search(collection, get_time, 1.5, common.BinarySearchEdge.NONE))
        self.assertEqual(4, common.binary_search(collection, get_time, 10.5, common.BinarySearchEdge.LOW))
        collection.pop()
        self.assertEqual(2, common.binary_search(collection, get_time, 3, common.BinarySearchEdge.HIGH))

    def test_backup_timeout_decorator(self):
        class TestException(Exception):
            pass
        base_timeout = 0.05
        #create a method that always throws an exception
        invocations = 0
        @common.backup_timeout(exc_type=TestException, behavior=common.BackupBehavior.DEFAULT, base_timeout=base_timeout, backoff_factor=2)
        def test_method():
            nonlocal invocations
            invocations += 1
            if invocations > 3:
                return 'Success'
            raise TestException()
        
        self.assertIsNone(test_method())
        time.sleep(base_timeout)
        self.assertIsNone(test_method())
        time.sleep(base_timeout*2)
        self.assertIsNone(test_method())
        time.sleep(base_timeout*2)
        self.assertIsNone(test_method())
        time.sleep(base_timeout*2)
        self.assertEqual('Success', test_method())

    def test_cached_series_decorator(self):
        cache = Path("./_test_cache")
        if cache.exists():
            shutil.rmtree(cache)
        @common.cached_series(
            cache_root=cache,
            unix_from_arg=0,
            unix_to_arg="unix_to",
            include_args=["type"],
            time_step_fn=lambda args: 10 if args[0] == 'type10' else 30,
            series_field="series",
            timestamp_field="time",
            return_series_only=True
        )
        def get_series(unix_from: float, *, unix_to: float, type: str):
            return {
                "name": type,
                "series": [{"time": float(it), "data": it} for it in range(int(unix_from), int(unix_to) )]
            }
        
        test1 = get_series(15, unix_to=29, type="type10")
        test2 = get_series(15, unix_to=29, type="other")
        self.assertEqual(14, len(test1))
        self.assertEqual(14, len(test2))
        self.assertEqual(15, test1[0]['data'])
        self.assertEqual(28, test2[-1]['data'])
        type10_path = cache / "type10"
        other_path = cache / "other"
        self.assertTrue(type10_path.exists())
        self.assertTrue(other_path.exists())
        self.assertTrue((type10_path/"1").exists())
        self.assertTrue((type10_path/"2").exists())
        self.assertTrue((other_path/"0").exists())
        self.assertEqual(10, len(json.loads((type10_path/"1").read_text())["series"]))
        self.assertEqual(10, len(json.loads((type10_path/"2").read_text())["series"]))
        self.assertEqual(30, len(json.loads((other_path/"0").read_text())["series"]))

    def test_cached_series_decorator_live(self):
        cache = Path("./_test_cache")
        if cache.exists():
            shutil.rmtree(cache)
        invocations = 0
        time_step = 24*3600
        @common.cached_series(
            cache_root=cache,
            unix_from_arg=0,
            unix_to_arg=1,
            include_args=[],
            time_step_fn= time_step,
            series_field=None,
            timestamp_field="time",
            live_delay=1
        )
        def get_series(unix_from: float,  unix_to: float):
            nonlocal invocations
            invocations += 1
            return [{"time": unix_from}, {"time": unix_to-1}]
        
        unix_to = time.time()
        unix_from = unix_to - 1000
        test1 = get_series(unix_from, unix_to)
        metapath = cache / "meta"
        livepath = cache / "live"
        self.assertTrue(metapath.exists())
        self.assertTrue(livepath.exists())
        meta = json.loads(metapath.read_text())["live"]
        self.assertEqual(int(unix_to)//time_step,meta["id"])
        self.assertGreaterEqual(meta["fetch"], unix_to)
        self.assertEqual(1, len(test1)) #get_series will be invoked with the lower chunk border, therefore the first entry will be filtered out

        get_series(unix_from, unix_to + 1)
        self.assertEqual(1, invocations)
        time.sleep(1)
        new_unix_to = time.time()
        series = get_series(unix_from, new_unix_to)
        self.assertEqual(3, len(series)) #now get_series will be invoked with the previous upper border
        self.assertGreater(series[2]["time"], unix_to)
        self.assertEqual(series[0]["time"] + 1, series[1]["time"])
        self.assertEqual(2, invocations)
        self.assertEqual(4, len(json.loads(livepath.read_text()))) #Make sure previous data is not deleted