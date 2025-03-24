import torch
from unittest import TestCase
from trading.models.utils import get_moving_average, get_normalized_by_largest, get_time_relativized


class TestUtils(TestCase):
    def test_get_time_relativized(self):
        tensor = torch.tensor([[1,2,3,4,5],[5,6,7,6,5]], dtype=torch.float32)
        expect = torch.tensor([[0,1/2,1/3],[0,1/6,-1/7]], dtype=torch.float32)
        result = get_time_relativized(tensor, start_index=1, count=3, dim=1, use_previous=False)
        self.assertEqual(6, (result==expect).sum().item())

        tensor = torch.tensor([[1,2,3,4,5],[5,6,7,6,5]], dtype=torch.float32)
        expect = torch.tensor([[1,1/2,1/3,1/4],[1/5,1/6,-1/7,-1/6]], dtype=torch.float32)
        result = get_time_relativized(tensor, start_index=1, count=-1, dim=1, use_previous=True)
        self.assertEqual(8, (result==expect).sum().item())

    def test_get_normalized_by_largest(self):
        tensor = torch.tensor([[1,2,3,4],[5,6,7,8]], dtype=torch.float32)
        expect = torch.tensor([[2/3,1],[6/7,1]], dtype=torch.float32)
        result = get_normalized_by_largest(tensor, 1, 2, dim=1)
        self.assertEqual(4, (result==expect).sum().item())

        tensor = torch.tensor([[1,2,torch.nan,4],[5,torch.inf,7,8]], dtype=torch.float32)
        expect = torch.tensor([[torch.nan,torch.nan],[torch.nan,0]], dtype=torch.float32)
        result = get_normalized_by_largest(tensor, 1,2, dim=1)
        self.assertEqual(1, (result==expect).sum().item())
        self.assertEqual(expect.isnan().sum().item(), result.isnan().sum().item())

        tensor = torch.tensor([[[1,2,3],[4,5,6]],[[7,8,9],[10,11,12]]], dtype=torch.float32)
        expect = torch.tensor([[[1/7,2/8,3/9],[4/10,5/11,6/12]],[[1,1,1],[1,1,1]]], dtype=torch.float32)
        result = get_normalized_by_largest(tensor)
        self.assertEqual(12, (result==expect).sum().item())

    def test_get_moving_average(self):
        tensor = torch.tensor([1,2,3,4,5,6,7,8], dtype=torch.float32)
        expect = torch.tensor([3,4,5,6,7], dtype=torch.float32)
        result = get_moving_average(tensor, start_index=3, dim=0, window=3)
        self.assertEqual(5, (expect==result).sum().item())

        dims = [10,100,10]
        count = 15
        tensor = torch.randn(tuple(dims), dtype=torch.float64)
        expect = torch.concat([tensor[:,0:1,:], tensor[:,0:1,:], tensor], dim=1)
        expect = (expect[:,:dims[1],:] + expect[:,1:dims[1]+1,:] + expect[:,2:dims[1]+2,:])/3
        expect = expect[:,:count,:]
        result = get_moving_average(tensor, start_index=-dims[1], count=count, dim=1, window=3)
        self.assertAlmostEqual(0, torch.abs(result-expect).mean().item(), 15)

        tensor = torch.tensor([[1,2,3,4,5,6,7]], dtype=torch.float32)
        expect = torch.tensor([[1,5/4,7/4,10/4,14/4,18/4,22/4]], dtype=torch.float32)
        result = get_moving_average(tensor, dim=1, window=4)
        self.assertEqual(7, (expect==result).sum().item())
