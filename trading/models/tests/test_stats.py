import unittest
import torch
from ..stats import StatCollector, StatContainer


class TestStats(unittest.TestCase):
    def test_stats(self):
        class TestCollectorTensor(StatCollector):
            def __init__(self):
                super().__init__('tensor')
            def _calculate(self, expect, output):
                return torch.logical_and(expect, output).sum()
            
        class TestCollectorFloat(StatCollector):
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
        
        c = StatContainer(c1, c2, name='test')
        c.clear()
        c.update(expect1, output1)
        c.update(expect2, output2)
        d = c.to_dict()
        self.assertEqual(d['tensor'], 0.5)