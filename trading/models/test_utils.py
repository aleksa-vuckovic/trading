import torch
from unittest import TestCase
from ..utils import dateutils
from .utils import StatCollector, StatContainer, normalize_in_place, relativize_in_place, get_next_time, TrainingPlan, Batches


class TestUtils(TestCase):

    def test_get_next_time(self):
        input = dateutils.str_to_unix('2025-01-23 02:12:22', tz=dateutils.ET)
        expect = dateutils.str_to_unix('2025-01-23 09:00:00', tz=dateutils.ET)
        self.assertEqual(expect, get_next_time(input))

        input = dateutils.str_to_unix('2025-01-17 20:01:12', tz=dateutils.ET)
        expect = dateutils.str_to_unix('2025-01-20 09:00:00', tz=dateutils.ET)
        self.assertEqual(expect, get_next_time(input))

        input = dateutils.str_to_unix('2025-01-20 11:00:00', tz=dateutils.ET)
        expect = dateutils.str_to_unix('2025-01-20 12:00:00', tz=dateutils.ET)
        self.assertEqual(expect, get_next_time(input))

    def test_get_next_time_by_hour(self):
        input = dateutils.str_to_unix('2025-01-23 02:12:22', tz=dateutils.ET)
        expect = dateutils.str_to_unix('2025-01-23 11:00:00', tz=dateutils.ET)
        self.assertEqual(expect, get_next_time(input, hour=11))

        input = dateutils.str_to_unix('2025-01-17 15:01:12', tz=dateutils.ET)
        expect = dateutils.str_to_unix('2025-01-20 15:00:00', tz=dateutils.ET)
        self.assertEqual(expect, get_next_time(input, hour=15))

        input = dateutils.str_to_unix('2025-01-20 11:00:00', tz=dateutils.ET)
        expect = dateutils.str_to_unix('2025-01-20 13:00:00', tz=dateutils.ET)
        self.assertEqual(expect, get_next_time(input, hour=13))

        input = dateutils.str_to_unix('2025-01-24 11:00:00', tz=dateutils.ET)
        expect = dateutils.str_to_unix('2025-01-27 11:00:00', tz=dateutils.ET)
        self.assertEqual(expect, get_next_time(input, hour=11))

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

    def test_relativize(self):
        tensor = torch.tensor([[1,2,3,4,5],[5,6,7,6,5]], dtype=torch.float32)
        expect = torch.tensor([[1,0,1/2,1/3,5],[5,0,1/6,-1/7,5]], dtype=torch.float32)
        relativize_in_place(tensor, start_index=1, count=3, dim=1, use_previous=False)
        self.assertEqual(10, (tensor==expect).sum().item())

        tensor = torch.tensor([[1,2,3,4,5],[5,6,7,6,5]], dtype=torch.float32)
        expect[0][1] = 1
        expect[0][4] = 1/4
        expect[1][1] = 1/5
        expect[1][4] = -1/6
        relativize_in_place(tensor, start_index=1, count=-1, dim=1, use_previous=True)
        self.assertEqual(10, (tensor==expect).sum().item())

        tensor = torch.tensor([[[1,2,3],[4,5,6]],[[6,5,4],[3,2,1]]])
        expect = torch.tensor([[[0,2,3],[3,5,6]],[[0,5,4],[-3/6,2,1]]])
        normalize_in_place(tensor[:,:,0], dim=1)

    def test_normalize_in_place(self):
        tensor = torch.tensor([[1,2,3,4],[5,6,7,8]], dtype=torch.float32)
        expect = torch.tensor([[1,2/3,1,4],[5,6/7,1,8]], dtype=torch.float32)
        maxes = normalize_in_place(tensor, 1, 2, dim=1)
        self.assertEqual(8, (tensor==expect).sum().item())
        self.assertEqual((2,1), tuple(maxes.shape))

        tensor = torch.tensor([[1,2,torch.nan,4],[5,torch.inf,7,8]], dtype=torch.float32)
        expect = torch.tensor([[1,torch.nan,torch.nan,4],[5,torch.nan,0,8]], dtype=torch.float32)
        normalize_in_place(tensor, 1,2, dim=1)
        self.assertEqual(5, (tensor==expect).sum().item())
        self.assertEqual(expect.isnan().sum().item(), tensor.isnan().sum().item())

        tensor = torch.tensor([[[1,2,3],[4,5,6]],[[7,8,9],[10,11,12]]], dtype=torch.float32)
        expect = torch.tensor([[[1/7,2/8,3/9],[4/10,5/11,6/12]],[[1,1,1],[1,1,1]]], dtype=torch.float32)
        maxes = normalize_in_place(tensor)
        self.assertEqual(12, (tensor==expect).sum().item())
        self.assertEqual((1,2,3), tuple(maxes.shape))


    def test_triggers(self):
        stat_trigger = TrainingPlan.StatTrigger(key = 'count', lower_bound=1.5, lower_bound_inclusive=False, upper_bound=4, upper_bound_inclusive=True, event='enter', trigger_once=True)
        stat_history = TrainingPlan.StatHistoryAction()
        class CountCollector(StatCollector):
            def __init__(self):
                super().__init__('count')
                self.i = 0
            def _calculate(self, expect, output):
                self.i += 1
                return self.i
        stats = StatContainer(CountCollector())
        def make_plan(stats: StatContainer):
            return TrainingPlan(torch.nn.Sigmoid()).with_optimizer(None).with_stats(stats, stats).with_batches(Batches([]), Batches([]))
        plan = make_plan(stats)
        stats.update(None, None)
        stat_history.execute(plan)
        stats.update(None, None)
        self.assertFalse(stat_trigger.check(plan))
        stat_history.execute(plan)
        stats.update(None, None)
        self.assertTrue(stat_trigger.check(plan))
        stat_history.execute(plan)
        stats.update(None, None)
        self.assertFalse(stat_trigger.check(plan))

        stats = StatContainer(CountCollector())
        stats.update(None, None)
        plan = make_plan(stats)
        plan.epoch = 2
        stat_history.execute(plan)
        stats.update(None, None)
        or_trigger = TrainingPlan.EpochTrigger(2, trigger_once=True) \
            | TrainingPlan.StatTrigger('count', lower_bound=2, trigger_once=True)
        self.assertTrue(or_trigger.check(plan))
        stat_history.execute(plan)
        stats.update(None, None)
        plan.epoch = 3
        self.assertTrue(or_trigger.check(plan))
        stat_history.execute(plan)
        stats.update(None, None)
        self.assertFalse(or_trigger.check(plan))

