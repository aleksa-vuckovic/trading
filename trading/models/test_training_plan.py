import unittest
import torch
from .training_plan import TrainingPlan
from .stats import StatCollector, StatContainer
from .utils import Batches

class TestTrainingPlan(unittest.TestCase):
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
            model = torch.nn.Linear(1,1)
            return TrainingPlan.Builder(model)\
                .with_optimizer(model.parameters())\
                .with_batches('train', Batches([]), stats=stats, backward=True)\
                .with_batches('val', Batches([]), stats=stats)\
                .build()
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

