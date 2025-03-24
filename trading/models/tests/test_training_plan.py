from pathlib import Path
from typing import override
import unittest
import torch

from fix import TimingConfig
from trading.core import Interval
from trading.models.abstract import AbstractModel, Aggregation, Quote, DataConfig, ModelConfig, PriceEstimator, PriceTarget
from trading.models.training_plan import TrainingPlan, StatTrigger, EpochTrigger, StatHistoryAction
from trading.models.stats import StatCollector, StatContainer
from trading.models.batches import Batches

config = ModelConfig(
    PriceEstimator(Quote.C, Interval.H1, slice(1,2), Aggregation.AVG),
    PriceTarget.LINEAR_0_10,
    TimingConfig.Builder().at(9).build(),
    DataConfig({Interval.H1: 10}),
    Path('./test')
)
class Model(AbstractModel):
    pass

model = Model(config)
optimizer = torch.optim.SGD(model.parameters())

class TestTrainingPlan(unittest.TestCase):
    def test_triggers(self):
        stat_trigger = StatTrigger('val', 'count', (1.5,4), event='enter', once=True)
        stat_history = StatHistoryAction()
        class CountCollector(StatCollector):
            def __init__(self):
                super().__init__('count')
                self.i = 0
            @override
            def _calculate(self, expect: torch.Tensor, output: torch.Tensor) -> torch.Tensor:
                self.i += 1
                return torch.tensor(self.i, dtype=torch.float32)
        stats = StatContainer(CountCollector(), name='test')
        class Model(AbstractModel):
            pass

        def make_plan(stats: StatContainer):
            model = torch.nn.Linear(1,1)
            return TrainingPlan.Builder(Model(config))\
                .with_optimizer(torch.optim.SGD(model))\
                .with_batches('train', Batches([]), stats=stats, backward=True)\
                .with_batches('val', Batches([]), stats=stats)\
                .build()
        plan = make_plan(stats)
        expect = torch.tensor([1,2,3])
        output = torch.tensor([1,2,4])
        stats.update(expect, output)
        stat_history.execute(plan)
        stats.update(expect, output)
        self.assertFalse(stat_trigger.check(plan))
        stat_history.execute(plan)
        stats.update(expect, output)
        self.assertTrue(stat_trigger.check(plan))
        stat_history.execute(plan)
        stats.update(expect, output)
        self.assertFalse(stat_trigger.check(plan))

        stats = StatContainer(CountCollector(), name='test')
        stats.update(expect, output)
        plan = make_plan(stats)
        plan.epoch = 2
        stat_history.execute(plan)
        stats.update(expect, output)
        or_trigger = EpochTrigger(2, once=True) \
            | StatTrigger('val', 'count', (2, float('+inf')), once=True)
        self.assertTrue(or_trigger.check(plan))
        stat_history.execute(plan)
        stats.update(expect, output)
        plan.epoch = 3
        self.assertTrue(or_trigger.check(plan))
        stat_history.execute(plan)
        stats.update(expect, output)
        self.assertFalse(or_trigger.check(plan))

