from pathlib import Path
from typing import override
import unittest
import torch
from torch import Tensor

from trading.core import Interval
from trading.models.base.model_config import Aggregation, Quote, ModelDataConfig, ModelConfig, PriceEstimator, PriceTarget
from trading.models.base.abstract_model import AbstractModel
from trading.models.base.training_plan import BatchGroupConfig, TrainingPlan, StatTrigger, EpochTrigger
from trading.models.base.stats import StatCollector, StatContainer
from trading.core.work_calendar import TimingConfig

config = ModelConfig(
    PriceEstimator(Quote.C, Interval.H1, slice(1,2), Aggregation.AVG),
    PriceTarget.LINEAR_0_10,
    TimingConfig.Builder().at(9).build(),
    ModelDataConfig({Interval.H1: 10}),
    {}
)
class Model(AbstractModel):
    def __init__(self, config: ModelConfig):
        super().__init__(config)
        self.layer = torch.nn.Linear(2,2)

model = Model(config)
optimizer = torch.optim.SGD(model.parameters())

class TestTrainingPlan(unittest.TestCase):
    def test_triggers(self):
        stat_trigger = StatTrigger('val', 'count', (1.5,4), event='enter', once=True)
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
                .with_optimizer(torch.optim.SGD(model.parameters()))\
                .with_folders(Path('./test'))\
                .with_batch_groups(
                    BatchGroupConfig('train', 1, 1, backward=True),
                    BatchGroupConfig('val', 1, 1)
                ).with_stats(stats).build()
        plan = make_plan(stats)
        def update(expect: Tensor, output: Tensor, push: bool = True):
            stats.update(expect, output)
            if push:
                plan.history.append({'train': stats.to_dict(), 'val': stats.to_dict(), 'epoch': plan.epoch})
                plan.epoch += 1
        expect = torch.tensor([1,2,3])
        output = torch.tensor([1,2,4])
        update(expect, output, True)
        self.assertFalse(stat_trigger.check(plan))
        update(expect, output, True)
        self.assertFalse(stat_trigger.check(plan))
        update(expect, output, True)
        self.assertTrue(stat_trigger.check(plan))
        update(expect, output, True)
        self.assertFalse(stat_trigger.check(plan))

        or_trigger = EpochTrigger(2, once=True) | StatTrigger('val', 'count', (1.9, float('+inf')), once=True)
        stats = StatContainer(CountCollector(), name='test')
        plan = make_plan(stats)
        update(expect, output, True)
        self.assertTrue(or_trigger.check(plan))
        update(expect, output, True)
        self.assertFalse(or_trigger.check(plan))
        update(expect, output, True)
        self.assertTrue(or_trigger.check(plan))
        update(expect, output, True)
        self.assertFalse(or_trigger.check(plan))

