import gc
import unittest
import torch
from typing import override
from torch import Tensor

from trading.core import Interval
from trading.models.base.model_config import Aggregation, Quote, ModelDataConfig, ModelConfig, PriceEstimator, PriceTarget
from trading.models.base.manager import HistoryFrame, ModelManager, StatTrigger, EpochTrigger
from trading.models.base.stats import StatCollector, StatContainer
from trading.core.work_calendar import TimingConfig
from trading.models.base.tests.model import Model

class CountCollector(StatCollector):
    def __init__(self):
        super().__init__('count')
        self.i = 0
    @override
    def _calculate(self, expect: torch.Tensor, output: torch.Tensor) -> torch.Tensor:
        self.i += 1
        return torch.tensor(self.i, dtype=torch.float32)

config = ModelConfig(
    PriceEstimator(Quote.C, Interval.H1, slice(1,2), Aggregation.AVG),
    PriceTarget.LINEAR_0_10,
    TimingConfig.Builder().at(9).build(),
    ModelDataConfig({Interval.H1: 10}),
    {}
)

manager: ModelManager|None = None

def update(stats: StatContainer, expect: Tensor, output: Tensor, push: bool = True):
    assert manager is not None
    stats.update(expect, output)
    if push:
        frame = HistoryFrame(manager.train_state.epoch)
        frame.stats = {'train': stats.to_dict(), 'val': stats.to_dict()}
        manager.train_state.history.append(frame)
        manager.train_state.epoch += 1

class TestTrainingPlan(unittest.TestCase):

    def setUp(self):
        global manager
        manager = ModelManager(Model(config))
    def tearDown(self) -> None:
        global manager
        if manager:
            manager = None
            gc.collect()
            ModelManager.delete_all(Model)
        
    def test_stat_trigger(self):
        assert manager is not None
        stat_trigger = StatTrigger('val', 'count', (1.5,4), event='enter', once=True)
        stats = StatContainer(CountCollector())

        expect = torch.tensor([1,2,3])
        output = torch.tensor([1,2,4])
        update(stats, expect, output, True)
        self.assertFalse(stat_trigger.check(manager))
        update(stats, expect, output, True)
        self.assertFalse(stat_trigger.check(manager))
        update(stats, expect, output, True)
        self.assertTrue(stat_trigger.check(manager))
        update(stats, expect, output, True)
        self.assertFalse(stat_trigger.check(manager))
    
    def test_or_triger(self):
        assert manager is not None
        or_trigger = EpochTrigger(2, once=True) | StatTrigger('val', 'count', (1.9, float('+inf')), once=True)
        stats = StatContainer(CountCollector())
        
        expect = torch.tensor([1,2,3])
        output = torch.tensor([1,2,4])
        update(stats, expect, output, True)
        self.assertTrue(or_trigger.check(manager))
        update(stats, expect, output, True)
        self.assertFalse(or_trigger.check(manager))
        update(stats, expect, output, True)
        self.assertTrue(or_trigger.check(manager))
        update(stats, expect, output, True)
        self.assertFalse(or_trigger.check(manager))

