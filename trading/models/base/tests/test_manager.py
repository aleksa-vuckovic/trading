import unittest
import torch
from typing import override
from torch import Tensor

from trading.core import Interval
from trading.core.timing_config import BasicTimingConfig
from trading.providers.nasdaq import Nasdaq
from trading.models.base.model_config import Aggregation, BarValues, PricingDataConfig, BaseModelConfig, PriceEstimator, PriceTarget
from trading.models.base.manager import HistoryFrame, ModelManager, StatTrigger, EpochTrigger
from trading.models.base.stats import StatCollector, StatContainer
from trading.models.base.tests.model import Model

class CountCollector(StatCollector):
    def __init__(self):
        super().__init__('count')
        self.i = 0
    @override
    def _calculate(self, expect: torch.Tensor, output: torch.Tensor) -> torch.Tensor:
        self.i += 1
        return torch.tensor(self.i, dtype=torch.float32)

config = BaseModelConfig(
    (Nasdaq.instance,),
    PricingDataConfig({Interval.H1: 10}),
    PriceEstimator(BarValues.C, Interval.H1, slice(1,2), Aggregation.AVG),
    PriceTarget.LINEAR_0_10,
    BasicTimingConfig.Builder().at(9).build()
)

class TestTriggers(unittest.TestCase):
    manager: ModelManager|None
    @override
    def setUp(self):
        self.manager = ModelManager.get(Model, config)
    @override
    def tearDown(self) -> None:
        self.manager = None
        ModelManager.delete_all(Model)

    def update(self, stats: StatContainer, expect: Tensor, output: Tensor, push: bool = True):
        assert self.manager is not None
        stats.update(expect, output)
        if push:
            frame = HistoryFrame(self.manager.train_state.epoch)
            frame.stats = {'train': stats.to_dict(), 'val': stats.to_dict()}
            self.manager.train_state.history.append(frame)
            self.manager.train_state.epoch += 1
        
    def test_stat_trigger(self):
        assert self.manager is not None
        stat_trigger = StatTrigger('val', 'count', (1.5,4), event='enter', once=True)
        stats = StatContainer(CountCollector())

        expect = torch.tensor([1,2,3])
        output = torch.tensor([1,2,4])
        self.update(stats, expect, output, True)
        self.assertFalse(stat_trigger.check(self.manager))
        self.update(stats, expect, output, True)
        self.assertFalse(stat_trigger.check(self.manager))
        self.update(stats, expect, output, True)
        self.assertTrue(stat_trigger.check(self.manager))
        self.update(stats, expect, output, True)
        self.assertFalse(stat_trigger.check(self.manager))
    
    def test_or_triger(self):
        assert self.manager is not None
        or_trigger = EpochTrigger(2, once=True) | StatTrigger('val', 'count', (1.9, float('+inf')), once=True)
        stats = StatContainer(CountCollector())
        
        expect = torch.tensor([1,2,3])
        output = torch.tensor([1,2,4])
        self.update(stats, expect, output, True)
        self.assertTrue(or_trigger.check(self.manager))
        self.update(stats, expect, output, True)
        self.assertFalse(or_trigger.check(self.manager))
        self.update(stats, expect, output, True)
        self.assertTrue(or_trigger.check(self.manager))
        self.update(stats, expect, output, True)
        self.assertFalse(or_trigger.check(self.manager))

class TestManager(unittest.TestCase):
    @override
    def tearDown(self):
        ModelManager.delete_all(Model)
    def test_model_config_storage(self):
        config1 = BaseModelConfig(
            (Nasdaq.instance,),
            PricingDataConfig({Interval.H1: 10, Interval.D1: 100}),
            PriceEstimator(BarValues.C, Interval.H1, slice(1,2), Aggregation.AVG),
            PriceTarget.LINEAR_0_10,
            BasicTimingConfig.Builder().around(11, delta_minute=30).build()
        )
        config2 = BaseModelConfig(
            (Nasdaq.instance,),
            PricingDataConfig({Interval.H1: 12, Interval.D1: 100}),
            PriceEstimator(BarValues.C, Interval.H1, slice(1,2), Aggregation.AVG),
            PriceTarget.TANH_10_10,
            BasicTimingConfig.Builder().around(11, delta_minute=30).build()
        )
        config1_copy = BaseModelConfig(
            (Nasdaq.instance,),
            PricingDataConfig({Interval.H1: 10, Interval.D1: 100}),
            PriceEstimator(BarValues.C, Interval.H1, slice(1,2), Aggregation.AVG),
            PriceTarget.LINEAR_0_10,
            BasicTimingConfig.Builder().around(11, delta_minute=30).build()
        )
        self.assertEqual(0, len(ModelManager.get_all(Model)))
        manager1 = ModelManager.get(Model, config1)
        self.assertEqual(config1, manager1.model.config)
        
        manager2 = ModelManager.get(Model, config2)
        self.assertEqual(config2, manager2.model.config)

        self.assertEqual(2, len(ModelManager.get_all(Model)))
        self.assertIs(manager1, ModelManager.get(Model, config1_copy))