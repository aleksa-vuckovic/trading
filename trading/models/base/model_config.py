#1
from __future__ import annotations
from typing import overload, Iterable, Iterator
import logging
import torch
from torch import Tensor
from pathlib import Path
from enum import Enum, auto
from matplotlib import pyplot as plt
from base.serialization import serializable, Serializable, serializer
from base.classes import equatable
from trading.core import Interval
from trading.core.work_calendar import TimingConfig
from trading.core.securities import Security
from trading.providers.aggregate import AggregateProvider

logger = logging.getLogger(__name__)

class Quote(Enum):
    O = 0
    H = 1
    L = 2
    C = 3
    V = 4
    
AFTER = "AFTER"

class Aggregation(Enum):
    FIRST = auto()
    LAST = auto()
    AVG = auto()
    MAX = auto()
    MIN = auto()
    @overload
    def apply(self, data: Tensor, dim:int=...) -> Tensor: ...
    @overload
    def apply(self, data: list, dim:int=...) -> list|float: ...
    def apply(self, data: Tensor|list, dim:int=-1) -> Tensor|list|float:
        if isinstance(data, list): return self.apply(torch.tensor(data, dtype=torch.float64), dim=dim).tolist()
        dims = len(data.shape)
        dim %= dims
        if self==Aggregation.FIRST: return data[tuple(slice(None,None) if it!=dim else 0 for it in range(dims))]
        if self==Aggregation.LAST: return data[tuple(slice(None,None) if it!=dim else -1 for it in range(dims))]
        if self==Aggregation.AVG: return data.mean(dim=dim)
        if self==Aggregation.MAX: return data.max(dim=dim).values
        if self==Aggregation.MIN: return data.min(dim=dim).values
        raise Exception(f"Unknown aggregation {self}")

@serializable()
@equatable()
class PriceEstimator(Serializable):
    """
    Estimates the sell price within a timeframe,
    in a way defined by the parameters.

    Args:
        quote: The quote used in the prediction.
        interval: The interval used in the prediction.
        index: The slice within a timeframe used to predict the price.
        agg: The aggregation applied to the prices within the slice.
        max_fill_ratio: The value is passed directly over to PricingProvider.get_pricing. 
    """
    def __init__(
        self,
        quote: Quote,
        interval: Interval,
        index: slice,
        agg: Aggregation,
        max_fill_ratio: float = 1
    ):
        self.quote = quote
        self.interval = interval
        self.index = index
        self.agg = agg
        self.max_fill_ratio = max_fill_ratio

    def estimate_tensor(self, tensor: Tensor) -> Tensor:
        dims = len(tensor.shape)
        index = tuple(slice(None,None) if it < dims-2 else self.index if it < dims - 1 else self.quote.value for it in range(dims))
        return self.agg.apply(tensor[index])
    
    def estimate_example(self, example: dict[str, Tensor]) -> Tensor:
        key = f"{AFTER}_{self.interval.name}"
        if key not in example: raise Exception(f"Can't estimate without {key}.")
        return self.estimate_tensor(example[key])

    def estimate(self, security: Security, unix_time: float) -> float:
        end_time = security.exchange.calendar.add_intervals(unix_time, self.interval, self.index.stop)
        prices = AggregateProvider.instance.get_pricing(unix_time, end_time, security, self.interval, interpolate=True, max_fill_ratio=self.max_fill_ratio)
        tensor = torch.tensor([it[self.quote.name] for it in prices], dtype=torch.float64)
        return float(self.agg.apply(tensor, dim=-1).item())

class PriceTarget(Enum):
    LINEAR_0_5 = 'Linear 0 to 5%'
    LINEAR_0_10 = 'Linear 0 to 10%'
    LINEAR_5_5 = 'Linear -5 to 5%'
    LINEAR_10_10 = 'Linear -10 to 10%'
    SIGMOID_0_5 = 'Sigmoid 0 to 5%'
    SIGMOID_0_10 = 'Sigmoid 0 to 10%'
    TANH_5_5 = 'Tanh -5 to 5%'
    TANH_10_10 = 'Tanh -10 to 10%'

    def get_price(self, normalized_values: Tensor):
        x = normalized_values
        if self == PriceTarget.LINEAR_0_5:
            return torch.clamp(x, min=0, max=0.05)
        if self == PriceTarget.LINEAR_0_10:
            return torch.clamp(x, min=0, max=0.1)
        if self == PriceTarget.LINEAR_5_5:
            return torch.clamp(x, min=-0.05, max=0.05)
        if self == PriceTarget.LINEAR_10_10:
            return torch.clamp(x, min=-0.1, max=0.1)
        if self == PriceTarget.SIGMOID_0_5:
            x = torch.clamp(x, min=-0.2, max=0.2)
            x = torch.exp(300*x-6)
            return x/(1+x)
        if self == PriceTarget.SIGMOID_0_10:
            x = torch.exp(150*x-7.5)
            return x/(1+x)
        if self == PriceTarget.TANH_5_5:
            x = torch.exp(-150*x)
            return (1-x)/(1+x)
        if self == PriceTarget.TANH_10_10:
            x = torch.exp(-60*x)
            return (1-x)/(1+x)
        raise Exception("Unknown price target type")
    
    def get_layer(self):
        if self in [PriceTarget.LINEAR_0_10, PriceTarget.LINEAR_0_5, PriceTarget.LINEAR_10_10, PriceTarget.LINEAR_5_5]:
            return torch.nn.Identity()
        if self in [PriceTarget.SIGMOID_0_10, PriceTarget.SIGMOID_0_5]:
            return torch.nn.Sigmoid()
        if self in [PriceTarget.TANH_10_10, PriceTarget.TANH_5_5]:
            return torch.nn.Tanh()
        raise Exception(f"Unknown PriceTarget {self}.")
    
    @staticmethod
    def plot():
        x = torch.linspace(-0.15, 0.15, 100, dtype=torch.float32)
        for i, pt in enumerate(PriceTarget):
            fig = plt.figure(i // 4)
            fig.suptitle(f'Window {i//4}')
            axes = fig.add_subplot(2,2,i%4 + 1)
            axes.plot(x, pt.get_price(x), label=pt.name)
            axes.set_title(pt.name)
            axes.grid(True)

        [plt.figure(it).tight_layout() for it in plt.get_fignums()]
        plt.show()

@equatable()
class DataConfig(Serializable):
    def __init__(self, counts: dict[Interval, int]):
        self.counts = counts

    @property
    def intervals(self) -> Iterable[Interval]:
        return sorted(self.counts.keys(), reverse=True)
    @property
    def min_interval(self) -> Interval:
        return sorted(self.intervals)[-1]
    @property
    def max_interval(self) -> Interval:
        return sorted(self.intervals)[0]
    @property
    def min_interval_count(self) -> int:
        return self.counts[self.min_interval]
    @property
    def max_interval_count(self) -> int:
        return self.counts[self.max_interval]

    class Iterator(Iterator[tuple[Interval, int]]):
        def __init__(self, data_config: DataConfig):
            self.data_config = data_config
            self.intervals = list(data_config.intervals)
            self.i = 0
        def __next__(self) -> tuple[Interval, int]:
            if self.i >= len(self.intervals):
                raise StopIteration()
            self.i += 1
            return self.intervals[self.i-1], self.data_config.counts[self.intervals[self.i-1]]

    def __iter__(self) -> Iterator[tuple[Interval, int]]:
        return DataConfig.Iterator(self)
    
    def __len__(self):
        return len(self.counts)
    
    def __getitem__(self, key: Interval|str) -> int:
        if isinstance(key, Interval):
            return self.counts[key]
        if isinstance(key, str) and any(it for it in Interval if it.name == key):
            return self.counts[Interval[key]]
        raise IndexError(f"Key {key} does not exist in this DataConfig.")
    
    def __contains__(self, key: Interval|str):
        if isinstance(key, Interval):
            return key in self.counts
        if isinstance(key, str) and any(it for it in Interval if it.name == key):
            return Interval[key] in self.counts
        return False
    
    def to_dict(self) -> dict: return {'counts': self.counts}
    @staticmethod
    def from_dict(data: dict) -> DataConfig:
        return DataConfig({Interval[key]: data[key] for key in data['counts']})

@serializable(skip_keys=['examples_folder'])
@equatable(skip_keys=['examples_folder'])
class ModelConfig(Serializable):
    def __init__(
        self, 
        estimator: PriceEstimator,
        target:  PriceTarget,
        timing: TimingConfig,
        data_config: DataConfig,
        examples_folder: Path,
        other: dict = {}
    ):
        self.estimator = estimator
        self.target = target
        self.timing = timing
        self.data_config = data_config
        self.examples_folder = examples_folder
        self.other = other
    
    def __str__(self) -> str:
        return f"""
estimator = {serializer.serialize(self.estimator, typed=False, indent=2)}
target = {self.target.name}
timing = {serializer.serialize(self.timing, typed=False, indent=2)}
data_config = {serializer.serialize(self.data_config, typed=False, indent=2)}
examples = {self.examples_folder}
other = {self.other}
"""
    