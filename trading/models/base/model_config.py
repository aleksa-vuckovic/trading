#1
from __future__ import annotations
from functools import cached_property
import logging
from matplotlib.axes import Axes
from numpy import ndarray
import torch
from typing import overload, Iterable, override, TypeVar
from torch import Tensor
from enum import Enum, auto
from matplotlib import pyplot as plt
from torch.nn.modules import Module

from base.reflection import transient
from base.serialization import GenericSerializer, Serializable, json_type
from base.types import ReadonlyDict, Equatable
from trading.core import Interval
from trading.core.timing_config import TimingConfig
from trading.core.securities import Exchange, Security
from trading.providers.aggregate import AggregateProvider

logger = logging.getLogger(__name__)

class BarValues(Enum):
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

class PriceOutputTarget(Equatable, Serializable):
    def __init__(self, interval: Interval, value: BarValues, index: slice, agg: Aggregation, modifier: PriceModifier):
        self.interval = interval
        self.value = value
        self.index = index
        self.agg = agg
        self.modifier = modifier

    def estimate(self, example: dict[str, Tensor]) -> Tensor:
        key = f"{AFTER}_{self.interval.name}"
        if key not in example: raise Exception(f"Can't estimate without {key}.")
        tensor = example[key]
        dims = len(tensor.shape)
        index = tuple(slice(None,None) if it < dims-2 else self.index if it < dims - 1 else self.value.value for it in range(dims))
        return self.agg.apply(tensor[index])

    def __repr__(self) -> str:
        return f"""PriceOutputTarget(interval={repr(self.interval)}, value={repr(self.value)}, index={repr(self.index)}, agg={repr(self.agg)}, modifier={repr(self.modifier)})"""

class PriceEstimator(Equatable, Serializable):
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
        value: BarValues,
        interval: Interval,
        index: slice,
        agg: Aggregation,
        max_fill_ratio: float = 1
    ):
        self.value = value
        self.interval = interval
        self.index = index
        self.agg = agg
        self.max_fill_ratio = max_fill_ratio

    def estimate_tensor(self, tensor: Tensor) -> Tensor:
        dims = len(tensor.shape)
        index = tuple(slice(None,None) if it < dims-2 else self.index if it < dims - 1 else self.value.value for it in range(dims))
        return self.agg.apply(tensor[index])
    
    def estimate_example(self, example: dict[str, Tensor]) -> Tensor:
        key = f"{AFTER}_{self.interval.name}"
        if key not in example: raise Exception(f"Can't estimate without {key}.")
        return self.estimate_tensor(example[key])

    def estimate(self, security: Security, unix_time: float) -> float:
        end_time = security.exchange.calendar.add_intervals(unix_time, self.interval, self.index.stop)
        prices = AggregateProvider.instance.get_pricing(unix_time, end_time, security, self.interval, interpolate=True, max_fill_ratio=self.max_fill_ratio)
        tensor = torch.tensor([it[self.value.name] for it in prices], dtype=torch.float64)
        return float(self.agg.apply(tensor, dim=-1).item())

class PriceModifier(Equatable, Serializable):
    """
    Modifies price change percentages to values that should be used as model outputs.
    """
    #abstract
    def _modify(self, values: Tensor) -> Tensor: ...
    def _revert(self, values: Tensor) -> Tensor: ...
    def layer(self) -> torch.nn.Module: ...
    #endregion

    def modify[T: (Tensor, float, list[float])](self, values: T) -> T:
        if isinstance(values, Tensor): return self._modify(values)
        if isinstance(values, float): return self._modify(torch.tensor(values, dtype=torch.float64)).item()
        if isinstance(values, list): return self._modify(torch.tensor(values, dtype=torch.float64)).tolist()
        raise Exception(f"Unsupported arg type {type(values)}.")
    def revert[T: (Tensor, float, list[float])](self, values: T) -> T:
        if isinstance(values, Tensor): return self._revert(values)
        if isinstance(values, float): return self._revert(torch.tensor(values, dtype=torch.float64)).item()
        if isinstance(values, list): return self._revert(torch.tensor(values, dtype=torch.float64)).tolist()
        raise Exception(f"Unsupported arg type {type(values)}.")
    
    def plot(self):
        fig = plt.figure(figsize=(10,6))
        fig.suptitle(repr(self))
        axes: list[Axes] = fig.subplots(1, 2)

        axes[0].set_title("Modify")
        x = torch.linspace(-0.5, 0.5, 200, dtype=torch.float32)
        axes[0].plot(x, self.modify(x), label="modified")
        axes[0].grid(True)

        axes[1].set_title("Revert")
        axes[1].plot(self.modify(x), self.revert(self.modify(x)), label="reverted")
        axes[1].grid(True)

        [plt.figure(it).tight_layout() for it in plt.get_fignums()]
        plt.show()
    
class LinearPriceModifier(PriceModifier):
    def __init__(self, lower: float, upper: float):
        self.lower = lower
        self.upper = upper
    @override
    def _modify(self, values: Tensor) -> Tensor:
        return torch.clamp(values, min=self.lower, max=self.upper)
    @override
    def _revert(self, values: Tensor) -> Tensor:
        return values
    @override
    def layer(self) -> Module: return torch.nn.Identity()
    @override
    def __repr__(self) -> str: return f"{type(self).__name__}({self.lower}, {self.upper})"
    
class SigmoidPriceModifier(PriceModifier):
    """
    output = m*sigmoid(ax+b)+n
    input = 1/a*sigrev((x-n)/m)-b
    """
    def __init__(self, price_lower: float, price_upper: float, tanh: bool = False):
        self.price_lower = price_lower
        self.price_upper = price_upper
        self.tanh = tanh

    @property
    def range(self) -> float: return 8
    @cached_property
    def a(self) -> float: return self.range/(self.price_upper-self.price_lower)
    @cached_property
    def b(self) -> float: return -self.range*self.price_lower/(self.price_upper-self.price_lower)-self.range/2
    @cached_property
    def m(self) -> float: return 2 if self.tanh else 1
    @cached_property
    def n(self) -> float: return -1 if self.tanh else 0
    
    @override
    def _modify(self, values: Tensor) -> Tensor: return self.m*torch.sigmoid(self.a*values+self.b)+self.n
    @override
    def _revert(self, values: Tensor) -> Tensor: return (torch.logit((values-self.n)/self.m)-self.b)/self.a
    @override
    def layer(self) -> Module: return torch.nn.Tanh() if self.tanh else torch.nn.Sigmoid()
    @override
    def __repr__(self) -> str: return f"{type(self).__name__}({self.price_lower}, {self.price_upper}, {self.tanh})"

@transient('intervals', 'min_interval', 'max_interval', 'min_interval_count', 'max_interval_count')
class PricingDataConfig(Equatable, Serializable):
    def __init__(self, counts: dict[Interval, int]):
        self.counts = ReadonlyDict(counts)

    @cached_property
    def intervals(self) -> Iterable[Interval]: return sorted(self.counts.keys(), reverse=True)
    @cached_property
    def min_interval(self) -> Interval: return sorted(self.intervals)[-1]
    @cached_property
    def max_interval(self) -> Interval: return sorted(self.intervals)[0]
    @cached_property
    def min_interval_count(self) -> int: return self.counts[self.min_interval]
    @cached_property
    def max_interval_count(self) -> int: return self.counts[self.max_interval]

    def __len__(self):
        return len(self.counts)
    
    def __getitem__(self, key: Interval|str) -> int:
        if isinstance(key, Interval):
            return self.counts[key]
        if isinstance(key, str) and any(it for it in Interval if it.name == key):
            return self.counts[Interval[key]]
        raise IndexError(f"Key {key} does not exist in this DataConfig.")


serializer = GenericSerializer(typed=False)
class BaseModelConfig(Equatable, Serializable):
    def __init__(
        self,
        exchanges: tuple[Exchange],
        pricing_data_config: PricingDataConfig,
        price_output_target: PriceOutputTarget,
        timing: TimingConfig
    ):
        self.exchanges = exchanges
        self.pricing_data_config = pricing_data_config
        self.price_output_target = price_output_target
        self.timing = timing
    
    def __repr__(self) -> str:
        return f"""BaseModelConfig(
    exchanges = {repr(self.exchanges)},
    pricing_data_config = {serializer.serialize(self.pricing_data_config, indent=2)},
    price_output_target = {repr(self.price_output_target)},
    timing = {serializer.serialize(self.timing, indent=2)}
)
"""
