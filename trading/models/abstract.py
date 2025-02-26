from __future__ import annotations
import logging
import json
import torch
import config
from torch import Tensor
from pathlib import Path
from tqdm import tqdm
from enum import Enum
from typing import Callable
from ..data import nasdaq, aggregate
from ..utils import dateutils, jsonutils
from ..utils.dateutils import TimingConfig
from ..utils.common import Interval, equatable
from ..utils.jsonutils import serializable
from .utils import PriceTarget

logger = logging.getLogger(__name__)

OPEN_I = 0
HIGH_I = 1
LOW_I = 2
CLOSE_I = 3
VOLUME_I = 4

QUOTES = ['open', 'high', 'low', 'close', 'volume']
QUOTE_I = {it[0]:i for i,it in enumerate(QUOTES)}

AFTER_KEY_PREFIX = "OUTPUT"

class Aggregation(Enum):
    FIRST = 'first'
    LAST = 'last'
    AVG = 'avg'
    MAX = 'max'
    MIN = 'min'
    def apply_tensor(self, tensor: Tensor, dim:int=-1) -> Tensor:
        dims = len(tensor.shape)
        while dim<0: dim+=dims
        if self==Aggregation.FIRST: return tensor[tuple(slice(None,None) if it!=dim else 0 for it in range(dims))]
        if self==Aggregation.LAST: return tensor[tuple(slice(None,None) if it!=dim else -1 for it in range(dims))]
        if self==Aggregation.AVG: return tensor.mean(dim=dim)
        if self==Aggregation.MAX: return tensor.max(dim=dim)
        if self==Aggregation.MIN: return tensor.min(dim=dim)
        raise Exception(f"Unknown aggregation {self}")
    def apply_list(self, data: list, dim:int=-1) -> list|float:
        tensor = torch.tensor(data, dtype=torch.float64)
        return self.apply_tensor(tensor, dim=dim).tolist()

@serializable()
@equatable()
class PriceEstimator:
    def __init__(
        self,
        quote: str,
        interval: Interval,
        index: slice,
        agg: Aggregation,
        max_fill_ratio: float = 1
    ):
        self.quote = quote
        self.quote_index = QUOTE_I[quote[0].lower()]
        self.interval = interval
        self.index = index
        self.agg = agg
        self.max_fill_ratio = max_fill_ratio

    def estimate_tensor(self, tensor: Tensor) -> Tensor:
        dims = len(tensor.shape)
        index = tuple(slice(None,None) if it < dims-2 else self.index if it < dims - 1 else self.quote_index for it in range(dims))
        return self.agg.apply_tensor(tensor[index])
    
    def estimate_example(self, example: dict[str, Tensor]) -> Tensor:
        key = f"{AFTER_KEY_PREFIX}_{self.interval.name}"
        if key not in example: raise Exception(f"Can't estimate without {key}.")
        return self.estimate_tensor(example[key])

    def estimate(self, ticker: nasdaq.NasdaqListedEntry, unix_time: float, tz=dateutils.ET) -> float:
        end_time = dateutils.add_intervals_unix(unix_time, self.interval, self.index.stop, tz=tz)
        prices, = aggregate.get_interpolated_pricing(ticker, unix_time, end_time, self.interval, return_quotes=[self.quote], max_fill_ratio=self.max_fill_ratio)
        tensor = torch.tensor(prices, dtype=torch.float64)
        self.agg.apply_tensor(tensor, dim=-1).item()


@equatable()
class DataConfig:
    def __init__(self, counts: dict[Interval, int]):
        self.counts = counts
        self.max_interval = sorted(counts.keys())[-1]
        self.min_interval = sorted(counts.keys())[0]
        self.max_interval_count = counts[self.max_interval]
        self.min_Interval_count = counts[self.min_interval]

    def to_dict(self) -> dict:
        return {interval.name:count for interval, count in self.counts.items()}
    
    class Iterator:
        def __init__(self, data_config: DataConfig):
            self.data_config = data_config
            self.intervals = list(data_config.counts.keys())
            self.i = 0
        def __next__(self) -> tuple[Interval, int]:
            if self.i >= len(self.intervals):
                raise StopIteration()
            self.i += 1
            return self.intervals[self.i-1], self.data_config.counts[self.intervals[self.i-1]]

    def __iter__(self):
        return DataConfig.Iterator(self)
    
    def __len__(self):
        return len(self.counts)
    
    def __getitem__(self, key: Interval|str):
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
    
    def intervals(self):
        return self.counts.keys()

    @staticmethod
    def from_dict(data: dict) -> DataConfig:
        data_points = {Interval[interval]:count for interval, count in data.items()}
        return DataConfig(data_points)


@serializable(skip_keys=['examples_folder'])
@equatable(skip_keys=['examples_folder'])
class ModelConfig:
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
estimator = {jsonutils.serialize(self.estimator, typed=False, indent=2)}
target = {self.target.name}
output = {self.output.name}
timing = {jsonutils.serialize(self.timing, typed=False, indent=2)}
inputs = {jsonutils.serialize(self.inputs, typed=False, indent=2)}
data = {self.other}
"""

        
class AbstractModel(torch.nn.Module):
    def __init__(self, config: ModelConfig):
        super().__init__()
        self.config = config
    def extract_tensors(self, example: dict[str, Tensor], with_output: bool = True) -> tuple[dict[str, Tensor]]|tuple[dict[str,Tensor],Tensor]:
        pass
    def print_summary(self, merge: int = 10):
        pass