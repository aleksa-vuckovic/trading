import torch
import math
import logging
import re
import os
from pathlib import Path
from enum import Enum
from pathlib import Path
from matplotlib import pyplot as plt
from datetime import timedelta
from ..utils import dateutils

logger = logging.getLogger(__name__)

def get_batch_files(path: Path) -> list[dict]:
    pattern = re.compile(r"([^_]+)_batch(\d+)-(\d+).pt")
    files = [ pattern.fullmatch(it) for it in os.listdir(path)]
    files = [ {'file': path / it.group(0), 'source': it.group(1), 'batch': int(it.group(2)), 'hour': int(it.group(3))} for it in files if it ]
    return sorted(files, key=lambda it: (it['source'], it['hour'], it['batch']))

def check_tensors(tensors: list[torch.Tensor] | dict[object, torch.Tensor], allow_zeros=True):
    if isinstance(tensors, list):
        for tensor in tensors: check_tensor(tensor)
    elif isinstance(tensors, dict):
        for tensor in tensors.values(): check_tensor(tensor)
def check_tensor(tensor: torch.Tensor, allow_zeros=True):
    result = tensor.isnan() | tensor.isinf()
    if not allow_zeros: result = result | (tensor == 0)
    bad_entries = result.sum().item()
    if bad_entries > 0:
        raise Exception(f"Found {bad_entries} unwanted inf, nan {'or 0 ' if allow_zeros else ''} values in tensors.")

def get_next_time(unix_time: float, hour: int | None = None) -> float:
    time = dateutils.unix_to_datetime(unix_time, tz = dateutils.ET)
    if time.minute or time.second or time.microsecond:
        time = time.replace(minute=0, second=0, microsecond=0)
        time = time + timedelta(hours = 1)
    if dateutils.is_weekend_datetime(time) or time.hour >= (hour or 16):
        time = time.replace(hour = hour or 9)
        time += timedelta(days=1)
        while dateutils.is_weekend_datetime(time):
            time += timedelta(days=1)
    elif time.hour < (hour or 9):
        time = time.replace(hour = hour or 9)
    else:
        time = time.replace(hour = time.hour + 1)
    return time.timestamp()

def get_prev_time(unix_time: float, hour: int | None = None) -> float:
    time = dateutils.unix_to_datetime(unix_time, tz = dateutils.ET)
    if time.minute or time.second or time.microsecond:
        time = time.replace(minute = 0, second = 0, microsecond = 0)
        time = time + timedelta(hours = 1)
    if dateutils.is_weekend_datetime(time) or time.hour <= (hour or 9):
        time = time.replace(hour = hour or 16)
        time -= timedelta(days=1)
        while dateutils.is_weekend_datetime(time):
            time -= timedelta(days = 1)
    elif time.hour > (hour or 16):
        time = time.replace(hour = hour or 16)
    else:
        time = time.replace(hour = time.hour - 1)
    return time.timestamp()

def relativize_in_place(tensor: torch.Tensor, start_index: int = 0, count: int = -1, dim: int = 0, use_previous: bool = False):
    """
    Process a time series by calculating relative difference between adjacent entries.
    The first entry is considered to have a 0% change, unless use_previous is set to True,
        in which case the entry before start_index is used.
    """
    total_dims = len(tensor.shape)
    if dim >= total_dims:
        raise Exception(f"Dimension {dim} not valid for shape {tensor.shape}.")
    i = start_index
    j = start_index+count if count>0 else tensor.shape[dim]
    if tensor.shape[dim] < j or j <= i:
        raise Exception(f"Slice {i}:{j} not valid for dimension {dim} of shape {tensor.shape}.")
    index = tuple()
    stepback_index = tuple()
    for it in range(total_dims):
        if dim == it:
            index += (slice(i,j),)
            stepback_index += (slice(i-1 if use_previous else i,j-1),)
        else:
            index += (slice(None),)
            stepback_index += (slice(None),)
    current = tensor[index]
    stepback = tensor[stepback_index]
    if not use_previous:
        fill_index = list(stepback_index)
        fill_index[dim] = slice(0,1)
        stepback = torch.cat([stepback[tuple(fill_index)], stepback], dim=dim)
    tensor[index] = (current - stepback) / stepback

def normalize_in_place(tensor: torch.Tensor, start_index: int = 0, count: int = -1, dim: int = 0) -> torch.Tensor:
    """
    Divides elements from start_index by the value of the largest element.
    For each batch separately.
    Returns the array of values used the normalize each batch, of shape (batches,)
    """
    total_dims = len(tensor.shape)
    if dim >= total_dims:
        raise Exception(f"Dimension {dim} not valid for shape {tensor.shape}.")
    i = start_index
    j = start_index+count if count>=0 else tensor.shape[dim]
    if tensor.shape[dim] < j:
        raise Exception(f"End index {j} not valid form dimension {dim} of shape {tensor.shape}.")
    index = tuple()
    for it in range(total_dims):
        if dim == it:
            index += (slice(i,j),)
        else:
            index += (slice(None),)
    maxes, indices = torch.max(tensor[index], dim=dim, keepdim=True)
    tensor[index] = tensor[index] / maxes
    return maxes

class PriceTarget(Enum):
    LINEAR_0_5 = 'Linear 0 to 5%'
    LINEAR_0_10 = 'Linear 0 to 10%'
    LINEAR_5_5 = 'Linear -5 to 5%'
    LINEAR_10_10 = 'Linear -10 to 10%'
    SIGMOID_0_5 = 'Sigmoid 0 to 5%'
    SIGMOID_0_10 = 'Sigmoid 0 to 10%'
    TANH_5_5 = 'Tanh -5 to 5%'
    TANH_10_10 = 'Tanh -10 to 10%'

    def get_price(self, normalized_values: torch.Tensor):
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

class Batches:
    def __init__(self, files: list[str | Path], merge: int = 1, device: str = "cpu", dtype = torch.float32):
        self.files = files
        self.merge = merge
        self.device = device
        self.dtype = dtype
    
    def __len__(self):
        return math.ceil(len(self.files)/self.merge)

    class Iterator:
        def __init__(self, batches):
            self.batches = batches
            self.i = 0
        def __next__(self):
            if self.i >= len(self.batches.files):
                raise StopIteration()
            files = self.batches.files[self.i:self.i+self.batches.merge]
            data = [torch.load(it, weights_only=True) for it in files]
            if isinstance(data[0], dict):
                data = {key:torch.cat([it[key] for it in data], dim=0).to(device=self.batches.device, dtype=self.batches.dtype) for key in data[0].keys()}
                shapes = {key:data[key].shape for key in data.keys()}
                logger.info(f"Loaded batch with shape {shapes}")
            else:
                data = torch.cat([torch.load(it, weights_only=True) for it in files], dim=0).to(device = self.batches.device, dtype=self.batches.dtype)
                logger.info(f"Loaded batch with shape {data.shape}")
            self.i += len(files)
            return data

    def __iter__(self):
        return Batches.Iterator(self)

class StatCollector:
    def __init__(self, name: str):
        self.name = name
        self.clear()
    
    def update(self, expect: torch.Tensor, output: torch.Tensor) -> torch.Tensor | float | int:
        result = self._calculate(expect, output)
        if isinstance(result, (int, float)): self.__update(result)
        else: self.__update(float(result.item()))
        return result

    def _calculate(self, expect: torch.Tensor, output: torch.Tensor) -> torch.Tensor | float | int:
        pass

    def __update(self, value: float | int):
        self.last = value
        self.count += 1
        self.total += value
        self.running = self.total / self.count
    
    def clear(self):
        self.last = 0
        self.count = 0
        self.total = 0
        self.running = 0

    def to_dict(self) -> dict:
        return {'running': self.running, 'last': self.last}
    
    def __str__(self):
        return f"{self.name}={self.running:.3f}({self.last:.2f})"

class StatContainer:
    stats: list[StatCollector]
    def __init__(self, *args, name: str | None = None):
        for arg in args:
            if not isinstance(arg, StatCollector):
                raise Exception(f'Unexpected arg type {type(arg)}')
        self.stats = list(args)
        self.name = name

    def update(self, expect: torch.Tensor, output: torch.Tensor) -> torch.Tensor | float | int | None:
        result = [it.update(expect, output) for it in self.stats]
        return result[0] if result else None
    
    def clear(self):
        [it.clear() for it in self.stats]
    
    def __str__(self):
        return ','.join([str(it) for it in self.stats])
    
    def to_dict(self):
        result = {it.name: it.to_dict() for it in self.stats}
        return {self.name: result} if self.name else result