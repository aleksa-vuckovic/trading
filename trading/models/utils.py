from __future__ import annotations
import torch
import math
import logging
import re
import os
from torch import Tensor
from pathlib import Path
from enum import Enum
from pathlib import Path
from matplotlib import pyplot as plt
from ..utils.common import get_full_classname

logger = logging.getLogger(__name__)

def get_model_device(model: torch.nn.Module) -> torch.device:
    return next(model.parameters()).device
def get_model_dtype(model: torch.nn.Module) -> torch.dtype:
    return next(model.parameters()).dtype
def get_model_name(model: torch.nn.Module) -> str:
    return get_full_classname(model).split(".")[-3]

def get_batch_files(path: Path) -> list[dict]:
    pattern = re.compile(r"hour(\d+)_time(\d+)_entry(\d+)_batch(\d+).pt")
    files = [ pattern.fullmatch(it) for it in os.listdir(path)]
    files = [ {'path': path / it.group(0), 'hour': int(it.group(1)), 'time': int(it.group(2)), 'entry': int(it.group(3)), 'batch': int(it.group(4))} for it in files if it ]
    return sorted(files, key=lambda it: (it['time'], it['entry'], it['hour']))

def check_tensors(tensors: list[Tensor] | tuple[Tensor] | dict[object, Tensor], allow_zeros=True):
    if isinstance(tensors, (list, tuple)):
        for tensor in tensors: check_tensor(tensor)
    elif isinstance(tensors, dict):
        for tensor in tensors.values(): check_tensor(tensor)
    else: raise ValueError("Expecting list, tuple or dict in check_tensors.")
def check_tensor(tensor: Tensor, allow_zeros=True):
    result = tensor.isnan() | tensor.isinf()
    if not allow_zeros: result = result | (tensor == 0)
    bad_entries = result.sum().item()
    if bad_entries > 0:
        raise Exception(f"Found {bad_entries} unwanted inf, nan {'or 0 ' if not allow_zeros else ''}values in tensors.")

def get_time_relativized(tensor: Tensor, start_index: int = 0, count: int = -1, dim: int = 0, use_previous: bool = False) -> Tensor:
    """
    Process a time series by calculating relative difference between adjacent entries.
    The first entry is considered to have a 0% change, unless use_previous is set to True,
        in which case the entry before start_index is used.
    """
    total_dims = len(tensor.shape)
    if dim >= total_dims: raise Exception(f"Dimension {dim} not valid for shape {tensor.shape}.")
    i = start_index if start_index >= 0 else start_index + tensor.shape[dim]
    j = start_index+count if count>0 else tensor.shape[dim]
    if tensor.shape[dim] < j or j <= i: raise Exception(f"Slice {i}:{j} not valid for dimension {dim} of shape {tensor.shape}.")
    if use_previous and i == 0: raise Exception(f"Can't use previous entry when start index is 0.")
    index = tuple(slice(None) if it != dim else slice(i,j) for it in range(total_dims))
    stepback_index = tuple(slice(None) if it !=dim else slice(i-1 if use_previous else i,j-1) for it in range(total_dims))
    current = tensor[index]
    stepback = tensor[stepback_index]
    if not use_previous:
        fill_index = list(stepback_index)
        fill_index[dim] = slice(0,1)
        stepback = torch.cat([stepback[tuple(fill_index)], stepback], dim=dim)
    return (current - stepback) / stepback

def get_normalized_by_largest(tensor: Tensor, start_index: int = 0, count: int = -1, dim: int = 0) -> Tensor:
    """
    Divides elements from start_index by the value of the largest element.
    For each batch separately.
    """
    total_dims = len(tensor.shape)
    if dim >= total_dims: raise Exception(f"Dimension {dim} not valid for shape {tensor.shape}.")
    i = start_index if start_index >= 0 else start_index + tensor.shape[dim]
    j = start_index+count if count>=0 else tensor.shape[dim]
    if tensor.shape[dim] < j: raise Exception(f"End index {j} not valid form dimension {dim} of shape {tensor.shape}.")
    index = tuple(slice(None) if it != dim else slice(i,j) for it in range(total_dims))
    maxes, indices = torch.max(tensor[index], dim=dim, keepdim=True)
    return tensor[index] / maxes

def get_moving_average(tensor: Tensor, start_index: int = 0, count: int = -1, dim: int = 1, window: int = 10) -> Tensor:
    """
    Get a tensor of moving averages accross the given dimension and with the given window size.
    Returns a tensor of the same shape.
    """
    total_dims = len(tensor.shape)
    if dim >= total_dims: raise Exception(f"Dimension {dim} not valid for shape {tensor.shape}.")
    i = start_index if start_index >= 0 else start_index + tensor.shape[dim]
    j = i+count if count>=0 else tensor.shape[dim]
    if i < 0 or tensor.shape[dim] < j: raise Exception(f"Indices {i},{j} not valid form dimension {dim} of shape {tensor.shape}.")
    if i < window - 1:
        extra = window-1-i
        zero_index = tuple(slice(None) if it != dim else slice(0,1) for it in range(total_dims))
        expand_index = tuple(-1 if it != dim else extra for it in range(total_dims))
        tensor = torch.concat([tensor[zero_index].expand(expand_index), tensor], dim=dim)
        i += extra
        j += extra
    result_shape = tuple(size if dimension != dim else j-i for dimension,size in enumerate(tensor.shape))
    result = torch.zeros(result_shape, dtype=tensor.dtype, device=tensor.device)
    for offset in range(window):
        index = tuple(slice(None) if it!=dim else slice(i-offset,j-offset) for it in range(total_dims))
        result += tensor[index]
    return result/window
    

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

    def to(self, device: str | None = None, dtype: str | None = None):
        if device: self.device = device
        if dtype: self.dtype = dtype
        return self
    
    def __len__(self):
        return math.ceil(len(self.files)/self.merge)

    class Iterator:
        batches: Batches
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
                logger.debug(f"Loaded batch with shape {shapes}")
            else:
                data = torch.cat([torch.load(it, weights_only=True) for it in files], dim=0).to(device = self.batches.device, dtype=self.batches.dtype)
                logger.debug(f"Loaded batch with shape {data.shape}")
            self.i += len(files)
            return data

    def __iter__(self):
        return Batches.Iterator(self)

