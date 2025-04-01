#1
from __future__ import annotations
from typing import Callable, Mapping, Sequence, Any, overload
import torch
import logging
import functools
from torch import Tensor

logger = logging.getLogger(__name__)

def check_tensors(tensors: Sequence[Tensor] | tuple[Tensor] | Mapping[Any, Tensor], allow_zeros=True):
    if isinstance(tensors, (Sequence, tuple)):
        for tensor in tensors: check_tensor(tensor)
    elif isinstance(tensors, Mapping):
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

def shuffle(tensor: Tensor, dim: int = 0) -> Tensor:
    dims = len(tensor.shape)
    indices = torch.randperm(tensor.shape[dim])
    index = tuple(indices if it == dim else slice(None) for it in range(dims))
    return tensor[index]

def __get_sampled(tensor: Tensor, count: int) -> Tensor:
    indices = torch.where(tensor)[0] #indices of True values
    indices = shuffle(indices, 0)[:count] #select random count indices
    result = torch.full(tensor.shape, False, dtype=torch.bool)
    result[indices] = True
    return result

def get_sampled(
    tensor: Tensor,
    bins: Sequence[tuple[float|tuple[float,float]|Callable[[Tensor], Tensor], float]]
) -> Tensor:
    """
    Sample the given tensor so that the ratio of the number of values within the given bins corresponds to the ratios argument.
    Args:
        tensor: The one dimensional tensor to sample from.
        bins: The bins to categorize the values into. Expected to be disjunctive. (Not checked)
            The second element of the tuple gives the expected ratio in the output set for the bin.
    Returns:
        A tensor of booleans for the selected entries.
    """
    ratios = [it[1]/sum(it[1] for it in bins) for it in bins]
    selected = [
        it[0](tensor) if callable(it[0])
        else ((tensor > it[0][0]) & (tensor <= it[0][1])) if isinstance(it[0], tuple)
        else tensor == it[0]
        for it in bins
    ]
    counts = [it.sum().item() for it in selected]
    max_counts = [count/ratio for count,ratio in zip(counts, ratios)]
    total_count = min(max_counts)
    counts = [int(ratio*total_count) for ratio in ratios]
    selected = [__get_sampled(selection, count) for selection, count in zip(selected, counts)]

    return functools.reduce(lambda x,y: torch.logical_or(x,y), selected, torch.full(tensor.shape, False, dtype=torch.bool))
