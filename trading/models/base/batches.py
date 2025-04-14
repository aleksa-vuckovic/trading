#1
from __future__ import annotations
from typing import Iterable, Iterator, Self
import re
import os
import math
import logging
from pathlib import Path
from torch import Tensor
import torch

from trading.core.securities import Exchange

logger = logging.getLogger(__name__)

class BatchFile:
    PATTERN = re.compile(r"([a-zA-Z]+)_(\d+)_(\d+).pt")
    def __init__(self, path: Path):
        match = BatchFile.PATTERN.fullmatch(path.name)
        if not match: raise Exception(f"File {path.name} does not match the batch file pattern.")
        self.path = path
        self.exchange = Exchange.for_mic(match.group(1))
        self.unix_time = int(match.group(2))
        self.entry = int(match.group(3))
    @staticmethod
    def get(folder: Path, time: float, entry: int, exchange: Exchange) -> BatchFile:
        return BatchFile(folder / f"{exchange.mic}_{int(time)}_{entry}.pt")
    @staticmethod
    def load(root: Path) -> list[BatchFile]:
        return sorted([BatchFile(root/it) for it in os.listdir(root) if it.endswith('.pt')], key=lambda it: (it.unix_time, it.entry))

class Batches(Iterable[dict[str, Tensor]]):
    def __init__(self, files: list[BatchFile], merge: int = 1, device: torch.device = torch.device("cpu"), dtype = torch.float32):
        self.files = files
        self.merge = merge
        self.device = device
        self.dtype = dtype

    def to(self, device: torch.device|None = None, dtype: torch.dtype|None = None) -> Self:
        if device: self.device = device
        if dtype: self.dtype = dtype
        return self
    
    def __len__(self):
        return math.ceil(len(self.files)/self.merge)

    class Iterator(Iterator[dict[str, Tensor]]):
        batches: Batches
        def __init__(self, batches):
            self.batches = batches
            self.i = 0
        def __next__(self) -> dict[str, Tensor]:
            if self.i >= len(self.batches.files):
                raise StopIteration()
            files = self.batches.files[self.i:self.i+self.batches.merge]
            data: list[dict[str, Tensor]] = [torch.load(it.path, weights_only=True) for it in files]
            merged_data = {key:torch.cat([it[key] for it in data], dim=0).to(device=self.batches.device, dtype=self.batches.dtype) for key in data[0].keys()}
            shapes = {key:merged_data[key].shape for key in merged_data}
            logger.debug(f"Loaded batch with shape {shapes}")
            self.i += len(files)
            return merged_data

    def __iter__(self):
        return Batches.Iterator(self)
