import torch
import math
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

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
            result = torch.cat([torch.load(it, weights_only=True) for it in files], dim=0).to(device = self.batches.device, dtype=self.batches.dtype)
            self.i += len(files)
            logger.info(f"Loaded batch with shape {result.shape}")
            return result

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