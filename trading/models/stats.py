import torch
from torch import Tensor
from typing import Callable

class StatCollector:
    def __init__(self, name: str):
        self.name = name
        self.clear()
    
    def update(self, expect: Tensor, output: Tensor) -> Tensor | float | int:
        result = self._calculate(expect, output)
        if isinstance(result, (int, float)): self.__update(result)
        else: self.__update(float(result.item()))
        return result

    def _calculate(self, expect: Tensor, output: Tensor) -> Tensor | float | int:
        pass

    def __update(self, value: float | int):
        self.last = value
        self.count += 1
        self.total += value
        self.running = self.total / self.count
    
    def clear(self):
        self.last = None
        self.count = 0
        self.total = 0
        self.running = 0

    def to_dict(self) -> dict:
        return {'last': self.last, 'count': self.count, 'running': self.running}
    
    def __str__(self):
        return f"{self.name}={self.running:.4f}({self.last:.2f})"

class StatContainer:
    stats: dict[str, StatCollector]
    primary: str
    def __init__(self, *args, name: str | None = None):
        for arg in args:
            if not isinstance(arg, StatCollector):
                raise Exception(f'Unexpected arg type {type(arg)}')
        self.stats = {it.name:it for it in args}
        self.primary = args[0].name
        self.name = name

    def update(self, expect: Tensor, output: Tensor) -> Tensor | float | int | None:
        result = {key:self.stats[key].update(expect, output) for key in self.stats}
        return result[self.primary]
    
    def clear(self):
        [it.clear() for it in self.stats.values()]

    def __getitem__(self, key):
        return self.stats[key].running
    
    def __contains__(self, key):
        return key in self.stats
    
    def __str__(self):
        return ','.join([str(it) for it in self.stats.values()])
    
    def to_dict(self):
        return {key: self.stats[key].running for key in self.stats}
    
class Accuracy(StatCollector):
    """
    Ovrlapping truths, divided by total output truths.
    """
    def __init__(self,
        name: str,
        to_bool_output: Callable[[Tensor], Tensor],
        to_bool_expect: Callable[[Tensor], Tensor]|None=None
    ):
        super().__init__(name)
        self.to_bool_output = to_bool_output
        self.to_bool_expect = to_bool_expect or to_bool_output
    
    def _calculate(self, expect, output):
        output = self.to_bool_output(output)
        expect = self.to_bool_expect(expect)
        hits = torch.logical_and(output, expect).sum().item()
        output_n = output.sum().item()
        expect_n = expect.sum().item()
        return hits / output_n if output_n else 1
    
class Precision(StatCollector):
    """
    Ovrlapping truths, divided by total expect truths.
    """
    def __init__(self,    
        name: str,
        to_bool_output: Callable[[Tensor], Tensor],
        to_bool_expect: Callable[[Tensor], Tensor]|None=None
    ):
        super().__init__(name)
        self.to_bool_output = to_bool_output
        self.to_bool_expect = to_bool_expect or to_bool_output

    def _calculate(self, expect, output):
        output = self.to_bool_output(output)
        expect = self.to_bool_expect(expect)
        hits = torch.logical_and(output, expect).sum().item()
        expect_n = expect.sum().item()
        return hits / expect_n if expect_n else 1