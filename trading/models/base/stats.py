#1
import torch
from torch import Tensor
from typing import Callable, override
from base.serialization import serializable, Serializable

@serializable()
class StatCollector(Serializable):
    def __init__(self, name: str):
        self.name = name
        self.clear()
    
    def update(self, expect: Tensor, output: Tensor) -> Tensor:
        result = self._calculate(expect, output)
        value = float(result.item())
        self.last = value
        self.count += 1
        self.total += value
        self.running = self.total / self.count
        return result

    def _calculate(self, expect: Tensor, output: Tensor) -> Tensor: ...
    
    def clear(self):
        self.last = None
        self.count = 0
        self.total = 0
        self.running = 0
    
    def __str__(self):
        return f"{self.name}={self.running:.4f}({self.last:.2f})"

class StatContainer:
    stats: dict[str, StatCollector]
    primary: str
    def __init__(self, *args: StatCollector, name: str):
        self.stats = {it.name:it for it in args}
        self.primary = args[0].name
        self.name = name

    def update(self, expect: Tensor, output: Tensor) -> Tensor:
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
    
    @override
    def _calculate(self, expect: Tensor, output: Tensor) -> Tensor:
        output = self.to_bool_output(output)
        expect = self.to_bool_expect(expect)
        hits = torch.logical_and(output, expect).sum()
        output_n = output.sum()
        return hits / output_n
    
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

    def _calculate(self, expect, output) -> Tensor:
        output = self.to_bool_output(output)
        expect = self.to_bool_expect(expect)
        hits = torch.logical_and(output, expect).sum()
        expect_n = expect.sum()
        return hits / expect_n
    
class TanhLoss(StatCollector):
    def __init__(self):
        super().__init__('loss')
    @override
    def _calculate(self, expect: Tensor, output: Tensor) -> Tensor:
        eps = 1e-5
        loss = -torch.log(1 + eps - torch.abs(output - expect) / (1+torch.abs(expect)))
        return loss.mean()
    
class SigmoidLoss(StatCollector):
    def __init__(self):
        super().__init__('loss')
    @override
    def _calculate(self, expect: Tensor, output: Tensor) -> Tensor:
        return -torch.log(torch.abs(expect - output)).mean()
    
class LinearLoss(StatCollector):
    def __init__(self):
        super().__init__('loss')
    @override
    def _calculate(self, expect: Tensor, output: Tensor) -> Tensor:
        return torch.nn.functional.mse_loss(expect, output)