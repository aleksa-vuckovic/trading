#2
from functools import cached_property
from itertools import chain
from typing import overload, Literal
import torch
from torch import Tensor
import torchinfo
import config
from trading.core import Interval
from trading.models.base.model_config import BaseModelConfig

class AbstractModel(torch.nn.Module):
    def __init__(self, config: BaseModelConfig):
        super().__init__()
        self.config = config

    #region Abstract
    @overload
    def extract_tensors(self, example: dict[str, Tensor], with_output: Literal[False]=...) -> dict[str,Tensor]: ...
    @overload
    def extract_tensors(self, example: dict[str, Tensor], with_output: Literal[True]) -> tuple[dict[str,Tensor],Tensor]: ...
    def extract_tensors(self, example: dict[str, Tensor], with_output: bool = False) -> dict[str, Tensor]|tuple[dict[str,Tensor],Tensor]:
        raise NotImplementedError()
    def predict(self, example: dict[str, Tensor]) -> Tensor:
        """
        The abstract model takes a dictionary of tensors as input.
        This input can be extracted from a generated example using extract_tensors,
        IF the example includes all the necessary components.
        """
        raise NotImplementedError()
    #endregion

    def forward(self, tensors: dict[str, Tensor]|Tensor, *args: Tensor) -> Tensor:
        if isinstance(tensors, Tensor):
            return self.predict({key: tensor for key, tensor in zip(self.sorted_input_keys, chain([tensors], args))})
        else:
            return self.predict(tensors)
    def __call__(self, tensors: dict[str, Tensor]|Tensor, *args: Tensor) -> Tensor:
        return super().__call__(tensors, *args)
    def print_summary(self, merge: int = 10):
        torchinfo.summary(self, input_size=[
            (config.models.batch_size*merge, *self.dummy_input[key].shape[1:]) for key in self.sorted_input_keys
        ])
    def get_device(self) -> torch.device:
        return next(self.parameters()).device
    def get_dtype(self) -> torch.dtype:
        return next(self.parameters()).dtype
    def get_name(self) -> str:
        return __name__.split(".")[-2]
    @cached_property
    def sorted_input_keys(self) -> list[str]:
        return sorted(self.dummy_input.keys())
    @cached_property
    def dummy_input(self) -> dict[str, Tensor]:
        test_example = {
            interval.name: torch.rand((1, 1000, 10), dtype=torch.float32)
            for interval in Interval
        }
        return self.extract_tensors(test_example)