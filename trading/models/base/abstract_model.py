#2
from typing import overload, Literal
import torch
from torch import Tensor
from trading.models.base.model_config import ModelConfig

class AbstractModel(torch.nn.Module):
    def __init__(self, config: ModelConfig):
        super().__init__()
        self.config = config
    @overload
    def extract_tensors(self, example: dict[str, Tensor], with_output: Literal[False]) -> dict[str,Tensor]: ...
    @overload
    def extract_tensors(self, example: dict[str, Tensor], with_output: Literal[True]=...) -> tuple[dict[str,Tensor],Tensor]: ...
    def extract_tensors(self, example: dict[str, Tensor], with_output: bool = True) -> dict[str, Tensor]|tuple[dict[str,Tensor],Tensor]:
        raise NotImplementedError()
    def forward(self, tensors: dict[str, Tensor]) -> Tensor: ...
    def __call__(self, arg1: dict[str, Tensor]) -> Tensor: ...
    def print_summary(self, merge: int = 10): ...

    def get_device(self) -> torch.device:
        return next(self.parameters()).device
    def get_dtype(self) -> torch.dtype:
        return next(self.parameters()).dtype
    def get_name(self) -> str:
        return __name__.split(".")[-2]
    