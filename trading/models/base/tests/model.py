

from typing import override
from torch import Tensor
from torch.nn import Linear
from trading.models.base import AbstractModel
from trading.models.base.model_config import BaseModelConfig

class Model(AbstractModel):
    def __init__(self, config: BaseModelConfig):
        super().__init__(config)
        self.layer = Linear(1,1)
    @override
    def forward(self, tensors: dict[str, Tensor]) -> Tensor:
        return self.layer(next(iter(tensors.values())))

