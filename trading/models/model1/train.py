import torch
import logging
from pathlib import Path
from typing import Callable
from ..stats import StatCollector, StatContainer, Accuracy, Precision
from ..training_plan import TrainingPlan, add_train_val_test_batches, add_triggers
from .network import Model
from . import generator

logger = logging.getLogger(__name__)
checkpoints_folder = Path(__file__).parent / 'checkpoints'
initial_lr = 10e-7
    
class CustomLoss(StatCollector):
    def __init__(self):
        super().__init__('loss')
    
    def _calculate(self, expect, output):
        eps = 1e-5
        loss = -torch.log(1 + eps - torch.abs(output - expect) / (1+torch.abs(expect)))
        return loss.mean()

def make_stats(name: str) -> StatContainer:
    return StatContainer(
        CustomLoss(),
        Accuracy(name='accuracy', to_bool_output=lambda it: it > 0.5),
        Precision(name='precision', to_bool_output=lambda it: it > 0.5),
        Accuracy(name='miss', to_bool_output=lambda it: it>0.2, to_bool_expect=lambda it: it<0),
        name=name
    )
    
def get_plan() -> TrainingPlan:
    model = Model()
    builder = TrainingPlan.Builder(model)
    builder.with_optimizer(torch.optim.Adam(model.parameters()))
    add_train_val_test_batches(builder, examples_folder=generator.FOLDER, make_stats=make_stats, merge=1, hour=None)
    add_triggers(builder, checkpoints_folder=checkpoints_folder, initial_lr=initial_lr)
    return builder.build()