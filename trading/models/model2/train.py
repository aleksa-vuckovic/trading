import logging
import torch
from pathlib import Path
from ..training_plan import TrainingPlan, add_train_val_test_batches, add_triggers
from ..stats import StatCollector, StatContainer, Accuracy, Precision
from . import generator
from .network import Model


logger = logging.getLogger(__name__)
initial_lr = 10e-6

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

def get_plan(hour: int) -> TrainingPlan:
    checkpoints_folder = Path(__file__).parent / f"checkpoints_{hour}"
    model = Model()
    builder = TrainingPlan.Builder(model)
    builder.with_optimizer(torch.optim.Adam(model.parameters()))
    add_train_val_test_batches(builder, examples_folder=generator.FOLDER, make_stats=make_stats, merge=5, hour=hour)
    add_triggers(
        builder,
        checkpoints_folder=checkpoints_folder,
        initial_lr=initial_lr,
        lr_steps=[(100, 10), (0.4, 5), (0.35, 2), (0.3, 1), (0.25, 0.5), (0.2, 0.1), (0.15, 0.05), (0.1, 0.01)]
    )
    return builder.build()