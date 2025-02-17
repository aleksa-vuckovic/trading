import logging
import torch
from pathlib import Path
from .. import model2
from ..training_plan import TrainingPlan, add_train_val_test_batches, add_triggers
from ..stats import StatCollector, StatContainer, Accuracy, Precision
from .network import Model

logger = logging.getLogger(__name__)
checkpoints_folder = Path(__file__).parent / 'checkpoints'
initial_lr = 10e-6

class CustomLoss(StatCollector):
    def __init__(self):
        super().__init__('loss')
    
    def _calculate(self, expect, output):
        return -torch.log(torch.abs(expect - output)).mean()

def make_stats(name: str) -> StatContainer:
    return StatContainer(
        CustomLoss(),
        Accuracy(name='accuracy', to_bool_output=lambda it: it > 0.7),
        Precision(name='precision', to_bool_output=lambda it: it > 0.7),
        Accuracy(name='miss', to_bool_output=lambda it: it>0.7, to_bool_expect=lambda it: it<0.005),
        name=name
    )

"""
Different from model2 in that it has 10 features, with moving averages.
Data up to 13:30
"""
def get_plan(hour: int) -> TrainingPlan:
    model = Model()
    plan = TrainingPlan.Builder(model)
    plan.with_optimizer(torch.optim.Adam(model.parameters()))
    add_train_val_test_batches(plan, examples_folder=model2.generator.FOLDER, make_stats=make_stats, hour=hour, merge=5)
    add_triggers(
        plan,
        checkpoints_folder=checkpoints_folder,
        initial_lr=initial_lr,
        lr_steps=[(100, 10), (0.4, 5), (0.35, 2), (0.3, 1), (0.25, 0.5), (0.2, 0.1), (0.15, 0.05), (0.1, 0.01)]
    )
    return plan.build()
