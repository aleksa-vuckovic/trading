import logging
import torch
from pathlib import Path
from ..training_plan import TrainingPlan, add_train_val_test_batches, add_triggers
from ..stats import StatContainer, Accuracy, Precision, TanhLoss
from ..abstract import ModelConfig
from ..generators import generator
from .network import Model


logger = logging.getLogger(__name__)
initial_lr = 10e-6

def make_stats(name: str) -> StatContainer:
    return StatContainer(
        TanhLoss(),
        Accuracy(name='accuracy', to_bool_output=lambda it: it > 0.7),
        Precision(name='precision', to_bool_output=lambda it: it > 0.7),
        Accuracy(name='miss', to_bool_output=lambda it: it>0.7, to_bool_expect=lambda it: it<0.005),
        name=name
    )

"""
Different from model2 in that it has 10 features, with moving averages.
Data up to 13:30
"""
def get_plan(config: ModelConfig) -> TrainingPlan:
    model = Model(config)
    plan = TrainingPlan.Builder(model)
    plan.with_optimizer(torch.optim.Adam(model.parameters()))
    add_train_val_test_batches(plan, examples_folder=generator.FOLDER, make_stats=make_stats, timing=config.timing, merge=5)
    add_triggers(
        plan,
        checkpoints_folder=Path(__file__).parent / f"checkpoints_{None}",
        initial_lr=initial_lr,
        lr_steps=[(100, 1), (0.7, 0.8), (0.6, 0.6), (0.5, 0.4), (0.4, 0.2), (0.35, 0.1), (0.3, 0.05), (0.25, 0.01), (0.2, 0.01), (0.15, 0.01), (0.1, 0.01)]
    )
    return plan.build()