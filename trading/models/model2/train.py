import logging
import torch
from pathlib import Path
from ..training_plan import TrainingPlan, add_train_val_test_batches, add_triggers
from .. import model1
from . import generator
from .network import Model


logger = logging.getLogger(__name__)
initial_lr = 10e-6
"""
Uses prices only.
    -Time relative hlcv
    -Relative span
    -Close relative to open (useless?)
"""
def get_plan(hour: int) -> TrainingPlan:
    checkpoints_folder = Path(__file__).parent / f"checkpoints_{hour}"
    model = Model()
    builder = TrainingPlan.Builder(model)
    builder.with_optimizer(torch.optim.Adam(model.parameters()))
    add_train_val_test_batches(builder, examples_folder=generator.FOLDER, make_stats=model1.train.make_stats, merge=10, hour=hour)
    add_triggers(
        builder,
        checkpoints_folder=checkpoints_folder,
        initial_lr=initial_lr,
        lr_steps=[(100, 10), (0.4, 5), (0.35, 2), (0.3, 1), (0.25, 0.5), (0.2, 0.1), (0.15, 0.05), (0.1, 0.01)]
    )
    return builder.build()