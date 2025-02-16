import logging
import torch
from pathlib import Path
from ..training_plan import TrainingPlan
from .. import model1
from .network import Model
from . import generator

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
    model1.train.add_batches(builder, examples_folder=generator.FOLDER, merge=10, hour=hour)
    model1.train.add_triggers(builder, checkpoints_folder=checkpoints_folder, initial_lr=initial_lr)
    return builder.build()