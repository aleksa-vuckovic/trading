import logging
import torch
from pathlib import Path
from ..model1.train import add_batches, add_triggers
from ..training_plan import TrainingPlan
from .network import Model, Extractor
from . import generator

logger = logging.getLogger(__name__)
checkpoints_folder = Path(__file__).parent / 'checkpoints'
initial_lr = 10e-6
"""
1. Trained up to 15:30.
2. Uses prices only.
    -Time relative hlcv
    -Relative span
    -Close relative to open (useless?)
"""
def get_plan() -> TrainingPlan:
    model = Model()
    builder = TrainingPlan.Builder(model)
    builder.with_optimizer(torch.optim.Adam(model.parameters()))
    add_batches(builder, examples_folder=generator.FOLDER, extractor=Extractor(), merge=10)
    add_triggers(builder, checkpoints_folder=checkpoints_folder, initial_lr=initial_lr)
    return builder.build()