import logging
import torch
from pathlib import Path
from ..model1.train import add_batches, add_triggers
from .. import model6
from ..training_plan import TrainingPlan
from . import generator

logger = logging.getLogger(__name__)
checkpoints_folder = Path(__file__).parent / 'checkpoints'
initial_lr = 10e-6

"""
This model is only different from model6 in that it's trained on data up to 13:30
Edit: seems to have been traned up to 15 after all.
"""
def get_plan() -> TrainingPlan:
    model = model6.train.Model()
    builder = TrainingPlan.Builder(model)
    builder.with_optimizer(torch.optim.Adam(model.parameters()))
    add_batches(builder, examples_folder=generator.FOLDER, extractor=model6.network.Extractor(), merge=10)
    add_triggers(builder, checkpoints_folder=checkpoints_folder, initial_lr=initial_lr)
    return builder.build()