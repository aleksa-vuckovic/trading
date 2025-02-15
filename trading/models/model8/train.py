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
Different from model7 in that it is trained with data up to 10:30.
"""
def get_plan() -> TrainingPlan:
    model = model6.train.Model()
    plan = TrainingPlan.Builder(model)
    plan.with_optimizer(torch.optim.Adam(model.parameters()))
    add_batches(plan, examples_folder=generator.FOLDER, extractor=model6.network.Extractor(), merge=10)
    add_triggers(plan, checkpoints_folder=checkpoints_folder, initial_lr=initial_lr)
    return plan.build()