import logging
import torch
from pathlib import Path
from ..model1.train import add_batches, add_triggers
from .. import model7
from ..training_plan import TrainingPlan
from .network import Model, Extractor

logger = logging.getLogger(__name__)
checkpoints_folder = Path(__file__).parent / 'checkpoints'
initial_lr = 10e-6

"""
Different from models 6-8 in that it has 10 features, with moving averages.
Data up to 13:30
"""
def get_plan() -> TrainingPlan:
    model = Model()
    plan = TrainingPlan.Builder(model)
    plan.with_optimizer(torch.optim.Adam(model.parameters()))
    add_batches(plan, examples_folder=model7.generator.FOLDER, extractor=Extractor(), merge=10)
    add_triggers(plan, checkpoints_folder=checkpoints_folder, initial_lr=initial_lr)
    return plan.build()
