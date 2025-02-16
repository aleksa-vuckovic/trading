import logging
import torch
from pathlib import Path
from .. import model1, model2
from ..training_plan import TrainingPlan
from .network import Model

logger = logging.getLogger(__name__)
checkpoints_folder = Path(__file__).parent / 'checkpoints'
initial_lr = 10e-6

"""
Different from models 6-8 in that it has 10 features, with moving averages.
Data up to 13:30
"""
def get_plan(hour: int) -> TrainingPlan:
    model = Model()
    plan = TrainingPlan.Builder(model)
    plan.with_optimizer(torch.optim.Adam(model.parameters()))
    model1.train.add_batches(plan, hour=hour, examples_folder=model2.generator.FOLDER, merge=10)
    model1.train.add_triggers(plan, checkpoints_folder=checkpoints_folder, initial_lr=initial_lr)
    return plan.build()
