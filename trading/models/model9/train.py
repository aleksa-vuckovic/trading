import logging
import torch
from pathlib import Path
from ..model1.train import add_stats, add_batches, add_triggers
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
def run_loop(max_epoch = 10000):
    plan = TrainingPlan(Model())
    plan.with_optimizer(torch.optim.Adam(plan.model.parameters()))
    add_stats(plan)
    add_batches(plan, examples_folder=model7.generator.FOLDER, extractor=Extractor(), merge=10)
    add_triggers(plan, checkpoints_folder=checkpoints_folder, initial_lr=initial_lr)
    plan.run(max_epoch=max_epoch)