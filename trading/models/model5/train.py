import logging
import torch
from pathlib import Path
from ..model1.train import add_stats, add_batches, add_triggers
from ..training_plan import TrainingPlan
from .network import Model, Extractor
from . import generator

logger = logging.getLogger(__name__)
checkpoints_folder = Path(__file__).parent / 'checkpoints'
initial_lr = 10e-6
"""
This is a model trained on data up to the period starting at 14:30.
    This means that it's effectively usable after 15:30.
"""
def run_loop(max_epoch = 10000):
    plan = TrainingPlan(Model())
    plan.with_optimizer(torch.optim.Adam(plan.model.parameters()))
    add_stats(plan)
    add_batches(plan, examples_folder=generator.FOLDER, extractor=Extractor(), merge=1)
    add_triggers(plan, checkpoints_folder=checkpoints_folder, initial_lr=initial_lr)
    plan.run(max_epoch=max_epoch)