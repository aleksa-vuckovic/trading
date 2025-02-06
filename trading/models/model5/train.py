import logging
import torch
from pathlib import Path
from ..model1.train import add_stats, add_batches, add_triggers
from ..utils import TrainingPlan
from .network import Model, extract_tensors

logger = logging.getLogger(__name__)
examples_folder = Path(__file__).parent / 'examples'
checkpoints_folder = Path(__file__).parent / 'checkpoints'
initial_lr = 10e-6
"""
This is a model trained on data up to the period starting at 14:30.
    That means that it is most effectively utilized between 15:30 when the data is initally
    available and 16 when the market closes.
"""
def run_loop(max_epoch = 10000):
    plan = TrainingPlan(Model())
    plan.with_optimizer(torch.optim.Adam(plan.model.parameters()))
    add_stats(plan)
    add_batches(plan, examples_folder=examples_folder, extract_tensors=extract_tensors, merge=1)
    add_triggers(plan, checkpoints_folder=checkpoints_folder, initial_lr=initial_lr)
    plan.run(max_epoch=max_epoch)