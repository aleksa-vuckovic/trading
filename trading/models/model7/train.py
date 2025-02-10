import logging
import torch
from pathlib import Path
from ..model1.train import add_stats, add_batches, add_triggers
from ..model5.network import extract_tensors
from ..model6.train import Model
from ..utils import TrainingPlan


"""
This model is only different from model6 in that it's trained on data up to the period starting at 13:30.
"""
logger = logging.getLogger(__name__)
examples_folder = Path(__file__).parent / 'examples'
checkpoints_folder = Path(__file__).parent / 'checkpoints'
initial_lr = 10e-6

def run_loop(max_epoch = 10000):
    plan = TrainingPlan(Model())
    plan.with_optimizer(torch.optim.Adam(plan.model.parameters()))
    add_stats(plan)
    add_batches(plan, examples_folder=examples_folder, extract_tensors=extract_tensors, merge=10)
    add_triggers(plan, checkpoints_folder=checkpoints_folder, initial_lr=initial_lr)
    plan.run(max_epoch=max_epoch)