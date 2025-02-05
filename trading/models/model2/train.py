import logging
import config
import torch
from pathlib import Path
from ..utils import TrainingPlan
from ..model1.train import add_stats, add_batches, add_triggers
from .network import Model, extract_tensors

logger = logging.getLogger(__name__)
checkpoint_file = Path(__file__).parent / 'checkpoint.pth'
initial_lr = 10e-4

def run_loop(max_epoch = 100000000):
    plan = TrainingPlan(Model())
    plan.with_optimizer(torch.optim.Adam(plan.model.parameters()))
    add_stats(plan)
    add_batches(plan, extract_tensors=extract_tensors, merge=0000//config.batch_size)
    add_triggers(plan, checkpoints_folder=checkpoint_file, initial_lr=initial_lr)
    plan.run(max_epoch=max_epoch)