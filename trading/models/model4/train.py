import logging
import config
from pathlib import Path
from torch import optim
from ..model1.train import add_stats, add_batches, add_triggers
from ..utils import TrainingPlan
from .network import Model, extract_tensors

logger = logging.getLogger(__name__)
checkpoints_folder = Path(__file__).parent / 'checkpoints'
initial_lr = 10e-6

def run_loop(max_epoch = 100000000):
    plan = TrainingPlan(Model())
    plan.with_optimizer(optim.SGD(plan.model.parameters(), weight_decay=0.1))
    add_stats(plan)
    add_batches(plan, extract_tensors=extract_tensors, merge=10000//config.batch_size)
    add_triggers(plan, checkpoints_folder=checkpoints_folder, initial_lr=initial_lr)
    plan.run(max_epoch=max_epoch)