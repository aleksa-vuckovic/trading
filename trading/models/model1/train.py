import torch
import logging
from pathlib import Path
from torch import optim
from tqdm import tqdm
from typing import Callable
from ..utils import StatCollector, StatContainer, Batches, get_batch_files, TrainingPlan
from .network import Model, extract_tensors

logger = logging.getLogger(__name__)
examples_folder = Path(__file__).parent / 'examples'
checkpoints_folder = Path(__file__).parent / 'checkpoints'
initial_lr = 10e-7

class Accuracy(StatCollector):
    def __init__(self):
        super().__init__('accuracy')
    
    def _calculate(self, expect, output):
        output = output > 0.5
        expect = expect > 0.5
        hits = torch.logical_and(output, expect).sum().item()
        output_n = output.sum().item()
        expect_n = expect.sum().item()
        return hits / output_n if output_n else 0 if expect_n else 1
    
class Precision(StatCollector):
    def __init__(self):
        super().__init__('precision')

    def _calculate(self, expect, output):
        output = output > 0.5
        expect = expect > 0.5
        hits = torch.logical_and(output, expect).sum().item()
        expect_n = expect.sum().item()
        return hits / expect_n if expect_n else 1

class Miss(StatCollector):
    def __init__(self):
        super().__init__('miss')
    
    def _calculate(self, expect, output):
        output = output > 0.2
        misses_n = torch.logical_and(expect < 0, output).sum().item()
        total_n = output.sum().item()
        return misses_n / total_n if total_n else 0
    
class CustomLoss(StatCollector):
    def __init__(self):
        super().__init__('loss')
    
    def _calculate(self, expect, output):
        eps = 1e-5
        loss = -torch.log(1 + eps - torch.abs(output - expect) / (1+torch.abs(expect)))
        return loss.mean()
    
def add_stats(plan: TrainingPlan):
    return plan.with_stats(
        StatContainer(CustomLoss(), Accuracy(), Precision(), Miss(), name='train'),
        StatContainer(CustomLoss(), Accuracy(), Precision(), Miss(), name='val')
    )

def add_batches(plan: TrainingPlan, examples_folder: Path = examples_folder, extract_tensors: Callable|None=None, merge:int=1):
    all_files = get_batch_files(examples_folder)[:10]
    training_files = [it['file'] for it in all_files if it['batch'] % 6]
    validation_files = [it['file'] for it in all_files if it['batch']%6 == 0]
    return plan.with_batches(
        Batches(training_files, extract_tensors=extract_tensors, merge=merge),
        Batches(validation_files,extract_tensors=extract_tensors, merge=merge)
    )

def add_triggers(plan: TrainingPlan, checkpoints_folder: Path, initial_lr: float) -> TrainingPlan:
    plan.when(TrainingPlan.AlwaysTrigger())\
        .then(TrainingPlan.StatHistoryAction())\
        .then(TrainingPlan.CheckpointAction(checkpoints_folder / 'primary_checkpoint.pth', primary=True))
    
    for loss, lr_factor in [(100, 1), (0.25, 0.5), (0.2, 0.1), (0.15, 0.05), (0.1, 0.01)]:
        plan.when(TrainingPlan.StatTrigger('loss', upper_bound=loss, trigger_once=True))\
            .then(TrainingPlan.CheckpointAction(checkpoints_folder / f'loss_{int(loss*100)}_checkpoint.pth'))\
            .then(TrainingPlan.LearningRateAction(initial_lr*lr_factor))
    for precision in range(5, 100, 5):
        plan.when(TrainingPlan.StatTrigger('precision', lower_bound=precision/100, trigger_once=True))\
            .then(TrainingPlan.CheckpointAction(checkpoints_folder / f"precision_{precision}_checkpoint.pth"))
    for accuracy in range(60, 100, 5):
        plan.when(TrainingPlan.StatTrigger('accuracy', lower_bound=accuracy/100, trigger_once=True))\
            .then(TrainingPlan.CheckpointAction(checkpoints_folder / f"accuracy_{accuracy}_checkpoint.pth"))
    
    return plan
    

def run_loop(max_epoch = 100000000):
    plan = TrainingPlan(Model())
    plan.with_optimizer(torch.optim.Adam(plan.model.parameters()))
    add_stats(plan)
    add_batches(plan, examples_folder=examples_folder, extract_tensors=extract_tensors, merge=1)
    add_triggers(plan, checkpoints_folder=checkpoints_folder, initial_lr=initial_lr)
    plan.run(max_epoch=max_epoch)