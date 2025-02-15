import torch
import logging
from pathlib import Path
from typing import Callable
from ..stats import StatCollector, StatContainer
from ..training_plan import TrainingPlan
from ..abstract import TensorExtractor
from ..utils import Batches, get_batch_files
from .network import Model, Extractor
from . import generator

logger = logging.getLogger(__name__)
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

def make_stats(name: str) -> StatContainer:
    return StatContainer(CustomLoss(), Accuracy(), Precision(), Miss(), name=name)

def add_batches(builder: TrainingPlan.Builder, examples_folder: Path, extractor: TensorExtractor, merge:int=1, make_stats: Callable[[str], StatContainer] = make_stats) -> TrainingPlan.Builder:
    all_files = get_batch_files(examples_folder)
    test_i = int(len(all_files)*0.05)
    train_files = [it['path'] for it in all_files[:-test_i] if it['batch'] % 6]
    val_files = [it['path'] for it in all_files[:-test_i] if it['batch']%6 == 0]
    test_files = [it['path'] for it in all_files[-test_i:]]
    return builder.with_batches(name='train', batches=Batches(train_files, extractor=extractor, merge=merge), stats=make_stats('train'), backward=True)\
        .with_batches(name='val', batches=Batches(val_files, extractor=extractor, merge=merge), stats=make_stats('val'), backward=False)\
        .with_batches(name='test', batche=Batches(test_files, extractor=extractor, merge=merge), stats=make_stats('train'), backward=False)

def add_triggers(builder: TrainingPlan.Builder, checkpoints_folder: Path, initial_lr: float) -> TrainingPlan.Builder:
    builder.when(TrainingPlan.AlwaysTrigger())\
        .then(TrainingPlan.StatHistoryAction())
    
    for loss, lr_factor in [(100, 1), (0.4, 1), (0.35, 1), (0.3, 1), (0.25, 0.5), (0.2, 0.1), (0.15, 0.05), (0.1, 0.01)]:
        builder.when(TrainingPlan.StatTrigger('loss', upper_bound=loss, trigger_once=True))\
            .then(TrainingPlan.CheckpointAction(checkpoints_folder / f'loss_{int(loss*100)}_checkpoint.pth'))\
            .then(TrainingPlan.LearningRateAction(initial_lr*lr_factor))
    for precision in range(5, 100, 5):
        builder.when(TrainingPlan.StatTrigger('precision', lower_bound=precision/100, trigger_once=True))\
            .then(TrainingPlan.CheckpointAction(checkpoints_folder / f"precision_{precision}_checkpoint.pth"))
    for accuracy in range(60, 100, 5):
        builder.when(TrainingPlan.StatTrigger('accuracy', lower_bound=accuracy/100, trigger_once=True))\
            .then(TrainingPlan.CheckpointAction(checkpoints_folder / f"accuracy_{accuracy}_checkpoint.pth"))
        
    def loss_plateau(values: list[float]) -> bool:
        high = max(values)
        last = values[-1]
        return last>=high or (high-last)/high < 0.001
    builder.when(TrainingPlan.StatHistoryTrigger('loss', group='val', count=10, criteria=loss_plateau) | TrainingPlan.EpochTrigger(threshold=100))\
        .then(TrainingPlan.StopAction())
        
    return builder.with_primary_checkpoint(TrainingPlan.CheckpointAction(checkpoints_folder / 'primary_checkpoint.pth'))
    

def get_plan() -> TrainingPlan:
    model = Model()
    builder = TrainingPlan.Builder(model)
    builder.with_optimizer(torch.optim.Adam(model.parameters()))
    add_batches(builder, examples_folder=generator.FOLDER, extractor=Extractor(), merge=1)
    add_triggers(builder, checkpoints_folder=checkpoints_folder, initial_lr=initial_lr)
    return builder.build()