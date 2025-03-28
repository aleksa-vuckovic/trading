#3
from typing import Callable, Sequence
from pathlib import Path
from trading.models.base.stats import StatContainer
from trading.models.base.batches import Batches, BatchFile
from trading.models.base.training_plan import LearningRateAction, TrainingPlan, AlwaysTrigger, StatTrigger,\
    CheckpointAction, LearningRateAction, StatHistoryTrigger, EpochTrigger, StopAction


def add_triggers(
    builder: TrainingPlan.Builder,
    checkpoints_folder: Path,
    initial_lr: float,
    lr_steps = [(100, 10), (0.4, 5), (0.35, 2), (0.3, 1), (0.25, 0.5), (0.2, 0.1), (0.15, 0.05), (0.1, 0.01)]
) -> TrainingPlan.Builder:

    for loss, lr_factor in lr_steps:
        builder.when(StatTrigger('val', 'loss', (float('-inf'), loss), once=True))\
            .then(CheckpointAction(checkpoints_folder / f'loss_{int(loss*100)}_checkpoint.pth'))\
            .then(LearningRateAction(initial_lr*lr_factor))
    for precision in range(5, 100, 5):
        builder.when(StatTrigger('val', 'precision', (precision/100, float('+inf')), once=True))\
            .then(CheckpointAction(checkpoints_folder / f"precision_{precision}_checkpoint.pth"))
    for accuracy in range(5, 100, 5):
        builder.when(StatTrigger('val', 'accuracy', (accuracy/100, float('+inf')), once=True))\
            .then(CheckpointAction(checkpoints_folder / f"accuracy_{accuracy}_checkpoint.pth"))
        
    def loss_plateau(values: Sequence[float]) -> bool:
        high = max(values)
        last = values[-1]
        return last>=high or (high-last)/high < 0.001
    builder.when(StatHistoryTrigger('loss', group='val', count=10, criteria=loss_plateau) | EpochTrigger(threshold=100))\
        .then(StopAction())
        
    return builder.with_primary_checkpoint(CheckpointAction(checkpoints_folder / 'primary_checkpoint.pth'))
