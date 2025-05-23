import logging
import torch
from typing import Sequence
from pathlib import Path
from trading.models.base.manager import BatchGroupConfig, ModelManager, StatTrigger, CheckpointAction, LearningRateAction, StatSlopeTrigger, StopAction, EpochTrigger, TrainConfig
from trading.models.base.stats import StatContainer, Accuracy, Precision, TanhLoss
from trading.models.base.model_config import BaseModelConfig
from trading.models.model4.model import Model


logger = logging.getLogger(__name__)
initial_lr = 10e-6

stats = StatContainer(
    TanhLoss(),
    Accuracy(name='accuracy', to_bool_output=lambda it: it > 0.7),
    Precision(name='precision', to_bool_output=lambda it: it > 0.7),
    Accuracy(name='miss', to_bool_output=lambda it: it>0.7, to_bool_expect=lambda it: it<0.005)
)

def train(inputs: list[Path], batch_group_configs: list[BatchGroupConfig], model_config: BaseModelConfig):
    manager = ModelManager.get(Model, model_config)
    train_config = TrainConfig(inputs, batch_group_configs, stats) 
    #triggers
    for loss, lr_factor in [(100, 1), (0.7, 0.8), (0.6, 0.6), (0.5, 0.4), (0.4, 0.2), (0.35, 0.1), (0.3, 0.05), (0.25, 0.01), (0.2, 0.01), (0.15, 0.01), (0.1, 0.01)]:
        train_config.when(StatTrigger('val', 'loss', (float('-inf'), loss), once=True))\
            .then(CheckpointAction(manager.checkpoints / f'loss_{int(loss*100)}_checkpoint.pth'))\
            .then(LearningRateAction(initial_lr*lr_factor))
    for precision in range(5, 100, 5):
        train_config.when(StatTrigger('val', 'precision', (precision/100, float('+inf')), once=True))\
            .then(CheckpointAction(manager.checkpoints / f"precision_{precision}_checkpoint.pth"))
    for accuracy in range(5, 100, 5):
        train_config.when(StatTrigger('val', 'accuracy', (accuracy/100, float('+inf')), once=True))\
            .then(CheckpointAction(manager.checkpoints / f"accuracy_{accuracy}_checkpoint.pth"))
    train_config.when(StatSlopeTrigger(key='loss', group='val', epochs=5, bounds=(-0.002,float('+inf'))) | EpochTrigger(threshold=100))\
        .then(StopAction())
    manager.train(train_config)
