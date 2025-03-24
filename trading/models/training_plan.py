from __future__ import annotations
from unittest.mock import Base
import torch
from torch import Tensor
import logging
from tqdm import tqdm
from pathlib import Path
from typing import Callable, Literal, NamedTuple, Sequence, final, override
from matplotlib import pyplot as plt
from base.classes import get_full_classname
from base import plotutils
from trading.core.work_calendar import TimingConfig
from trading.models.stats import StatContainer
from trading.models.utils import Batches, BatchFile
from trading.models.abstract import AbstractModel

logger = logging.getLogger(__name__)
STAT_HISTORY = 'stat_history'

#region Rules
class Trigger:
    def check(self, plan: TrainingPlan) -> bool: ...
    def state_dict(self) -> dict: ...
    def load_state_dict(self, data: dict): ...
    def __or__(self, value) -> OrTrigger:
        return OrTrigger(self, value)
    def __and__(self, value) -> AndTrigger:
        return AndTrigger(self, value)
class BaseTrigger(Trigger):
    def __init__(self, once: bool = False):
        self.once = once
        self.triggered = False
    @override
    @final
    def check(self, plan: TrainingPlan) -> bool:
        if self.once and self.triggered: return False
        if self._check(plan):
            self.triggered = True
            return True
        return False
    def _check(self, plan: TrainingPlan) -> bool: ...
    def state_dict(self) -> dict:
        return {'triggered': self.triggered}
    def load_state_dict(self, data: dict) -> None:
        self.triggered = data['triggered']

class OrTrigger(BaseTrigger):
    def __init__(self, *args: Trigger, once: bool = False):
        super().__init__(once)
        self.criteria = list(args)
    @override
    def _check(self, plan: TrainingPlan) -> bool:
        for it in self.criteria:
            if it.check(plan): return True
        return False
    @override
    def state_dict(self) -> dict:
        return {**super().state_dict(), 'criteria': [it.state_dict() for it in self.criteria]}
    @override
    def load_state_dict(self, data: dict) -> None:
        super().load_state_dict(data)
        for criteria, state_dict in zip(self.criteria, data['criteria']): criteria.load_state_dict(state_dict)

class AndTrigger(BaseTrigger):
    def __init__(self, *args: Trigger, once: bool = False):
        super().__init__(once)
        self.criteria = list(args)
    @override
    def _check(self, plan: TrainingPlan) -> bool:
        for it in self.criteria:
            if not it.check(plan): return False
        return True
    @override
    def state_dict(self) -> dict:
        return {**super().state_dict(), 'criteria': [it.state_dict() for it in self.criteria]}
    @override
    def load_state_dict(self, data: dict) -> None:
        super().load_state_dict(data)
        for criteria, state_dict in zip(self.criteria, data['criteria']): criteria.load_state_dict(state_dict)
    
class BoundedTrigger(BaseTrigger):
    in_bounds: bool|None

    def __init__(
        self,
        bounds: tuple[float,float],
        event: Literal['enter','exit','both','in','out'] = 'enter',
        once: bool = False
    ):
        super().__init__(once)
        self.bounds = bounds
        self.event = event
        self.in_bounds = None

    def is_trigger(self, cur:bool) -> bool:
        if self.event == 'enter': return self.in_bounds == False and cur
        elif self.event == 'exit': return self.in_bounds == True and not cur
        elif self.event == 'both': return self.in_bounds is not None and self.in_bounds ^ cur
        elif self.event == 'in': return cur
        elif self.event == 'out': return not cur
        else: raise Exception(f'Unknown event {self.event}.')

    def get_value(self, plan: TrainingPlan) -> float: ...
    @override
    def _check(self, plan: TrainingPlan) -> bool:
        value = self.get_value(plan)
        in_bounds = value > self.bounds[0] and value < self.bounds[1]
        result = self.is_trigger(in_bounds)
        self.in_bounds = in_bounds
        if result: logger.info(f"Triggered bounded {self}, with value {value}. Bounds: {self.bounds}.")
        return result
    
class StatTrigger(BoundedTrigger):
    def __init__(self,
        group: str,
        key: str,
        bounds: tuple[float, float],
        event: Literal['enter','exit','both','in','out'] = 'enter',
        once: bool = False
    ):
        super().__init__(bounds, event, once)
        self.group = group
        self.key = key
        
    @override
    def get_value(self, plan: TrainingPlan) -> float:
        stats = [it.stats for it in plan.batch_groups if it.name == self.group][0]
        return stats[self.key]
    
    def __str__(self) -> str:
        return f"{self.__class__.__name__}(key='{self.key}',group='{self.group}')"
    
class StatHistoryTrigger(BaseTrigger):
    def __init__(self,
        key: str,
        group: str = 'val',
        count: int = 10,
        criteria: Callable[[Sequence[float]], bool] = lambda values: True,
        desc: str|None = None,
        once: bool = False
    ):
        super().__init__(once)
        self.key = key
        self.group = group
        self.count = count
        self.criteria = criteria
        self.desc = desc
    @override
    def _check(self, plan) -> bool:
        if STAT_HISTORY not in plan.data: return False
        history = plan.data[STAT_HISTORY]
        if len(history) < self.count: return False
        values = [it[self.group][self.key] for it in history[-self.count:]]
        result = self.criteria(values)
        if result:
            logger.info(f"Triggered stat history trigger ({self.desc or 'no description'}) with values {values}.")
        return result

class EpochTrigger(BaseTrigger):
    def __init__(self, threshold: int, once: bool = True):
        super().__init__(once)
        self.threshold = threshold
    @override
    def _check(self, plan: TrainingPlan):
        if plan.epoch >= self.threshold:
            logger.info(f"Triggered EpochTrigger for epoch {plan.epoch}.")
            return True
        else: return False
    
class AlwaysTrigger(BaseTrigger):
    def __init__(self, once = False):
        super().__init__(once)
    @override
    def _check(self, plan: TrainingPlan) -> bool:
        return True
    
class Action:
    def execute(self, plan: TrainingPlan):
        pass

class StatHistoryAction(Action):
    @override
    def execute(self, plan: TrainingPlan):
        if STAT_HISTORY not in plan.data:
            plan.data[STAT_HISTORY] = []
        entry: dict = {it.name:it.stats.to_dict() for it in plan.batch_groups}
        entry['epoch'] = plan.epoch
        plan.data[STAT_HISTORY].append(entry)

class CheckpointAction(Action):
    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
    @override
    def execute(self, plan: TrainingPlan):
        if self.path.exists():
            logger.info(f"Skipping non primary checkpoint because '{self.path}' already exists.")
        else:
            self.save(plan)
            logger.info(f"Saved checkpoint to '{self.path}'.")
    def save(self, plan: TrainingPlan):
        torch.save(plan.state_dict(), self.path)
    def restore(self, plan: TrainingPlan):
        if not self.path.exists():
            logger.info(f"No prior state, starting from scratch.")
            return
        state_dict = torch.load(self.path, weights_only=True, map_location=plan.device)
        plan.load_state_dict(state_dict)
        logger.info(f"Loaded state from epoch {plan.epoch}.")

class LearningRateAction(Action):
    def __init__(self, value: float):
        self.value = value
    @override
    def execute(self, plan: TrainingPlan):
        for param_group in plan.optimizer.param_groups:
            param_group['lr'] = self.value
        logger.info(f"Updated learning rate to {self.value}.")

class StopAction(Action):
    @override
    def execute(self, plan: TrainingPlan):
        plan.stop = True

class Rule:
    actions: list[Action]
    def __init__(self, criteria: Trigger):
        self.criteria = criteria
        self.actions = []
    def execute(self, plan: TrainingPlan):
        if self.criteria.check(plan):
            for action in self.actions:
                action.execute(plan)
    def state_dict(self) -> dict:
        return {'criteria': self.criteria.state_dict()}
    def load_state_dict(self, data: dict):
        self.criteria.load_state_dict(data['criteria'])
#endregion

class BatchGroup(NamedTuple):
    name: str
    batches: Batches
    stats: StatContainer
    backward: bool = False

class TrainingPlan:
    """
    - Keeps track of statistics, the model, and optimizer.
    - Runs the training loop.
    - Allows actions to be triggered based on conditions, evaluated at the end of each epoch.
        This includes learning rate updates, saving checkpoints and custom actions derived from Action.
    """
    device: torch.device
    dtype: torch.dtype
    model: AbstractModel
    optimizer: torch.optim.Optimizer
    batch_groups: list[BatchGroup]
    rules: list[Rule]
    primary_checkpoint: CheckpointAction|None
    epoch: int
    data: dict
    stop: bool

    def state_dict(self) -> dict:
        return {
            'model': self.model.state_dict(),
            'optimizer': self.optimizer.state_dict(),
            'rules': [it.state_dict() for it in self.rules],
            'epoch': self.epoch,
            'data': self.data,
            'stop': self.stop
        }
    def load_state_dict(self, data: dict):
        self.model.load_state_dict(data['model'])
        self.optimizer.load_state_dict(data['optimizer'])
        for rule, state_dict in zip(self.rules, data['rules']): rule.load_state_dict(state_dict)
        self.epoch = data['epoch']
        self.data = data['data']
        self.stop = data['stop']

    class _ActionBuilder:
        def __init__(self, rule: Rule):
            self.rule = rule
        def then(self, action: Action) -> TrainingPlan._ActionBuilder:
            self.rule.actions.append(action)
            return self

    class Builder:
        def __init__(self, model: AbstractModel):
            self.plan = TrainingPlan()
            self.plan.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            self.plan.dtype = torch.float32
            self.plan.model = model.to(device = self.plan.device, dtype = self.plan.dtype)
            self.plan.batch_groups = []
            self.plan.rules = []
            self.plan.primary_checkpoint = None
            self.plan.epoch = 1
            self.plan.data = {}
            self.plan.stop = False
        
        def with_optimizer(self, optimizer: torch.optim.Optimizer) -> TrainingPlan.Builder:
            self.plan.optimizer = optimizer
            return self
        
        def with_batches(self, name: str, batches: Batches, stats: StatContainer, backward: bool = False) -> TrainingPlan.Builder:
            self.plan.batch_groups.append(BatchGroup(name, batches.to(device = self.plan.device, dtype = self.plan.dtype), stats, backward=backward))
            return self
    
        def when(self, trigger: Trigger) -> TrainingPlan._ActionBuilder:
            rule = Rule(trigger)
            self.plan.rules.append(rule)
            return TrainingPlan._ActionBuilder(rule)

        def with_primary_checkpoint(self, checkpoint: CheckpointAction) -> TrainingPlan.Builder:
            self.plan.primary_checkpoint = checkpoint
            return self
        
        def build(self) -> TrainingPlan:
            if not self.plan.optimizer or not self.plan.batch_groups:
                raise Exception(f"The plan has not been properly initialized.")
            if self.plan.primary_checkpoint: self.plan.primary_checkpoint.restore(self.plan)
            return self.plan

    def run(self, max_epoch = 10000000):
        try:
            logger.info(f"Running loop on device: {self.device}.")
            logger.info(f"Using {len(self.batch_groups)} batch groups.")
            for entry in self.batch_groups:
                logger.info(f"Batch group {entry.name} with {len(entry.batches)} batches.")
            logger.info(f"Model {get_full_classname(self.model)}.")
            logger.info(f"Optimizer {type(self.optimizer)}.")
            
            while not self.stop and self.epoch < max_epoch:
                logger.info(f"Running epoch {self.epoch}")
                print(f"---------EPOCH {self.epoch}-------------------------------------")
                for batch_group in self.batch_groups:
                    if batch_group.backward:
                        self.model.train()
                        with tqdm(batch_group.batches, desc=f"Epoch {self.epoch} ({batch_group.name})", leave=True) as bar:
                            for batch in bar:
                                input, expect = self.model.extract_tensors(batch, with_output=True)
                                self.optimizer.zero_grad()
                                output: Tensor = self.model(*input).squeeze()
                                loss = batch_group.stats.update(expect, output)
                                loss.backward()
                                self.optimizer.step()
                                bar.set_postfix_str(str(batch_group.stats))
                        print(f"Training group '{batch_group.name}' stats: {batch_group.stats}")
                    else:
                        self.model.eval()
                        with torch.no_grad():
                            with tqdm(batch_group.batches, desc = f'Evaluating {batch_group.name} batches...', leave=False) as bar:
                                for batch in bar:
                                    tensors = self.model.extract_tensors(batch)
                                    input = tensors[:-1]
                                    expect = tensors[-1]
                                    output = self.model(*input).squeeze()
                                    batch_group.stats.update(expect, output)
                            print(f"Evaluation group '{batch_group.name}' stats: {batch_group.stats}")

                for rule in self.rules: rule.execute(self)
                if self.primary_checkpoint: self.primary_checkpoint.save(self)
                for batch_group in self.batch_groups: batch_group.stats.clear()
                self.epoch += 1

            logger.info(f"Stopped at epoch {self.epoch}")
        except:
            logger.error(f"Error running loop.", exc_info=True)

    def plot_history(self):
        if not STAT_HISTORY in self.data or not self.data[STAT_HISTORY]:
            raise Exception('No history to plot')
        history = self.data[STAT_HISTORY]
        epochs = [it['epoch'] for it in history]
        groups = [it.name for it in self.batch_groups]

        metrics = set(key for group in groups for key in history[0][group].keys())
        for metric in metrics:
            values = {group:[it[group][metric] for it in history] for group in groups if metric in history[0][group]}
            #rows = 1 if len(values) <= 2 else 2 if len(values) <= 6 else 3
            #cols = math.ceil(len(values)/rows)
            fig, axes = plt.subplots(1,1)
            fig.suptitle(f"Metric: {metric}")
            axes.set_xlabel('Epoch')
            for group, color in zip(values.keys(), plotutils.COLORS):
                axes.plot(epochs, values[group], color=color, label=group)
            axes.legend()
        plt.show()



def add_train_val_test_batches(
    builder: TrainingPlan.Builder,
    examples_folder: Path,
    make_stats: Callable[[str], StatContainer],
    timing: TimingConfig|None = None,
    merge:int=1
) -> TrainingPlan.Builder:
    all_files = [it for it in BatchFile.load(examples_folder) if not timing or it.unix_time in timing]
    test_i = int(len(all_files)*0.05)
    val_files = all_files[:-test_i:6]
    train_files = [it for it in all_files[:-test_i] if it not in val_files]
    test_files = all_files[-test_i:]
    return builder.with_batches(name='train', batches=Batches(train_files, merge=merge), stats=make_stats('train'), backward=True)\
        .with_batches(name='val', batches=Batches(val_files, merge=merge), stats=make_stats('val'), backward=False)\
        .with_batches(name='test', batches=Batches(test_files, merge=merge), stats=make_stats('train'), backward=False)

def add_triggers(
    builder: TrainingPlan.Builder,
    checkpoints_folder: Path,
    initial_lr: float,
    lr_steps = [(100, 10), (0.4, 5), (0.35, 2), (0.3, 1), (0.25, 0.5), (0.2, 0.1), (0.15, 0.05), (0.1, 0.01)]
) -> TrainingPlan.Builder:
    builder.when(AlwaysTrigger())\
        .then(StatHistoryAction())
    
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
