from __future__ import annotations
import torch
import logging
import math
from tqdm import tqdm
from pathlib import Path
from typing import Callable, NamedTuple
from matplotlib import pyplot as plt
from ..utils import plotutils
from .stats import StatContainer
from .utils import Batches
from .abstract import AbstractModel

logger = logging.getLogger(__name__)
STAT_HISTORY = 'stat_history'

class TrainingPlan:
    """
    - Keeps track of statistics, the model, and optimizer.
    - Runs the training loop.
    - Allows actions to be triggered based on conditions, evaluated at the end of each epoch.
        This includes learning rate updates, saving checkpoints and custom actions derived from Action.
    """

    #region Rules
    class Trigger:
        def check(self, plan: TrainingPlan) -> bool:
            return False
        def __or__(self, value):
            return TrainingPlan.OrTrigger(self, value)
        def __and__(self, value):
            return TrainingPlan.AndTrigger(self, value)
    class OrTrigger(Trigger):
        def __init__(self, *args):
            self.criteria = list(args)
        def check(self, plan: TrainingPlan):
            return any([it.check(plan) for it in self.criteria])
    class AndTrigger(Trigger):
        def __init__(self, *args):
            self.criteria = list(args)
        def check(self, plan):
            return all([it.check(plan) for it in self.criteria])
        
    class StatTrigger(Trigger):
        def __init__(self,
            key: str,
            group: str = 'val',
            lower_bound: float | None = None,
            lower_bound_inclusive: bool = True,
            upper_bound: float | None = None,
            upper_bound_inclusive: bool = True,
            event: str = 'enter', #enter/exit/both/in/out
            trigger_once: bool = False
        ):
            self.key = key
            self.group = group
            self.lower_bound = lower_bound
            self.upper_bound = upper_bound
            self.trigger_once = trigger_once
            self.triggered = False
            self.prev_in_bounds = None

            lower_bound = lower_bound if lower_bound is None or lower_bound_inclusive else lower_bound + 1e-10
            upper_bound = upper_bound if upper_bound is None or upper_bound_inclusive else upper_bound - 1e-10
            if not lower_bound and not upper_bound: self.in_bounds = lambda it: None
            elif not lower_bound: self.in_bounds = lambda it: it <= upper_bound
            elif not upper_bound: self.in_bounds = lambda it: it >= lower_bound
            else: self.in_bounds = lambda it: (it <= upper_bound and it >= lower_bound)

            if event == 'enter': self.is_trigger = lambda prev, cur: not prev and cur
            elif event == 'exit': self.is_trigger = lambda prev, cur: (prev is None or prev) and not cur
            elif event == 'both': self.is_trigger = lambda prev, cur: prev is None or prev ^ cur
            elif event == 'in': self.is_trigger = lambda prev, cur: cur
            elif event == 'out': self.is_trigger = lambda prev, cur: not cur
            else: raise Exception(f'Unknown event {event}.')

        def check(self, plan) -> bool:
            if self.trigger_once and self.triggered: return False
            stats = [it.stats for it in plan.batch_groups if it.name == self.group][0]
            prev = self.prev_in_bounds
            cur = self.in_bounds(stats[self.key])
            self.prev_in_bounds = cur
            if self.is_trigger(prev, cur):
                logger.info(f"Triggered stat trigger for '{self.group}.{self.key}', with value {stats[self.key]}. (lower bound {self.lower_bound}, upper bound {self.upper_bound})")
                self.triggered = True
                return True
            else: return False
        
    class StatHistoryTrigger(Trigger):
        def __init__(self,
            key: str,
            group: str = 'val',
            count: int = 10,
            criteria: Callable[[list[object]], bool] = lambda values: True,
            trigger_once: bool = True,
            desc: str|None = None
        ):
            self.key = key
            self.group = group
            self.count = count
            self.criteria = criteria
            self.trigger_once = trigger_once
            self.triggered = False
            self.desc = desc
        def check(self, plan) -> bool:
            if self.triggered and self.trigger_once: return False
            if STAT_HISTORY not in plan.data: return False
            history = plan.data[STAT_HISTORY]
            if len(history) < self.count: return False
            values = [it[self.group][self.key] for it in history[-self.count:]]
            if self.criteria(values):
                logger.info(f"Triggered stat history trigger ({self.desc or 'no description'}) with values {values}.")
                self.triggered = True
                return True
            return False
    
    class EpochTrigger(Trigger):
        def __init__(self, threshold: int, trigger_once: bool = True):
            self.threshold = threshold
            self.trigger_once = trigger_once
            self.triggered = False
        def check(self, plan: TrainingPlan):
            epoch = plan.epoch
            if self.trigger_once and self.triggered: return False
            if epoch >= self.threshold:
                logger.info(f"Triggered EpochTrigger for epoch {epoch}.")
                self.triggered = True
                return True
            else: return False
        
    class AlwaysTrigger(Trigger):
        def __init__(self, trigger_once = False):
            self.trigger_once = trigger_once
            self.triggered = False
        def check(self, plan):
            if self.triggered and self.trigger_once:
                return False
            self.triggered = True
            return True
        
    class Action:
        def execute(self, plan: TrainingPlan):
            pass
    
    class StatHistoryAction(Action):
        def execute(self, plan: TrainingPlan):
            if STAT_HISTORY not in plan.data:
                plan.data[STAT_HISTORY] = []
            entry = {it.name:it.stats.to_dict() for it in plan.batch_groups}
            entry['epoch'] = plan.epoch
            plan.data[STAT_HISTORY].append(entry)
    
    class CheckpointAction(Action):
        def __init__(self, path: Path):
            self.path = path
            path.parent.mkdir(parents=True, exist_ok=True)
        def execute(self, plan: TrainingPlan):
            if self.path.exists():
                logger.info(f"Skipping non primary checkpoint because '{self.path}' already exists.")
            else:
                self.save(plan)
                logger.info(f"Saved checkpoint to '{self.path}'.")
        def save(self, plan: TrainingPlan):
            save_dict = {
                'model_state_dict': plan.model.state_dict(),
                'optimizer_state_dict': plan.optimizer.state_dict(),
                'epoch': plan.epoch,
                'data': plan.data,
                'stop': plan.stop
            }
            torch.save(save_dict, self.path)
        def restore(self, plan: TrainingPlan):
            if not self.path.exists():
                logger.info(f"No prior state, starting from scratch.")
                return
            save_dict = torch.load(self.path, weights_only=True)
            plan.model.load_state_dict(save_dict['model_state_dict'])
            plan.optimizer.load_state_dict(save_dict['optimizer_state_dict'])
            plan.epoch = save_dict['epoch'] + 1
            plan.data = {**plan.data, **save_dict['data']}
            if 'stop' in save_dict: plan.stop = save_dict['stop']
            logger.info(f"Loaded state from epoch {save_dict['epoch']}.")
    
    class LearningRateAction(Action):
        def __init__(self, value: float):
            self.value = value
        def execute(self, plan: TrainingPlan):
            for param_group in plan.optimizer.param_groups:
                param_group['lr'] = self.value
            logger.info(f"Updated learning rate to {self.value}.")

    class StopAction(Action):
        def execute(self, plan: TrainingPlan):
            plan.stop = True

    class _Rule(Action):
        actions: list[TrainingPlan.Action]
        def __init__(self, criteria: TrainingPlan.Trigger):
            self.criteria = criteria
            self.actions = []
        def execute(self, plan: TrainingPlan):
            if self.criteria.check(plan):
                for action in self.actions:
                    action.execute(plan)
    #endregion

    class _BatchGroup(NamedTuple):
        name: str
        batches: Batches
        stats: StatContainer
        backward: bool = False

    device: str
    dtype: str
    model: AbstractModel
    optimizer: torch.optim.Optimizer
    batch_groups: list[_BatchGroup]
    rules: list[_Rule]
    primary_checkpoint: CheckpointAction
    epoch: int
    data: dict
    stop: bool

    class _ActionBuilder:
        def __init__(self, rule: TrainingPlan._Rule):
            self.rule = rule
        def then(self, action: TrainingPlan.Action) -> TrainingPlan._ActionBuilder:
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
            self.plan.batch_groups.append(TrainingPlan._BatchGroup(name, batches.to(device = self.plan.device, dtype = self.plan.dtype), stats, backward=backward))
            return self
    
        def when(self, trigger: TrainingPlan.Trigger) -> TrainingPlan._ActionBuilder:
            rule = TrainingPlan._Rule(trigger)
            self.plan.rules.append(rule)
            return TrainingPlan._ActionBuilder(rule)

        def with_primary_checkpoint(self, checkpoint: TrainingPlan.CheckpointAction) -> TrainingPlan.Builder:
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
            logger.info(f"Model {type(self.model).__module__}.{type(self.model).__name__}.")
            logger.info(f"Optimizer {type(self.optimizer)}.")
            
            while not self.stop and self.epoch < max_epoch:
                logger.info(f"Running epoch {self.epoch}")
                for batch_group in self.batch_groups:
                    if batch_group.backward:
                        self.model.train()
                        with tqdm(batch_group.batches, desc=f"Epoch {self.epoch} ({batch_group.name})", leave=False) as bar:
                            for batch in bar:
                                tensors = self.model.extract_tensors(batch)
                                input = tensors[:-1]
                                expect = tensors[-1]
                                self.optimizer.zero_grad()
                                output = self.model(*input).squeeze()
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
                                    input = batch[:-1]
                                    expect = batch[-1]
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
            