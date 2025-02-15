from __future__ import annotations
import torch
import logging
from tqdm import tqdm
from pathlib import Path
from typing import Callable, NamedTuple
from .stats import StatContainer
from .utils import Batches

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
            self.kind = group
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
            stats = [it.stats for it in plan.batch_groups if it.name == self.kind][0]
            prev = self.prev_in_bounds
            cur = self.in_bounds(stats[self.key])
            self.prev_in_bounds = cur
            if self.is_trigger(prev, cur):
                logger.info(f"Triggered stat trigger for '{self.kind}.{self.key}', with value {stats[self.key]}.")
                self.triggered = True
                return True
            else: return False
        
    class StatHistoryTrigger(Trigger):
        def __init__(self,
            key: str,
            kind: str = 'val',
            count: int = 10,
            criteria: Callable[[list[object]], bool] = lambda values: True,
            trigger_once: bool = True
        ):
            self.key = key
            self.kind = kind
            self.count = count
            self.criteria = criteria
            self.trigger_once = trigger_once
            self.triggered = False
        def check(self, plan) -> bool:
            if self.triggered and self.trigger_once: return False
            if STAT_HISTORY not in plan.data: return False
            history = plan.data[STAT_HISTORY]
            if len(history) < self.count: return False
            values = [it[self.kind][self.key] for it in history[-self.count:]]
            if self.criteria(values):
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
                'data': plan.data
            }
            torch.save(save_dict, self.path)
        def restore(self, plan: TrainingPlan):
            if not self.primary: return
            if not self.path.exists():
                logger.info(f"No prior state, starting from scratch.")
                return
            save_dict = torch.load(self.path, weights_only=True)
            plan.model.load_state_dict(save_dict['model_state_dict'])
            plan.optimizer.load_state_dict(save_dict['optimizer_state_dict'])
            plan.epoch = save_dict['epoch'] + 1
            plan.data = {**plan.data, **save_dict['data']}
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

    class _ActionBuilder:
        def __init__(self, plan: TrainingPlan, rule: TrainingPlan._Rule):
            self.plan = plan
            self.rule = rule
        def then(self, action: TrainingPlan.Action) -> TrainingPlan._ActionBuilder:
            self.rule.actions.append(action)
            return self
        
    def when(self, trigger: TrainingPlan.Trigger):
        rule = TrainingPlan._Rule(trigger)
        self.rules.append(rule)
        return TrainingPlan._ActionBuilder(self, rule)
    #endregion

    rules: list[_Rule]
    class _BatchGroup(NamedTuple):
        name: str
        batches: Batches
        stats: StatContainer
        backward: bool = False
    batch_groups: list[_BatchGroup]
    primary_checkpoint: CheckpointAction
    def __init__(self, model: torch.nn.Module):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.dtype = torch.float32
        self.epoch = 1
        self.model = model.to(device=self.device, dtype=self.dtype)
        self.data = {}
        self.batch_groups = []
        self.rules = []
        self.stop = False
        self.primary_checkpoint = None

    def with_optimizer(self, optimizer: torch.optim.Optimizer) -> TrainingPlan:
        self.optimizer = optimizer
        return self
    
    def with_batches(self, name: str, batches: Batches, stats: StatContainer, backward: bool = False) -> TrainingPlan:
        self.batch_groups.append(TrainingPlan._BatchGroup(name, batches.to(device = self.device, dtype = self.dtype), stats, backward=backward))
        return self
    
    def with_primary_checkpoint(self, checkpoint: CheckpointAction) -> TrainingPlan:
        self.primary_checkpoint = checkpoint
        return self

    def run(self, max_epoch = 10000000):
        try:
            logger.info(f"Running loop on device: {self.device}.")
            logger.info(f"Using {len(self.batch_groups)} batch groups.")
            for entry in self.batch_groups:
                logger.info(f"Batch group {entry.name} with {len(entry.batches)} batches.")
            logger.info(f"Model {type(self.model).__module__}.{type(self.model).__name__}.")
            logger.info(f"Optimizer {type(self.optimizer)}.")
            if self.primary_checkpoint: self.primary_checkpoint.restore(self)
            while not self.stop and self.epoch < max_epoch:
                for batch_group in self.batch_groups:
                    if batch_group.backward:
                        self.model.train()
                        with tqdm(batch_group.batches, desc=f"Epoch {self.epoch} ({batch_group.name})", leave=False) as bar:
                            for batch in bar:
                                input = batch[:-1]
                                expect = batch[-1]
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