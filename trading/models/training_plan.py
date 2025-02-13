from __future__ import annotations
import torch
import logging
from tqdm import tqdm
from pathlib import Path
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
        def check_initial(self, plan: TrainingPlan) -> bool:
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
        def check_initial(self, plan):
            return any([it.check_initial(plan) for it in self.criteria])
    class AndTrigger(Trigger):
        def __init__(self, *args):
            self.criteria = list(args)
        def check(self, plan):
            return all([it.check(plan) for it in self.criteria])
        def check_initial(self, plan):
            return all([it.check_initial(plan) for it in self.criteria])
        
    class StatTrigger(Trigger):
        def __init__(self,
            key: str,
            use_val: bool = True, #or train
            lower_bound: float | None = None,
            lower_bound_inclusive: bool = True,
            upper_bound: float | None = None,
            upper_bound_inclusive: bool = True,
            event: str = 'enter', #enter/exit/both/in/out
            trigger_once: bool = False
        ):
            self.key = key
            self.use_val = use_val
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
            stats = plan.val_stats if self.use_val else plan.train_stats
            prev = self.prev_in_bounds
            cur = self.in_bounds(stats[self.key])
            self.prev_in_bounds = cur
            if self.is_trigger(prev, cur):
                logger.info(f"Triggered stat trigger for '{self.key}' ({'val' if self.use_val else 'train'}), with value {stats[self.key]}.")
                self.triggered = True
                return True
            else: return False
    
    class EpochTrigger(Trigger):
        def __init__(self, threshold: int, trigger_once: bool = True):
            self.threshold = threshold
            self.trigger_once = trigger_once
            self.triggered = False
        def _check(self, epoch: int):
            if self.trigger_once and self.triggered: return False
            if epoch >= self.threshold:
                logger.info(f"Triggered EpochTrigger for epoch {epoch}.")
                self.triggered = True
                return True
            else: return False
        def check(self, plan):
            return self._check(plan.epoch)
        def check_initial(self, plan):
            return self._check(plan.epoch-1)
        
    class AlwaysTrigger(Trigger):
        def __init__(self, trigger_once = False, trigger_initial = True):
            self.trigger_once = trigger_once
            self.triggered = False
            self.trigger_initial =trigger_initial
        def check(self, plan):
            if self.triggered and self.trigger_once:
                return False
            return True
        def check_initial(self, plan):
            if self.trigger_initial: return self.check(plan)
            return False
        
    class Action:
        def execute(self, plan: TrainingPlan):
            pass
        def execute_initial(self, plan: TrainingPlan):
            pass
    
    class StatHistoryAction(Action):
        def execute(self, plan: TrainingPlan):
            if STAT_HISTORY not in plan.data:
                plan.data[STAT_HISTORY] = []
            entry = {
                'train': plan.train_stats.to_dict(),
                'val': plan.val_stats.to_dict(),
                'epoch': plan.epoch
            }
            if plan.test_stats and plan.test_batches: entry['test'] = plan.test_stats.to_dict()
            plan.data[STAT_HISTORY].append(entry)
    
    class CheckpointAction(Action):
        def __init__(self, path: Path, primary: bool = False):
            self.path = path
            self.primary = primary
            path.parent.mkdir(parents=True, exist_ok=True)
        def execute(self, plan: TrainingPlan):
            if not self.primary and self.path.exists():
                logger.info(f"Skipping non primary checkpoint because '{self.path}' already exists.")
                return
            save_dict = {
                'model_state_dict': plan.model.state_dict(),
                'optimizer_state_dict': plan.optimizer.state_dict(),
                'epoch': plan.epoch,
                'data': plan.data
            }
            torch.save(save_dict, self.path)
            if not self.primary:
                logger.info(f"Saved non primary checkpoint to '{self.path}'.")
        def execute_initial(self, plan: TrainingPlan):
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
        def execute_initial(self, plan: TrainingPlan):
            self.execute(plan)

    class _Rule(Action):
        actions: list[TrainingPlan.Action]
        def __init__(self, criteria: TrainingPlan.Trigger):
            self.criteria = criteria
            self.actions = []
        def execute(self, plan: TrainingPlan):
            if self.criteria.check(plan):
                for action in self.actions:
                    action.execute(plan)
        def execute_initial(self, plan: TrainingPlan):
            if self.criteria.check_initial(plan):
                for action in self.actions:
                    action.execute_initial(plan)

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
    def __init__(self, model: torch.nn.Module):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.dtype = torch.float32
        self.epoch = 1
        self.model = model.to(device=self.device, dtype=self.dtype)
        self.data = {}
        self.rules = []

    def with_optimizer(self, optimizer: torch.optim.Optimizer) -> TrainingPlan:
        self.optimizer = optimizer
        return self
    
    def with_stats(self, train_stats: StatContainer, val_stats: StatContainer, test_stats: StatContainer|None = None) -> TrainingPlan:
        self.train_stats = train_stats
        self.val_stats = val_stats
        self.test_stats = test_stats
        return self
    
    def with_batches(self, train_batches: Batches, val_batches: Batches, test_batches: Batches|None = None) -> TrainingPlan:
        self.train_batches = train_batches.to(device = self.device, dtype = self.dtype)
        self.val_batches = val_batches.to(device = self.device, dtype = self.dtype)
        self.test_batches = test_batches and test_batches.to(device = self.device, dtype = self.dtype)
        return self

    def run(self, max_epoch = 10000):
        try:
            logger.info(f"Running loop on device: {self.device}.")
            logger.info(f"Using {len(self.train_batches)} training and {len(self.val_batches)} validation batches.")
            logger.info(f"Model {type(self.model).__module__}.{type(self.model).__name__}.")
            logger.info(f"Optimizer {type(self.optimizer)}.")
            for rule in self.rules: rule.execute_initial(self)
            while self.epoch < max_epoch:
                self.model.train()
                with tqdm(self.train_batches, desc=f"Epoch {self.epoch}", leave=True) as bar:
                    for batch in bar:
                        input = batch[:-1]
                        expect = batch[-1]
                        self.optimizer.zero_grad()
                        output = self.model(*input).squeeze()
                        loss = self.train_stats.update(expect, output)
                        loss.backward()
                        self.optimizer.step()
                        bar.set_postfix_str(str(self.train_stats))

                self.model.eval()
                with torch.no_grad():
                    with tqdm(self.val_batches, desc = 'Validation...', leave=False) as bar:
                        for batch in bar:
                            input = batch[:-1]
                            expect = batch[-1]
                            output = self.model(*input).squeeze()
                            self.val_stats.update(expect, output)
                    print(f"Validation: {self.val_stats}")
                    if self.test_batches and self.test_stats:
                        with tqdm(self.test_batches, desc = 'Testing...', leave=False) as bar:
                            for batch in bar:
                                input = batch[:-1]
                                expect = batch[-1]
                                output = self.model(*input).squeeze()
                                self.test_stats.update(expect, output)
                        print(f"Testing: {self.test_stats}")
                for rule in self.rules: rule.execute(self)
                self.train_stats.clear()
                self.val_stats.clear()
                if self.test_stats: self.test_stats.clear()
                self.epoch += 1

            logger.info(f"Stopped at epoch {self.epoch}")
        except:
            logger.error(f"Error running loop.", exc_info=True)