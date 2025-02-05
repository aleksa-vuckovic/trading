from __future__ import annotations
import torch
import math
import logging
import re
import os
from pathlib import Path
from enum import Enum
from pathlib import Path
from tqdm import tqdm
from typing import Callable, Any
from matplotlib import pyplot as plt
from datetime import timedelta
from ..utils import dateutils

logger = logging.getLogger(__name__)
STAT_HISTORY = 'stat_history'

def get_batch_files(path: Path) -> list[dict]:
    pattern = re.compile(r"([^_]+)_batch(\d+)-(\d+).pt")
    files = [ pattern.fullmatch(it) for it in os.listdir(path)]
    files = [ {'file': path / it.group(0), 'source': it.group(1), 'batch': int(it.group(2)), 'hour': int(it.group(3))} for it in files if it ]
    return sorted(files, key=lambda it: (it['source'], it['hour'], it['batch']))

def check_tensors(tensors: list[torch.Tensor] | dict[object, torch.Tensor], allow_zeros=True):
    if isinstance(tensors, list):
        for tensor in tensors: check_tensor(tensor)
    elif isinstance(tensors, dict):
        for tensor in tensors.values(): check_tensor(tensor)
def check_tensor(tensor: torch.Tensor, allow_zeros=True):
    result = tensor.isnan() | tensor.isinf()
    if not allow_zeros: result = result | (tensor == 0)
    bad_entries = result.sum().item()
    if bad_entries > 0:
        raise Exception(f"Found {bad_entries} unwanted inf, nan {'or 0 ' if allow_zeros else ''} values in tensors.")

def get_next_time(unix_time: float, hour: int | None = None) -> float:
    time = dateutils.unix_to_datetime(unix_time, tz = dateutils.ET)
    if time.minute or time.second or time.microsecond:
        time = time.replace(minute=0, second=0, microsecond=0)
        time = time + timedelta(hours = 1)
    if dateutils.is_weekend_datetime(time) or time.hour >= (hour or 16):
        time = time.replace(hour = hour or 9)
        time += timedelta(days=1)
        while dateutils.is_weekend_datetime(time):
            time += timedelta(days=1)
    elif time.hour < (hour or 9):
        time = time.replace(hour = hour or 9)
    else:
        time = time.replace(hour = time.hour + 1)
    return time.timestamp()

def get_prev_time(unix_time: float, hour: int | None = None) -> float:
    time = dateutils.unix_to_datetime(unix_time, tz = dateutils.ET)
    if time.minute or time.second or time.microsecond:
        time = time.replace(minute = 0, second = 0, microsecond = 0)
        time = time + timedelta(hours = 1)
    if dateutils.is_weekend_datetime(time) or time.hour <= (hour or 9):
        time = time.replace(hour = hour or 16)
        time -= timedelta(days=1)
        while dateutils.is_weekend_datetime(time):
            time -= timedelta(days = 1)
    elif time.hour > (hour or 16):
        time = time.replace(hour = hour or 16)
    else:
        time = time.replace(hour = time.hour - 1)
    return time.timestamp()

def relativize_in_place(tensor: torch.Tensor, start_index: int = 0, count: int = -1, dim: int = 0, use_previous: bool = False):
    """
    Process a time series by calculating relative difference between adjacent entries.
    The first entry is considered to have a 0% change, unless use_previous is set to True,
        in which case the entry before start_index is used.
    """
    total_dims = len(tensor.shape)
    if dim >= total_dims:
        raise Exception(f"Dimension {dim} not valid for shape {tensor.shape}.")
    i = start_index
    j = start_index+count if count>0 else tensor.shape[dim]
    if tensor.shape[dim] < j or j <= i:
        raise Exception(f"Slice {i}:{j} not valid for dimension {dim} of shape {tensor.shape}.")
    index = tuple()
    stepback_index = tuple()
    for it in range(total_dims):
        if dim == it:
            index += (slice(i,j),)
            stepback_index += (slice(i-1 if use_previous else i,j-1),)
        else:
            index += (slice(None),)
            stepback_index += (slice(None),)
    current = tensor[index]
    stepback = tensor[stepback_index]
    if not use_previous:
        fill_index = list(stepback_index)
        fill_index[dim] = slice(0,1)
        stepback = torch.cat([stepback[tuple(fill_index)], stepback], dim=dim)
    tensor[index] = (current - stepback) / stepback

def normalize_in_place(tensor: torch.Tensor, start_index: int = 0, count: int = -1, dim: int = 0) -> torch.Tensor:
    """
    Divides elements from start_index by the value of the largest element.
    For each batch separately.
    Returns the array of values used the normalize each batch, of shape (batches,)
    """
    total_dims = len(tensor.shape)
    if dim >= total_dims:
        raise Exception(f"Dimension {dim} not valid for shape {tensor.shape}.")
    i = start_index
    j = start_index+count if count>=0 else tensor.shape[dim]
    if tensor.shape[dim] < j:
        raise Exception(f"End index {j} not valid form dimension {dim} of shape {tensor.shape}.")
    index = tuple()
    for it in range(total_dims):
        if dim == it:
            index += (slice(i,j),)
        else:
            index += (slice(None),)
    maxes, indices = torch.max(tensor[index], dim=dim, keepdim=True)
    tensor[index] = tensor[index] / maxes
    return maxes

class PriceTarget(Enum):
    LINEAR_0_5 = 'Linear 0 to 5%'
    LINEAR_0_10 = 'Linear 0 to 10%'
    LINEAR_5_5 = 'Linear -5 to 5%'
    LINEAR_10_10 = 'Linear -10 to 10%'
    SIGMOID_0_5 = 'Sigmoid 0 to 5%'
    SIGMOID_0_10 = 'Sigmoid 0 to 10%'
    TANH_5_5 = 'Tanh -5 to 5%'
    TANH_10_10 = 'Tanh -10 to 10%'

    def get_price(self, normalized_values: torch.Tensor):
        x = normalized_values
        if self == PriceTarget.LINEAR_0_5:
            return torch.clamp(x, min=0, max=0.05)
        if self == PriceTarget.LINEAR_0_10:
            return torch.clamp(x, min=0, max=0.1)
        if self == PriceTarget.LINEAR_5_5:
            return torch.clamp(x, min=-0.05, max=0.05)
        if self == PriceTarget.LINEAR_10_10:
            return torch.clamp(x, min=-0.1, max=0.1)
        if self == PriceTarget.SIGMOID_0_5:
            x = torch.exp(300*x-6)
            return x/(1+x)
        if self == PriceTarget.SIGMOID_0_10:
            x = torch.exp(150*x-7.5)
            return x/(1+x)
        if self == PriceTarget.TANH_5_5:
            x = torch.exp(-150*x)
            return (1-x)/(1+x)
        if self == PriceTarget.TANH_10_10:
            x = torch.exp(-60*x)
            return (1-x)/(1+x)
        raise Exception("Unknown price target type")
    
    @staticmethod
    def plot():
        x = torch.linspace(-0.15, 0.15, 100, dtype=torch.float32)
        for i, pt in enumerate(PriceTarget):
            fig = plt.figure(i // 4)
            fig.suptitle(f'Window {i//4}')
            axes = fig.add_subplot(2,2,i%4 + 1)
            axes.plot(x, pt.get_price(x), label=pt.name)
            axes.set_title(pt.name)
            axes.grid(True)

        [plt.figure(it).tight_layout() for it in plt.get_fignums()]
        plt.show()

class Batches:
    def __init__(self, files: list[str | Path], merge: int = 1, extract_tensors: Callable|None = None, device: str = "cpu", dtype = torch.float32):
        self.files = files
        self.merge = merge
        self.extract_tensors = extract_tensors
        self.device = device
        self.dtype = dtype

    def to(self, device: str | None = None, dtype: str | None = None):
        if device: self.device = device
        if dtype: self.dtype = dtype
        return self
    
    def __len__(self):
        return math.ceil(len(self.files)/self.merge)

    class Iterator:
        def __init__(self, batches):
            self.batches = batches
            self.i = 0
        def __next__(self):
            if self.i >= len(self.batches.files):
                raise StopIteration()
            files = self.batches.files[self.i:self.i+self.batches.merge]
            data = [torch.load(it, weights_only=True) for it in files]
            if isinstance(data[0], dict):
                data = {key:torch.cat([it[key] for it in data], dim=0).to(device=self.batches.device, dtype=self.batches.dtype) for key in data[0].keys()}
                shapes = {key:data[key].shape for key in data.keys()}
                logger.debug(f"Loaded batch with shape {shapes}")
            else:
                data = torch.cat([torch.load(it, weights_only=True) for it in files], dim=0).to(device = self.batches.device, dtype=self.batches.dtype)
                logger.debug(f"Loaded batch with shape {data.shape}")
            if self.batches.extract_tensors:
                data = self.batches.extract_tensors(data)
            self.i += len(files)
            return data

    def __iter__(self):
        return Batches.Iterator(self)

class StatCollector:
    def __init__(self, name: str):
        self.name = name
        self.clear()
    
    def update(self, expect: torch.Tensor, output: torch.Tensor) -> torch.Tensor | float | int:
        result = self._calculate(expect, output)
        if isinstance(result, (int, float)): self.__update(result)
        else: self.__update(float(result.item()))
        return result

    def _calculate(self, expect: torch.Tensor, output: torch.Tensor) -> torch.Tensor | float | int:
        pass

    def __update(self, value: float | int):
        self.last = value
        self.count += 1
        self.total += value
        self.running = self.total / self.count
    
    def clear(self):
        self.last = None
        self.count = 0
        self.total = 0
        self.running = 0

    def to_dict(self) -> dict:
        return {'last': self.last, 'count': self.count, 'running': self.running}
    
    def __str__(self):
        return f"{self.name}={self.running:.4f}({self.last:.2f})"

class StatContainer:
    stats: dict[str, StatCollector]
    primary: str
    def __init__(self, *args, name: str | None = None):
        for arg in args:
            if not isinstance(arg, StatCollector):
                raise Exception(f'Unexpected arg type {type(arg)}')
        self.stats = {it.name:it for it in args}
        self.primary = args[0].name
        self.name = name

    def update(self, expect: torch.Tensor, output: torch.Tensor) -> torch.Tensor | float | int | None:
        result = {key:self.stats[key].update(expect, output) for key in self.stats}
        return result[self.primary]
    
    def clear(self):
        [it.clear() for it in self.stats.values()]

    def __getitem__(self, key):
        return self.stats[key].running
    
    def __contains__(self, key):
        return key in self.stats
    
    def __str__(self):
        return ','.join([str(it) for it in self.stats.values()])
    
    def to_dict(self):
        return {key: self.stats[key].running for key in self.stats}
    
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
            plan.data[STAT_HISTORY].append({'train': plan.train_stats.to_dict(), 'val': plan.val_stats.to_dict(), 'epoch': plan.epoch})
    
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
            logger.info(f"Loaded state from epoch {plan.epoch}.")
    
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
    
    def with_stats(self, train_stats: StatContainer, val_stats: StatContainer) -> TrainingPlan:
        self.train_stats = train_stats
        self.val_stats = val_stats
        return self
    
    def with_batches(self, train_batches: Batches, val_batches: Batches) -> TrainingPlan:
        self.train_batches = train_batches.to(device = self.device, dtype = self.dtype)
        self.val_batches = val_batches.to(device = self.device, dtype = self.dtype)
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
                for rule in self.rules: rule.execute(self)
                self.train_stats.clear()
                self.val_stats.clear()
                self.epoch += 1

            logger.info(f"Stopped at epoch {self.epoch}")
        except:
            logger.error(f"Error running loop.", exc_info=True)