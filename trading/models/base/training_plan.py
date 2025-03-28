#2
from __future__ import annotations
import functools
from typing import Callable, Iterable, Literal, NamedTuple, Sequence, TypedDict, final, override
import torch
from torch import Tensor
import logging
from tqdm import tqdm
from pathlib import Path
from matplotlib import pyplot as plt
from base.types import get_full_classname
from base import equatable, plotutils, serializable
from base.serialization import Serializable
from trading.models.base.stats import StatContainer
from trading.models.base.batches import BatchFile, Batches
from trading.models.base.abstract_model import AbstractModel
from trading.models.base.tensors import get_sampled

logger = logging.getLogger(__name__)

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
        return plan.history[-1][self.group][self.key]
    
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
    def _check(self, plan: TrainingPlan) -> bool:
        if len(plan.history) < self.count: return False
        values = [it[self.group][self.key] for it in plan.history[-self.count:]]
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

class BatchGroupConfig:
    def __init__(self,
        name: str,
        ratio: float,
        merge: int,
        sampling: list[tuple[float|tuple[float,float], float]]|None = None,
        backward: bool = False
    ):
        self.name = name
        self.ratio = ratio
        self.merge = merge
        self.sampling = sampling
        self.backward = backward

class BatchGroup:
    def __init__(self, config: BatchGroupConfig, batches: Batches):
        self.config = config
        self.batches = batches

class TrainingPlan:
    """
    - Keeps track of statistics, the model, and optimizer.
    - Runs the training loop.
    - Allows actions to be triggered based on conditions, evaluated at the end of each epoch.
        This includes learning rate updates, saving checkpoints and custom actions derived from Action.
    """
    model: AbstractModel
    optimizer: torch.optim.Optimizer
    device: torch.device
    dtype: torch.dtype

    folders: list[Path]
    batch_group_configs: list[BatchGroupConfig]

    rules: list[Rule]
    primary_checkpoint: CheckpointAction|None
    stats: StatContainer
    history: list[dict]

    epoch: int
    data: dict
    stop: bool

    def state_dict(self) -> dict:
        return {
            'model': self.model.state_dict(),
            'optimizer': self.optimizer.state_dict(),
            'stat_history': self.history,
            'rules': [it.state_dict() for it in self.rules],
            'epoch': self.epoch,
            'data': self.data,
            'stop': self.stop
        }
    def load_state_dict(self, data: dict):
        self.model.load_state_dict(data['model'])
        self.optimizer.load_state_dict(data['optimizer'])
        self.history = data['stat_history']
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
            self.plan.folders = []
            self.plan.batch_group_configs = []
            self.plan.rules = []
            self.plan.primary_checkpoint = None
            self.plan.history = []
            self.plan.epoch = 1
            self.plan.data = {}
            self.plan.stop = False
        
        def with_optimizer(self, optimizer: torch.optim.Optimizer) -> TrainingPlan.Builder:
            self.plan.optimizer = optimizer
            return self
        
        def with_folders(self, *args: Path) -> TrainingPlan.Builder:
            self.plan.folders.extend(args)
            return self

        def with_batch_groups(self, *args: BatchGroupConfig):
            self.plan.batch_group_configs.extend(args)
            return self
        
        def with_stats(self, stats: StatContainer):
            self.plan.stats = stats
            return self
    
        def when(self, trigger: Trigger) -> TrainingPlan._ActionBuilder:
            rule = Rule(trigger)
            self.plan.rules.append(rule)
            return TrainingPlan._ActionBuilder(rule)

        def with_primary_checkpoint(self, checkpoint: CheckpointAction) -> TrainingPlan.Builder:
            self.plan.primary_checkpoint = checkpoint
            return self
        
        def build(self) -> TrainingPlan:
            if not hasattr(self.plan, 'optimizer')\
                or not self.plan.batch_group_configs\
                or not self.plan.folders\
                or not hasattr(self.plan, 'stats'):
                raise Exception(f"The plan has not been properly initialized.")
            if self.plan.primary_checkpoint: self.plan.primary_checkpoint.restore(self.plan)
            return self.plan

    def run(self, max_epoch = 10000000):
        batch_groups = self.create_batch_groups()
        try:
            logger.info(f"Running loop on device: {self.device}.")
            logger.info(f"Using {len(batch_groups)} batch groups.")
            for entry in batch_groups:
                logger.info(f"Batch group {entry.config.name} with {len(entry.batches)} batches.")
            logger.info(f"Model {get_full_classname(self.model)}.")
            logger.info(f"Optimizer {type(self.optimizer)}.")
            
            while not self.stop and self.epoch < max_epoch:
                logger.info(f"Running epoch {self.epoch}")
                print(f"---------EPOCH {self.epoch}-------------------------------------")
                stat_frame: dict = {'epoch': self.epoch}
                for batch_group in batch_groups:
                    self.stats.clear()
                    if batch_group.config.backward:
                        self.model.train()
                        with tqdm(batch_group.batches, desc=f"Epoch {self.epoch} ({batch_group.config.name})", leave=True) as bar:
                            for batch in bar:
                                input, expect = self.model.extract_tensors(batch, with_output=True)
                                if batch_group.config.sampling:
                                    sample = get_sampled(expect, batch_group.config.sampling)
                                    input = {key: value[sample] for key,value in input.items()}
                                    expect = expect[sample]
                                    logger.info(f"Using sample of {sample.shape[0]} ({sample.sum().item()/sample.shape[0]*100:.1f}%) for batch group '{batch_group.config.name}'.")
                                self.optimizer.zero_grad()
                                output: Tensor = self.model(*input).squeeze()
                                loss = self.stats.update(expect, output)
                                loss.backward()
                                self.optimizer.step()
                                bar.set_postfix_str(str(self.stats))
                        print(f"Training group '{batch_group.config.name}' stats: {self.stats}")
                    else:
                        self.model.eval()
                        with torch.no_grad():
                            with tqdm(batch_group.batches, desc = f"Evaluating '{batch_group.config.name}' batches...", leave=False) as bar:
                                for batch in bar:
                                    input, expect = self.model.extract_tensors(batch)
                                    if batch_group.config.sampling:
                                        sample = get_sampled(expect, batch_group.config.sampling)
                                        input = {key: value[sample] for key,value in input.items()}
                                        expect = expect[sample]
                                    output = self.model(*input).squeeze()
                                    self.stats.update(expect, output)
                            print(f"Evaluation group '{batch_group.config.name}' stats: {self.stats}")
                    stat_frame[batch_group.config.name] = self.stats.to_dict()

                self.history.append(stat_frame)
                for rule in self.rules: rule.execute(self)
                if self.primary_checkpoint: self.primary_checkpoint.save(self)
                self.epoch += 1

            logger.info(f"Stopped at epoch {self.epoch}")
        except:
            logger.error(f"Error running loop.", exc_info=True)

    def plot_history(self):
        history = self.history
        epochs = [it['epoch'] for it in history]
        groups = [it.name for it in self.batch_group_configs]

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

    def create_batch_groups(self) -> list[BatchGroup]:
        files: list[BatchFile] = []
        files = functools.reduce(lambda files, folder: files +  BatchFile.load(folder), self.folders, files)
        files = sorted(files, key=lambda it: it.unix_time)

        files = [it for it in files if self.model.config.timing.contains(it.unix_time, it.exchange.calendar)]

        total = sum(it.ratio for it in self.batch_group_configs)
        counts = [int(it.ratio/total*len(files)) for it in self.batch_group_configs]
        for i in range(len(files)-sum(counts)): counts[i] += 1
        result: list[BatchGroup] = []
        for count, config in zip(counts, self.batch_group_configs):
            result.append(BatchGroup(config, Batches(files[:count], merge=config.merge, device = self.device, dtype = self.dtype)))
            files = files[count:]
        return result
    