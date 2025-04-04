#3
from __future__ import annotations
import functools
import shutil
import time
import torch
import logging
import gc
from typing import Literal, Sequence, final, override
from pathlib import Path
from tqdm import tqdm
from matplotlib import pyplot as plt
from torch import Tensor
from sqlalchemy import Engine, create_engine, select
from sqlalchemy.orm import Session, Mapped, declarative_base, mapped_column

from base import plotutils
from base.algos import interpolate
from base.serialization import SerializedObject
from base.reflection import get_module, get_full_classname
from trading.models.base.abstract_model import AbstractModel
from trading.models.base.batches import BatchFile, Batches
from trading.models.base.stats import StatContainer
from trading.models.base.model_config import BaseModelConfig
from trading.models.base.tensors import get_sampled

logger = logging.getLogger(__name__)

#region Triggers 'n' Rules
class Trigger:
    def check(self, manager: ModelManager) -> bool: ...
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
    def check(self, manager: ModelManager) -> bool:
        if self.once and self.triggered: return False
        if self._check(manager):
            self.triggered = True
            logger.info(f"Triggered {repr(self)}.")
            return True
        return False
    def _check(self, manager: ModelManager) -> bool: ...
    def state_dict(self) -> dict:
        return {'triggered': self.triggered}
    def load_state_dict(self, data: dict) -> None:
        self.triggered = data['triggered']
    @override
    def __repr__(self) -> str:
        return f"{type(self).__name__}({",".join(f"{key}={value}"for key,value in self.__dict__.items())})"

class OrTrigger(BaseTrigger):
    def __init__(self, *args: Trigger, once: bool = False):
        super().__init__(once)
        self.criteria = list(args)
    @override
    def _check(self, manager: ModelManager) -> bool:
        for it in self.criteria:
            if it.check(manager): return True
        return False
    @override
    def state_dict(self) -> dict:
        return {**super().state_dict(), 'criteria': [it.state_dict() for it in self.criteria]}
    @override
    def load_state_dict(self, data: dict) -> None:
        super().load_state_dict(data)
        for criteria, state_dict in zip(self.criteria, data['criteria']): criteria.load_state_dict(state_dict)
    @override
    def __repr__(self) -> str: return "OrTrigger()"

class AndTrigger(BaseTrigger):
    def __init__(self, *args: Trigger, once: bool = False):
        super().__init__(once)
        self.criteria = list(args)
    @override
    def _check(self, manager: ModelManager) -> bool:
        for it in self.criteria:
            if not it.check(manager): return False
        return True
    @override
    def state_dict(self) -> dict:
        return {**super().state_dict(), 'criteria': [it.state_dict() for it in self.criteria]}
    @override
    def load_state_dict(self, data: dict) -> None:
        super().load_state_dict(data)
        for criteria, state_dict in zip(self.criteria, data['criteria']): criteria.load_state_dict(state_dict)
    @override
    def __repr__(self) -> str: return "AndTrigger()"

class BoundedTrigger(BaseTrigger):
    type Event = Literal['enter','exit','both','in','out']
    in_bounds: bool|None
    def __init__(
        self,
        bounds: tuple[float,float],
        event: Event = 'enter',
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

    def get_value(self, manager: ModelManager) -> float|None: ...
    @override
    def _check(self, manager: ModelManager) -> bool:
        value = self.get_value(manager)
        if value is None: return False
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
        event: BoundedTrigger.Event = 'enter',
        once: bool = False
    ):
        super().__init__(bounds, event, once)
        self.group = group
        self.key = key
        
    @override
    def get_value(self, manager: ModelManager) -> float|None:
        try:
            return manager.train_state.history[-1].stats[self.group][self.key]
        except:
            return None

class StatSlopeTrigger(BoundedTrigger):
    def __init__(self, *,
        key: str = 'loss',
        group: str = 'val',
        epochs: int = 5,
        bounds: tuple[float,float],
        event: BoundedTrigger.Event = 'in',
        once: bool = False
    ):
        super().__init__(bounds, event, once)
        self.key = key
        self.group = group
        self.epochs = epochs
    @override
    def get_value(self, manager: ModelManager) -> float | None:
        if len(manager.train_state.history) < self.epochs: return None
        values = [it.stats[self.group][self.key] for it in manager.train_state.history[-self.epochs:]]
        y_ret = interpolate(list(range(self.epochs)), values, [1,2], method='linear')
        return y_ret[1]-y_ret[0]

class EpochTrigger(BaseTrigger):
    def __init__(self, threshold: int, once: bool = True):
        super().__init__(once)
        self.threshold = threshold
    @override
    def _check(self, manager: ModelManager):
        if manager.train_state.epoch >= self.threshold:
            logger.info(f"Triggered EpochTrigger for epoch {manager.train_state.epoch}.")
            return True
        else: return False
    
class AlwaysTrigger(BaseTrigger):
    def __init__(self, once = False):
        super().__init__(once)
    @override
    def _check(self, manager: ModelManager) -> bool:
        return True
    
class Action:
    def execute(self, manager: ModelManager):
        pass

class CheckpointAction(Action):
    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
    @override
    def execute(self, manager: ModelManager):
        if self.path.exists():
            logger.info(f"Skipping non primary checkpoint because '{self.path}' already exists.")
        else:
            self.save(manager)
            logger.info(f"Saved checkpoint to '{self.path}'.")
    def save(self, manager: ModelManager):
        torch.save(manager.state_dict(), self.path)
    def restore(self, manager: ModelManager):
        if not self.path.exists():
            logger.info(f"No prior state, starting from scratch.")
            return
        state_dict = torch.load(self.path, weights_only=False, map_location=manager.device)
        manager.load_state_dict(state_dict)
        logger.info(f"Loaded state from epoch {manager.train_state.epoch}.")

class LearningRateAction(Action):
    def __init__(self, value: float):
        self.value = value
    @override
    def execute(self, manager: ModelManager):
        for param_group in manager.train_state.optimizer.param_groups:
            param_group['lr'] = self.value
        logger.info(f"Updated learning rate to {self.value}.")

class StopAction(Action):
    @override
    def execute(self, manager: ModelManager):
        manager.train_state.stop = True

class Rule:
    actions: list[Action]
    def __init__(self, criteria: Trigger):
        self.criteria = criteria
        self.actions = []
    def execute(self, manager: ModelManager):
        if self.criteria.check(manager):
            for action in self.actions:
                action.execute(manager)
    def state_dict(self) -> dict:
        return {'criteria': self.criteria.state_dict()}
    def load_state_dict(self, data: dict):
        self.criteria.load_state_dict(data['criteria'])
#endregion

#region Training helpers
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

class TrainConfig:
    rules: list[Rule]
    def __init__(self, inputs: list[Path], batch_group_configs: list[BatchGroupConfig], stats: StatContainer):
        self.inputs = inputs
        self.batch_group_configs = batch_group_configs
        self.stats = stats
        self.rules = []

    class _ActionBuilder:
        def __init__(self, rule: Rule):
            self.rule = rule
        def then(self, action: Action) -> TrainConfig._ActionBuilder:
            self.rule.actions.append(action)
            return self
    
    def when(self, trigger: Trigger) -> TrainConfig._ActionBuilder:
            rule = Rule(trigger)
            self.rules.append(rule)
            return TrainConfig._ActionBuilder(rule)

class BatchGroup:
    def __init__(self, config: BatchGroupConfig, batches: Batches):
        self.config = config
        self.batches = batches

class TrainState:
    epoch: int
    data: dict
    stop: bool
    history: list[HistoryFrame]
    def __init__(self, optimizer: torch.optim.Optimizer):
        self.epoch = 1
        self.data = {}
        self.stop = False
        self.history = []
        self.optimizer = optimizer

class HistoryFrame:
    stats: dict[str, dict[str, float]]
    def __init__(self, epoch: int):
        self.epoch = epoch
        self.unix_time = time.time()
        self.stats = {}
#endregion

#region Storage
Base = declarative_base()

class ModelConfigEntity(Base):
    __tablename__ = "ModelConfigEntity"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    content: Mapped[BaseModelConfig] = mapped_column(SerializedObject(), nullable=False)

"""    def __init__(self, content: ModelConfig):
        self.content = content"""

_DATA = "data"
_DB = "data.db"
_CHECKPOINTS = "checkpoints"
_PRIMARY_CHECKPOINT = "primary_checkpoint.pt"
_BACKTESTS = "backtests"
#endregion

class ModelManager[T: AbstractModel]:
    model: T
    def __init__(self, model: T):
        # basics
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.dtype = torch.float32
        self.model = model
        # get storage paths
        folder = ModelManager.get_folder(type(model))
        self.engine = ModelManager.get_engine(type(model))
        Base.metadata.create_all(self.engine)
        with Session(self.engine) as session:
            entity = session.scalar(select(ModelConfigEntity).where(ModelConfigEntity.content == self.model.config))
            if entity is None:
                entity = ModelConfigEntity(content = model.config)
                session.add(entity)
                session.commit()
            self.id = entity.id
        self.root = folder/_DATA/str(self.id)
        self.checkpoints = self.root/_CHECKPOINTS
        self.primary_checkpoint = self.checkpoints / _PRIMARY_CHECKPOINT
        self.backtests = self.root/_BACKTESTS
        
        # get train state
        self.train_state = TrainState(torch.optim.Adam(self.model.parameters()))
        if self.primary_checkpoint.exists():
            CheckpointAction(self.primary_checkpoint).restore(self)

    def state_dict(self) -> dict:
        return {
            'model': self.model.state_dict(),
            'train_state': {
                'optimizer': self.train_state.optimizer.state_dict(),
                'history': self.train_state.history,
                'epoch': self.train_state.epoch,
                'data': self.train_state.data,
                'stop': self.train_state.stop
            }
        }
    def load_state_dict(self, data: dict):
        self.model.load_state_dict(data['model'])
        train_state = data['train_state']
        self.train_state.optimizer.load_state_dict(train_state['optimizer'])
        self.train_state.history = train_state['history']
        self.train_state.epoch = train_state['epoch']
        self.train_state.data = train_state['data']
        self.train_state.stop = train_state['stop']

    def create_batch_groups(self, config: TrainConfig) -> list[BatchGroup]:
        files: list[BatchFile] = []
        files = functools.reduce(lambda files, folder: files +  BatchFile.load(folder), config.inputs, files)
        files = sorted(files, key=lambda it: it.unix_time)
        files = [it for it in files if self.model.config.timing.contains(it.unix_time, it.exchange.calendar)]

        total = sum(it.ratio for it in config.batch_group_configs)
        counts = [int(it.ratio/total*len(files)) for it in config.batch_group_configs]
        for i in range(len(files)-sum(counts)): counts[i] += 1
        result: list[BatchGroup] = []
        for count, batch_group_config in zip(counts, config.batch_group_configs):
            result.append(BatchGroup(batch_group_config, Batches(files[:count], merge=batch_group_config.merge, device = self.device, dtype = self.dtype)))
            files = files[count:]
        return result

    def train(self, config: TrainConfig, max_epoch = 10000000):
        batch_groups = self.create_batch_groups(config)
        primary_checkpoint = CheckpointAction(self.primary_checkpoint)
        stats = config.stats
        try:
            logger.info(f"Running loop on device: {self.device}.")
            logger.info(f"Using {len(batch_groups)} batch groups.")
            for entry in batch_groups:
                logger.info(f"Batch group {entry.config.name} with {len(entry.batches)} batches.")
            logger.info(f"Model {get_full_classname(self.model)}.")
            logger.info(f"Optimizer {type(self.train_state.optimizer)}.")
            logger.info(f"---------RUNNING INITIAL TRIGGER LOOP")
            for rule in config.rules: rule.execute(self)
            logger.info(f"---------FINISHED INITIAL TRIGGER LOOP")
            
            while not self.train_state.stop and self.train_state.epoch < max_epoch:
                logger.info(f"Running epoch {self.train_state.epoch}")
                print(f"---------EPOCH {self.epoch}-------------------------------------")
                frame = HistoryFrame(self.train_state.epoch)
                for batch_group in batch_groups:
                    stats.clear()
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
                                self.train_state.optimizer.zero_grad()
                                output: Tensor = self.model(*input).squeeze()
                                loss = stats.update(expect, output)
                                loss.backward()
                                self.train_state.optimizer.step()
                                bar.set_postfix_str(str(stats))
                        print(f"Training group '{batch_group.config.name}' stats: {stats}")
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
                                    stats.update(expect, output)
                            print(f"Evaluation group '{batch_group.config.name}' stats: {stats}")
                    frame.stats[batch_group.config.name] = stats.to_dict()

                self.train_state.history.append(frame)
                for rule in config.rules: rule.execute(self)
                primary_checkpoint.save(self)
                self.epoch += 1

            logger.info(f"Stopped at epoch {self.epoch}")
        except:
            logger.error(f"Error running loop.", exc_info=True)

    def plot_history(self):
        history = self.train_state.history
        epochs = [it.epoch for it in history]
        groups = set(key for frame in history for key in frame.stats.keys())
        metrics = set(key for frame in history for group in frame.stats.keys() for key in frame.stats[group].keys())
        for metric in metrics:
            values = {
                group: [
                    frame.stats[group][metric] for frame in history
                        if group in group in frame.stats and metric in frame.stats[group]
                ] for group in groups
            }
            #rows = 1 if len(values) <= 2 else 2 if len(values) <= 6 else 3
            #cols = math.ceil(len(values)/rows)
            fig, axes = plt.subplots(1,1)
            fig.suptitle(f"Metric: {metric}")
            axes.set_xlabel('Epoch')
            for group, color in zip(values.keys(), plotutils.COLORS):
                axes.plot(epochs, values[group], color=color, label=group)
            axes.legend()
        plt.show()

    def delete(self):
        if self.root.exists():
            shutil.rmtree(self.root)
        with Session(self.engine) as session:
            entity = session.scalar(select(ModelConfigEntity).where(ModelConfigEntity.content == self.model.config))
            if entity is not None: session.delete(entity)
            session.commit()

    @staticmethod
    def get_folder(model_type: type[T]) -> Path:
        file = get_module(model_type).__file__
        if file is None: raise Exception(f"Can't determine the file path of model: {model_type}.")
        return Path(file).parent
    
    engines: dict[type, Engine] = {}
    @staticmethod
    def get_engine[M: AbstractModel](model_type: type[M]) -> Engine:
        if model_type not in ModelManager.engines:
            ModelManager.engines[model_type] = create_engine(f"sqlite:///{ModelManager.get_folder(model_type)}/{_DB}")
        return ModelManager.engines[model_type]

    instances: dict[type, dict[BaseModelConfig, ModelManager]] = {}
    @staticmethod
    def get[M: AbstractModel](model_type: type[M], config: BaseModelConfig) -> ModelManager[M]:
        if model_type not in ModelManager.instances:
            ModelManager.instances[model_type] = {}
        if config not in ModelManager.instances[model_type]:
            ModelManager.instances[model_type][config] = ModelManager(model_type(config))
        return ModelManager.instances[model_type][config]
    @staticmethod
    def get_all[M: AbstractModel](model_type: type[M]) -> Sequence[ModelManager[M]]:
        engine = ModelManager.get_engine(model_type)
        with Session(engine) as session:
            return [ModelManager.get(model_type, it.content) for it in session.scalars(select(ModelConfigEntity))]
        
    @staticmethod
    def delete_all[M: AbstractModel](model_type: type[M]):
        folder = ModelManager.get_folder(model_type)
        if model_type in ModelManager.engines:
            del ModelManager.engines[model_type]
        if model_type in ModelManager.instances:
            del ModelManager.instances[model_type]
        gc.collect()
        db = folder / _DB
        data = folder / _DATA
        if db.exists(): db.unlink()
        if data.exists(): shutil.rmtree(data)
