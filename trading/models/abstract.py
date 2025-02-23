from __future__ import annotations
import logging
import json
import torch
import config
from torch import Tensor
from pathlib import Path
from tqdm import tqdm
from enum import Enum
from ..data import nasdaq, aggregate
from ..utils import dateutils, jsonutils
from ..utils.dateutils import TimingConfig
from ..utils.common import Interval, equatable
from ..utils.jsonutils import serializable
from .utils import PriceTarget, ModelOutput

logger = logging.getLogger(__name__)

OPEN_I = 0
HIGH_I = 1
LOW_I = 2
CLOSE_I = 3
VOLUME_I = 4

QUOTES = ['open', 'high', 'low', 'close', 'volume']
QUOTE_I = {it[0]:i for i,it in enumerate(QUOTES)}

class ExampleGenerator:
    STATE_FILE = '_loop_state.json'
    def generate_example(
        self,
        ticker: nasdaq.NasdaqListedEntry,
        end_time: float,
        with_output: bool = True
    ) -> dict[str, Tensor]:
        pass
    def plot_statistics(self, **kwargs):
        pass
    def run(self, timing: TimingConfig):
        pass
    def _run_loop(
        self,
        folder: Path,
        timing: TimingConfig,
        step: float,
        tickers: list[nasdaq.NasdaqListedEntry]|None = None,
        time_frame_start: float = dateutils.str_to_unix(config.time_frame_start),
        time_frame_end: float = dateutils.str_to_unix(config.time_frame_end),
        start_time_offset: float = 75*24*3600,
        batch_size: int = config.batch_size
    ):
        if not folder.exists(): folder.mkdir(parents=True, exist_ok=True)
        state_path = folder / ExampleGenerator.STATE_FILE
        if not state_path.exists(): state_path.write_text('{}')
        tickers = tickers or aggregate.get_sorted_tickers()

        current: list[dict[str, Tensor]] = []
        unix_time:int = round(time_frame_start)
        entry:int = len(tickers)
        iter:int = 0

        msg = f"----Generating examples for timing config: {jsonutils.serialize(timing, typed=False)}"
        logger.info(msg)
        print(msg)
        while True:
            with tqdm(total=batch_size, desc=f'Generating for {unix_time} iter {iter}', leave=True) as bar:
                while len(current) < batch_size and entry < len(tickers):
                    try:
                        ticker = tickers[entry]
                        first_trade_time = aggregate.get_first_trade_time(ticker)
                        start_time = max(first_trade_time+start_time_offset, time_frame_start+start_time_offset)
                        if start_time > unix_time:
                            logger.info(f"Skipping {ticker.symbol} at index {entry} for time {dateutils.unix_to_datetime(unix_time)} because of first trade time.")
                            entry = len(tickers)
                            continue
                        current.append(self.generate_example(ticker, float(unix_time)))
                        logger.info(f'Generated example for {ticker.symbol} for end time {str(dateutils.unix_to_datetime(unix_time))}')
                        bar.update(1)
                        entry += 1
                    except KeyboardInterrupt:
                        raise
                    except:
                        logger.error(f"Failed to generate example for {ticker.symbol} for {unix_time}", exc_info=True)
            
            total_state = json.loads(state_path.read_text())
            if current:
                data = {key:torch.stack([it[key] for it in current], dim=0) for key in current[0].keys()}
                batch_file = folder / f"time{unix_time}_entry{entry}_iter{iter}.pt"
                if batch_file.exists(): raise Exception(f"Batch file {batch_file} already exists.")
                torch.save(data, batch_file)
                logger.info(f"Wrote batch number {iter} for timestamp {unix_time}.")
                iter += 1
                state = {'entry': entry, 'iter': iter}
                key = str(unix_time)
                total_state[key] = state
                state_path.write_text(json.dumps(total_state))
                current.clear()
            
            if entry >= len(tickers):
                unix_time = round(timing.get_next_unix(unix_time, step))
                if unix_time > time_frame_end:
                    logger.info(f"Stopping generation, time {unix_time} is now bigger than end time {time_frame_end}.")
                    break
                key = str(unix_time)
                if key in total_state:
                    entry = total_state[key]['entry']
                    iter = total_state[key]['iter']
                else:
                    entry = 0
                    iter = 0
                

class Aggregation(Enum):
    FIRST = 'first'
    LAST = 'last'
    AVG = 'avg'
    MAX = 'max'
    MIN = 'min'
    def apply_tensor(self, tensor: Tensor, dim:int=-1) -> Tensor:
        dims = len(tensor.shape)
        while dim<0: dim+=dims
        if self==Aggregation.FIRST: return tensor[tuple(slice(None,None) if it!=dim else 0 for it in range(dims))]
        if self==Aggregation.LAST: return tensor[tuple(slice(None,None) if it!=dim else -1 for it in range(dims))]
        if self==Aggregation.AVG: return tensor.mean(dim=dim)
        if self==Aggregation.MAX: return tensor.max(dim=dim)
        if self==Aggregation.MIN: return tensor.min(dim=dim)
        raise Exception(f"Unknown aggregation {self}")
    def apply_list(self, data: list, dim:int=-1) -> list|float:
        tensor = torch.tensor(data, dtype=torch.float64)
        return self.apply_tensor(tensor, dim=dim).tolist()

@serializable()
@equatable()
class PriceEstimator:
    def __init__(
        self,
        quote: str,
        interval: Interval,
        index: slice,
        agg: Aggregation,
        max_fill_ratio: float = 1
    ):
        self.quote = quote
        self.quote_index = QUOTE_I[quote[0].lower()]
        self.interval = interval
        self.index = index
        self.agg = agg
        self.max_fill_ratio = max_fill_ratio

    def estimate_tensor(self, tensor: Tensor) -> Tensor:
        dims = len(tensor.shape)
        index = tuple(slice(None,None) if it < dims-2 else self.index if it < dims - 1 else self.quote_index for it in range(dims))
        return self.agg.apply_tensor(tensor[index])

    def estimate(self, ticker: nasdaq.NasdaqListedEntry, unix_time: float, tz=dateutils.ET) -> float:
        end_time = dateutils.add_intervals_unix(unix_time, self.interval, self.index.stop, tz=tz)
        prices, = aggregate.get_interpolated_pricing(ticker, unix_time, end_time, self.interval, return_quotes=[self.quote], max_fill_ratio=self.max_fill_ratio)
        tensor = torch.tensor(prices, dtype=torch.float64)
        self.agg.apply_tensor(tensor, dim=-1).item()


@serializable()
@equatable()
class ModelConfig:
    def __init__(self, 
        estimator: PriceEstimator,
        target:  PriceTarget,
        output: ModelOutput,
        timing: TimingConfig,
        data: dict = {}
    ):
        self.estimator = estimator
        self.target = target
        self.output = output
        self.timing = timing
        self.data = data
    
    def __str__(self) -> str:
        return f"""
estimator = {jsonutils.serialize(self.estimator, typed=False, indent=2)}
target = {self.target.name}
output = {self.output.name}
timing = {jsonutils.serialize(self.timing, typed=False, indent=2)}
data = {self.data}
"""

        
class AbstractModel(torch.nn.Module):
    def __init__(self, config: ModelConfig):
        super().__init__()
        self.config = config
    def extract_tensors(self, example: dict[str, Tensor]) -> tuple[Tensor, ...]:
        pass
    def print_summary(self, merge: int = 10):
        pass