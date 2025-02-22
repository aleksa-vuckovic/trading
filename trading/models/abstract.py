from __future__ import annotations
import logging
import json
import torch
import config
import re
from torch import Tensor
from pathlib import Path
from tqdm import tqdm
from typing import NamedTuple
from enum import Enum, auto
from ..data import nasdaq, aggregate
from ..utils import dateutils
from ..utils.common import Interval

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
    def run_loop(self, hour: int):
        pass
    def plot_statistics(self, **kwargs):
        pass
    def run(self):
        """
        Run loop for all hours
        """
        for hour in range(11, 17):
            self.run_loop(hour)
    def _run_loop(
        self,
        folder: Path,
        hour: int = 16,
        tickers: list[nasdaq.NasdaqListedEntry]|None = None,
        time_frame_start: float = dateutils.str_to_unix(config.time_frame_start),
        time_frame_end: float = dateutils.str_to_unix(config.time_frame_end),
        start_time_offset: float = 75*24*3600,
        batch_size: int = config.batch_size
    ):
        if not folder.exists(): folder.mkdir(parents=True, exist_ok=True)
        state_path = folder / ExampleGenerator.STATE_FILE
        if not state_path.exists(): state_path.write_text('{}')
        total_state = json.loads(state_path.read_text())
        if str(hour) not in total_state: state = {'iter': 0, 'unix_time': time_frame_start + start_time_offset, 'entry': -1}
        else: state = total_state[str(hour)]
        iter: int = state['iter']
        unix_time: float = state['unix_time']
        entry: int = state['entry']
        tickers = tickers or aggregate.get_sorted_tickers()

        current: list[dict[str, Tensor]] = []

        while True:
            with tqdm(total=batch_size, desc=f'Generating batch {iter+1} ({hour})', leave=True) as bar:
                while len(current) < batch_size:
                    if entry >= len(tickers) - 1 or entry < 0:
                        new_time = dateutils.get_next_working_time_unix(unix_time, hour=hour)
                        if new_time > time_frame_end:
                            logger.info(f"Finished. (new time is now bigger than end time)")
                            break
                        entry = 0
                        unix_time = new_time
                    else:
                        entry += 1
                    try:
                        ticker = tickers[entry]
                        first_trade_time = aggregate.get_first_trade_time(ticker)
                        start_time = max(first_trade_time+start_time_offset, time_frame_start+start_time_offset)
                        if start_time > unix_time:
                            logger.info(f"Skipping {ticker.symbol} at index {entry} for time {dateutils.unix_to_datetime(unix_time)} because of first trade time.")
                            entry = len(tickers) - 1
                            continue
                        current.append(self.generate_example(ticker, unix_time+1))
                        logger.info(f'Generated example for {ticker.symbol} for end time {str(dateutils.unix_to_datetime(unix_time+1))}')
                        bar.update(1)
                    except KeyboardInterrupt:
                        raise
                    except:
                        logger.error(f"Failed to generate example for {ticker.symbol} for {unix_time}", exc_info=True)
            if not current:
                break
            data = {key:torch.stack([it[key] for it in current], dim=0) for key in current[0].keys()}
            iter += 1
            batch_file = folder / f"hour{hour}_time{int(unix_time)}_entry{entry}_batch{iter}.pt"
            if batch_file.exists():
                raise Exception(f"Batch file {batch_file} already exists.")
            torch.save(data, batch_file)
            state = {'iter': iter, 'unix_time': unix_time, 'entry': entry}
            total_state = json.loads(state_path.read_text())
            total_state[str(hour)] = state
            state_path.write_text(json.dumps(total_state))
            logger.info(f"Wrote batch number {iter}({hour}).")
            if len(current) < config.batch_size:
                break
            current.clear()

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
        index = tuple(slice(None,None) if it < dims-2 else self.slice if it < dims - 1 else self.quote_index for it in range(dims))
        return self.agg.apply_tensor(tensor[index])

    def estimate(self, ticker: nasdaq.NasdaqListedEntry, unix_time: float, tz=dateutils.ET) -> float:
        end_time = dateutils.add_intervals_unix(unix_time, self.interval, self.index.stop, tz=tz)
        prices, = aggregate.get_interpolated_pricing(ticker, unix_time, end_time, self.interval, return_quotes=[self.quote], max_fill_ratio=self.max_fill_ratio)
        tensor = torch.tensor(prices, dtype=torch.float64)
        self.agg.apply_tensor(tensor, dim=-1).item()
    
    def to_str(self) -> str:
        return f"quote{self.quote}_interval{self.interval.name}_{self.index}_agg{self.agg.name}_mfr{int(self.max_fill_ratio*100)}"
    
    @staticmethod
    def from_str(value: str):
        data = re.match(r"quote([^_]+)_interval([^_])_(slice[^_]+)_agg([^_]+)_mfr(\d+)", value)
        if not data: raise Exception(f"Can't parse PriceEstimator string '{value}'")
        quote = data.group(1)
        interval = Interval[data.group(2)]
        slice = eval(data.group(3))
        agg = Aggregation[data.group(4)]


class ModelConfig:
    """
    Args:
        projection_period: The projected period length in business days.
        description: Short description of what the model is trained to do.
    """
    def __init__(self, estimator: PriceEstimator):
        pass
        
class AbstractModel(torch.nn.Module):
    def __init__(self):
        super().__init__()
    def extract_tensors(self, example: dict[str, Tensor]) -> tuple[Tensor, ...]:
        pass
    def print_summary(self, merge: int = 10):
        pass