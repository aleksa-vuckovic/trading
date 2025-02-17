import logging
import json
import torch
import config
from torch import Tensor
from pathlib import Path
from tqdm import tqdm
from typing import NamedTuple
from ..data import nasdaq, aggregate
from ..utils import dateutils

logger = logging.getLogger(__name__)

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
                        new_time = dateutils.get_next_working_time(unix_time, hour=hour)
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


class ModelMetadata(NamedTuple):
    """
    Args:
        projection_period: The projected period length in business days.
        description: Short description of what the model is trained to do.
    """
    projection_period: int
    description: str


class AbstractModel(torch.nn.Module):
    def __init__(self):
        super().__init__()
    def extract_tensors(self, example: dict[str, Tensor]) -> tuple[Tensor, ...]:
        pass
    def print_summary(self, merge: int = 10):
        pass
    def get_metadata(self) -> ModelMetadata:
        pass


   