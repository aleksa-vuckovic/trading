import logging
import json
import config
import torch
from tqdm import tqdm
from typing import Callable
from torch import Tensor
from pathlib import Path
from trading.securities import nasdaq, aggregate
from base import serialization
from trading.core.work_calendar import TimingConfig, XNAS



logger = logging.getLogger(__name__)

class AbstractGenerator:
    STATE_FILE = '_loop_state.json'
    def generate_example(
        self,
        ticker: nasdaq.NasdaqSecurity,
        end_time: float,
        with_output: bool = True
    ) -> dict[str, Tensor]:
        pass
    def plot_statistics(self, **kwargs):
        pass
    def run(self):
        pass
    def _run_loop(
        self,
        *,
        folder: Path,
        timing: TimingConfig, # should have a configured interval
        tickers_fn: list[nasdaq.NasdaqSecurity]|Callable[[float], list[nasdaq.NasdaqSecurity]]|None = None,
        time_frame: tuple[float, float],
        batch_size: int = config.batch_size
    ):
        if not folder.exists(): folder.mkdir(parents=True, exist_ok=True)
        state_path = folder / AbstractGenerator.STATE_FILE
        if not state_path.exists(): state_path.write_text('{}')
        state = json.loads(state_path.read_text())

        unix_time: int = round(time_frame[0])
        entry: int = 0
        iter: int = 0
        tickers = []
        current: list[dict[str, Tensor]] = []
        
        msg = f"""----Generating examples into {folder}
        Timing config: {serialization.serialize(timing, typed=False)}
        Start time: {XNAS.unix_to_datetime(time_frame[0])}
        End time: {XNAS.unix_to_datetime(time_frame[1])}"""
        logger.info(msg)
        print(msg)
        while True:
            with tqdm(total=batch_size, desc=f'Generating for {unix_time} iter {iter}', leave=True) as bar:
                while len(current) < batch_size and entry < len(tickers):
                    try:
                        ticker = tickers[entry]
                        current.append(self.generate_example(ticker, float(unix_time), with_output=True))
                        logger.info(f"Generated example for {ticker.symbol} for end time {str(XNAS.unix_to_datetime(unix_time))}")
                        bar.update(1)
                    except KeyboardInterrupt:
                        raise
                    except:
                        logger.error(f"Failed to generate example for {ticker.symbol} for {unix_time}", exc_info=True)
                    entry += 1
            
            total_state = json.loads(state_path.read_text())
            if current:
                data = {key:torch.stack([it[key] for it in current], dim=0) for key in current[0].keys()}
                batch_file = folder / f"time{unix_time}_entry{entry}_iter{iter}.pt"
                if batch_file.exists(): raise Exception(f"Batch file {batch_file} already exists.")
                torch.save(data, batch_file)
                logger.info(f"Wrote batch number {iter} for timestamp {unix_time}.")
                iter += 1
                state = {'entry': entry, 'iter': iter, 'finished': entry >= len(tickers)}
                key = str(unix_time)
                total_state[key] = state
                state_path.write_text(json.dumps(total_state))
                current.clear()
            
            if entry < len(tickers): continue
            while True:
                unix_time = round(timing.get_next_time(unix_time))
                if unix_time > time_frame[1]:
                    logger.info(f"Stopping generation, time {unix_time} is now bigger than end time {time_frame[1]}.")
                    return
                key = str(unix_time)
                if key in total_state:
                    if total_state[key]['finished']: continue
                    entry = total_state[key]['entry']
                    iter = total_state[key]['iter']
                    break
                entry = 0
                iter = 0
                break
            tickers = tickers_fn(unix_time) if callable(tickers_fn) else tickers_fn if tickers_fn else aggregate.get_sorted_tickers()
