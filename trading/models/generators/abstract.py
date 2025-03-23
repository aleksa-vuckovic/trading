import logging
import json
import config
import torch
from tqdm import tqdm
from typing import Callable
from torch import Tensor
from pathlib import Path

from base import dates
from base.serialization import serializer
from trading.core.securities import Security
from trading.core.work_calendar import TimingConfig




logger = logging.getLogger(__name__)

class AbstractGenerator:
    STATE_FILE = '_loop_state.json'
    def generate_example(
        self,
        security: Security,
        end_time: float,
        with_output: bool = True
    ) -> dict[str, Tensor]: ...
    def plot_statistics(self, **kwargs): ...
    def run(self): ...
    def _run_loop(
        self,
        *,
        folder: Path,
        timing: TimingConfig, # should have a configured interval
        securities_fn: list[Security]|Callable[[float], list[Security]],
        time_frame: tuple[float, float],
        batch_size: int = config.models.batch_size
    ):
        if not folder.exists(): folder.mkdir(parents=True, exist_ok=True)
        state_path = folder / AbstractGenerator.STATE_FILE
        if not state_path.exists(): state_path.write_text('{}')
        state = json.loads(state_path.read_text())

        unix_time: int = round(time_frame[0])
        entry: int = 0
        iter: int = 0
        securities: list[Security] = []
        current: list[dict[str, Tensor]] = []
        
        msg = f"""----Generating examples into {folder}
        Timing config: {serializer.serialize(timing, typed=False)}
        Start time: {dates.unix_to_datetime(time_frame[0], tz=dates.CET)}
        End time: {dates.unix_to_datetime(time_frame[1], tz=dates.CET)}"""
        logger.info(msg)
        print(msg)
        while True:
            with tqdm(total=batch_size, desc=f'Generating for {unix_time} iter {iter}', leave=True) as bar:
                while len(current) < batch_size and entry < len(securities):
                    security = securities[entry]
                    try:
                        current.append(self.generate_example(security, float(unix_time), with_output=True))
                        logger.info(f"Generated example for {security.symbol} for end time {dates.unix_to_datetime(unix_time,tz=dates.CET)}")
                        bar.update(1)
                    except KeyboardInterrupt:
                        raise
                    except:
                        logger.error(f"Failed to generate example for {security.symbol} for {unix_time}", exc_info=True)
                    entry += 1
            
            total_state = json.loads(state_path.read_text())
            if current:
                data = {key:torch.stack([it[key] for it in current], dim=0) for key in current[0].keys()}
                batch_file = folder / f"time{unix_time}_entry{entry}_iter{iter}.pt"
                if batch_file.exists(): raise Exception(f"Batch file {batch_file} already exists.")
                torch.save(data, batch_file)
                logger.info(f"Wrote batch number {iter} for timestamp {unix_time}.")
                iter += 1
                state = {'entry': entry, 'iter': iter, 'finished': entry >= len(securities)}
                key = str(unix_time)
                total_state[key] = state
                state_path.write_text(json.dumps(total_state))
                current.clear()
            
            if entry < len(securities): continue
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
            securities = securities_fn(unix_time) if callable(securities_fn) else securities_fn
