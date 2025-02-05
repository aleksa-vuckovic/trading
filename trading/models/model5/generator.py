import logging
import json
import torch
from pathlib import Path
from tqdm import tqdm
import config
from ...utils import dateutils
from ...data import aggregate
from ..utils import get_next_time
from . import example


logger = logging.getLogger(__name__)
examples_folder = Path(__file__).parent / 'examples'
if not examples_folder.exists():
    examples_folder.mkdir()
time_frame_end = dateutils.str_to_unix(config.time_frame_end)
time_frame_start = dateutils.str_to_unix(config.time_frame_start)
h_offset = 75*24*3600

def run_ordered_loop(hour: int = 16):
    tickers = aggregate.get_sorted_tickers()
    state_path = Path(__file__).parent / 'ordered_loop_state.json'
    if not state_path.exists():
        state_path.write_text('{}')
    total_state = json.loads(state_path.read_text())
    if str(hour) not in total_state:
        state = {'iter': 0, 'unix_time': time_frame_start, 'entry': -1}
    else:
        state = total_state[str(hour)]
    iter: int = state['iter']
    unix_time: float = state['unix_time']
    entry: int = state['entry']

    current: list[dict[str, torch.Tensor]] = []
    while True:
        with tqdm(total=config.batch_size, desc=f'Generating batch {iter+1} ({hour})', leave=True) as bar:
            while len(current) < config.batch_size:
                if entry >= len(tickers) - 1 or entry < 0:
                    new_time = get_next_time(unix_time, hour=hour)
                    if new_time > time_frame_end:
                        logger.info(f"Finished. (new time is now bigger than end time)")
                        break
                    entry = 0
                    unix_time = new_time
                else:
                    entry += 1
                try:
                    ticker = tickers[entry]['ticker']
                    first_trade_time = tickers[entry]['unix_time']
                    start_time = max(first_trade_time+h_offset, time_frame_start)
                    if start_time > unix_time:
                        logger.info(f"Skipping {ticker.symbol} at index {entry} for time {dateutils.unix_to_datetime(unix_time)} because of first trade time.")
                        entry = len(tickers) - 1
                        continue
                    current.append(example.generate_example(ticker, unix_time+60)) #One min later because the interval is open at the end in all series returning methods.
                    bar.update(1)
                except:
                    logger.error(f"Failed to generate example for {ticker.symbol} for {unix_time}", exc_info=True)
        if not current:
            break
        data = {key:torch.stack([it[key] for it in current], dim=0) for key in current[0].keys()}
        iter += 1
        batch_file = examples_folder / f"{config.batch_prefix}_batch{iter}-{hour}.pt"
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
