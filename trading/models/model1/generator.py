from . import example
from ...utils import dateutils
from ...data import nasdaq, aggregate
import time
import logging
from pathlib import Path
import json
from datetime import datetime, timedelta
import torch
from tqdm import tqdm

H_OFFSET = 75*24*3600
END_TIME = time.time() - 24*3600
START_TIME = time.time() - 729*24*3600 + H_OFFSET

logger = logging.getLogger(__name__)
examples_folder = Path(__file__).parent / 'examples'
if not examples_folder.exists():
    examples_folder.mkdir()

tickers = aggregate.get_sorted_tickers()

def get_next_time(unix_time: float, hour: int | None = None) -> float:
    date = dateutils.unix_to_datetime(unix_time, tz = dateutils.EST)
    if date.minute or date.second or date.microsecond:
        date = date.replace(minute=0, second=0, microsecond=0)
        date = date + timedelta(hours = 1)
    if date.weekday() >= 5 or date.hour >= (hour or 16):
        date = date.replace(hour = hour or 9)
        date += timedelta(days=1)
        while date.weekday() >= 5:
            date += timedelta(days=1)
    elif date.hour < (hour or 9):
        date = date.replace(hour = hour or 9)
    else:
        date = date.replace(hour = date.hour + 1)
    return date.timestamp()

def run_ordered_loop(hour: int = 16):
    state_path = Path(__file__).parent / 'ordered_loop_state.json'
    if not state_path.exists():
        state_path.write_text('{}')
    total_state = json.loads(state_path.read_text())
    if str(hour) not in total_state:
        state = {'iter': 0, 'unix_time': START_TIME, 'entry': -1}
    else:
        state = total_state[str(hour)]
    iter: int = state['iter']
    unix_time: float = state['unix_time']
    entry: int = state['entry']

    current = []
    while True:
        with tqdm(total=1000, desc=f'Generating batch {iter+1} ({hour})', leave=True) as bar:
            while len(current) < 1000:
                if entry >= len(tickers) - 1 or entry < 0:
                    new_time = get_next_time(unix_time, hour=hour)
                    if new_time > END_TIME:
                        logger.info(f"Finished. (new time is now bigger than end time)")
                        break
                    entry = 0
                    unix_time = new_time
                else:
                    entry += 1
                try:
                    ticker = tickers[entry]['ticker']
                    first_trade_time = tickers[entry]['unix_time']
                    start_time = max(first_trade_time+H_OFFSET, START_TIME)
                    if start_time > unix_time:
                        logger.info(f"Skipping {ticker.symbol} at index {entry} for time {dateutils.unix_to_datetime(unix_time)} because of first trade time.")
                        entry = len(tickers) - 1
                        continue
                    current.append(example.generate_example(ticker, unix_time))
                    bar.update(1)
                except:
                    logger.error(f"Failed to generate example for {ticker.symbol} for {unix_time}", exc_info=True)
        if not current:
            break
        tensor = torch.stack(current, dim=0)
        iter += 1
        torch.save(tensor, examples_folder / f"batch{iter}-{hour}.pt")
        state = {'iter': iter, 'unix_time': unix_time, 'entry': entry}
        total_state = json.loads(state_path.read_text())
        total_state[str(hour)] = state
        state_path.write_text(json.dumps(total_state))
        logger.info(f"Wrote batch number {iter}({hour})")
        if len(current) < 1000:
            break
        current.clear()
