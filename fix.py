import logging
import re
import os
import torch
import json
import config
from pathlib import Path
from trading.models.utils import get_batch_files
from trading.utils.dateutils import TimingConfig
from trading.utils import dateutils
from trading.data import aggregate


logger = logging.getLogger(__name__)

def fix_examples(root: Path):
    for file in get_batch_files(root):
        path: Path = file['path']
        time = file['time']
        hour = file['hour']
        entry = file['entry']
        batch = file['batch']
        newpath = root / f"time{round(time)}_entry{entry}_iter{batch}.pt"
        #path.rename(newpath)
        print(f"Renamed {path} to {newpath}")
    tickers = aggregate.get_sorted_tickers()
    state_path = root / "_loop_state.json"
    state = json.loads(state_path.read_text())
    new_state = {}
    for hour in state:
        timing = TimingConfig.Builder().at(hour = int(hour), minute = 0).build()
        cur = dateutils.str_to_unix(config.time_frame_start, tz=dateutils.ET)
        end = dateutils.str_to_unix(config.time_frame_end, tz=dateutils.ET)

        while True:
            cur = round(timing.get_next_unix(cur, 3600))
            if cur >= end: break
            new_state[str(cur)] = {'entry': len(tickers), 'iter': 3}
    print(f"New state\n{new_state}")
    #state_path.write_text(json.dumps(new_state))
